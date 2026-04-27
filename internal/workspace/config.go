/*
Copyright 2026 The OtterScale Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package workspace

import (
	"context"
	"fmt"
	"maps"
	"net"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlutil "sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/yaml"

	tenantv1alpha1 "github.com/otterscale/api/tenant/v1alpha1"
)

// Service coordinates for gateway endpoint resolution.
const (
	globalConfigNamespace = "otterscale-system"
	globalConfigName      = "global-workspace-config"

	modelGatewayNamespace   = "llm-d"
	modelGatewayServiceName = "otterscale-llm-d-infra-inference-gateway-istio"
	modelGatewayPortName    = "default"

	objectGatewayNamespace   = "rook-ceph"
	objectGatewayServiceName = "rook-ceph-rgw-ceph-objectstore-external"
	objectGatewayPortName    = "rgw"

	// serviceEndpointHint is a placeholder shown to users indicating where to
	// substitute the actual NodePort number for the ServiceEndpoint.
	serviceEndpointHint = "<NodePort>"
)

// ReconcileConfig ensures a workspace-config ConfigMap exists in the workspace
// namespace with ModelGatewayEndpoint and ObjectGatewayEndpoint entries.
// If the required cluster services or kubeadm-config are not available, this function
// logs a warning and returns nil so that other workspace resources are not blocked.
func ReconcileConfig(ctx context.Context, c client.Client, scheme *runtime.Scheme, w *tenantv1alpha1.Workspace, version string) error {
	logger := log.FromContext(ctx)

	data := make(map[string]string)

	clusterIP, err := resolveClusterIP(ctx, c)
	switch {
	case apierrors.IsNotFound(err):
		logger.Info("kubeadm-config not found, skipping service-based endpoint discovery")
	case err != nil:
		return err
	default:
		modelNodePort, err := resolveServiceNodePort(ctx, c, modelGatewayNamespace, modelGatewayServiceName, modelGatewayPortName)
		switch {
		case apierrors.IsNotFound(err):
			logger.Info("Model gateway service not found, skipping ModelGatewayEndpoint",
				"namespace", modelGatewayNamespace, "service", modelGatewayServiceName)
		case err != nil:
			return err
		default:
			data["ModelGatewayEndpoint"] = fmt.Sprintf("%s:%d", clusterIP, modelNodePort)
		}

		objectNodePort, err := resolveServiceNodePort(ctx, c, objectGatewayNamespace, objectGatewayServiceName, objectGatewayPortName)
		switch {
		case apierrors.IsNotFound(err):
			logger.Info("Object gateway service not found, skipping ObjectGatewayEndpoint",
				"namespace", objectGatewayNamespace, "service", objectGatewayServiceName)
		case err != nil:
			return err
		default:
			data["ObjectGatewayEndpoint"] = fmt.Sprintf("%s:%d", clusterIP, objectNodePort)
		}

		data["ServiceEndpoint"] = fmt.Sprintf("%s:%s", clusterIP, serviceEndpointHint)
	}

	// The otterscale-system ConfigMap overrides any auto-discovered endpoints.
	var globalOverrides corev1.ConfigMap
	switch err := c.Get(ctx, types.NamespacedName{Name: globalConfigName, Namespace: globalConfigNamespace}, &globalOverrides); {
	case apierrors.IsNotFound(err):
		logger.Info("Global workspace-config ConfigMap not found, no overrides applied",
			"namespace", globalConfigNamespace, "name", globalConfigName)
	case err != nil:
		return err
	default:
		maps.Copy(data, globalOverrides.Data)
	}

	if len(data) == 0 {
		logger.Info("No gateway endpoints configured, skipping workspace-config ConfigMap creation")
		return nil
	}

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ConfigName,
			Namespace: w.Spec.Namespace,
		},
	}

	op, err := ctrlutil.CreateOrUpdate(ctx, c, cm, func() error {
		cm.Labels = LabelsForWorkspace(w.Name, version)
		cm.Data = data
		return ctrlutil.SetControllerReference(w, cm, scheme)
	})
	if err != nil {
		return err
	}
	if op != ctrlutil.OperationResultNone {
		logger.Info("ConfigMap reconciled", "operation", op, "name", cm.Name)
	}
	return nil
}

// kubeadmClusterConfig is a minimal representation of the kubeadm ClusterConfiguration,
// used only to extract the controlPlaneEndpoint field.
type kubeadmClusterConfig struct {
	ControlPlaneEndpoint string `json:"controlPlaneEndpoint"`
}

// resolveClusterIP reads the kubeadm-config ConfigMap from kube-system and extracts
// the control plane IP from the controlPlaneEndpoint field.
//
// TODO: RKE2 support — kubeadm-config may not exist on RKE2 clusters, need fallback.
func resolveClusterIP(ctx context.Context, c client.Client) (string, error) {
	var cm corev1.ConfigMap
	if err := c.Get(ctx, types.NamespacedName{Name: "kubeadm-config", Namespace: "kube-system"}, &cm); err != nil {
		return "", err
	}

	clusterConfigYAML, ok := cm.Data["ClusterConfiguration"]
	if !ok {
		return "", fmt.Errorf("kubeadm-config ConfigMap missing ClusterConfiguration key")
	}

	var cfg kubeadmClusterConfig
	if err := yaml.Unmarshal([]byte(clusterConfigYAML), &cfg); err != nil {
		return "", fmt.Errorf("failed to parse ClusterConfiguration: %w", err)
	}

	if cfg.ControlPlaneEndpoint == "" {
		return "", fmt.Errorf("controlPlaneEndpoint is empty in kubeadm-config")
	}

	host, _, err := net.SplitHostPort(cfg.ControlPlaneEndpoint)
	if err != nil {
		// If there's no port, treat the whole value as the host
		return cfg.ControlPlaneEndpoint, nil
	}
	return host, nil
}

// resolveServiceNodePort reads a Service and returns the NodePort for the named port.
func resolveServiceNodePort(ctx context.Context, c client.Client, namespace, name, portName string) (int32, error) {
	var svc corev1.Service
	if err := c.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, &svc); err != nil {
		return 0, err
	}

	for _, port := range svc.Spec.Ports {
		if port.Name == portName {
			if port.NodePort == 0 {
				return 0, fmt.Errorf("service %s/%s port %q has no NodePort assigned", namespace, name, portName)
			}
			return port.NodePort, nil
		}
	}

	return 0, fmt.Errorf("service %s/%s has no port named %q", namespace, name, portName)
}
