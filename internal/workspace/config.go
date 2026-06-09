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
	"strings"

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

	cephPortName  = "http-rook-ceph"
	modelPortName = "http-kserve"
)

var (
	modelGatewayLabels = map[string]string{
		"gateway.envoyproxy.io/owning-gateway-name":      "kserve-ingress-gateway",
		"gateway.envoyproxy.io/owning-gateway-namespace": "kserve",
	}
	objectGatewayLabels = map[string]string{
		"gateway.envoyproxy.io/owning-gateway-name":      "rook-ceph-ingress-gateway",
		"gateway.envoyproxy.io/owning-gateway-namespace": "rook-ceph",
	}
)

// ReconcileConfig ensures a workspace-config ConfigMap exists in the workspace
// namespace with ModelGatewayEndpoint and ObjectGatewayEndpoint entries.
// If the required cluster services or kubeadm-config are not available, this function
// logs a warning and returns nil so that other workspace resources are not blocked.
func ReconcileConfig(ctx context.Context, c client.Client, scheme *runtime.Scheme, w *tenantv1alpha1.Workspace, version string) error {
	logger := log.FromContext(ctx)

	data := make(map[string]string)

	clusterIP, err := resolveClusterIP(ctx, c)
	if err != nil {
		logger.Info("Unable to resolve cluster control-plane IP, skipping service-based endpoint discovery",
			"reason", err.Error())
	} else {
		modelNodePort, err := resolveServiceNodePort(ctx, c, modelGatewayLabels, modelPortName)
		switch {
		case apierrors.IsNotFound(err):
			logger.Info("Model gateway service not found, skipping ModelGatewayEndpoint",
				"labels", modelGatewayLabels)
		case err != nil:
			return err
		default:
			data["ModelGatewayEndpoint"] = fmt.Sprintf("%s:%d", clusterIP, modelNodePort)
		}

		objectNodePort, err := resolveServiceNodePort(ctx, c, objectGatewayLabels, cephPortName)
		switch {
		case apierrors.IsNotFound(err):
			logger.Info("Object gateway service not found, skipping ObjectGatewayEndpoint",
				"labels", objectGatewayLabels)
		case err != nil:
			return err
		default:
			data["ObjectGatewayEndpoint"] = fmt.Sprintf("%s:%d", clusterIP, objectNodePort)
		}

		data["ServiceEndpoint"] = clusterIP
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
// resolveClusterIP returns the control-plane IP/hostname.
// Resolution order:
//  1. kubeadm-config ConfigMap  (kubeadm / RKE1 clusters)
//  2. kube-apiserver static pod (RKE2 clusters — token-server-address)
func resolveClusterIP(ctx context.Context, c client.Client) (string, error) {
	// ── 1. kubeadm path ────────────────────────────────────────────────────────
	if host, err := resolveFromKubeadmConfig(ctx, c); err == nil {
		return host, nil
	}

	// ── 2. RKE2: kube-apiserver static pod ────────────────────────────────────
	if host, err := resolveFromKubeAPIServerPod(ctx, c); err == nil {
		return host, nil
	}

	return "", fmt.Errorf("unable to resolve cluster control-plane IP: " +
		"no kubeadm-config, and no kube-apiserver static pod found")
}

// resolveFromKubeadmConfig reads the kubeadm ClusterConfiguration ConfigMap.
func resolveFromKubeadmConfig(ctx context.Context, c client.Client) (string, error) {
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

// resolveFromKubeAPIServerPod finds the kube-apiserver static pod that RKE2
// runs in kube-system and extracts --advertise-address from its command args.
//
// RKE2 names the pod "kube-apiserver-<nodeName>" with the label:
//
//	component=kube-apiserver
func resolveFromKubeAPIServerPod(ctx context.Context, c client.Client) (string, error) {
	var podList corev1.PodList
	if err := c.List(ctx, &podList,
		client.InNamespace("kube-system"),
		client.MatchingLabels{"component": "kube-apiserver"},
	); err != nil {
		return "", fmt.Errorf("failed to list kube-apiserver pods: %w", err)
	}

	if len(podList.Items) == 0 {
		return "", fmt.Errorf("no kube-apiserver pods found in kube-system")
	}

	// Iterate all containers (there is normally exactly one) and scan args.
	for _, pod := range podList.Items {
		for _, container := range pod.Spec.Containers {
			for _, arg := range container.Args {
				if val, ok := strings.CutPrefix(arg, "--advertise-address="); ok && val != "" {
					return val, nil
				}
			}
		}
	}

	return "", fmt.Errorf("kube-apiserver pod found but --advertise-address not present in command args")
}

// resolveServiceNodePort finds a Service matching the given labels and returns
// the NodePort for the named port. Returns an apierrors.NotFound error if no
// matching Service exists.
func resolveServiceNodePort(ctx context.Context, c client.Client, labels map[string]string, portName string) (int32, error) {
	var svcList corev1.ServiceList
	if err := c.List(ctx, &svcList, client.MatchingLabels(labels)); err != nil {
		return 0, err
	}
	if len(svcList.Items) == 0 {
		return 0, apierrors.NewNotFound(corev1.Resource("services"), fmt.Sprintf("labels=%v", labels))
	}
	if len(svcList.Items) > 1 {
		return 0, fmt.Errorf("multiple services found matching labels %v", labels)
	}
	svc := svcList.Items[0]

	for _, port := range svc.Spec.Ports {
		if port.Name == portName {
			if port.NodePort == 0 {
				return 0, fmt.Errorf("service %s/%s port %q has no NodePort assigned", svc.Namespace, svc.Name, portName)
			}
			return port.NodePort, nil
		}
	}

	return 0, fmt.Errorf("service %s/%s has no port named %q", svc.Namespace, svc.Name, portName)
}
