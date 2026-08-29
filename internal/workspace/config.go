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
	"errors"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlutil "sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	tenantv1alpha1 "github.com/otterscale/tenant-operator/api/v1alpha1"
)

const (
	// OperatorConfigName is the operator-wide ConfigMap, in OperatorNamespace,
	// holding settings that cannot be discovered from the cluster (see
	// RancherProjectIDKey). It is never copied into a workspace, and unlike the
	// Harbor credentials Secret it is optional.
	OperatorConfigName = "tenant-operator-config"

	// PlatformGatewayNamespace and PlatformGatewayName locate the Gateway whose
	// spec.addresses carry the address clients outside the cluster connect to.
	PlatformGatewayNamespace = "envoy-gateway-system"
	PlatformGatewayName      = "otterscale-gateway"

	// ModelGatewayNamespace and ModelGatewayName locate the KServe Gateway whose
	// listeners define the model serving endpoint.
	ModelGatewayNamespace = "kserve"
	ModelGatewayName      = "kserve-gateway"
)

// modelGatewayProtocols are the listener protocols the model endpoint is derived
// from, in preference order, each with the URL scheme it implies. The protocol
// is authoritative: listener names are free-form.
var modelGatewayProtocols = []struct {
	protocol gatewayv1.ProtocolType
	scheme   string
}{
	{gatewayv1.HTTPSProtocolType, "https"},
	{gatewayv1.HTTPProtocolType, "http"},
}

// errGatewayAddressUnusable marks a platform Gateway that exists but does not
// declare exactly one usable address. That is a configuration problem, not a
// transient failure, so callers skip the endpoint instead of retrying forever.
var errGatewayAddressUnusable = errors.New("platform gateway does not declare exactly one usable address")

// ReconcileConfig ensures the workspace-config ConfigMap matches the endpoints
// currently derivable from the cluster's Gateways.
//
// A Gateway that is absent or unconfigured is not an error: its entry is left
// out so other workspace resources are not blocked, and the ConfigMap is removed
// once nothing resolves at all. Only genuine API failures are returned, so a
// transient error retries instead of writing a partial view.
func ReconcileConfig(ctx context.Context, c client.Client, scheme *runtime.Scheme, w *tenantv1alpha1.Workspace, version string) error {
	logger := log.FromContext(ctx)

	data := make(map[string]string)

	externalAddress, err := resolveExternalAddress(ctx, c)
	switch {
	case apierrors.IsNotFound(err) || meta.IsNoMatchError(err) || errors.Is(err, errGatewayAddressUnusable):
		logger.Info("Platform gateway address unavailable, skipping ServiceEndpoint",
			"namespace", PlatformGatewayNamespace, "name", PlatformGatewayName, "reason", err.Error())
	case err != nil:
		return err
	default:
		data["ServiceEndpoint"] = "http://" + externalAddress
	}

	// A listener without a hostname falls back to externalAddress, so an
	// unresolved platform gateway can leave this endpoint empty too.
	modelEndpoint, err := resolveModelGatewayEndpoint(ctx, c, externalAddress)
	switch {
	case apierrors.IsNotFound(err) || meta.IsNoMatchError(err):
		logger.Info("Model gateway not found, skipping ModelGatewayEndpoint",
			"namespace", ModelGatewayNamespace, "name", ModelGatewayName)
	case err != nil:
		return err
	case modelEndpoint == "":
		logger.Info("Model gateway has no usable listener, skipping ModelGatewayEndpoint",
			"namespace", ModelGatewayNamespace, "name", ModelGatewayName)
	default:
		data["ModelGatewayEndpoint"] = modelEndpoint
	}

	// Nothing resolved: drop any ConfigMap left over from when it did, rather
	// than serving endpoints whose source is gone.
	if len(data) == 0 {
		logger.Info("No gateway endpoints resolved, removing the workspace-config ConfigMap")
		return deleteConfig(ctx, c, w)
	}

	cm := workspaceConfigMap(w)
	op, err := ctrlutil.CreateOrUpdate(ctx, c, cm, func() error {
		cm.Labels = LabelsForWorkspace(w.Name, version)
		cm.Data = data
		return ctrlutil.SetControllerReference(w, cm, scheme)
	})
	if err != nil {
		return fmt.Errorf("reconciling ConfigMap: %w", err)
	}
	if op != ctrlutil.OperationResultNone {
		logger.Info("ConfigMap reconciled", "operation", op, "name", cm.Name)
	}
	return nil
}

func workspaceConfigMap(w *tenantv1alpha1.Workspace) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ConfigName,
			Namespace: w.Spec.Namespace,
		},
	}
}

// deleteConfig removes the workspace-config ConfigMap. A ConfigMap that was
// never created is not an error.
func deleteConfig(ctx context.Context, c client.Client, w *tenantv1alpha1.Workspace) error {
	if err := client.IgnoreNotFound(c.Delete(ctx, workspaceConfigMap(w))); err != nil {
		return fmt.Errorf("deleting ConfigMap: %w", err)
	}
	return nil
}

// operatorConfigData returns the data of the tenant-operator-config ConfigMap.
// A missing ConfigMap yields a nil map, so callers treat "not configured" and
// "configured with nothing" identically.
func operatorConfigData(ctx context.Context, c client.Client) (map[string]string, error) {
	var cm corev1.ConfigMap
	key := types.NamespacedName{Name: OperatorConfigName, Namespace: OperatorNamespace}
	if err := c.Get(ctx, key, &cm); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("reading operator ConfigMap %s: %w", key, err)
	}
	return cm.Data, nil
}

// IsOperatorConfig reports whether obj is the operator-wide ConfigMap.
func IsOperatorConfig(obj client.Object) bool {
	if obj == nil {
		return false
	}
	return obj.GetName() == OperatorConfigName &&
		obj.GetNamespace() == OperatorNamespace
}

// IsEndpointSourceGateway reports whether obj is one of the Gateways the
// workspace endpoints are derived from: the platform Gateway supplying the
// external address, or the KServe model Gateway supplying the model endpoint.
func IsEndpointSourceGateway(obj client.Object) bool {
	if obj == nil {
		return false
	}
	switch obj.GetNamespace() {
	case PlatformGatewayNamespace:
		return obj.GetName() == PlatformGatewayName
	case ModelGatewayNamespace:
		return obj.GetName() == ModelGatewayName
	}
	return false
}

// resolveExternalAddress returns the address clients outside the cluster reach
// the platform gateway at, from its spec.addresses. A supported deployment
// declares exactly one; anything else is an error rather than a guess.
func resolveExternalAddress(ctx context.Context, c client.Client) (string, error) {
	var gateway gatewayv1.Gateway
	key := types.NamespacedName{Name: PlatformGatewayName, Namespace: PlatformGatewayNamespace}
	if err := c.Get(ctx, key, &gateway); err != nil {
		return "", fmt.Errorf("reading platform Gateway %s: %w", key, err)
	}

	addresses := make([]string, 0, len(gateway.Spec.Addresses))
	for _, address := range gateway.Spec.Addresses {
		if address.Value != "" {
			addresses = append(addresses, address.Value)
		}
	}

	switch len(addresses) {
	case 0:
		return "", fmt.Errorf("%w: spec.addresses is empty", errGatewayAddressUnusable)
	case 1:
		return addresses[0], nil
	default:
		return "", fmt.Errorf("%w: found %d (%s)",
			errGatewayAddressUnusable, len(addresses), strings.Join(addresses, ", "))
	}
}

// resolveModelGatewayEndpoint returns the URL clients reach the KServe model
// gateway at. HTTPS wins over HTTP, and within a protocol a listener hostname
// wins over the platform gateway address.
//
// A hostname leaves the port implicit ("https://models.example.com") — it comes
// with DNS and a certificate on the protocol's standard port. The fallback
// address spells the port out ("https://10.0.0.1:443"), since connecting by
// address bypasses that setup.
//
// Returns an empty string when the Gateway serves neither protocol, or when no
// listener has a hostname and externalAddress is empty.
func resolveModelGatewayEndpoint(ctx context.Context, c client.Client, externalAddress string) (string, error) {
	var gateway gatewayv1.Gateway
	key := types.NamespacedName{Name: ModelGatewayName, Namespace: ModelGatewayNamespace}
	if err := c.Get(ctx, key, &gateway); err != nil {
		return "", fmt.Errorf("reading model Gateway %s: %w", key, err)
	}

	for _, preferred := range modelGatewayProtocols {
		// Several listeners may share a protocol; a hostname on any of them beats
		// falling back to the raw address.
		var fallbackPort gatewayv1.PortNumber
		var served bool
		for _, listener := range gateway.Spec.Listeners {
			if listener.Protocol != preferred.protocol {
				continue
			}
			if listener.Hostname != nil && *listener.Hostname != "" {
				return fmt.Sprintf("%s://%s", preferred.scheme, *listener.Hostname), nil
			}
			if !served {
				served, fallbackPort = true, listener.Port
			}
		}
		if served && externalAddress != "" {
			return fmt.Sprintf("%s://%s:%d", preferred.scheme, externalAddress, fallbackPort), nil
		}
	}
	return "", nil
}
