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
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
)

func newConfigTestClient(t *testing.T, objects ...client.Object) client.Client {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add core scheme: %v", err)
	}
	if err := gatewayv1.Install(scheme); err != nil {
		t.Fatalf("add gateway scheme: %v", err)
	}
	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
}

func platformGateway(namespace string, addresses ...string) *gatewayv1.Gateway {
	specAddresses := make([]gatewayv1.GatewaySpecAddress, 0, len(addresses))
	for _, address := range addresses {
		specAddresses = append(specAddresses, gatewayv1.GatewaySpecAddress{Value: address})
	}
	return &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Name: PlatformGatewayName, Namespace: namespace},
		Spec: gatewayv1.GatewaySpec{
			GatewayClassName: "envoy",
			Addresses:        specAddresses,
		},
	}
}

func TestResolveExternalAddress(t *testing.T) {
	t.Parallel()

	// wantErr distinguishes the two skip reasons ReconcileConfig keys off:
	// a missing Gateway (NotFound) versus one that exists but is misconfigured
	// (errGatewayAddressUnusable). Anything else must surface as a real error.
	const (
		errNone = iota
		errNotFound
		errUnusable
	)

	tests := []struct {
		name        string
		objects     []client.Object
		wantAddress string
		wantErr     int
	}{
		{
			name:        "single declared address",
			objects:     []client.Object{platformGateway(PlatformGatewayNamespace, "10.102.197.202")},
			wantAddress: "10.102.197.202",
			wantErr:     errNone,
		},
		{
			name:    "gateway missing",
			wantErr: errNotFound,
		},
		{
			name:    "gateway declares no addresses",
			objects: []client.Object{platformGateway(PlatformGatewayNamespace)},
			wantErr: errUnusable,
		},
		{
			name: "gateway declares an empty address value",
			objects: []client.Object{
				platformGateway(PlatformGatewayNamespace, ""),
			},
			wantErr: errUnusable,
		},
		{
			name: "gateway declares several addresses",
			objects: []client.Object{
				platformGateway(PlatformGatewayNamespace, "10.0.0.1", "10.0.0.2"),
			},
			wantErr: errUnusable,
		},
		{
			name: "same-named gateway in another namespace is ignored",
			objects: []client.Object{
				platformGateway("kserve", "10.0.0.9"),
			},
			wantErr: errNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := resolveExternalAddress(context.Background(), newConfigTestClient(t, tt.objects...))
			switch tt.wantErr {
			case errNone:
				if err != nil {
					t.Fatalf("resolveExternalAddress() error = %v, want nil", err)
				}
			case errNotFound:
				if !apierrors.IsNotFound(err) {
					t.Fatalf("resolveExternalAddress() error = %v, want NotFound", err)
				}
			case errUnusable:
				if !errors.Is(err, errGatewayAddressUnusable) {
					t.Fatalf("resolveExternalAddress() error = %v, want errGatewayAddressUnusable", err)
				}
			}
			if got != tt.wantAddress {
				t.Errorf("resolveExternalAddress() = %q, want %q", got, tt.wantAddress)
			}
		})
	}
}

func TestIsEndpointSourceGateway(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		namespace string
		gwName    string
		want      bool
	}{
		{"platform gateway", PlatformGatewayNamespace, PlatformGatewayName, true},
		{"model gateway", ModelGatewayNamespace, ModelGatewayName, true},
		{"other gateway in the platform namespace", PlatformGatewayNamespace, "some-other-gateway", false},
		{"other gateway in the KServe namespace", ModelGatewayNamespace, "some-other-gateway", false},
		{"platform gateway name elsewhere", "default", PlatformGatewayName, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := IsEndpointSourceGateway(&gatewayv1.Gateway{
				ObjectMeta: metav1.ObjectMeta{Name: tt.gwName, Namespace: tt.namespace},
			})
			if got != tt.want {
				t.Errorf("IsEndpointSourceGateway(%s/%s) = %v, want %v", tt.namespace, tt.gwName, got, tt.want)
			}
		})
	}

	if IsEndpointSourceGateway(nil) {
		t.Error("IsEndpointSourceGateway(nil) = true, want false")
	}
}

func modelGateway(listeners ...gatewayv1.Listener) *gatewayv1.Gateway {
	return &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Name: ModelGatewayName, Namespace: ModelGatewayNamespace},
		Spec: gatewayv1.GatewaySpec{
			GatewayClassName: "envoy",
			Listeners:        listeners,
		},
	}
}

// listener builds a Listener with a unique name. The name is deliberately
// unrelated to the protocol: endpoint resolution must key off Protocol only.
func listener(protocol gatewayv1.ProtocolType, port int32, hostname *string) gatewayv1.Listener {
	l := gatewayv1.Listener{
		Name:     gatewayv1.SectionName(fmt.Sprintf("listener-%d", port)),
		Port:     port,
		Protocol: protocol,
	}
	if hostname != nil {
		h := gatewayv1.Hostname(*hostname)
		l.Hostname = &h
	}
	return l
}

func TestResolveModelGatewayEndpoint(t *testing.T) {
	t.Parallel()

	hostname := "models.example.com"
	other := "other.example.com"
	empty := ""

	tests := []struct {
		name            string
		objects         []client.Object
		externalAddress string
		wantEndpoint    string
		wantErr         bool
	}{
		{
			name: "HTTPS hostname wins over the external address",
			objects: []client.Object{modelGateway(
				listener(gatewayv1.HTTPProtocolType, 80, nil),
				listener(gatewayv1.HTTPSProtocolType, 443, &hostname),
			)},
			externalAddress: "10.0.0.1",
			wantEndpoint:    "https://models.example.com",
		},
		{
			name: "HTTPS without a hostname spells out the port",
			objects: []client.Object{modelGateway(
				listener(gatewayv1.HTTPSProtocolType, 443, nil),
				listener(gatewayv1.HTTPProtocolType, 80, nil),
			)},
			externalAddress: "10.0.0.1",
			wantEndpoint:    "https://10.0.0.1:443",
		},
		{
			name: "an empty hostname counts as no hostname",
			objects: []client.Object{modelGateway(
				listener(gatewayv1.HTTPSProtocolType, 443, &empty),
				listener(gatewayv1.HTTPProtocolType, 80, nil),
			)},
			externalAddress: "10.0.0.1",
			wantEndpoint:    "https://10.0.0.1:443",
		},
		{
			name: "a hostname on any HTTPS listener wins over the address",
			objects: []client.Object{modelGateway(
				listener(gatewayv1.HTTPSProtocolType, 443, nil),
				listener(gatewayv1.HTTPSProtocolType, 8443, &other),
			)},
			externalAddress: "10.0.0.1",
			wantEndpoint:    "https://other.example.com",
		},
		{
			name:            "only HTTP",
			objects:         []client.Object{modelGateway(listener(gatewayv1.HTTPProtocolType, 80, nil))},
			externalAddress: "10.0.0.1",
			wantEndpoint:    "http://10.0.0.1:80",
		},
		{
			name:            "only HTTP, carrying a hostname",
			objects:         []client.Object{modelGateway(listener(gatewayv1.HTTPProtocolType, 80, &hostname))},
			externalAddress: "10.0.0.1",
			wantEndpoint:    "http://models.example.com",
		},
		{
			name:            "non-standard HTTPS port",
			objects:         []client.Object{modelGateway(listener(gatewayv1.HTTPSProtocolType, 8443, nil))},
			externalAddress: "10.0.0.1",
			wantEndpoint:    "https://10.0.0.1:8443",
		},
		{
			name: "HTTPS wins even when HTTP is listed first and has the hostname",
			objects: []client.Object{modelGateway(
				listener(gatewayv1.HTTPProtocolType, 80, &hostname),
				listener(gatewayv1.HTTPSProtocolType, 443, nil),
			)},
			externalAddress: "10.0.0.1",
			wantEndpoint:    "https://10.0.0.1:443",
		},
		{
			name: "a listener named https but serving HTTP stays http",
			objects: []client.Object{modelGateway(func() gatewayv1.Listener {
				l := listener(gatewayv1.HTTPProtocolType, 80, nil)
				l.Name = "https"
				return l
			}())},
			externalAddress: "10.0.0.1",
			wantEndpoint:    "http://10.0.0.1:80",
		},
		{
			name:            "HTTP without a resolved external address",
			objects:         []client.Object{modelGateway(listener(gatewayv1.HTTPProtocolType, 80, nil))},
			externalAddress: "",
			wantEndpoint:    "",
		},
		{
			name: "no external address falls through to a listener with a hostname",
			objects: []client.Object{modelGateway(
				listener(gatewayv1.HTTPSProtocolType, 443, nil),
				listener(gatewayv1.HTTPProtocolType, 80, &hostname),
			)},
			externalAddress: "",
			wantEndpoint:    "http://models.example.com",
		},
		{
			name:            "no protocol the operator derives endpoints from",
			objects:         []client.Object{modelGateway(listener(gatewayv1.TCPProtocolType, 8080, nil))},
			externalAddress: "10.0.0.1",
			wantEndpoint:    "",
		},
		{
			name:            "gateway missing",
			externalAddress: "10.0.0.1",
			wantErr:         true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := newConfigTestClient(t, tt.objects...)
			got, err := resolveModelGatewayEndpoint(context.Background(), c, tt.externalAddress)
			if tt.wantErr {
				if !apierrors.IsNotFound(err) {
					t.Fatalf("resolveModelGatewayEndpoint() error = %v, want NotFound", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveModelGatewayEndpoint() error = %v", err)
			}
			if got != tt.wantEndpoint {
				t.Errorf("resolveModelGatewayEndpoint() = %q, want %q", got, tt.wantEndpoint)
			}
		})
	}
}
