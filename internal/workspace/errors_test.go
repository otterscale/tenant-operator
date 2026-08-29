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
	"strings"
	"testing"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	tenantv1alpha1 "github.com/otterscale/tenant-operator/api/v1alpha1"
)

// The controller puts err.Error() verbatim into the Ready condition and the
// Warning event, so an unwrapped client error reaches the user as a bare
// "Operation cannot be fulfilled on resourcequotas ..." with nothing saying
// which sync step produced it. These tests fail any Reconcile* that hands a
// client error back without naming the resource it was working on.

// errWrite is what every write in these tests fails with. Its Conflict status
// also proves the wrapping stays transparent to apierrors.IsConflict.
var errWrite = apierrors.NewConflict(
	schema.GroupResource{Resource: "test"}, "test", errors.New("synthetic write failure"))

// failWritesClient returns a client that serves reads from objs but fails every
// create, update and delete.
func failWritesClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()

	s := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{
		corev1.AddToScheme,
		rbacv1.AddToScheme,
		networkingv1.AddToScheme,
		sourcev1.AddToScheme,
		gatewayv1.Install,
		tenantv1alpha1.AddToScheme,
	} {
		if err := add(s); err != nil {
			t.Fatalf("building scheme: %v", err)
		}
	}

	return fake.NewClientBuilder().
		WithScheme(s).
		WithObjects(objs...).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(context.Context, client.WithWatch, client.Object, ...client.CreateOption) error {
				return errWrite
			},
			Update: func(context.Context, client.WithWatch, client.Object, ...client.UpdateOption) error {
				return errWrite
			},
			Delete: func(context.Context, client.WithWatch, client.Object, ...client.DeleteOption) error {
				return errWrite
			},
		}).
		Build()
}

func workspaceForErrorTest() *tenantv1alpha1.Workspace {
	return &tenantv1alpha1.Workspace{
		Name: "ws-errors", UID: "ws-errors-uid",
		Spec: tenantv1alpha1.WorkspaceSpec{
			Namespace: "ns-errors",
			Members: []tenantv1alpha1.WorkspaceMember{
				{Role: tenantv1alpha1.MemberRoleAdmin, Subject: "alice"},
			},
		},
	}
}

func TestReconcileErrorsNameTheirResource(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	if err := tenantv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("building scheme: %v", err)
	}

	tests := []struct {
		name string
		// seed is read-only cluster content the call needs.
		seed []client.Object
		// mutate shapes the spec to select the write or the delete path.
		mutate func(*tenantv1alpha1.Workspace)
		call   func(ctx context.Context, c client.Client, w *tenantv1alpha1.Workspace) error
		want   string
	}{
		{
			name: "namespace",
			call: func(ctx context.Context, c client.Client, w *tenantv1alpha1.Workspace) error {
				return ReconcileNamespace(ctx, c, scheme, w, "v0")
			},
			want: "reconciling Namespace",
		},
		{
			name: "role binding write",
			call: func(ctx context.Context, c client.Client, w *tenantv1alpha1.Workspace) error {
				return ReconcileRoleBindings(ctx, c, scheme, w, "v0")
			},
			want: `reconciling RoleBinding for role "admin"`,
		},
		{
			name:   "role binding delete",
			mutate: func(w *tenantv1alpha1.Workspace) { w.Spec.Members = nil },
			call: func(ctx context.Context, c client.Client, w *tenantv1alpha1.Workspace) error {
				return ReconcileRoleBindings(ctx, c, scheme, w, "v0")
			},
			want: `deleting RoleBinding for role "admin"`,
		},
		{
			name: "resource quota write",
			mutate: func(w *tenantv1alpha1.Workspace) {
				w.Spec.ResourceQuota = &corev1.ResourceQuotaSpec{}
			},
			call: func(ctx context.Context, c client.Client, w *tenantv1alpha1.Workspace) error {
				return ReconcileResourceQuota(ctx, c, scheme, w, "v0")
			},
			want: "reconciling ResourceQuota",
		},
		{
			name: "resource quota delete",
			call: func(ctx context.Context, c client.Client, w *tenantv1alpha1.Workspace) error {
				return ReconcileResourceQuota(ctx, c, scheme, w, "v0")
			},
			want: "deleting ResourceQuota",
		},
		{
			name: "limit range write",
			mutate: func(w *tenantv1alpha1.Workspace) {
				w.Spec.LimitRange = &corev1.LimitRangeSpec{}
			},
			call: func(ctx context.Context, c client.Client, w *tenantv1alpha1.Workspace) error {
				return ReconcileLimitRange(ctx, c, scheme, w, "v0")
			},
			want: "reconciling LimitRange",
		},
		{
			name: "limit range delete",
			call: func(ctx context.Context, c client.Client, w *tenantv1alpha1.Workspace) error {
				return ReconcileLimitRange(ctx, c, scheme, w, "v0")
			},
			want: "deleting LimitRange",
		},
		{
			name: "network policy write",
			mutate: func(w *tenantv1alpha1.Workspace) {
				w.Spec.NetworkIsolation.Enabled = true
			},
			call: func(ctx context.Context, c client.Client, w *tenantv1alpha1.Workspace) error {
				return ReconcileNetworkIsolation(ctx, c, scheme, w, "v0")
			},
			want: "reconciling NetworkPolicy",
		},
		{
			name: "network policy delete",
			call: func(ctx context.Context, c client.Client, w *tenantv1alpha1.Workspace) error {
				return ReconcileNetworkIsolation(ctx, c, scheme, w, "v0")
			},
			want: "deleting NetworkPolicy",
		},
		{
			// A resolvable endpoint sends ReconcileConfig down its write path.
			name: "config write",
			seed: []client.Object{platformGateway(PlatformGatewayNamespace, "10.0.0.1")},
			call: func(ctx context.Context, c client.Client, w *tenantv1alpha1.Workspace) error {
				return ReconcileConfig(ctx, c, scheme, w, "v0")
			},
			want: "reconciling ConfigMap",
		},
		{
			// No Gateway resolves, so the ConfigMap is removed instead.
			name: "config delete",
			call: func(ctx context.Context, c client.Client, w *tenantv1alpha1.Workspace) error {
				return ReconcileConfig(ctx, c, scheme, w, "v0")
			},
			want: "deleting ConfigMap",
		},
		{
			name: "helm repository",
			call: func(ctx context.Context, c client.Client, w *tenantv1alpha1.Workspace) error {
				return ReconcileHelmRepository(ctx, c, scheme, w, "v0", "https://harbor.example.com")
			},
			want: "reconciling HelmRepository",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			w := workspaceForErrorTest()
			if tt.mutate != nil {
				tt.mutate(w)
			}

			err := tt.call(context.Background(), failWritesClient(t, tt.seed...), w)
			if err == nil {
				t.Fatalf("expected an error, got nil")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("error = %q, want it to contain %q", err, tt.want)
			}
			// The controller classifies errors by unwrapping what it is handed.
			if !apierrors.IsConflict(err) {
				t.Errorf("error = %q, want it to still unwrap to the underlying Conflict", err)
			}
		})
	}
}

// ReconcileNamespace returns NamespaceConflictError from inside its mutate
// function, and the controller only treats it as permanent if errors.As can
// still find it through the wrap.
func TestReconcileNamespaceConflictSurvivesWrapping(t *testing.T) {
	t.Parallel()

	w := workspaceForErrorTest()
	occupied := &corev1.Namespace{
		Name:            w.Spec.Namespace,
		OwnerReferences: []metav1.OwnerReference{{UID: "somebody-else"}},
		// The guard keys off a non-zero CreationTimestamp to tell an existing
		// namespace from the empty object CreateOrUpdate builds before a
		// create, and the fake client does not set one.
		CreationTimestamp: metav1.Now(),
	}

	scheme := runtime.NewScheme()
	if err := tenantv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("building scheme: %v", err)
	}

	err := ReconcileNamespace(context.Background(), failWritesClient(t, occupied), scheme, w, "v0")
	if err == nil {
		t.Fatal("expected a conflict error, got nil")
	}

	if _, ok := errors.AsType[*NamespaceConflictError](err); !ok {
		t.Fatalf("error = %q, want errors.As to still find NamespaceConflictError", err)
	}
	if !strings.Contains(err.Error(), "reconciling Namespace") {
		t.Errorf("error = %q, want it to name the step as well", err)
	}
}
