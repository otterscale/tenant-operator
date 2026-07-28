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
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	tenantv1alpha1 "github.com/otterscale/api/tenant/v1alpha1"
)

func TestReconcileFluxRBAC(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := rbacv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := tenantv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	w := &tenantv1alpha1.Workspace{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-workspace",
			UID:  types.UID("test-workspace-uid"),
		},
		Spec: tenantv1alpha1.WorkspaceSpec{Namespace: "test-workspace"},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	ctx := context.Background()

	if err := ReconcileFluxRBAC(ctx, c, scheme, w, "v1.2.3"); err != nil {
		t.Fatalf("ReconcileFluxRBAC() error = %v", err)
	}

	key := types.NamespacedName{Name: WorkspaceReconcilerName, Namespace: w.Spec.Namespace}
	serviceAccount := &corev1.ServiceAccount{}
	if err := c.Get(ctx, key, serviceAccount); err != nil {
		t.Fatalf("get ServiceAccount: %v", err)
	}
	if !metav1.IsControlledBy(serviceAccount, w) {
		t.Error("ServiceAccount is not controlled by Workspace")
	}

	role := &rbacv1.Role{}
	if err := c.Get(ctx, key, role); err != nil {
		t.Fatalf("get Role: %v", err)
	}
	wantRules := []rbacv1.PolicyRule{{
		APIGroups: []string{"*"},
		Resources: []string{"*"},
		Verbs:     []string{"*"},
	}}
	if !reflect.DeepEqual(role.Rules, wantRules) {
		t.Errorf("Role rules = %#v, want %#v", role.Rules, wantRules)
	}

	roleBinding := &rbacv1.RoleBinding{}
	if err := c.Get(ctx, key, roleBinding); err != nil {
		t.Fatalf("get RoleBinding: %v", err)
	}
	wantSubjects := []rbacv1.Subject{{
		Kind:      rbacv1.ServiceAccountKind,
		Name:      WorkspaceReconcilerName,
		Namespace: w.Spec.Namespace,
	}}
	if !reflect.DeepEqual(roleBinding.Subjects, wantSubjects) {
		t.Errorf("RoleBinding subjects = %#v, want %#v", roleBinding.Subjects, wantSubjects)
	}
	if roleBinding.RoleRef.Kind != "Role" || roleBinding.RoleRef.Name != WorkspaceReconcilerName {
		t.Errorf("RoleBinding roleRef = %#v", roleBinding.RoleRef)
	}

	role.Rules = nil
	if err := c.Update(ctx, role); err != nil {
		t.Fatalf("update Role drift: %v", err)
	}
	roleBinding.Subjects = nil
	if err := c.Update(ctx, roleBinding); err != nil {
		t.Fatalf("update RoleBinding drift: %v", err)
	}
	serviceAccount.Labels = nil
	if err := c.Update(ctx, serviceAccount); err != nil {
		t.Fatalf("update ServiceAccount drift: %v", err)
	}

	if err := ReconcileFluxRBAC(ctx, c, scheme, w, "v1.2.3"); err != nil {
		t.Fatalf("ReconcileFluxRBAC() after drift error = %v", err)
	}
	if err := c.Get(ctx, key, role); err != nil {
		t.Fatalf("get reconciled Role: %v", err)
	}
	if !reflect.DeepEqual(role.Rules, wantRules) {
		t.Errorf("reconciled Role rules = %#v, want %#v", role.Rules, wantRules)
	}
	if err := c.Get(ctx, key, roleBinding); err != nil {
		t.Fatalf("get reconciled RoleBinding: %v", err)
	}
	if !reflect.DeepEqual(roleBinding.Subjects, wantSubjects) {
		t.Errorf("reconciled RoleBinding subjects = %#v, want %#v", roleBinding.Subjects, wantSubjects)
	}
	if err := c.Get(ctx, key, serviceAccount); err != nil {
		t.Fatalf("get reconciled ServiceAccount: %v", err)
	}
	if serviceAccount.Labels["app.kubernetes.io/managed-by"] != "tenant-operator" {
		t.Errorf("reconciled ServiceAccount labels = %#v", serviceAccount.Labels)
	}
}
