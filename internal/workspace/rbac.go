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

	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlutil "sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	tenantv1alpha1 "github.com/otterscale/tenant-operator/api/v1alpha1"
)

// ReconcileRoleBindings groups members by role and creates the necessary bindings.
func ReconcileRoleBindings(ctx context.Context, c client.Client, scheme *runtime.Scheme, w *tenantv1alpha1.Workspace, version string) error {
	membersByRole := make(map[tenantv1alpha1.MemberRole][]tenantv1alpha1.WorkspaceMember)
	for _, member := range w.Spec.Members {
		membersByRole[member.Role] = append(membersByRole[member.Role], member)
	}

	// Deterministic order.
	for _, role := range tenantv1alpha1.AllMemberRoles() {
		if err := reconcileRoleBinding(ctx, c, scheme, w, version, role, membersByRole[role]); err != nil {
			return err
		}
	}
	return nil
}

// reconcileRoleBinding binds a role to its members, deleting the binding when
// the role has none.
func reconcileRoleBinding(ctx context.Context, c client.Client, scheme *runtime.Scheme, w *tenantv1alpha1.Workspace, version string, role tenantv1alpha1.MemberRole, members []tenantv1alpha1.WorkspaceMember) error {
	binding := &rbacv1.RoleBinding{
		Name:      RoleBindingName + "-" + string(role),
		Namespace: w.Spec.Namespace,
	}

	if len(members) == 0 {
		if err := client.IgnoreNotFound(c.Delete(ctx, binding)); err != nil {
			return fmt.Errorf("deleting RoleBinding for role %q: %w", role, err)
		}
		return nil
	}

	op, err := ctrlutil.CreateOrUpdate(ctx, c, binding, func() error {
		binding.Labels = LabelsForWorkspace(w.Name, version)
		binding.Subjects = make([]rbacv1.Subject, 0, len(members))
		for _, m := range members {
			binding.Subjects = append(binding.Subjects, rbacv1.Subject{
				Kind:     rbacv1.UserKind,
				APIGroup: rbacv1.GroupName,
				Name:     m.Subject,
			})
		}
		binding.RoleRef = rbacv1.RoleRef{
			Kind:     clusterRoleKind,
			APIGroup: rbacv1.GroupName,
			Name:     string(role),
		}
		return ctrlutil.SetControllerReference(w, binding, scheme)
	})
	if err != nil {
		return fmt.Errorf("reconciling RoleBinding for role %q: %w", role, err)
	}
	if op != ctrlutil.OperationResultNone {
		log.FromContext(ctx).Info("RoleBinding reconciled", "operation", op, "name", binding.Name)
	}
	return nil
}
