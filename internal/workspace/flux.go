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

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlutil "sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	tenantv1alpha1 "github.com/otterscale/api/tenant/v1alpha1"
)

const WorkspaceReconcilerName = "workspace-reconciler"

// ReconcileFluxRBAC ensures Flux reconciles Workspace workloads with
// namespace-scoped permissions instead of the Flux controller identity.
func ReconcileFluxRBAC(ctx context.Context, c client.Client, scheme *runtime.Scheme, w *tenantv1alpha1.Workspace, version string) error {
	labels := LabelsForWorkspace(w.Name, version)

	serviceAccount := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      WorkspaceReconcilerName,
			Namespace: w.Spec.Namespace,
		},
	}
	op, err := ctrlutil.CreateOrUpdate(ctx, c, serviceAccount, func() error {
		serviceAccount.Labels = labels
		return ctrlutil.SetControllerReference(w, serviceAccount, scheme)
	})
	if err != nil {
		return err
	}
	logReconciledFluxRBAC(ctx, op, "ServiceAccount", serviceAccount.Name)

	desiredRoleRef := rbacv1.RoleRef{
		APIGroup: rbacv1.GroupName,
		Kind:     "ClusterRole",
		Name:     string(tenantv1alpha1.MemberRoleEdit),
	}

	roleBinding := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      WorkspaceReconcilerName,
			Namespace: w.Spec.Namespace,
		},
	}
	switch err := c.Get(ctx, client.ObjectKeyFromObject(roleBinding), roleBinding); {
	case err == nil && roleBinding.RoleRef != desiredRoleRef:
		// roleRef is immutable, so an in-place update is rejected by the API server; recreate instead.
		if err := c.Delete(ctx, roleBinding); err != nil {
			return err
		}
		roleBinding = &rbacv1.RoleBinding{
			ObjectMeta: metav1.ObjectMeta{
				Name:      WorkspaceReconcilerName,
				Namespace: w.Spec.Namespace,
			},
		}
	case err != nil && !apierrors.IsNotFound(err):
		return err
	}

	op, err = ctrlutil.CreateOrUpdate(ctx, c, roleBinding, func() error {
		roleBinding.Labels = labels
		roleBinding.Subjects = []rbacv1.Subject{{
			Kind:      rbacv1.ServiceAccountKind,
			Name:      WorkspaceReconcilerName,
			Namespace: w.Spec.Namespace,
		}}
		roleBinding.RoleRef = desiredRoleRef
		return ctrlutil.SetControllerReference(w, roleBinding, scheme)
	})
	if err != nil {
		return err
	}
	logReconciledFluxRBAC(ctx, op, "RoleBinding", roleBinding.Name)

	return nil
}

func logReconciledFluxRBAC(ctx context.Context, op ctrlutil.OperationResult, kind, name string) {
	if op != ctrlutil.OperationResultNone {
		log.FromContext(ctx).Info(kind+" reconciled", "operation", op, "name", name)
	}
}
