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

// ReconcileTokenReviewer grants the namespace's default ServiceAccount create on
// tokenreviews, which no RoleBinding can carry: TokenReview is non-namespaced.
func ReconcileTokenReviewer(ctx context.Context, c client.Client, scheme *runtime.Scheme, w *tenantv1alpha1.Workspace, version string) error {
	binding := &rbacv1.ClusterRoleBinding{
		Name: TokenReviewerClusterRoleBindingName(w.Name),
	}

	op, err := ctrlutil.CreateOrUpdate(ctx, c, binding, func() error {
		binding.Labels = LabelsForWorkspace(w.Name, version)
		binding.Subjects = []rbacv1.Subject{{
			Kind:      rbacv1.ServiceAccountKind,
			Name:      defaultServiceAccountName,
			Namespace: w.Spec.Namespace,
		}}
		binding.RoleRef = rbacv1.RoleRef{
			Kind:     clusterRoleKind,
			APIGroup: rbacv1.GroupName,
			Name:     TokenReviewerClusterRoleName,
		}
		return ctrlutil.SetControllerReference(w, binding, scheme)
	})
	if err != nil {
		return fmt.Errorf("reconciling ClusterRoleBinding %q: %w", binding.Name, err)
	}
	if op != ctrlutil.OperationResultNone {
		log.FromContext(ctx).Info("ClusterRoleBinding reconciled", "operation", op, "name", binding.Name)
	}

	return nil
}
