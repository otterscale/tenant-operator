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

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlutil "sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	tenantv1alpha1 "github.com/otterscale/tenant-operator/api/v1alpha1"
)

// ReconcileResourceQuota applies quota constraints if defined, or deletes the quota if removed.
func ReconcileResourceQuota(ctx context.Context, c client.Client, scheme *runtime.Scheme, w *tenantv1alpha1.Workspace, version string) error {
	quota := &corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ResourceQuotaName,
			Namespace: w.Spec.Namespace,
		},
	}

	if w.Spec.ResourceQuota == nil {
		if err := client.IgnoreNotFound(c.Delete(ctx, quota)); err != nil {
			return fmt.Errorf("deleting ResourceQuota: %w", err)
		}
		return nil
	}

	op, err := ctrlutil.CreateOrUpdate(ctx, c, quota, func() error {
		quota.Labels = LabelsForWorkspace(w.Name, version)
		quota.Spec = *w.Spec.ResourceQuota
		return ctrlutil.SetControllerReference(w, quota, scheme)
	})
	if err != nil {
		return fmt.Errorf("reconciling ResourceQuota: %w", err)
	}
	if op != ctrlutil.OperationResultNone {
		log.FromContext(ctx).Info("ResourceQuota reconciled", "operation", op, "name", quota.Name)
	}
	return nil
}

// ReconcileLimitRange applies default limits if defined, or deletes the range if removed.
func ReconcileLimitRange(ctx context.Context, c client.Client, scheme *runtime.Scheme, w *tenantv1alpha1.Workspace, version string) error {
	limits := &corev1.LimitRange{
		ObjectMeta: metav1.ObjectMeta{
			Name:      LimitRangeName,
			Namespace: w.Spec.Namespace,
		},
	}

	if w.Spec.LimitRange == nil {
		if err := client.IgnoreNotFound(c.Delete(ctx, limits)); err != nil {
			return fmt.Errorf("deleting LimitRange: %w", err)
		}
		return nil
	}

	op, err := ctrlutil.CreateOrUpdate(ctx, c, limits, func() error {
		limits.Labels = LabelsForWorkspace(w.Name, version)
		limits.Spec = *w.Spec.LimitRange
		return ctrlutil.SetControllerReference(w, limits, scheme)
	})
	if err != nil {
		return fmt.Errorf("reconciling LimitRange: %w", err)
	}
	if op != ctrlutil.OperationResultNone {
		log.FromContext(ctx).Info("LimitRange reconciled", "operation", op, "name", limits.Name)
	}
	return nil
}
