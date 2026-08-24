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

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlutil "sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	tenantv1alpha1 "github.com/otterscale/tenant-operator/api/v1alpha1"
)

const (
	rancherProjectIDAnnotation = "field.cattle.io/projectId"

	// RancherProjectIDKey is the tenant-operator-config key carrying the
	// Rancher Project ID, in "<cluster-id>:<project-id>" form. It is operator-wide
	// configuration rather than a Workspace spec field, so every workspace
	// namespace this operator manages joins the same Rancher Project.
	RancherProjectIDKey = "RancherProjectID"
)

// podSecurityLabels returns the Pod Security admission labels applied to every
// workspace namespace: enforce at baseline as a practical multi-tenant floor,
// warn/audit at restricted to nudge workloads toward the stricter profile
// without blocking them.
func podSecurityLabels() map[string]string {
	return map[string]string{
		"pod-security.kubernetes.io/enforce": "baseline",
		"pod-security.kubernetes.io/warn":    "restricted",
		"pod-security.kubernetes.io/audit":   "restricted",
	}
}

// NamespaceConflictError is a permanent error indicating the target namespace
// already exists but is not owned by this workspace.
type NamespaceConflictError struct {
	Name string
}

func (e *NamespaceConflictError) Error() string {
	return fmt.Sprintf("namespace %s exists but is not owned by this workspace", e.Name)
}

// ReconcileNamespace ensures the Namespace exists and is properly labeled.
func ReconcileNamespace(ctx context.Context, c client.Client, scheme *runtime.Scheme, w *tenantv1alpha1.Workspace, version string) error {
	namespace := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: w.Spec.Namespace,
		},
	}

	globalConfig, err := operatorConfigData(ctx, c)
	if err != nil {
		return err
	}
	rancherProjectID := globalConfig[RancherProjectIDKey]

	op, err := ctrlutil.CreateOrUpdate(ctx, c, namespace, func() error {
		// Safety check: Prevent taking over existing namespaces not owned by us
		if !IsOwned(namespace.OwnerReferences, w.UID) && !namespace.CreationTimestamp.IsZero() {
			return &NamespaceConflictError{Name: namespace.Name}
		}

		if namespace.Labels == nil {
			namespace.Labels = map[string]string{}
		}

		maps.Copy(namespace.Labels, LabelsForWorkspace(w.Name, version))
		maps.Copy(namespace.Labels, podSecurityLabels())

		if rancherProjectID != "" {
			if namespace.Annotations == nil {
				namespace.Annotations = map[string]string{}
			}
			namespace.Annotations[rancherProjectIDAnnotation] = rancherProjectID
		}

		// Set OwnerReference to ensure garbage collection works
		return ctrlutil.SetControllerReference(w, namespace, scheme)
	})
	if err != nil {
		return err
	}
	if op != ctrlutil.OperationResultNone {
		log.FromContext(ctx).Info("Namespace reconciled", "operation", op, "name", namespace.Name)
	}
	return nil
}
