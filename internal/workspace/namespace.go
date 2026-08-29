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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
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

// ValidateNamespaceAvailable reports whether the workspace's target namespace is
// still free to create.
//
// This is the admission-time counterpart of NamespaceConflictError, and exists
// because that error is unrecoverable: reconcile refuses to adopt a namespace it
// does not own, spec.namespace is immutable, and the conflict does not requeue —
// so a Workspace admitted onto an occupied namespace can never become Ready and
// cannot be repaired, only deleted and recreated. Admission is the last point at
// which the caller can still be told to pick another name.
//
// Only meaningful on create. Afterwards the namespace exists precisely because
// reconcile created it.
func ValidateNamespaceAvailable(ctx context.Context, reader client.Reader, ws *tenantv1alpha1.Workspace) error {
	err := reader.Get(ctx, types.NamespacedName{Name: ws.Spec.Namespace}, &corev1.Namespace{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("checking whether namespace %q is available: %w", ws.Spec.Namespace, err)
	}
	return fmt.Errorf(
		"namespace %q already exists: a workspace creates its own namespace, so choose a different spec.namespace or leave it empty to have one generated",
		ws.Spec.Namespace)
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
		// %w, not %v: the mutate function above returns NamespaceConflictError
		// through here, and the controller classifies it as permanent with
		// errors.As. Flattening it would silently turn that into a retry loop.
		return fmt.Errorf("reconciling Namespace %q: %w", namespace.Name, err)
	}
	if op != ctrlutil.OperationResultNone {
		log.FromContext(ctx).Info("Namespace reconciled", "operation", op, "name", namespace.Name)
	}
	return nil
}
