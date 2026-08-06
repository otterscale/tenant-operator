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
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	tenantv1alpha1 "github.com/otterscale/api/tenant/v1alpha1"
)

const testRancherProjectID = "c-m-abcde:p-vwxyz"

func TestReconcileNamespaceRancherProjectID(t *testing.T) {
	t.Parallel()

	t.Run("sets the annotation without changing existing namespace metadata", func(t *testing.T) {
		w, c, scheme := newNamespaceTest(t, testRancherProjectID)
		w.Spec.LicenseInjection = true

		reconcileNamespaceForTest(t, c, scheme, w)
		namespace := getNamespaceForTest(t, c, w.Spec.Namespace)

		if got := namespace.Annotations[rancherProjectIDAnnotation]; got != testRancherProjectID {
			t.Fatalf("annotation = %q, want %q", got, testRancherProjectID)
		}
		for key, value := range map[string]string{
			"app.kubernetes.io/managed-by":       "tenant-operator",
			"pod-security.kubernetes.io/enforce": "baseline",
			"pod-security.kubernetes.io/warn":    "restricted",
			"pod-security.kubernetes.io/audit":   "restricted",
			licenseInjectLabelKey:                "true",
		} {
			if got := namespace.Labels[key]; got != value {
				t.Errorf("label %q = %q, want %q", key, got, value)
			}
		}
		if !metav1.IsControlledBy(namespace, w) {
			t.Error("namespace is not controlled by the workspace")
		}
	})

	t.Run("corrects a changed annotation", func(t *testing.T) {
		w, c, scheme := newNamespaceTest(t, testRancherProjectID)
		reconcileNamespaceForTest(t, c, scheme, w)

		namespace := getNamespaceForTest(t, c, w.Spec.Namespace)
		namespace.Annotations[rancherProjectIDAnnotation] = "local:p-wrong"
		if err := c.Update(context.Background(), namespace); err != nil {
			t.Fatalf("update namespace: %v", err)
		}

		reconcileNamespaceForTest(t, c, scheme, w)
		if got := getNamespaceForTest(t, c, w.Spec.Namespace).Annotations[rancherProjectIDAnnotation]; got != testRancherProjectID {
			t.Fatalf("annotation = %q, want %q", got, testRancherProjectID)
		}
	})

	t.Run("does not add the annotation for an empty spec", func(t *testing.T) {
		w, c, scheme := newNamespaceTest(t, "")
		reconcileNamespaceForTest(t, c, scheme, w)

		if _, ok := getNamespaceForTest(t, c, w.Spec.Namespace).Annotations[rancherProjectIDAnnotation]; ok {
			t.Error("annotation was added for an empty Rancher Project ID")
		}
	})

	t.Run("preserves an existing annotation for an empty spec", func(t *testing.T) {
		w, c, scheme := newNamespaceTest(t, "")
		reconcileNamespaceForTest(t, c, scheme, w)

		namespace := getNamespaceForTest(t, c, w.Spec.Namespace)
		namespace.Annotations = map[string]string{rancherProjectIDAnnotation: "local:p-existing"}
		if err := c.Update(context.Background(), namespace); err != nil {
			t.Fatalf("update namespace: %v", err)
		}

		reconcileNamespaceForTest(t, c, scheme, w)
		if got := getNamespaceForTest(t, c, w.Spec.Namespace).Annotations[rancherProjectIDAnnotation]; got != "local:p-existing" {
			t.Fatalf("annotation = %q, want %q", got, "local:p-existing")
		}
	})
}

func newNamespaceTest(t *testing.T, rancherProjectID string) (*tenantv1alpha1.Workspace, client.Client, *runtime.Scheme) {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add core scheme: %v", err)
	}
	if err := tenantv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add tenant scheme: %v", err)
	}

	w := &tenantv1alpha1.Workspace{
		ObjectMeta: metav1.ObjectMeta{
			Name: "workspace-a",
			UID:  types.UID("workspace-a-uid"),
		},
		Spec: tenantv1alpha1.WorkspaceSpec{
			Namespace:        "workspace-a",
			RancherProjectID: rancherProjectID,
		},
	}

	return w, fake.NewClientBuilder().WithScheme(scheme).Build(), scheme
}

func reconcileNamespaceForTest(t *testing.T, c client.Client, scheme *runtime.Scheme, w *tenantv1alpha1.Workspace) {
	t.Helper()
	if err := ReconcileNamespace(context.Background(), c, scheme, w, "test"); err != nil {
		t.Fatalf("reconcile namespace: %v", err)
	}
}

func getNamespaceForTest(t *testing.T, c client.Client, name string) *corev1.Namespace {
	t.Helper()
	namespace := &corev1.Namespace{}
	if err := c.Get(context.Background(), client.ObjectKey{Name: name}, namespace); err != nil {
		t.Fatalf("get namespace: %v", err)
	}
	return namespace
}
