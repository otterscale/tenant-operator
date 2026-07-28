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
	"time"

	"github.com/fluxcd/pkg/apis/meta"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	tenantv1alpha1 "github.com/otterscale/api/tenant/v1alpha1"
)

func TestReconcileHelmRepositoryCreatesExpectedSources(t *testing.T) {
	k8sClient, scheme, w := newHelmRepositoryTestClient(t)

	if err := ReconcileHelmRepository(
		context.Background(),
		k8sClient,
		scheme,
		w,
		"test-version",
		"https://harbor.example.com",
	); err != nil {
		t.Fatalf("ReconcileHelmRepository returned error: %v", err)
	}

	t.Run("private workspace source", func(t *testing.T) {
		repo := getHelmRepository(t, k8sClient, w.Spec.Namespace, HelmRepositoryName)

		if repo.Spec.Type != sourcev1.HelmRepositoryTypeOCI {
			t.Errorf("expected type %q, got %q", sourcev1.HelmRepositoryTypeOCI, repo.Spec.Type)
		}
		if repo.Spec.URL != "oci://harbor.example.com/test-namespace" {
			t.Errorf("expected workspace URL, got %q", repo.Spec.URL)
		}
		if repo.Spec.Interval.Duration != 5*time.Minute {
			t.Errorf("expected interval %s, got %s", 5*time.Minute, repo.Spec.Interval.Duration)
		}
		if repo.Spec.Insecure {
			t.Error("expected secure HelmRepository")
		}
		if repo.Spec.SecretRef == nil || repo.Spec.SecretRef.Name != ImagePullSecretName {
			t.Errorf("expected SecretRef %q, got %v", ImagePullSecretName, repo.Spec.SecretRef)
		}
		if repo.Labels[LabelFromHarbor] != labelValueTrue {
			t.Errorf("expected %s=%s", LabelFromHarbor, labelValueTrue)
		}
		if repo.Labels[LabelInternal] != labelValueTrue {
			t.Errorf("expected %s=%s", LabelInternal, labelValueTrue)
		}

		owner := metav1.GetControllerOf(repo)
		if owner == nil || owner.Name != w.Name || owner.UID != w.UID {
			t.Errorf("expected Workspace owner %q with UID %q, got %v", w.Name, w.UID, owner)
		}
	})

	t.Run("public library source", func(t *testing.T) {
		repo := getHelmRepository(t, k8sClient, w.Spec.Namespace, HarborDefaultProjectName)

		if repo.Spec.Type != sourcev1.HelmRepositoryTypeOCI {
			t.Errorf("expected type %q, got %q", sourcev1.HelmRepositoryTypeOCI, repo.Spec.Type)
		}
		if repo.Spec.URL != "oci://harbor.example.com/library" {
			t.Errorf("expected library URL, got %q", repo.Spec.URL)
		}
		if repo.Spec.Interval.Duration != 5*time.Minute {
			t.Errorf("expected interval %s, got %s", 5*time.Minute, repo.Spec.Interval.Duration)
		}
		if repo.Spec.Insecure {
			t.Error("expected secure HelmRepository")
		}

		// The public library source needs no credentials and must remain visible to users.
		if repo.Spec.SecretRef != nil {
			t.Errorf("expected no SecretRef, got %q", repo.Spec.SecretRef.Name)
		}
		if _, internal := repo.Labels[LabelInternal]; internal {
			t.Errorf("expected no %s label", LabelInternal)
		}
		if repo.Labels[LabelFromHarbor] != labelValueTrue {
			t.Errorf("expected %s=%s", LabelFromHarbor, labelValueTrue)
		}

		owner := metav1.GetControllerOf(repo)
		if owner == nil || owner.Name != w.Name || owner.UID != w.UID {
			t.Errorf("expected Workspace owner %q with UID %q, got %v", w.Name, w.UID, owner)
		}
	})
}

func TestReconcileHelmRepositoryIsIdempotent(t *testing.T) {
	k8sClient, scheme, w := newHelmRepositoryTestClient(t)

	for range 2 {
		if err := ReconcileHelmRepository(
			context.Background(),
			k8sClient,
			scheme,
			w,
			"test-version",
			"https://harbor.example.com",
		); err != nil {
			t.Fatalf("ReconcileHelmRepository returned error: %v", err)
		}
	}

	getHelmRepository(t, k8sClient, w.Spec.Namespace, HelmRepositoryName)
	getHelmRepository(t, k8sClient, w.Spec.Namespace, HarborDefaultProjectName)

	repositories := &sourcev1.HelmRepositoryList{}
	if err := k8sClient.List(context.Background(), repositories, client.InNamespace(w.Spec.Namespace)); err != nil {
		t.Fatalf("failed to list HelmRepositories: %v", err)
	}
	if len(repositories.Items) != 2 {
		t.Errorf("expected 2 HelmRepositories, got %d", len(repositories.Items))
	}
}

func TestReconcileHelmRepositoryCorrectsLibraryDrift(t *testing.T) {
	library := &sourcev1.HelmRepository{
		ObjectMeta: metav1.ObjectMeta{
			Name:      HarborDefaultProjectName,
			Namespace: "test-namespace",
			Labels: map[string]string{
				LabelFromHarbor: "false",
				LabelInternal:   labelValueTrue,
			},
		},
		Spec: sourcev1.HelmRepositorySpec{
			SecretRef: &meta.LocalObjectReference{Name: "wrong-secret"},
			Type:      sourcev1.HelmRepositoryTypeDefault,
			URL:       "oci://wrong.example.com/wrong-project",
			Interval:  metav1.Duration{Duration: time.Minute},
			Insecure:  true,
		},
	}

	k8sClient, scheme, w := newHelmRepositoryTestClient(t, library)
	if err := ReconcileHelmRepository(
		context.Background(),
		k8sClient,
		scheme,
		w,
		"test-version",
		"https://harbor.example.com",
	); err != nil {
		t.Fatalf("ReconcileHelmRepository returned error: %v", err)
	}

	repo := getHelmRepository(t, k8sClient, w.Spec.Namespace, HarborDefaultProjectName)
	if repo.Spec.Type != sourcev1.HelmRepositoryTypeOCI {
		t.Errorf("expected type %q, got %q", sourcev1.HelmRepositoryTypeOCI, repo.Spec.Type)
	}
	if repo.Spec.URL != "oci://harbor.example.com/library" {
		t.Errorf("expected corrected library URL, got %q", repo.Spec.URL)
	}
	if repo.Spec.Interval.Duration != 5*time.Minute {
		t.Errorf("expected corrected interval %s, got %s", 5*time.Minute, repo.Spec.Interval.Duration)
	}
	if repo.Spec.Insecure {
		t.Error("expected corrected secure HelmRepository")
	}
	if repo.Spec.SecretRef != nil {
		t.Errorf("expected corrected nil SecretRef, got %q", repo.Spec.SecretRef.Name)
	}
	if repo.Labels[LabelFromHarbor] != labelValueTrue {
		t.Errorf("expected corrected %s=%s", LabelFromHarbor, labelValueTrue)
	}
	if _, internal := repo.Labels[LabelInternal]; internal {
		t.Errorf("expected corrected resource without %s label", LabelInternal)
	}
}

func newHelmRepositoryTestClient(t *testing.T, objects ...client.Object) (client.Client, *runtime.Scheme, *tenantv1alpha1.Workspace) {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := tenantv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add Workspace types to scheme: %v", err)
	}
	if err := sourcev1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add HelmRepository types to scheme: %v", err)
	}

	w := &tenantv1alpha1.Workspace{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-workspace",
			UID:  types.UID("test-workspace-uid"),
		},
		Spec: tenantv1alpha1.WorkspaceSpec{
			Namespace: "test-namespace",
		},
	}
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
	return k8sClient, scheme, w
}

func getHelmRepository(t *testing.T, k8sClient client.Client, namespace, name string) *sourcev1.HelmRepository {
	t.Helper()

	repo := &sourcev1.HelmRepository{}
	key := client.ObjectKey{Name: name, Namespace: namespace}
	if err := k8sClient.Get(context.Background(), key, repo); err != nil {
		t.Fatalf("failed to get HelmRepository %q: %v", name, err)
	}
	return repo
}

func TestBuildHelmRepositoryURL(t *testing.T) {
	tests := []struct {
		name         string
		harborURL    string
		projectName  string
		wantURL      string
		wantInsecure bool
		wantErr      bool
	}{
		{
			name:         "standard https URL",
			harborURL:    "https://harbor.example.com",
			projectName:  "my-workspace",
			wantURL:      "oci://harbor.example.com/my-workspace",
			wantInsecure: false,
		},
		{
			name:         "URL with port",
			harborURL:    "https://harbor.example.com:8443",
			projectName:  "test-ns",
			wantURL:      "oci://harbor.example.com:8443/test-ns",
			wantInsecure: false,
		},
		{
			name:         "http URL is insecure",
			harborURL:    "http://harbor.local",
			projectName:  "dev",
			wantURL:      "oci://harbor.local/dev",
			wantInsecure: true,
		},
		{
			name:         "http URL with port is insecure",
			harborURL:    "http://harbor.default.svc.cluster.local:80",
			projectName:  "my-ns",
			wantURL:      "oci://harbor.default.svc.cluster.local:80/my-ns",
			wantInsecure: true,
		},
		{
			name:        "invalid URL",
			harborURL:   "://invalid",
			projectName: "ns",
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotURL, gotInsecure, err := buildHelmRepositoryURL(tt.harborURL, tt.projectName)
			if (err != nil) != tt.wantErr {
				t.Errorf("buildHelmRepositoryURL() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotURL != tt.wantURL {
				t.Errorf("buildHelmRepositoryURL() URL = %q, want %q", gotURL, tt.wantURL)
			}
			if gotInsecure != tt.wantInsecure {
				t.Errorf("buildHelmRepositoryURL() insecure = %v, want %v", gotInsecure, tt.wantInsecure)
			}
		})
	}
}
