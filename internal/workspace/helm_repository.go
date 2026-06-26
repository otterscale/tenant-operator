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
	"net/url"
	"time"

	"github.com/fluxcd/pkg/apis/meta"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlutil "sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	tenantv1alpha1 "github.com/otterscale/api/tenant/v1alpha1"
)

const labelValueTrue = "true"

// ReconcileHelmRepository ensures a FluxCD HelmRepository (type: oci) exists
// in the workspace namespace, pointing to the Harbor OCI registry for the workspace project.
func ReconcileHelmRepository(ctx context.Context, c client.Client, scheme *runtime.Scheme, w *tenantv1alpha1.Workspace, version string, harborURL string) error {
	repoURL, insecure, err := buildHelmRepositoryURL(harborURL, w.Spec.Namespace)
	if err != nil {
		return fmt.Errorf("building HelmRepository URL: %w", err)
	}

	repo := &sourcev1.HelmRepository{
		ObjectMeta: metav1.ObjectMeta{
			Name:      HelmRepositoryName,
			Namespace: w.Spec.Namespace,
		},
	}

	op, err := ctrlutil.CreateOrUpdate(ctx, c, repo, func() error {
		labels := LabelsForWorkspace(w.Name, version)
		labels[LabelFromHarbor] = labelValueTrue
		labels[LabelInternal] = labelValueTrue
		repo.Labels = labels

		repo.Spec = sourcev1.HelmRepositorySpec{
			SecretRef: &meta.LocalObjectReference{
				Name: ImagePullSecretName,
			},
			Type:     sourcev1.HelmRepositoryTypeOCI,
			URL:      repoURL,
			Interval: metav1.Duration{Duration: 5 * time.Minute},
			Insecure: insecure,
		}
		return ctrlutil.SetControllerReference(w, repo, scheme)
	})
	if err != nil {
		return err
	}
	if op != ctrlutil.OperationResultNone {
		log.FromContext(ctx).Info("HelmRepository reconciled", "operation", op, "name", repo.Name)
	}
	return nil
}

// buildHelmRepositoryURL constructs an OCI URL from a Harbor base URL and project name.
// For example: "https://harbor.example.com" + "my-project" → "oci://harbor.example.com/my-project"
// It also returns whether the Harbor URL uses an insecure (non-TLS) scheme.
func buildHelmRepositoryURL(harborURL, projectName string) (repoURL string, insecure bool, err error) {
	u, err := url.Parse(harborURL)
	if err != nil {
		return "", false, fmt.Errorf("parsing harbor URL %q: %w", harborURL, err)
	}
	return fmt.Sprintf("oci://%s/%s", u.Host, projectName), u.Scheme == "http", nil
}
