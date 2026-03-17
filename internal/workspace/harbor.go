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
	"encoding/base64"
	"encoding/json"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlutil "sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	tenantv1alpha1 "github.com/otterscale/api/tenant/v1alpha1"
	"github.com/otterscale/tenant-operator/internal/harbor"
)

// ReconcileHarbor ensures the Harbor project, robot account, docker-registry Secret,
// and default ServiceAccount imagePullSecrets are configured for the workspace.
func ReconcileHarbor(
	ctx context.Context,
	c client.Client,
	scheme *runtime.Scheme,
	w *tenantv1alpha1.Workspace,
	version string,
	harborClient harbor.Client,
	harborURL string,
) error {
	logger := log.FromContext(ctx)
	projectName := w.Spec.Namespace
	robotName := w.Spec.Namespace

	// 1. Ensure Harbor project exists
	if err := harborClient.EnsureProject(ctx, projectName); err != nil {
		return fmt.Errorf("ensuring Harbor project: %w", err)
	}

	// 2. Reconcile project members from workspace spec
	harborMembers := make([]harbor.ProjectMember, 0, len(w.Spec.Members))
	for _, m := range w.Spec.Members {
		harborMembers = append(harborMembers, harbor.ProjectMember{
			Username: m.Subject, // we use the subject to identify the harbor user
			RoleID:   harborRoleID(m.Role),
		})
	}
	if err := harborClient.ReconcileProjectMembers(ctx, projectName, harborMembers); err != nil {
		return fmt.Errorf("reconciling Harbor project members: %w", err)
	}

	// 3. Ensure robot account exists
	creds, created, err := harborClient.EnsureRobotAccount(ctx, projectName, robotName)
	if err != nil {
		return fmt.Errorf("ensuring Harbor robot account: %w", err)
	}

	// 3. Create docker-registry Secret only when robot was newly created
	if created {
		dockerConfigJSON, err := buildDockerConfigJSON(harborURL, creds.Name, creds.Secret)
		if err != nil {
			return fmt.Errorf("building docker config JSON: %w", err)
		}

		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      ImagePullSecretName,
				Namespace: w.Spec.Namespace,
			},
		}

		op, err := ctrlutil.CreateOrUpdate(ctx, c, secret, func() error {
			secret.Labels = LabelsForWorkspace(w.Name, version)
			secret.Type = corev1.SecretTypeDockerConfigJson
			secret.Data = map[string][]byte{
				corev1.DockerConfigJsonKey: dockerConfigJSON,
			}
			return ctrlutil.SetControllerReference(w, secret, scheme)
		})
		if err != nil {
			return fmt.Errorf("reconciling image pull Secret: %w", err)
		}
		if op != ctrlutil.OperationResultNone {
			logger.Info("Image pull Secret reconciled", "operation", op, "name", secret.Name)
		}
	} else {
		// Robot already exists — ensure the Secret exists (it should have been created previously).
		// If the Secret is missing (e.g. manually deleted), we cannot recreate it without
		// regenerating the robot credentials. Log a warning for observability.
		secret := &corev1.Secret{}
		if err := c.Get(ctx, types.NamespacedName{Name: ImagePullSecretName, Namespace: w.Spec.Namespace}, secret); err != nil {
			if client.IgnoreNotFound(err) != nil {
				return fmt.Errorf("checking image pull Secret: %w", err)
			}
			logger.Info("Image pull Secret missing but Harbor robot already exists, Secret cannot be recreated without regenerating robot credentials")
		}
	}

	// 4. Patch default ServiceAccount to include imagePullSecrets
	if err := ensureImagePullSecret(ctx, c, w.Spec.Namespace); err != nil {
		return fmt.Errorf("patching default ServiceAccount: %w", err)
	}

	return nil
}

// ensureImagePullSecret patches the default ServiceAccount to include the workspace image pull secret.
func ensureImagePullSecret(ctx context.Context, c client.Client, namespace string) error {
	sa := &corev1.ServiceAccount{}
	if err := c.Get(ctx, types.NamespacedName{Name: "default", Namespace: namespace}, sa); err != nil {
		return err
	}

	// Check if already present
	for _, ref := range sa.ImagePullSecrets {
		if ref.Name == ImagePullSecretName {
			return nil
		}
	}

	// Patch to add the image pull secret
	patch := client.MergeFrom(sa.DeepCopy())
	sa.ImagePullSecrets = append(sa.ImagePullSecrets, corev1.LocalObjectReference{
		Name: ImagePullSecretName,
	})
	if err := c.Patch(ctx, sa, patch); err != nil {
		return err
	}

	log.FromContext(ctx).Info("Default ServiceAccount patched with imagePullSecrets", "namespace", namespace)
	return nil
}

// harborRoleID maps a workspace member role to a Harbor project role ID.
func harborRoleID(role tenantv1alpha1.MemberRole) int {
	switch role {
	case tenantv1alpha1.MemberRoleAdmin:
		return harbor.RoleProjectAdmin
	case tenantv1alpha1.MemberRoleEdit:
		return harbor.RoleDeveloper
	case tenantv1alpha1.MemberRoleView:
		return harbor.RoleGuest
	default:
		return harbor.RoleGuest
	}
}

// buildDockerConfigJSON constructs the .dockerconfigjson content for a docker-registry secret.
func buildDockerConfigJSON(registryURL, username, password string) ([]byte, error) {
	auth := base64.StdEncoding.EncodeToString([]byte(username + ":" + password))
	config := map[string]any{
		"auths": map[string]any{
			registryURL: map[string]string{
				"username": username,
				"password": password,
				"auth":     auth,
			},
		},
	}
	return json.Marshal(config)
}
