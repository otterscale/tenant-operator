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
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlutil "sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	tenantv1alpha1 "github.com/otterscale/tenant-operator/api/v1alpha1"
	"github.com/otterscale/tenant-operator/internal/harbor"
)

const (
	// OperatorSecretName is the operator-wide Secret, in OperatorNamespace,
	// holding the Harbor admin robot credentials. It is read on every reconcile,
	// so rotating the credentials needs no restart.
	//
	// Unlike the optional tenant-operator-config ConfigMap, this Secret is a
	// deployment prerequisite: every workspace gets a Harbor project, so there is
	// no "Harbor disabled" mode to fall back to.
	OperatorSecretName = "tenant-operator-secret"

	// HarborURLKey, HarborRobotNameKey and HarborRobotSecretKey are the
	// OperatorSecretName keys carrying those credentials.
	HarborURLKey         = "HARBOR_URL"
	HarborRobotNameKey   = "HARBOR_ROBOT_NAME"
	HarborRobotSecretKey = "HARBOR_ROBOT_SECRET"
)

// HarborCredentials are the Harbor admin robot credentials the operator uses to
// provision per-workspace projects and robot accounts.
type HarborCredentials struct {
	URL         string
	RobotName   string
	RobotSecret string
}

// OperatorHarborCredentials returns the Harbor credentials held by the
// operator-wide tenant-operator-secret.
//
// The Secret is a prerequisite, not an opt-in, so both a missing Secret and one
// short of a key are errors: reconcile reports Ready=False and retries, and the
// watch on the Secret picks the credentials up as soon as they are in place.
// Degrading to a half-configured Harbor client would instead leave workspaces
// silently without registry access.
func OperatorHarborCredentials(ctx context.Context, c client.Client) (*HarborCredentials, error) {
	key := types.NamespacedName{Name: OperatorSecretName, Namespace: OperatorNamespace}

	var secret corev1.Secret
	if err := c.Get(ctx, key, &secret); err != nil {
		return nil, fmt.Errorf("reading Harbor credentials from Secret %s: %w", key, err)
	}

	creds := &HarborCredentials{
		URL:         string(secret.Data[HarborURLKey]),
		RobotName:   string(secret.Data[HarborRobotNameKey]),
		RobotSecret: string(secret.Data[HarborRobotSecretKey]),
	}

	var missing []string
	for _, field := range []struct {
		key   string
		value string
	}{
		{HarborURLKey, creds.URL},
		{HarborRobotNameKey, creds.RobotName},
		{HarborRobotSecretKey, creds.RobotSecret},
	} {
		if field.value == "" {
			missing = append(missing, field.key)
		}
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("secret %s is missing required keys: %s", key, strings.Join(missing, ", "))
	}

	return creds, nil
}

// IsOperatorSecret reports whether obj is the operator-wide Harbor credentials
// Secret.
func IsOperatorSecret(obj client.Object) bool {
	if obj == nil {
		return false
	}
	return obj.GetName() == OperatorSecretName &&
		obj.GetNamespace() == OperatorNamespace
}

// ReconcileHarbor ensures the Harbor project, robot account, docker-registry
// Secret and default ServiceAccount imagePullSecrets are configured.
//
// Returns the members whose Harbor user accounts do not exist yet. The rest of
// reconcile completes normally; the caller requeues so they get added once
// Harbor provisions them.
func ReconcileHarbor(
	ctx context.Context,
	c client.Client,
	scheme *runtime.Scheme,
	w *tenantv1alpha1.Workspace,
	version string,
	harborClient harbor.Client,
	harborURL string,
) ([]string, error) {
	logger := log.FromContext(ctx)
	projectName := w.Spec.Namespace
	robotName := w.Spec.Namespace

	if err := harborClient.EnsureProject(ctx, projectName); err != nil {
		return nil, fmt.Errorf("ensuring Harbor project: %w", err)
	}

	// Service accounts are excluded — they have no Harbor user identity.
	harborMembers := make([]harbor.ProjectMember, 0, len(w.Spec.Members))
	for _, m := range w.Spec.Members {
		if m.ServiceAccount {
			continue
		}
		harborMembers = append(harborMembers, harbor.ProjectMember{
			// Harbor federates against the same OIDC provider as the cluster, so
			// the RBAC subject is already the name Harbor knows the user by.
			Username: m.Subject,
			RoleID:   harborRoleID(m.Role),
		})
	}
	missingMembers, err := harborClient.ReconcileProjectMembers(ctx, projectName, harborMembers)
	if err != nil {
		return nil, fmt.Errorf("reconciling Harbor project members: %w", err)
	}
	if len(missingMembers) > 0 {
		logger.Info("Some Harbor users do not exist yet; skipping and will retry on next reconcile", "missing", missingMembers)
	}

	creds, err := harborClient.EnsureRobot(ctx, projectName, robotName)
	if err != nil {
		return nil, fmt.Errorf("ensuring Harbor robot account: %w", err)
	}

	// A robot that was already there yields no credentials — Harbor reveals a
	// secret only at the moment it sets one. That is fine while the image pull
	// Secret written at creation is still present; when it is not, a new secret
	// is the only way back.
	//
	// The gap is not just manual deletion: the robot is created before the Secret
	// is written, so any failure in between (a conflict, a restart) leaves exactly
	// this state, and without the refresh the workspace could never pull again.
	if creds == nil {
		missing, err := imagePullSecretMissing(ctx, c, w.Spec.Namespace)
		if err != nil {
			return nil, err
		}
		if missing {
			logger.Info("Image pull Secret is missing; refreshing the Harbor robot secret to rebuild it",
				"robot", robotName)
			creds, err = harborClient.RefreshRobotSecret(ctx, projectName, robotName)
			if err != nil {
				return nil, fmt.Errorf("refreshing Harbor robot secret: %w", err)
			}
		}
	}

	// Write the Secret whenever this reconcile obtained credentials. A failure
	// here leaves the Secret missing and the next reconcile refreshes again, so
	// the loop converges instead of getting stuck.
	if creds != nil {
		if err := reconcileImagePullSecret(ctx, c, scheme, w, version, harborURL, creds); err != nil {
			return nil, err
		}
	}

	if err := ensureImagePullSecret(ctx, c, w.Spec.Namespace); err != nil {
		return nil, err
	}

	return missingMembers, nil
}

// imagePullSecretMissing reports whether the workspace's image pull Secret is
// absent. A read failure is returned rather than reported as missing: the
// refresh it would trigger invalidates the live secret.
func imagePullSecretMissing(ctx context.Context, c client.Client, namespace string) (bool, error) {
	key := types.NamespacedName{Name: ImagePullSecretName, Namespace: namespace}
	err := c.Get(ctx, key, &corev1.Secret{})
	if apierrors.IsNotFound(err) {
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("checking image pull Secret: %w", err)
	}
	return false, nil
}

// reconcileImagePullSecret writes the docker-registry Secret the workspace's
// pods and its Flux HelmRepository authenticate to Harbor with.
func reconcileImagePullSecret(
	ctx context.Context,
	c client.Client,
	scheme *runtime.Scheme,
	w *tenantv1alpha1.Workspace,
	version string,
	harborURL string,
	creds *harbor.RobotCredentials,
) error {
	dockerConfigJSON, err := buildDockerConfigJSON(harborURL, creds.Name, creds.Secret)
	if err != nil {
		return fmt.Errorf("building docker config JSON: %w", err)
	}

	secret := &corev1.Secret{
		Name:      ImagePullSecretName,
		Namespace: w.Spec.Namespace,
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
		log.FromContext(ctx).Info("Image pull Secret reconciled", "operation", op, "name", secret.Name)
	}
	return nil
}

// defaultServiceAccountName is the ServiceAccount Kubernetes assigns to pods
// that do not name one, so listing the image pull Secret on it makes the
// workspace registry reachable by default.
const defaultServiceAccountName = "default"

// ensureImagePullSecret lists the workspace image pull Secret on the namespace's
// default ServiceAccount.
//
// Kubernetes creates that ServiceAccount asynchronously, so on a freshly created
// namespace it may not exist yet. Creating it here is safe — Kubernetes will not
// add a second one. No owner reference is set: the ServiceAccount belongs to the
// namespace, not to us.
func ensureImagePullSecret(ctx context.Context, c client.Client, namespace string) error {
	sa := &corev1.ServiceAccount{
		Name:      defaultServiceAccountName,
		Namespace: namespace,
	}

	op, err := ctrlutil.CreateOrUpdate(ctx, c, sa, func() error {
		for _, ref := range sa.ImagePullSecrets {
			if ref.Name == ImagePullSecretName {
				return nil
			}
		}
		sa.ImagePullSecrets = append(sa.ImagePullSecrets, corev1.LocalObjectReference{
			Name: ImagePullSecretName,
		})
		return nil
	})
	if err != nil {
		return fmt.Errorf("reconciling default ServiceAccount: %w", err)
	}
	if op != ctrlutil.OperationResultNone {
		log.FromContext(ctx).Info("Default ServiceAccount reconciled with imagePullSecrets",
			"namespace", namespace, "operation", op)
	}
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

// buildDockerConfigJSON builds the .dockerconfigjson content for a
// docker-registry Secret.
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
