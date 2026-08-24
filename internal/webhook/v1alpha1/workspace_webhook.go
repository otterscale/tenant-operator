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

package v1alpha1

import (
	"context"
	"crypto/rand"
	"fmt"
	"strings"

	utilrand "k8s.io/apimachinery/pkg/util/rand"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	tenantv1alpha1 "github.com/otterscale/tenant-operator/api/v1alpha1"
	"github.com/otterscale/tenant-operator/internal/workspace"

	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

// SetupWorkspaceWebhookWithManager registers the webhook for Workspace in the manager.
// operatorSA is the full service account identity of the controller-manager
// (e.g. "system:serviceaccount:otterscale-system:tenant-operator-controller-manager")
// used to exempt the operator's own reconciliation updates from workspace-level authorization.
func SetupWorkspaceWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr, &tenantv1alpha1.Workspace{}).
		WithDefaulter(&WorkspaceCustomDefaulter{}).
		WithValidator(&WorkspaceCustomValidator{
			Reader: mgr.GetClient(),
		}).
		Complete()
}

// +kubebuilder:webhook:path=/mutate-tenant-otterscale-io-v1alpha1-workspace,mutating=true,failurePolicy=fail,sideEffects=None,groups=tenant.otterscale.io,resources=workspaces,verbs=create;update,versions=v1alpha1,name=mworkspace-v1alpha1.kb.io,admissionReviewVersions=v1

// WorkspaceCustomDefaulter is responsible for setting default values on the Workspace resource
// during CREATE and UPDATE operations. It synchronizes member subjects as labels to enable
// external API label selectors (e.g., "find all workspaces a user belongs to").
//
// NOTE: The +kubebuilder:object:generate=false marker prevents controller-gen from generating DeepCopy methods,
// as it is used only for temporary operations and does not need to be deeply copied.
type WorkspaceCustomDefaulter struct{}

// Default implements admission.Defaulter[*tenantv1alpha1.Workspace] so a webhook will be registered for the Kind Workspace.
// It ensures that labels with the prefix "user.otterscale.io/" mirror the current member subjects,
// removing stale entries and preserving all other labels.
func (d *WorkspaceCustomDefaulter) Default(ctx context.Context, ws *tenantv1alpha1.Workspace) error {
	log.FromContext(ctx).Info("Defaulting for Workspace", "name", ws.GetName())

	if ws.Spec.Namespace == "" {
		ws.Spec.Namespace = generateNamespaceName()
	}

	defaultMemberLabels(ws)
	return nil
}

// generateNamespaceName produces a 6-character namespace name:
// 1 random lowercase letter [a-z] + 5 characters from the Kubernetes generateName charset.
func generateNamespaceName() string {
	var b [1]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("crypto/rand is unavailable: " + err.Error())
	}
	prefix := string(rune('a' + b[0]%26))
	return prefix + utilrand.String(5)
}

// defaultMemberLabels synchronizes member subjects as labels on the Workspace.
// Labels with the prefix "user.otterscale.io/" are managed; all other labels are preserved.
func defaultMemberLabels(ws *tenantv1alpha1.Workspace) {
	labels := ws.GetLabels()
	if labels == nil {
		labels = make(map[string]string)
	}

	// Build desired user labels from spec
	desired := make(map[string]struct{}, len(ws.Spec.Members))
	for _, m := range ws.Spec.Members {
		desired[workspace.UserLabelPrefix+m.Subject] = struct{}{}
	}

	// Remove stale user labels
	for k := range labels {
		if strings.HasPrefix(k, workspace.UserLabelPrefix) {
			if _, ok := desired[k]; !ok {
				delete(labels, k)
			}
		}
	}

	// Set desired user labels
	for k := range desired {
		labels[k] = "true"
	}

	ws.SetLabels(labels)
}

// +kubebuilder:webhook:path=/validate-tenant-otterscale-io-v1alpha1-workspace,mutating=false,failurePolicy=fail,sideEffects=None,groups=tenant.otterscale.io,resources=workspaces,verbs=create;update;delete,versions=v1alpha1,name=vworkspace-v1alpha1.kb.io,admissionReviewVersions=v1

// WorkspaceCustomValidator enforces workspace-level authorization on all
// mutating operations. Create requests require the caller to list themselves
// as an admin member. Update and delete requests require workspace-admin
// (or cluster-level privileged) identity.
//
// The authorization logic itself is kept in internal/workspace/ for
// testability; this validator is intentionally thin.
type WorkspaceCustomValidator struct {
	// OperatorSA is the full service account identity of the controller-manager.
	// It is injected at startup so the operator works regardless of the namespace it is deployed in.
	OperatorSA string
	// Reader is used to look up ClusterRoleBindings and existing Workspaces.
	Reader client.Reader
}

// ValidateCreate ensures the requesting user is listed as an admin member
// of the new Workspace. Privileged callers (system:masters, operator SA,
// cluster-admin) bypass the check.
func (v *WorkspaceCustomValidator) ValidateCreate(ctx context.Context, ws *tenantv1alpha1.Workspace) (admission.Warnings, error) {
	log.FromContext(ctx).Info("Validating Workspace creation", "name", ws.GetName())

	req, err := admission.RequestFromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("unable to retrieve admission request from context: %w", err)
	}

	if err := workspace.ValidateWorkspaceName(ws.Name); err != nil {
		return nil, err
	}

	if err := workspace.ValidateNamespaceUniqueness(ctx, v.Reader, ws); err != nil {
		return nil, err
	}

	return nil, workspace.AuthorizeCreation(ctx, v.Reader, req.UserInfo, ws, v.OperatorSA)
}

// ValidateUpdate ensures only workspace admins (or privileged identities) can
// modify an existing Workspace. The check uses oldObj so that a user cannot
// grant themselves admin and approve in the same request.
func (v *WorkspaceCustomValidator) ValidateUpdate(ctx context.Context, oldWorkspace, newWorkspace *tenantv1alpha1.Workspace) (admission.Warnings, error) {
	log.FromContext(ctx).Info("Validating Workspace update", "name", newWorkspace.GetName())

	req, err := admission.RequestFromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("unable to retrieve admission request from context: %w", err)
	}

	return nil, workspace.AuthorizeModification(ctx, v.Reader, req.UserInfo, oldWorkspace, v.OperatorSA)
}

// ValidateDelete ensures only workspace admins (or privileged identities) can
// delete a Workspace.
func (v *WorkspaceCustomValidator) ValidateDelete(ctx context.Context, ws *tenantv1alpha1.Workspace) (admission.Warnings, error) {
	log.FromContext(ctx).Info("Validating Workspace deletion", "name", ws.GetName())

	req, err := admission.RequestFromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("unable to retrieve admission request from context: %w", err)
	}

	return nil, workspace.AuthorizeModification(ctx, v.Reader, req.UserInfo, ws, v.OperatorSA)
}
