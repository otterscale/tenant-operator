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

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	utilrand "k8s.io/apimachinery/pkg/util/rand"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	tenantv1alpha1 "github.com/otterscale/tenant-operator/api/v1alpha1"
	"github.com/otterscale/tenant-operator/internal/workspace"

	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

// SetupWorkspaceWebhookWithManager registers the Workspace webhook.
//
// Both hooks read through mgr.GetAPIReader() rather than the cached client: a
// cache lagging the API server by a moment would admit the very name conflicts
// these checks exist to prevent, and workspaces are few enough that the extra
// round trip costs nothing.
func SetupWorkspaceWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr, &tenantv1alpha1.Workspace{}).
		WithDefaulter(&WorkspaceCustomDefaulter{
			Reader: mgr.GetAPIReader(),
		}).
		WithValidator(&WorkspaceCustomValidator{
			Reader: mgr.GetAPIReader(),
			Client: mgr.GetClient(),
		}).
		Complete()
}

// +kubebuilder:webhook:path=/mutate-tenant-otterscale-io-v1alpha1-workspace,mutating=true,failurePolicy=fail,sideEffects=None,groups=tenant.otterscale.io,resources=workspaces,verbs=create;update,versions=v1alpha1,name=mworkspace-v1alpha1.kb.io,admissionReviewVersions=v1

// WorkspaceCustomDefaulter sets default values on CREATE and UPDATE. It mirrors
// member subjects as labels so external APIs can select on them (e.g. "find all
// workspaces a user belongs to").
type WorkspaceCustomDefaulter struct {
	// Reader looks up existing Namespaces so a generated name is not handed out
	// on top of one. Must be uncached — see SetupWorkspaceWebhookWithManager.
	Reader client.Reader
}

// Default fills in a generated namespace when none was given, and makes the
// "user.otterscale.io/" labels mirror the current member subjects.
func (d *WorkspaceCustomDefaulter) Default(ctx context.Context, ws *tenantv1alpha1.Workspace) error {
	log.FromContext(ctx).Info("Defaulting for Workspace", "name", ws.GetName())

	if ws.Spec.Namespace == "" {
		name, err := d.generateAvailableNamespaceName(ctx)
		if err != nil {
			return err
		}
		ws.Spec.Namespace = name
	}

	defaultMemberLabels(ws)
	return nil
}

// namespaceNameAttempts bounds the search for a free generated name. The name
// space is large enough that the first candidate almost always wins.
const namespaceNameAttempts = 5

// generateAvailableNamespaceName returns a generated name no Namespace holds yet.
//
// A collision would produce a Workspace that can never reconcile and can never
// be fixed, since spec.namespace is immutable once admitted. See
// workspace.ValidateNamespaceAvailable.
func (d *WorkspaceCustomDefaulter) generateAvailableNamespaceName(ctx context.Context) (string, error) {
	for range namespaceNameAttempts {
		name := generateNamespaceName()
		err := d.Reader.Get(ctx, client.ObjectKey{Name: name}, &corev1.Namespace{})
		if apierrors.IsNotFound(err) {
			return name, nil
		}
		if err != nil {
			return "", fmt.Errorf("checking generated namespace name %q: %w", name, err)
		}
	}
	return "", fmt.Errorf("could not generate an unused namespace name in %d attempts", namespaceNameAttempts)
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

// defaultMemberLabels mirrors member subjects as "user.otterscale.io/" labels.
// Labels outside that prefix are preserved untouched.
func defaultMemberLabels(ws *tenantv1alpha1.Workspace) {
	labels := ws.GetLabels()
	if labels == nil {
		labels = make(map[string]string)
	}

	desired := make(map[string]struct{}, len(ws.Spec.Members))
	for _, m := range ws.Spec.Members {
		desired[workspace.UserLabelPrefix+m.Subject] = struct{}{}
	}

	for k := range labels {
		if strings.HasPrefix(k, workspace.UserLabelPrefix) {
			if _, ok := desired[k]; !ok {
				delete(labels, k)
			}
		}
	}

	for k := range desired {
		labels[k] = "true"
	}

	ws.SetLabels(labels)
}

// +kubebuilder:webhook:path=/validate-tenant-otterscale-io-v1alpha1-workspace,mutating=false,failurePolicy=fail,sideEffects=None,groups=tenant.otterscale.io,resources=workspaces,verbs=create;update;delete,versions=v1alpha1,name=vworkspace-v1alpha1.kb.io,admissionReviewVersions=v1

// WorkspaceCustomValidator enforces workspace-level authorization on mutating
// operations: create requires the caller to list themselves as an admin member,
// update and delete require workspace-admin or cluster-privileged identity.
//
// The authorization logic itself lives in internal/workspace/ for testability.
type WorkspaceCustomValidator struct {
	// Reader looks up existing Workspaces and Namespaces to check that the
	// target namespace can still be claimed. Must be uncached — see
	// SetupWorkspaceWebhookWithManager.
	Reader client.Reader

	// Client submits the SubjectAccessReview backing the cluster-wide access
	// check. Writes never go through the cache, so the manager's client is fine.
	Client client.Client
}

// ValidateCreate ensures the requesting user is listed as an admin member of the
// new Workspace. Privileged callers bypass the check.
func (v *WorkspaceCustomValidator) ValidateCreate(ctx context.Context, ws *tenantv1alpha1.Workspace) (admission.Warnings, error) {
	log.FromContext(ctx).Info("Validating Workspace creation", "name", ws.GetName())

	req, err := admission.RequestFromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("unable to retrieve admission request from context: %w", err)
	}

	if err := workspace.ValidateWorkspaceName(ws.Name); err != nil {
		return nil, err
	}

	// Uniqueness first: when another Workspace already claims the namespace, its
	// name is the more useful thing to put in front of the caller.
	if err := workspace.ValidateNamespaceUniqueness(ctx, v.Reader, ws); err != nil {
		return nil, err
	}

	if err := workspace.ValidateNamespaceAvailable(ctx, v.Reader, ws); err != nil {
		return nil, err
	}

	return nil, workspace.AuthorizeCreation(ctx, v.Client, req.UserInfo, ws)
}

// ValidateUpdate ensures only workspace admins (or privileged identities) can
// modify an existing Workspace. It checks the old object, so a user cannot grant
// themselves admin and approve it in the same request.
func (v *WorkspaceCustomValidator) ValidateUpdate(ctx context.Context, oldWorkspace, newWorkspace *tenantv1alpha1.Workspace) (admission.Warnings, error) {
	log.FromContext(ctx).Info("Validating Workspace update", "name", newWorkspace.GetName())

	req, err := admission.RequestFromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("unable to retrieve admission request from context: %w", err)
	}

	return nil, workspace.AuthorizeModification(ctx, v.Client, req.UserInfo, oldWorkspace)
}

// ValidateDelete ensures only workspace admins (or privileged identities) can
// delete a Workspace.
func (v *WorkspaceCustomValidator) ValidateDelete(ctx context.Context, ws *tenantv1alpha1.Workspace) (admission.Warnings, error) {
	log.FromContext(ctx).Info("Validating Workspace deletion", "name", ws.GetName())

	req, err := admission.RequestFromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("unable to retrieve admission request from context: %w", err)
	}

	return nil, workspace.AuthorizeModification(ctx, v.Client, req.UserInfo, ws)
}
