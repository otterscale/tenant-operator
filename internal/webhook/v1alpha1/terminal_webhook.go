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
	"fmt"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	consolev1alpha1 "github.com/otterscale/api/console/v1alpha1"
	"github.com/otterscale/tenant-operator/internal/terminal"

	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

// SetupTerminalWebhookWithManager registers the webhook for Terminal in the manager.
// operatorSA is the full service account identity of the controller-manager
// (e.g. "system:serviceaccount:otterscale-system:tenant-operator-controller-manager")
// used to exempt the operator's own reconciliation updates from Terminal-level authorization.
func SetupTerminalWebhookWithManager(mgr ctrl.Manager, operatorSA string) error {
	return ctrl.NewWebhookManagedBy(mgr, &consolev1alpha1.Terminal{}).
		WithDefaulter(&TerminalCustomDefaulter{}).
		WithValidator(&TerminalCustomValidator{
			OperatorSA: operatorSA,
			Reader:     mgr.GetClient(),
		}).
		Complete()
}

// +kubebuilder:webhook:path=/mutate-console-otterscale-io-v1alpha1-terminal,mutating=true,failurePolicy=fail,sideEffects=None,groups=console.otterscale.io,resources=terminals,verbs=create;update,versions=v1alpha1,name=mterminal-v1alpha1.kb.io,admissionReviewVersions=v1

// TerminalCustomDefaulter is responsible for setting default values on the Terminal resource
// during CREATE and UPDATE operations. It keeps the queryable SubjectLabel in sync with
// spec.subject.
//
// NOTE: The +kubebuilder:object:generate=false marker prevents controller-gen from generating DeepCopy methods,
// as it is used only for temporary operations and does not need to be deeply copied.
type TerminalCustomDefaulter struct{}

// Default implements admission.Defaulter[*consolev1alpha1.Terminal] so a webhook will be registered for the Kind Terminal.
// It ensures the "console.otterscale.io/subject" label always mirrors spec.subject, so Terminals
// can be looked up with a label selector (e.g. `kubectl get terminals -l console.otterscale.io/subject=...`).
func (d *TerminalCustomDefaulter) Default(ctx context.Context, t *consolev1alpha1.Terminal) error {
	log.FromContext(ctx).Info("Defaulting for Terminal", "name", t.GetName())

	labels := t.GetLabels()
	if labels == nil {
		labels = make(map[string]string)
	}
	labels[terminal.SubjectLabel] = t.Spec.Subject
	t.SetLabels(labels)

	return nil
}

// +kubebuilder:webhook:path=/validate-console-otterscale-io-v1alpha1-terminal,mutating=false,failurePolicy=fail,sideEffects=None,groups=console.otterscale.io,resources=terminals,verbs=create;update;delete,versions=v1alpha1,name=vterminal-v1alpha1.kb.io,admissionReviewVersions=v1

// TerminalCustomValidator enforces Terminal-level authorization on all
// mutating operations. RBAC alone lets any authenticated user call the
// terminals API at all (see the terminal-user Role/RoleBinding in
// config/console) — this validator closes the rest: a caller may only
// create, update, or delete a Terminal whose spec.subject matches their own
// identity.
//
// The authorization logic itself is kept in internal/terminal/ for
// testability; this validator is intentionally thin.
type TerminalCustomValidator struct {
	// OperatorSA is the full service account identity of the controller-manager.
	// It is injected at startup so the operator works regardless of the namespace it is deployed in.
	OperatorSA string
	// Reader is used to look up ClusterRoleBindings for privileged ClusterRole checks.
	Reader client.Reader
}

// ValidateCreate ensures the requesting user's identity matches the new
// Terminal's spec.subject. Privileged callers (system:masters, operator SA,
// cluster-admin) bypass the check.
func (v *TerminalCustomValidator) ValidateCreate(ctx context.Context, t *consolev1alpha1.Terminal) (admission.Warnings, error) {
	log.FromContext(ctx).Info("Validating Terminal creation", "name", t.GetName())

	req, err := admission.RequestFromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("unable to retrieve admission request from context: %w", err)
	}

	return nil, terminal.AuthorizeCreation(ctx, v.Reader, req.UserInfo, t, v.OperatorSA)
}

// ValidateUpdate ensures only the Terminal's own subject (or a privileged
// identity) can modify it. The check uses oldTerminal so a caller cannot
// grant themselves ownership and approve in the same request — though
// spec.subject is separately enforced immutable by the CRD itself.
func (v *TerminalCustomValidator) ValidateUpdate(ctx context.Context, oldTerminal, newTerminal *consolev1alpha1.Terminal) (admission.Warnings, error) {
	log.FromContext(ctx).Info("Validating Terminal update", "name", newTerminal.GetName())

	req, err := admission.RequestFromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("unable to retrieve admission request from context: %w", err)
	}

	return nil, terminal.AuthorizeModification(ctx, v.Reader, req.UserInfo, oldTerminal, v.OperatorSA)
}

// ValidateDelete ensures only the Terminal's own subject (or a privileged
// identity) can delete it.
func (v *TerminalCustomValidator) ValidateDelete(ctx context.Context, t *consolev1alpha1.Terminal) (admission.Warnings, error) {
	log.FromContext(ctx).Info("Validating Terminal deletion", "name", t.GetName())

	req, err := admission.RequestFromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("unable to retrieve admission request from context: %w", err)
	}

	return nil, terminal.AuthorizeModification(ctx, v.Reader, req.UserInfo, t, v.OperatorSA)
}
