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

package controller

import (
	"context"
	"errors"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	consolev1alpha1 "github.com/otterscale/api/console/v1alpha1"
	"github.com/otterscale/tenant-operator/internal/terminal"
)

// TerminalReconciler reconciles a Terminal object.
// It ensures that a Terminal's Pod exists (and self-heals it if it
// terminated), and garbage-collects Terminals that have been idle for longer
// than their effective idle timeout.
//
// The controller is intentionally kept thin: it orchestrates the
// reconciliation flow, while the actual Pod spec and authorization logic
// reside in internal/terminal/.
type TerminalReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder events.EventRecorder
}

// ConditionTypeReady is the condition type that indicates whether a
// Terminal's Pod has been successfully reconciled and is ready.
const ConditionTypeReady = "Ready"

// RBAC Permissions required by the controller.
//
// These are cluster-scoped ClusterRole rules rather than a Role scoped to
// the console namespace via a `namespace=` marker: controller-gen would
// still emit that as a namespaced Role, but this operator's config/default
// kustomization applies a blanket `namespace: otterscale-system` transform
// to every namespaced resource it bundles — including ones that already
// have a different, explicit namespace set — which would silently relocate
// such a Role out of "console". Cluster-wide is also the existing pattern
// this operator already uses for Workspace's per-namespace child resources,
// and the operator's own ServiceAccount identity is fully trusted regardless
// of scope; the actual security boundary that matters is that regular users
// get no RoleBinding at all for Pods in the console namespace (see
// config/console), which this doesn't change either way.
// +kubebuilder:rbac:groups=console.otterscale.io,resources=terminals,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=console.otterscale.io,resources=terminals/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch

// Reconcile is the main loop for the controller.
//
// Deletion is handled entirely by Kubernetes garbage collection: the Pod is
// created with an OwnerReference pointing to the Terminal (both live in the
// same namespace), so it is automatically cascade-deleted when the Terminal
// is removed. No finalizer is needed.
func (r *TerminalReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithName(req.Name)
	ctx = log.IntoContext(ctx, logger)

	var t consolev1alpha1.Terminal
	if err := r.Get(ctx, req.NamespacedName, &t); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// 1. Idle check first: if this Terminal is already past its idle
	// timeout, delete it outright rather than bothering to reconcile (and
	// possibly just-create) a Pod we're about to tear down anyway.
	elapsed := time.Since(lastActivity(&t))
	timeout := idleTimeout(&t)
	if elapsed >= timeout {
		if err := r.Delete(ctx, &t); err != nil {
			return ctrl.Result{}, client.IgnoreNotFound(err)
		}
		r.Recorder.Eventf(&t, nil, corev1.EventTypeNormal, "IdleTimeout", "Reconcile",
			"Terminal deleted after %s of inactivity", elapsed.Round(time.Second))
		return ctrl.Result{}, nil
	}

	// 2. Reconcile the Pod.
	pod, err := terminal.ReconcilePod(ctx, r.Client, r.Scheme, &t)
	var collision *terminal.ErrSubjectCollision
	switch {
	case errors.As(err, &collision):
		return ctrl.Result{}, r.setFailed(ctx, &t, "SubjectCollision", err.Error())
	case err != nil:
		_ = r.setFailed(ctx, &t, "ReconcileError", err.Error())
		r.Recorder.Eventf(&t, nil, corev1.EventTypeWarning, "ReconcileError", "Reconcile", err.Error())
		return ctrl.Result{}, err
	}

	// 3. Reflect the observed Pod state back to the user.
	if err := r.updateStatus(ctx, &t, pod); err != nil {
		return ctrl.Result{}, err
	}

	// 4. Requeue exactly when this Terminal would become idle, so idle-GC
	// fires promptly without polling. A backend patching status.lastActivityTime
	// won't itself trigger a reconcile (see the GenerationChangedPredicate
	// note on SetupWithManager below) — this scheduled wake-up is what
	// re-reads the latest value and self-corrects.
	return ctrl.Result{RequeueAfter: timeout - elapsed}, nil
}

// idleTimeout returns the effective idle timeout for t: its own
// spec.idleTimeoutSeconds when set, otherwise terminal.DefaultIdleTimeout.
func idleTimeout(t *consolev1alpha1.Terminal) time.Duration {
	if t.Spec.IdleTimeoutSeconds > 0 {
		return time.Duration(t.Spec.IdleTimeoutSeconds) * time.Second
	}
	return terminal.DefaultIdleTimeout
}

// lastActivity returns t.Status.LastActivityTime, falling back to
// t.CreationTimestamp when it was never patched — otherwise a brand new
// Terminal with no recorded activity yet would immediately look idle.
func lastActivity(t *consolev1alpha1.Terminal) time.Time {
	if t.Status.LastActivityTime != nil {
		return t.Status.LastActivityTime.Time
	}
	return t.CreationTimestamp.Time
}

// setFailed patches status.phase to Failed with the given reason/message via
// a status subresource patch.
func (r *TerminalReconciler) setFailed(ctx context.Context, t *consolev1alpha1.Terminal, reason, message string) error {
	patch := client.MergeFrom(t.DeepCopy())
	t.Status.Phase = "Failed"
	t.Status.ObservedGeneration = t.Generation
	meta.SetStatusCondition(&t.Status.Conditions, metav1.Condition{
		Type:               ConditionTypeReady,
		Status:             metav1.ConditionFalse,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: t.Generation,
	})
	return r.Status().Patch(ctx, t, patch)
}

// updateStatus calculates status based on the observed Pod and patches it.
// A nil pod means the Pod was just created (or a terminated one was just
// deleted for recreation) — the caller hasn't observed it as Ready yet.
func (r *TerminalReconciler) updateStatus(ctx context.Context, t *consolev1alpha1.Terminal, pod *corev1.Pod) error {
	newStatus := t.Status.DeepCopy()
	newStatus.ObservedGeneration = t.Generation
	newStatus.PodName = terminal.PodName(t.Spec.Subject)

	readyCondition := metav1.Condition{
		Type:               ConditionTypeReady,
		ObservedGeneration: t.Generation,
	}
	switch {
	case pod == nil:
		newStatus.Phase = "Creating"
		newStatus.PodReady = false
		readyCondition.Status = metav1.ConditionFalse
		readyCondition.Reason = "Creating"
		readyCondition.Message = "Pod is being created"
	case isPodReady(pod):
		newStatus.Phase = "Ready"
		newStatus.PodReady = true
		readyCondition.Status = metav1.ConditionTrue
		readyCondition.Reason = "PodReady"
		readyCondition.Message = "Pod is ready"
	default:
		newStatus.Phase = "Pending"
		newStatus.PodReady = false
		readyCondition.Status = metav1.ConditionFalse
		readyCondition.Reason = "PodNotReady"
		readyCondition.Message = "Waiting for Pod to become ready"
	}
	meta.SetStatusCondition(&newStatus.Conditions, readyCondition)

	if equality.Semantic.DeepEqual(t.Status, *newStatus) {
		return nil
	}

	patch := client.MergeFrom(t.DeepCopy())
	t.Status = *newStatus
	if err := r.Status().Patch(ctx, t, patch); err != nil {
		return err
	}
	log.FromContext(ctx).Info("Terminal status updated", "phase", t.Status.Phase)
	return nil
}

func isPodReady(pod *corev1.Pod) bool {
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

// SetupWithManager registers the controller with the Manager.
//
// GenerationChangedPredicate matters here: the caller (not this controller)
// frequently PATCHes status.lastActivityTime — potentially on every
// keystroke — and status-only updates don't bump metadata.generation, so
// this predicate filters those out and keeps an active session from
// flooding the controller with reconciles. Idle-GC accuracy isn't affected:
// the RequeueAfter scheduled by Reconcile always re-reads the latest
// lastActivityTime when it fires.
func (r *TerminalReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&consolev1alpha1.Terminal{},
			builder.WithPredicates(predicate.GenerationChangedPredicate{}),
		).
		Owns(&corev1.Pod{}).
		Named("terminal").
		Complete(r)
}
