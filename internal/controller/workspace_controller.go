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
	"cmp"
	"context"
	"errors"
	"slices"

	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	istioapisecurityv1 "istio.io/client-go/pkg/apis/security/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/events"

	tenantv1alpha1 "github.com/otterscale/api/tenant/v1alpha1"
	"github.com/otterscale/tenant-operator/internal/workspace"
)

// WorkspaceReconciler reconciles a Workspace object.
// It ensures that the underlying Namespace, RBAC roles, ResourceQuotas, and NetworkPolicies
// match the desired state defined in the Workspace CR.
//
// The controller is intentionally kept thin: it orchestrates the reconciliation flow,
// while the actual resource synchronization logic resides in internal/core/workspace/.
type WorkspaceReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Version  string
	Recorder events.EventRecorder

	istioDetector *IstioDetector
}

// RBAC Permissions required by the controller:
// +kubebuilder:rbac:groups=tenant.otterscale.io,resources=workspaces,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=tenant.otterscale.io,resources=workspaces/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch
// +kubebuilder:rbac:groups="",resources=namespaces;resourcequotas;limitranges,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apiextensions.k8s.io,resources=customresourcedefinitions,verbs=get;list;watch
// +kubebuilder:rbac:groups=networking.k8s.io,resources=networkpolicies,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=rolebindings,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=clusterroles,verbs=bind,resourceNames=admin;edit;view
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=clusterrolebindings,verbs=get;list;watch
// +kubebuilder:rbac:groups=security.istio.io,resources=authorizationpolicies;peerauthentications,verbs=get;list;watch;create;update;patch;delete

// Reconcile is the main loop for the controller.
// It implements the level-triggered reconciliation logic with a thin orchestration pattern:
// Fetch -> Domain Sync -> Status Update.
//
// Member-to-label synchronization is handled by the Mutating Webhook (WorkspaceCustomDefaulter),
// ensuring labels are always consistent before the object reaches etcd.
//
// Deletion is handled entirely by Kubernetes garbage collection: all child resources
// are created with OwnerReferences pointing to the Workspace, so they are automatically
// cascade-deleted when the Workspace is removed. No finalizer is needed.
func (r *WorkspaceReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithName(req.Name)
	ctx = log.IntoContext(ctx, logger)

	// 1. Fetch the Workspace instance
	var w tenantv1alpha1.Workspace
	if err := r.Get(ctx, req.NamespacedName, &w); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// 2. Reconcile all domain resources
	if err := r.reconcileResources(ctx, &w); err != nil {
		return r.handleReconcileError(ctx, &w, err)
	}

	// 3. Update Status (Reflect the observed state back to the user)
	if err := r.updateStatus(ctx, &w); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

// reconcileResources orchestrates the domain-level resource sync in order.
func (r *WorkspaceReconciler) reconcileResources(ctx context.Context, w *tenantv1alpha1.Workspace) error {
	istioEnabled := r.istioDetector.IsEnabled()

	if err := workspace.ReconcileNamespace(ctx, r.Client, r.Scheme, w, r.Version, istioEnabled); err != nil {
		return err
	}
	if err := workspace.ReconcileRoleBindings(ctx, r.Client, r.Scheme, w, r.Version); err != nil {
		return err
	}
	if err := workspace.ReconcileResourceQuota(ctx, r.Client, r.Scheme, w, r.Version); err != nil {
		return err
	}
	if err := workspace.ReconcileLimitRange(ctx, r.Client, r.Scheme, w, r.Version); err != nil {
		return err
	}
	return workspace.ReconcileNetworkIsolation(ctx, r.Client, r.Scheme, w, r.Version, istioEnabled)
}

// handleReconcileError categorizes errors and updates status accordingly.
// Permanent errors (e.g. namespace conflict) do NOT requeue to avoid infinite loops.
// Transient errors are returned to the controller-runtime for exponential backoff retry.
func (r *WorkspaceReconciler) handleReconcileError(ctx context.Context, w *tenantv1alpha1.Workspace, err error) (ctrl.Result, error) {
	var nce *workspace.NamespaceConflictError
	if errors.As(err, &nce) {
		// Permanent error: do not requeue, just update status
		r.setReadyConditionFalse(ctx, w, "NamespaceConflict", err.Error())
		r.Recorder.Eventf(w, nil, corev1.EventTypeWarning, "NamespaceConflict", "Reconcile", err.Error())
		return ctrl.Result{}, nil
	}

	// Transient error: update status and requeue
	r.setReadyConditionFalse(ctx, w, "ReconcileError", err.Error())
	r.Recorder.Eventf(w, nil, corev1.EventTypeWarning, "ReconcileError", "Reconcile", err.Error())
	return ctrl.Result{}, err
}

// setReadyConditionFalse updates the Ready condition to False via status patch.
// Errors are logged rather than propagated to avoid masking the original reconcile error.
func (r *WorkspaceReconciler) setReadyConditionFalse(ctx context.Context, w *tenantv1alpha1.Workspace, reason, message string) {
	logger := log.FromContext(ctx)

	patch := client.MergeFrom(w.DeepCopy())
	meta.SetStatusCondition(&w.Status.Conditions, metav1.Condition{
		Type:               workspace.ConditionTypeReady,
		Status:             metav1.ConditionFalse,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: w.Generation,
	})
	w.Status.ObservedGeneration = w.Generation

	if err := r.Status().Patch(ctx, w, patch); err != nil {
		logger.Error(err, "Failed to patch Ready=False status condition", "reason", reason)
	}
}

// SetupWithManager registers the controller with the Manager and defines watches.
//
// Istio detection is handled dynamically: the controller watches CustomResourceDefinition
// objects filtered to the security.istio.io group. When an Istio CRD is created or deleted,
// the IstioDetector refreshes its state and all Workspaces are re-enqueued so the
// reconciler can adapt (e.g. switch between NetworkPolicy and Istio AuthorizationPolicy).
//
// If Istio CRDs are already present at startup, the controller also registers Owns()
// watches for PeerAuthentication and AuthorizationPolicy to detect external drift.
func (r *WorkspaceReconciler) SetupWithManager(mgr ctrl.Manager) error {
	r.istioDetector = NewIstioDetector(mgr.GetConfig())

	b := ctrl.NewControllerManagedBy(mgr).
		For(&tenantv1alpha1.Workspace{},
			// Filter out status-only updates: only reconcile on spec changes (generation bump).
			// Label synchronization is handled by the mutating webhook, so we no longer need
			// to watch for label changes.
			builder.WithPredicates(predicate.GenerationChangedPredicate{}),
		).
		// Watch for changes in owned resources to trigger reconciliation
		Owns(&corev1.Namespace{}).
		Owns(&corev1.ResourceQuota{}).
		Owns(&corev1.LimitRange{}).
		Owns(&rbacv1.RoleBinding{}).
		Owns(&networkingv1.NetworkPolicy{}).
		// Watch CRD changes to detect Istio installation or removal at runtime.
		// When an Istio CRD event fires, mapCRDToWorkspaces refreshes the detector
		// and enqueues every Workspace for re-reconciliation.
		Watches(
			&apiextensionsv1.CustomResourceDefinition{},
			handler.EnqueueRequestsFromMapFunc(r.mapCRDToWorkspaces),
			builder.WithPredicates(istioCRDPredicate()),
		).
		Named("workspace")

	// If Istio is already available at startup, also watch owned Istio resources
	// so that external drift (e.g. manual deletion) is detected immediately.
	//
	// Known limitation: if Istio is installed *after* the operator starts,
	// these Owns() watches will NOT be registered because controller-runtime
	// does not support adding watches dynamically after the controller starts.
	// The CRD watcher above will still detect the installation and re-enqueue
	// all Workspaces to create the Istio resources, but subsequent external
	// drift on those Istio resources (e.g. someone manually deletes a
	// PeerAuthentication) will not trigger reconciliation until the operator
	// is restarted. Restarting the operator resolves this, as Istio CRDs will
	// then be present at startup and the Owns() watches will be registered.
	if r.istioDetector.IsEnabled() {
		b.Owns(&istioapisecurityv1.PeerAuthentication{})
		b.Owns(&istioapisecurityv1.AuthorizationPolicy{})
	}

	return b.Complete(r)
}

// mapCRDToWorkspaces is called when an Istio-related CRD is created or deleted.
// It refreshes the IstioDetector and enqueues all Workspace objects for re-reconciliation.
func (r *WorkspaceReconciler) mapCRDToWorkspaces(ctx context.Context, _ client.Object) []reconcile.Request {
	logger := log.FromContext(ctx).WithName("crd-watch")

	r.istioDetector.Refresh()
	logger.Info("Istio CRD change detected, re-enqueuing all Workspaces",
		"istioEnabled", r.istioDetector.IsEnabled())

	var workspaces tenantv1alpha1.WorkspaceList
	if err := r.List(ctx, &workspaces); err != nil {
		logger.Error(err, "Failed to list Workspaces for CRD change re-enqueue")
		return nil
	}

	requests := make([]reconcile.Request, len(workspaces.Items))
	for i, w := range workspaces.Items {
		requests[i] = reconcile.Request{
			NamespacedName: types.NamespacedName{Name: w.Name},
		}
	}
	return requests
}

// istioCRDPredicate filters CRD events to only those belonging to the security.istio.io group.
func istioCRDPredicate() predicate.Predicate {
	return predicate.NewPredicateFuncs(func(obj client.Object) bool {
		crd, ok := obj.(*apiextensionsv1.CustomResourceDefinition)
		if !ok {
			return false
		}
		return crd.Spec.Group == "security.istio.io"
	})
}

// updateStatus calculates the status based on the current observed state and patches the resource.
func (r *WorkspaceReconciler) updateStatus(ctx context.Context, w *tenantv1alpha1.Workspace) error {
	newStatus := w.Status.DeepCopy()
	newStatus.ObservedGeneration = w.Generation

	// Update Namespace reference
	newStatus.NamespaceRef = &tenantv1alpha1.ResourceReference{
		Name: w.Spec.Namespace,
	}

	// Update RoleBindings references (deterministic order to prevent status flapping)
	rolesInUse := make(map[tenantv1alpha1.MemberRole]bool)
	for _, m := range w.Spec.Members {
		rolesInUse[m.Role] = true
	}

	// Build refs in deterministic order: admin, edit, view
	orderedRoles := []tenantv1alpha1.MemberRole{
		tenantv1alpha1.MemberRoleAdmin,
		tenantv1alpha1.MemberRoleEdit,
		tenantv1alpha1.MemberRoleView,
	}
	newStatus.RoleBindingRefs = nil
	for _, role := range orderedRoles {
		if rolesInUse[role] {
			newStatus.RoleBindingRefs = append(newStatus.RoleBindingRefs, tenantv1alpha1.ResourceReference{
				Name:      workspace.RoleBindingName + "-" + string(role),
				Namespace: w.Spec.Namespace,
			})
		}
	}

	// Update ResourceQuota reference
	if w.Spec.ResourceQuota != nil {
		newStatus.ResourceQuotaRef = &tenantv1alpha1.ResourceReference{
			Name:      workspace.ResourceQuotaName,
			Namespace: w.Spec.Namespace,
		}
	} else {
		newStatus.ResourceQuotaRef = nil
	}

	// Update LimitRange reference
	if w.Spec.LimitRange != nil {
		newStatus.LimitRangeRef = &tenantv1alpha1.ResourceReference{
			Name:      workspace.LimitRangeName,
			Namespace: w.Spec.Namespace,
		}
	} else {
		newStatus.LimitRangeRef = nil
	}

	// Update Network Isolation resources
	if w.Spec.NetworkIsolation.Enabled {
		if r.istioDetector.IsEnabled() {
			newStatus.NetworkPolicyRef = nil
			newStatus.PeerAuthenticationRef = &tenantv1alpha1.ResourceReference{
				Name:      workspace.PeerAuthenticationName,
				Namespace: w.Spec.Namespace,
			}
			newStatus.AuthorizationPolicyRef = &tenantv1alpha1.ResourceReference{
				Name:      workspace.AuthorizationPolicyName,
				Namespace: w.Spec.Namespace,
			}
		} else {
			newStatus.NetworkPolicyRef = &tenantv1alpha1.ResourceReference{
				Name:      workspace.NetworkPolicyName,
				Namespace: w.Spec.Namespace,
			}
			newStatus.PeerAuthenticationRef = nil
			newStatus.AuthorizationPolicyRef = nil
		}
	} else {
		newStatus.NetworkPolicyRef = nil
		newStatus.PeerAuthenticationRef = nil
		newStatus.AuthorizationPolicyRef = nil
	}

	// Set Ready condition
	meta.SetStatusCondition(&newStatus.Conditions, metav1.Condition{
		Type:               workspace.ConditionTypeReady,
		Status:             metav1.ConditionTrue,
		Reason:             "Reconciled",
		Message:            "Workspace resources are successfully reconciled",
		ObservedGeneration: w.Generation,
	})

	// Sort conditions by type for stable ordering
	slices.SortFunc(newStatus.Conditions, func(a, b metav1.Condition) int {
		return cmp.Compare(a.Type, b.Type)
	})

	// Check for changes before making an API call to reduce load on the API server
	if !equality.Semantic.DeepEqual(w.Status, *newStatus) {
		patch := client.MergeFrom(w.DeepCopy())
		w.Status = *newStatus
		if err := r.Status().Patch(ctx, w, patch); err != nil {
			return err
		}
		log.FromContext(ctx).Info("Workspace status updated")
		r.Recorder.Eventf(w, nil, corev1.EventTypeNormal, "Reconciled", "Reconcile",
			"Workspace resources reconciled for namespace %s", w.Spec.Namespace)
	}

	return nil
}
