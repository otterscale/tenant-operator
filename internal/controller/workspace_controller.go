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
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/events"

	tenantv1alpha1 "github.com/otterscale/tenant-operator/api/v1alpha1"
	"github.com/otterscale/tenant-operator/internal/harbor"
	"github.com/otterscale/tenant-operator/internal/workspace"
)

// WorkspaceReconciler reconciles a Workspace object.
//
// The controller is intentionally thin: it orchestrates the reconciliation flow,
// while the resource synchronization logic lives in internal/workspace/.
type WorkspaceReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Version  string
	Recorder events.EventRecorder

	// NewHarborClient defaults to harbor.NewClient when unset; tests substitute a
	// fake so reconciliation can run without a Harbor server.
	NewHarborClient func(baseURL, username, password string) harbor.Client
}

func (r *WorkspaceReconciler) harborClient(creds *workspace.HarborCredentials) harbor.Client {
	newClient := r.NewHarborClient
	if newClient == nil {
		newClient = harbor.NewClient
	}
	return newClient(creds.URL, creds.RobotName, creds.RobotSecret)
}

// +kubebuilder:rbac:groups=tenant.otterscale.io,resources=workspaces,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=tenant.otterscale.io,resources=workspaces/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch
// +kubebuilder:rbac:groups="",resources=namespaces;resourcequotas;limitranges,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=networking.k8s.io,resources=networkpolicies,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=rolebindings,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=clusterroles,verbs=bind,resourceNames=admin;edit;view
// The webhook asks the cluster authorizer about cluster-wide access instead of
// reading cluster-wide RBAC itself:
// +kubebuilder:rbac:groups=authorization.k8s.io,resources=subjectaccessreviews,verbs=create
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=serviceaccounts,verbs=get;list;watch;create;update;patch
// The only ConfigMap the operator reads is its own tenant-operator-config; it
// writes none, so no create/update/delete here:
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch
// +kubebuilder:rbac:groups=source.toolkit.fluxcd.io,resources=helmrepositories,verbs=get;list;watch;create;update;patch;delete

// missingHarborMembersRequeue bounds how long a member waits to be added after
// Harbor provisions their user on first OIDC login.
const missingHarborMembersRequeue = 5 * time.Minute

// Reconcile runs the level-triggered flow: Fetch -> Domain Sync -> Status Update.
//
// Member-to-label synchronization is handled by the mutating webhook
// (WorkspaceCustomDefaulter), so labels are consistent before the object reaches etcd.
//
// Deletion needs no finalizer: every child resource carries an OwnerReference to
// the Workspace, so Kubernetes garbage collection cascades.
func (r *WorkspaceReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithName(req.Name)
	ctx = log.IntoContext(ctx, logger)

	var w tenantv1alpha1.Workspace
	if err := r.Get(ctx, req.NamespacedName, &w); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	missingHarborMembers, err := r.reconcileResources(ctx, &w)
	if err != nil {
		return r.handleReconcileError(ctx, &w, err)
	}

	if err := r.updateStatus(ctx, &w, missingHarborMembers); err != nil {
		return ctrl.Result{}, err
	}

	// There is no watch on Harbor users, so a timed requeue is the only way
	// pending members get picked up without a spec change.
	if len(missingHarborMembers) > 0 {
		return ctrl.Result{RequeueAfter: missingHarborMembersRequeue}, nil
	}

	return ctrl.Result{}, nil
}

// reconcileResources orchestrates the domain-level resource sync in order.
// Returns the members whose Harbor user accounts do not yet exist.
func (r *WorkspaceReconciler) reconcileResources(ctx context.Context, w *tenantv1alpha1.Workspace) ([]string, error) {
	if err := workspace.ReconcileNamespace(ctx, r.Client, r.Scheme, w, r.Version); err != nil {
		return nil, err
	}
	// Read per reconcile rather than at startup, so rotating the credentials
	// takes effect without a restart.
	creds, err := workspace.OperatorHarborCredentials(ctx, r.Client)
	if err != nil {
		return nil, err
	}
	missingHarborMembers, err := workspace.ReconcileHarbor(
		ctx, r.Client, r.Scheme, w, r.Version, r.harborClient(creds), creds.URL)
	if err != nil {
		return nil, err
	}
	if err := workspace.ReconcileHelmRepository(ctx, r.Client, r.Scheme, w, r.Version, creds.URL); err != nil {
		return nil, err
	}

	if err := workspace.ReconcileRoleBindings(ctx, r.Client, r.Scheme, w, r.Version); err != nil {
		return nil, err
	}
	if err := workspace.ReconcileServiceAccount(ctx, r.Client, r.Scheme, w, r.Version); err != nil {
		return nil, err
	}
	if err := workspace.ReconcileResourceQuota(ctx, r.Client, r.Scheme, w, r.Version); err != nil {
		return nil, err
	}
	if err := workspace.ReconcileLimitRange(ctx, r.Client, r.Scheme, w, r.Version); err != nil {
		return nil, err
	}
	if err := workspace.ReconcileNetworkIsolation(ctx, r.Client, r.Scheme, w, r.Version); err != nil {
		return nil, err
	}
	return missingHarborMembers, nil
}

// handleReconcileError categorizes errors and updates status accordingly.
// Permanent errors (e.g. namespace conflict) do not requeue, unless the status
// patch itself fails. Transient errors go back to controller-runtime for backoff.
func (r *WorkspaceReconciler) handleReconcileError(ctx context.Context, w *tenantv1alpha1.Workspace, err error) (ctrl.Result, error) {
	if _, ok := errors.AsType[*workspace.NamespaceConflictError](err); ok {
		patchErr := r.setReadyConditionFalse(ctx, w, "NamespaceConflict", err.Error())
		r.eventf(w, corev1.EventTypeWarning, "NamespaceConflict", err.Error())
		if patchErr != nil {
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}
		return ctrl.Result{}, nil
	}

	// A write conflict is the API server's optimistic lock, not a workspace
	// problem, and the fan-out watches make it routine. Hand it straight back for
	// the rate-limited retry without touching status or emitting an event:
	// Ready=False for a collision the next attempt resolves teaches users to
	// ignore both.
	if apierrors.IsConflict(err) {
		log.FromContext(ctx).V(1).Info("Retrying after a conflicting write", "error", err.Error())
		return ctrl.Result{}, err
	}

	// Transient: surface it on the status, then hand the original error back for
	// backoff. A failed status patch is dropped deliberately — a retry is already
	// coming, and reporting the patch error would lose the real failure.
	_ = r.setReadyConditionFalse(ctx, w, "ReconcileError", err.Error())
	r.eventf(w, corev1.EventTypeWarning, "ReconcileError", err.Error())
	return ctrl.Result{}, err
}

// eventf records an event whose note is already assembled.
//
// EventRecorder.Eventf treats note as a format string, so an error text passed
// straight through would render its %-verbs as %!s(MISSING) — and Harbor error
// notes carry percent-encoded response bodies. Hence the %s placeholder.
func (r *WorkspaceReconciler) eventf(w *tenantv1alpha1.Workspace, eventType, reason, message string) {
	r.Recorder.Eventf(w, nil, eventType, reason, "Reconcile", "%s", message)
}

// observedRef returns a reference to name/namespace only once that object is
// present, so the status never advertises a resource that is not there.
//
// obj is only a typed container for the lookup. A missing object yields a nil
// reference; any other API failure is returned so the caller retries rather than
// clearing the reference on a transient error.
func (r *WorkspaceReconciler) observedRef(
	ctx context.Context,
	obj client.Object,
	name, namespace string,
) (*tenantv1alpha1.ResourceReference, error) {
	if err := r.Get(ctx, client.ObjectKey{Name: name, Namespace: namespace}, obj); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("observing %s/%s: %w", namespace, name, err)
	}
	return &tenantv1alpha1.ResourceReference{Name: name, Namespace: namespace}, nil
}

// setReadyConditionFalse patches the Ready condition to False, returning the
// patch error so callers can decide whether to retry.
func (r *WorkspaceReconciler) setReadyConditionFalse(ctx context.Context, w *tenantv1alpha1.Workspace, reason, message string) error {
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
		log.FromContext(ctx).Error(err, "Failed to patch Ready=False status condition", "reason", reason)
		return err
	}
	return nil
}

// requiredKind is an external kind the operator cannot work without, paired with
// the component that provides it so a missing CRD can be reported by name.
type requiredKind struct {
	provider string
	gvk      schema.GroupVersionKind
}

// requiredKinds lists those kinds. Every workspace gets a HelmRepository, so a
// cluster missing that API cannot serve a single workspace.
func requiredKinds() []requiredKind {
	return []requiredKind{
		{
			provider: "Flux source-controller",
			gvk:      sourcev1.GroupVersion.WithKind("HelmRepository"),
		},
	}
}

// SetupWithManager registers the controller with the Manager and defines watches.
func (r *WorkspaceReconciler) SetupWithManager(mgr ctrl.Manager) error {
	// Checked before the builder so the operator names the missing prerequisite
	// itself. Otherwise the watch fails once its informer starts, reporting an
	// unknown kind and taking the whole manager — webhooks included — down.
	for _, required := range requiredKinds() {
		if !kindServed(mgr, required.gvk) {
			return fmt.Errorf("%s is not served by the cluster: the %s CRDs are required by the tenant operator",
				required.gvk, required.provider)
		}
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&tenantv1alpha1.Workspace{},
			builder.WithPredicates(predicate.GenerationChangedPredicate{}),
		).
		Owns(&corev1.Namespace{}).
		Owns(&corev1.ResourceQuota{}).
		Owns(&corev1.LimitRange{}).
		Owns(&corev1.Secret{}).
		Owns(&corev1.ServiceAccount{}).
		Owns(&rbacv1.RoleBinding{}).
		Owns(&networkingv1.NetworkPolicy{}).
		Owns(&sourcev1.HelmRepository{}).
		Watches(
			&corev1.ConfigMap{},
			handler.EnqueueRequestsFromMapFunc(r.enqueueAllWorkspacesIf(workspace.IsOperatorConfig)),
			builder.WithPredicates(operatorConfigChangedPredicate()),
		).
		Watches(
			&corev1.Secret{},
			handler.EnqueueRequestsFromMapFunc(r.enqueueAllWorkspacesIf(workspace.IsOperatorSecret)),
			builder.WithPredicates(operatorSecretChangedPredicate()),
		).
		Named("workspace").
		Complete(r)
}

// kindServed reports whether the cluster serves the given kind. The RESTMapper
// is cached, so installing a CRD afterwards requires an operator restart.
func kindServed(mgr ctrl.Manager, gvk schema.GroupVersionKind) bool {
	_, err := mgr.GetRESTMapper().RESTMapping(gvk.GroupKind(), gvk.Version)
	return err == nil
}

// Every operator-wide input — the tenant-operator-config ConfigMap and the
// tenant-operator-secret Secret — is watched the same way: narrow the event
// stream to the one object the operator reads, then fan a real change out
// across every Workspace. The two helpers below express that shape once; each
// input supplies only its matcher and its notion of "changed".

// enqueueAllWorkspacesIf maps an event on an operator-wide input to a reconcile
// of every Workspace. Objects the operator does not read are dropped, so a stray
// ConfigMap or Secret never fans out.
func (r *WorkspaceReconciler) enqueueAllWorkspacesIf(matches func(client.Object) bool) handler.MapFunc {
	return func(ctx context.Context, obj client.Object) []reconcile.Request {
		if !matches(obj) {
			return nil
		}
		return r.allWorkspaceRequests(ctx)
	}
}

// operatorInputPredicate builds the event filter for one operator-wide input.
// changed decides whether an update touched the part reconcile consumes, so
// metadata churn does not fan out. Creates and deletes always pass.
func operatorInputPredicate[T client.Object](
	matches func(client.Object) bool,
	changed func(oldObj, newObj T) bool,
) predicate.Funcs {
	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			return matches(e.Object)
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			return matches(e.Object)
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			if !matches(e.ObjectNew) {
				return false
			}
			oldObj, ok1 := e.ObjectOld.(T)
			newObj, ok2 := e.ObjectNew.(T)
			if !ok1 || !ok2 {
				return false
			}
			return changed(oldObj, newObj)
		},
		GenericFunc: func(e event.GenericEvent) bool {
			return matches(e.Object)
		},
	}
}

// operatorConfigChangedPredicate narrows ConfigMap events to
// tenant-operator-config, and updates to changes of its data.
func operatorConfigChangedPredicate() predicate.Funcs {
	return operatorInputPredicate(workspace.IsOperatorConfig,
		func(oldConfig, newConfig *corev1.ConfigMap) bool {
			return !maps.Equal(oldConfig.Data, newConfig.Data)
		})
}

// operatorSecretChangedPredicate narrows Secret events to the
// tenant-operator-secret, and updates to changes of the credentials themselves.
func operatorSecretChangedPredicate() predicate.Funcs {
	return operatorInputPredicate(workspace.IsOperatorSecret,
		func(oldSecret, newSecret *corev1.Secret) bool {
			return !maps.EqualFunc(oldSecret.Data, newSecret.Data, bytes.Equal)
		})
}

// allWorkspaceRequests enqueues every Workspace, for watches on operator-wide
// inputs where a single change affects all workspaces alike.
func (r *WorkspaceReconciler) allWorkspaceRequests(ctx context.Context) []reconcile.Request {
	var wsList tenantv1alpha1.WorkspaceList
	if err := r.List(ctx, &wsList); err != nil {
		return nil
	}

	requests := make([]reconcile.Request, 0, len(wsList.Items))
	for _, ws := range wsList.Items {
		requests = append(requests, reconcile.Request{
			Name: ws.Name,
		})
	}
	return requests
}

// updateStatus computes the status from the observed state and patches it.
// A non-empty missingHarborMembers sets Ready=False with reason
// HarborMembersPending so the partial state is visible to users.
func (r *WorkspaceReconciler) updateStatus(ctx context.Context, w *tenantv1alpha1.Workspace, missingHarborMembers []string) error {
	newStatus := w.Status.DeepCopy()
	newStatus.ObservedGeneration = w.Generation

	newStatus.NamespaceRef = &tenantv1alpha1.ResourceReference{
		Name: w.Spec.Namespace,
	}

	// Deterministic order, to prevent status flapping.
	rolesInUse := make(map[tenantv1alpha1.MemberRole]bool)
	for _, m := range w.Spec.Members {
		rolesInUse[m.Role] = true
	}

	newStatus.RoleBindingRefs = nil
	for _, role := range tenantv1alpha1.AllMemberRoles() {
		if rolesInUse[role] {
			newStatus.RoleBindingRefs = append(newStatus.RoleBindingRefs, tenantv1alpha1.ResourceReference{
				Name:      workspace.RoleBindingName + "-" + string(role),
				Namespace: w.Spec.Namespace,
			})
		}
	}

	if w.Spec.ResourceQuota != nil {
		newStatus.ResourceQuotaRef = &tenantv1alpha1.ResourceReference{
			Name:      workspace.ResourceQuotaName,
			Namespace: w.Spec.Namespace,
		}
	} else {
		newStatus.ResourceQuotaRef = nil
	}

	if w.Spec.LimitRange != nil {
		newStatus.LimitRangeRef = &tenantv1alpha1.ResourceReference{
			Name:      workspace.LimitRangeName,
			Namespace: w.Spec.Namespace,
		}
	} else {
		newStatus.LimitRangeRef = nil
	}

	// Resources whose presence reconcile cannot guarantee from the spec alone are
	// observed rather than derived:
	//   - the image pull Secret is written when the Harbor robot is created, so a
	//     Secret deleted afterwards cannot be rebuilt without new credentials
	//   - the HelmRepository can be removed out-of-band by a Flux operator
	var err error
	if newStatus.ImagePullSecretRef, err = r.observedRef(
		ctx, &corev1.Secret{}, workspace.ImagePullSecretName, w.Spec.Namespace,
	); err != nil {
		return err
	}
	if newStatus.HelmRepositoryRef, err = r.observedRef(
		ctx, &sourcev1.HelmRepository{}, workspace.HelmRepositoryName, w.Spec.Namespace,
	); err != nil {
		return err
	}

	if w.Spec.NetworkIsolation.Enabled {
		newStatus.NetworkPolicyRef = &tenantv1alpha1.ResourceReference{
			Name:      workspace.NetworkPolicyName,
			Namespace: w.Spec.Namespace,
		}
	} else {
		newStatus.NetworkPolicyRef = nil
	}

	readyCondition := metav1.Condition{
		Type:               workspace.ConditionTypeReady,
		Status:             metav1.ConditionTrue,
		Reason:             "Reconciled",
		Message:            "Workspace resources are successfully reconciled",
		ObservedGeneration: w.Generation,
	}
	if len(missingHarborMembers) > 0 {
		readyCondition.Status = metav1.ConditionFalse
		readyCondition.Reason = "HarborMembersPending"
		readyCondition.Message = fmt.Sprintf(
			"Waiting for Harbor users to be provisioned: %s",
			strings.Join(missingHarborMembers, ", "),
		)
	}
	meta.SetStatusCondition(&newStatus.Conditions, readyCondition)

	// Stable ordering.
	slices.SortFunc(newStatus.Conditions, func(a, b metav1.Condition) int {
		return cmp.Compare(a.Type, b.Type)
	})

	// Only patch on a real change, to spare the API server.
	if !equality.Semantic.DeepEqual(w.Status, *newStatus) {
		patch := client.MergeFrom(w.DeepCopy())
		w.Status = *newStatus
		if err := r.Status().Patch(ctx, w, patch); err != nil {
			return fmt.Errorf("patching Workspace status: %w", err)
		}
		log.FromContext(ctx).Info("Workspace status updated")
		r.Recorder.Eventf(w, nil, corev1.EventTypeNormal, "Reconciled", "Reconcile",
			"Workspace resources reconciled for namespace %s", w.Spec.Namespace)
	}

	return nil
}
