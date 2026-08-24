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
	"k8s.io/apimachinery/pkg/types"
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
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	tenantv1alpha1 "github.com/otterscale/tenant-operator/api/v1alpha1"
	"github.com/otterscale/tenant-operator/internal/harbor"
	"github.com/otterscale/tenant-operator/internal/workspace"
)

// WorkspaceReconciler reconciles a Workspace object.
// It ensures that the underlying Namespace, RBAC roles, ResourceQuotas, and NetworkPolicies
// match the desired state defined in the Workspace CR.
//
// The controller is intentionally kept thin: it orchestrates the reconciliation flow,
// while the actual resource synchronization logic resides in internal/workspace/.
type WorkspaceReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Version  string
	Recorder events.EventRecorder

	// NewHarborClient builds the Harbor API client from the credentials found in
	// the operator-wide Secret. Defaults to harbor.NewClient when unset; tests
	// substitute a fake so reconciliation can run without a Harbor server.
	NewHarborClient func(baseURL, username, password string) harbor.Client
}

// harborClient builds the Harbor API client for the given credentials, falling
// back to the real HTTP client when no factory was injected.
func (r *WorkspaceReconciler) harborClient(creds *workspace.HarborCredentials) harbor.Client {
	newClient := r.NewHarborClient
	if newClient == nil {
		newClient = harbor.NewClient
	}
	return newClient(creds.URL, creds.RobotName, creds.RobotSecret)
}

// RBAC Permissions required by the controller:
// +kubebuilder:rbac:groups=tenant.otterscale.io,resources=workspaces,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=tenant.otterscale.io,resources=workspaces/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch
// +kubebuilder:rbac:groups="",resources=namespaces;resourcequotas;limitranges,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=networking.k8s.io,resources=networkpolicies,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=rolebindings,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=clusterroles,verbs=bind,resourceNames=admin;edit;view
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=clusterrolebindings,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=serviceaccounts,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups=source.toolkit.fluxcd.io,resources=helmrepositories,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=gateway.networking.k8s.io,resources=gateways,verbs=get;list;watch

// missingHarborMembersRequeue is how long to wait before retrying when Harbor
// members are missing. Harbor provisions users on first OIDC login, so the
// retry window just needs to be short enough to pick them up promptly.
const missingHarborMembersRequeue = 5 * time.Minute

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
	missingHarborMembers, err := r.reconcileResources(ctx, &w)
	if err != nil {
		return r.handleReconcileError(ctx, &w, err)
	}

	// 3. Update Status (Reflect the observed state back to the user)
	if err := r.updateStatus(ctx, &w, missingHarborMembers); err != nil {
		return ctrl.Result{}, err
	}

	// 4. Requeue if some Harbor users haven't been provisioned yet so they get
	//    added once Harbor has them. There's no watch on Harbor users, so a
	//    timed requeue is the only way these get picked up without a spec change.
	if len(missingHarborMembers) > 0 {
		return ctrl.Result{RequeueAfter: missingHarborMembersRequeue}, nil
	}

	return ctrl.Result{}, nil
}

// reconcileResources orchestrates the domain-level resource sync in order.
// Returns the list of workspace members whose Harbor user accounts do not yet
// exist — the caller requeues so they get added when Harbor provisions them.
func (r *WorkspaceReconciler) reconcileResources(ctx context.Context, w *tenantv1alpha1.Workspace) ([]string, error) {
	if err := workspace.ReconcileNamespace(ctx, r.Client, r.Scheme, w, r.Version); err != nil {
		return nil, err
	}
	// The Harbor credentials are read here rather than at startup so rotating
	// them takes effect on the next reconcile instead of requiring a restart.
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
	if err := workspace.ReconcileResourceQuota(ctx, r.Client, r.Scheme, w, r.Version); err != nil {
		return nil, err
	}
	if err := workspace.ReconcileLimitRange(ctx, r.Client, r.Scheme, w, r.Version); err != nil {
		return nil, err
	}
	if err := workspace.ReconcileConfig(ctx, r.Client, r.Scheme, w, r.Version); err != nil {
		return nil, err
	}
	if err := workspace.ReconcileNetworkIsolation(ctx, r.Client, r.Scheme, w, r.Version); err != nil {
		return nil, err
	}
	return missingHarborMembers, nil
}

// handleReconcileError categorizes errors and updates status accordingly.
// Permanent errors (e.g. namespace conflict) do NOT requeue to avoid infinite loops,
// unless the status patch itself fails — in which case a delayed requeue ensures the
// status eventually reflects the error.
// Transient errors are returned to the controller-runtime for exponential backoff retry.
func (r *WorkspaceReconciler) handleReconcileError(ctx context.Context, w *tenantv1alpha1.Workspace, err error) (ctrl.Result, error) {
	var nce *workspace.NamespaceConflictError
	if errors.As(err, &nce) {
		patchErr := r.setReadyConditionFalse(ctx, w, "NamespaceConflict", err.Error())
		r.Recorder.Eventf(w, nil, corev1.EventTypeWarning, "NamespaceConflict", "Reconcile", err.Error())
		if patchErr != nil {
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}
		return ctrl.Result{}, nil
	}

	// Transient error: surface it on the status, then hand the original error back
	// so controller-runtime retries with backoff. A failed status patch is
	// deliberately dropped here — unlike the permanent case above there is
	// already a retry coming, and reporting the patch error instead would lose
	// the reason the reconcile actually failed.
	_ = r.setReadyConditionFalse(ctx, w, "ReconcileError", err.Error())
	r.Recorder.Eventf(w, nil, corev1.EventTypeWarning, "ReconcileError", "Reconcile", err.Error())
	return ctrl.Result{}, err
}

// observedRef returns a reference to name/namespace once that object is actually
// present, so the status does not advertise a resource that is not there.
//
// obj is only a typed container for the lookup; its contents are not used. A
// missing object yields a nil reference, while any other API failure is returned
// so the caller retries instead of clearing the reference on a transient error.
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

// setReadyConditionFalse updates the Ready condition to False via status patch.
// Returns the patch error so callers can decide whether to retry.
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

// requiredKinds lists those kinds. Every workspace is given a HelmRepository and
// its service endpoints are derived from Gateways, so a cluster missing either
// API cannot serve a single workspace.
func requiredKinds() []requiredKind {
	return []requiredKind{
		{
			provider: "Flux source-controller",
			gvk:      sourcev1.GroupVersion.WithKind("HelmRepository"),
		},
		{
			provider: "Gateway API",
			gvk: schema.GroupVersionKind{
				Group:   gatewayv1.GroupName,
				Version: gatewayv1.GroupVersion.Version,
				Kind:    "Gateway",
			},
		},
	}
}

// SetupWithManager registers the controller with the Manager and defines watches.
func (r *WorkspaceReconciler) SetupWithManager(mgr ctrl.Manager) error {
	// Checked before the builder so the operator names the missing prerequisite
	// itself. A watch on a kind the cluster does not serve would otherwise fail
	// while its informer starts, reporting an unknown kind without saying it is
	// required, and taking the whole manager — webhooks included — down with it.
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
		Owns(&corev1.ConfigMap{}).
		Owns(&corev1.Secret{}).
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
		Watches(
			&gatewayv1.Gateway{},
			handler.EnqueueRequestsFromMapFunc(r.enqueueAllWorkspacesIf(workspace.IsEndpointSourceGateway)),
			builder.WithPredicates(gatewayChangedPredicate()),
		).
		Named("workspace").
		Complete(r)
}

// kindServed reports whether the cluster serves the given kind. The RESTMapper
// is cached, so this is a startup-time decision: installing a CRD afterwards
// requires an operator restart before the kind is seen.
func kindServed(mgr ctrl.Manager, gvk schema.GroupVersionKind) bool {
	_, err := mgr.GetRESTMapper().RESTMapping(gvk.GroupKind(), gvk.Version)
	return err == nil
}

// Every operator-wide input the workspaces derive from — the two Gateways, the
// tenant-operator-config ConfigMap and the tenant-operator-secret Secret — is
// watched the same way: narrow the event stream to the one object the operator
// reads, then fan a real change out across every Workspace. The two helpers
// below express that shape once; each input only supplies its own matcher and
// its own notion of "the part I actually read changed".

// enqueueAllWorkspacesIf maps an event on an operator-wide input to a reconcile
// of every Workspace, since one change affects them all alike. Events on objects
// the operator does not read are dropped, so a stray ConfigMap or Secret never
// fans out.
func (r *WorkspaceReconciler) enqueueAllWorkspacesIf(matches func(client.Object) bool) handler.MapFunc {
	return func(ctx context.Context, obj client.Object) []reconcile.Request {
		if !matches(obj) {
			return nil
		}
		return r.allWorkspaceRequests(ctx)
	}
}

// operatorInputPredicate builds the event filter for one operator-wide input.
// changed decides whether an update touched the part of the object reconcile
// consumes, so metadata churn does not fan out across every Workspace. Creates
// and deletes always pass: either can change what reconcile resolves.
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

// gatewayChangedPredicate narrows Gateway events to the Gateways the workspace
// endpoints are derived from, and updates to the two fields those endpoints
// read: spec.addresses and spec.listeners.
func gatewayChangedPredicate() predicate.Funcs {
	return operatorInputPredicate(workspace.IsEndpointSourceGateway,
		func(oldGateway, newGateway *gatewayv1.Gateway) bool {
			return !equality.Semantic.DeepEqual(oldGateway.Spec.Addresses, newGateway.Spec.Addresses) ||
				!equality.Semantic.DeepEqual(oldGateway.Spec.Listeners, newGateway.Spec.Listeners)
		})
}

// operatorConfigChangedPredicate narrows ConfigMap events to the
// tenant-operator-config ConfigMap, and updates to changes of its data.
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

// allWorkspaceRequests enqueues every Workspace. Used by watches on
// operator-wide inputs, where a single change affects all workspaces alike.
func (r *WorkspaceReconciler) allWorkspaceRequests(ctx context.Context) []reconcile.Request {
	var wsList tenantv1alpha1.WorkspaceList
	if err := r.List(ctx, &wsList); err != nil {
		return nil
	}

	requests := make([]reconcile.Request, 0, len(wsList.Items))
	for _, ws := range wsList.Items {
		requests = append(requests, reconcile.Request{
			NamespacedName: types.NamespacedName{
				Name: ws.Name,
			},
		})
	}
	return requests
}

// updateStatus calculates the status based on the current observed state and patches the resource.
// When missingHarborMembers is non-empty, Ready is set to False with reason
// HarborMembersPending so the Workspace surfaces the partial state to users.
func (r *WorkspaceReconciler) updateStatus(ctx context.Context, w *tenantv1alpha1.Workspace, missingHarborMembers []string) error {
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

	newStatus.RoleBindingRefs = nil
	for _, role := range tenantv1alpha1.AllMemberRoles() {
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

	// References to resources whose presence reconcile cannot guarantee from the
	// spec alone are observed rather than derived:
	//   - the workspace-config ConfigMap exists only while an endpoint resolves
	//   - the image pull Secret is written when the Harbor robot is created, so a
	//     Secret deleted afterwards cannot be rebuilt without new credentials
	//   - the HelmRepository can be removed out-of-band by a Flux operator
	var err error
	if newStatus.ConfigMapRef, err = r.observedRef(
		ctx, &corev1.ConfigMap{}, workspace.ConfigName, w.Spec.Namespace,
	); err != nil {
		return err
	}
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

	// Update NetworkPolicy reference
	if w.Spec.NetworkIsolation.Enabled {
		newStatus.NetworkPolicyRef = &tenantv1alpha1.ResourceReference{
			Name:      workspace.NetworkPolicyName,
			Namespace: w.Spec.Namespace,
		}
	} else {
		newStatus.NetworkPolicyRef = nil
	}

	// Set Ready condition. If some Harbor members are missing, report the
	// partial state so users can see why those identities aren't in Harbor yet.
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
