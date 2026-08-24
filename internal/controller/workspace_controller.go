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
	missingHarborMembers, harborEnabled, err := r.reconcileResources(ctx, &w)
	if err != nil {
		return r.handleReconcileError(ctx, &w, err)
	}

	// 3. Update Status (Reflect the observed state back to the user)
	if err := r.updateStatus(ctx, &w, missingHarborMembers, harborEnabled); err != nil {
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
//
// Returns the list of workspace members whose Harbor user accounts do not yet
// exist — the caller requeues so they get added when Harbor provisions them —
// and whether the Harbor integration was configured for this pass, which the
// status update needs in order to publish or clear the Harbor resource refs.
func (r *WorkspaceReconciler) reconcileResources(ctx context.Context, w *tenantv1alpha1.Workspace) ([]string, bool, error) {
	if err := workspace.ReconcileNamespace(ctx, r.Client, r.Scheme, w, r.Version); err != nil {
		return nil, false, err
	}
	if err := workspace.ReconcileServiceAccount(ctx, r.Client, r.Scheme, w, r.Version); err != nil {
		return nil, false, err
	}

	// Harbor is opt-in: its credentials live in the operator-wide Secret, read
	// here rather than at startup so adding or rotating them takes effect on the
	// next reconcile instead of requiring a restart.
	creds, err := workspace.OperatorHarborCredentials(ctx, r.Client)
	if err != nil {
		return nil, false, err
	}
	var missingHarborMembers []string
	if creds != nil {
		missing, err := workspace.ReconcileHarbor(ctx, r.Client, r.Scheme, w, r.Version, r.harborClient(creds), creds.URL)
		if err != nil {
			return nil, false, err
		}
		missingHarborMembers = missing
		if err := workspace.ReconcileHelmRepository(ctx, r.Client, r.Scheme, w, r.Version, creds.URL); err != nil {
			return nil, false, err
		}
	}

	if err := workspace.ReconcileRoleBindings(ctx, r.Client, r.Scheme, w, r.Version); err != nil {
		return nil, false, err
	}
	if err := workspace.ReconcileResourceQuota(ctx, r.Client, r.Scheme, w, r.Version); err != nil {
		return nil, false, err
	}
	if err := workspace.ReconcileLimitRange(ctx, r.Client, r.Scheme, w, r.Version); err != nil {
		return nil, false, err
	}
	if err := workspace.ReconcileConfig(ctx, r.Client, r.Scheme, w, r.Version); err != nil {
		return nil, false, err
	}
	if err := workspace.ReconcileNetworkIsolation(ctx, r.Client, r.Scheme, w, r.Version); err != nil {
		return nil, false, err
	}
	return missingHarborMembers, creds != nil, nil
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

	// Transient error: update status and requeue
	_ = r.setReadyConditionFalse(ctx, w, "ReconcileError", err.Error())
	r.Recorder.Eventf(w, nil, corev1.EventTypeWarning, "ReconcileError", "Reconcile", err.Error())
	return ctrl.Result{}, err
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

// SetupWithManager registers the controller with the Manager and defines watches.
func (r *WorkspaceReconciler) SetupWithManager(mgr ctrl.Manager) error {
	b := ctrl.NewControllerManagedBy(mgr).
		For(&tenantv1alpha1.Workspace{},
			builder.WithPredicates(predicate.GenerationChangedPredicate{}),
		).
		Owns(&corev1.Namespace{}).
		Owns(&corev1.ResourceQuota{}).
		Owns(&corev1.LimitRange{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&corev1.Secret{}).
		Owns(&corev1.ServiceAccount{}).
		Owns(&rbacv1.RoleBinding{}).
		Owns(&networkingv1.NetworkPolicy{}).
		Owns(&sourcev1.HelmRepository{}).
		Watches(
			&corev1.ConfigMap{},
			handler.EnqueueRequestsFromMapFunc(r.findWorkspacesForOperatorConfig),
			builder.WithPredicates(operatorConfigChangedPredicate()),
		).
		Watches(
			&corev1.Secret{},
			handler.EnqueueRequestsFromMapFunc(r.findWorkspacesForOperatorSecret),
			builder.WithPredicates(operatorSecretChangedPredicate()),
		)

	// The Gateway API is optional. Without its CRDs the informer would fail to
	// start and take the whole manager down, so only watch Gateways when the kind
	// is actually served — ReconcileConfig already degrades to "no endpoints" there.
	if gatewayAPIServed(mgr) {
		b = b.Watches(
			&gatewayv1.Gateway{},
			handler.EnqueueRequestsFromMapFunc(r.findWorkspacesForGateway),
			builder.WithPredicates(gatewayChangedPredicate()),
		)
	} else {
		mgr.GetLogger().Info("Gateway API not served by the cluster, " +
			"workspace endpoints will not react to Gateway changes")
	}

	return b.Named("workspace").Complete(r)
}

// gatewayAPIServed reports whether the cluster serves the Gateway kind. The
// RESTMapper is cached, so this is a startup-time decision: installing the
// Gateway API afterwards requires an operator restart for the watch to appear.
func gatewayAPIServed(mgr ctrl.Manager) bool {
	_, err := mgr.GetRESTMapper().RESTMapping(
		schema.GroupKind{Group: gatewayv1.GroupName, Kind: "Gateway"},
		gatewayv1.GroupVersion.Version,
	)
	return err == nil
}

func (r *WorkspaceReconciler) findWorkspacesForGateway(ctx context.Context, obj client.Object) []reconcile.Request {
	if !workspace.IsEndpointSourceGateway(obj) {
		return nil
	}
	return r.allWorkspaceRequests(ctx)
}

// gatewayChangedPredicate filters Gateway events down to the Gateways the
// workspace endpoints are derived from, and for updates down to the two fields
// those endpoints read: spec.addresses and spec.listeners.
func gatewayChangedPredicate() predicate.Funcs {
	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			return workspace.IsEndpointSourceGateway(e.Object)
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			return workspace.IsEndpointSourceGateway(e.Object)
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			if !workspace.IsEndpointSourceGateway(e.ObjectNew) {
				return false
			}
			oldGateway, ok1 := e.ObjectOld.(*gatewayv1.Gateway)
			newGateway, ok2 := e.ObjectNew.(*gatewayv1.Gateway)
			if !ok1 || !ok2 {
				return false
			}
			return !equality.Semantic.DeepEqual(oldGateway.Spec.Addresses, newGateway.Spec.Addresses) ||
				!equality.Semantic.DeepEqual(oldGateway.Spec.Listeners, newGateway.Spec.Listeners)
		},
		GenericFunc: func(e event.GenericEvent) bool {
			return workspace.IsEndpointSourceGateway(e.Object)
		},
	}
}

// isOperatorConfig reports whether obj is the operator-wide tenant-operator-config
// ConfigMap, which holds settings the operator cannot discover from the cluster
// (e.g. the Rancher Project ID).
func isOperatorConfig(obj client.Object) bool {
	if obj == nil {
		return false
	}
	return obj.GetName() == workspace.OperatorConfigName &&
		obj.GetNamespace() == workspace.OperatorConfigNamespace
}

func (r *WorkspaceReconciler) findWorkspacesForOperatorConfig(ctx context.Context, obj client.Object) []reconcile.Request {
	if !isOperatorConfig(obj) {
		return nil
	}
	return r.allWorkspaceRequests(ctx)
}

// operatorConfigChangedPredicate filters ConfigMap events down to meaningful
// changes of the tenant-operator-config ConfigMap. Updates that leave Data
// untouched are dropped so unrelated metadata churn does not fan out a
// reconcile across every Workspace.
func operatorConfigChangedPredicate() predicate.Funcs {
	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			return isOperatorConfig(e.Object)
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			return isOperatorConfig(e.Object)
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			if !isOperatorConfig(e.ObjectNew) {
				return false
			}
			oldConfig, ok1 := e.ObjectOld.(*corev1.ConfigMap)
			newConfig, ok2 := e.ObjectNew.(*corev1.ConfigMap)
			if !ok1 || !ok2 {
				return false
			}
			return !maps.Equal(oldConfig.Data, newConfig.Data)
		},
		GenericFunc: func(e event.GenericEvent) bool {
			return isOperatorConfig(e.Object)
		},
	}
}

func (r *WorkspaceReconciler) findWorkspacesForOperatorSecret(ctx context.Context, obj client.Object) []reconcile.Request {
	if !workspace.IsOperatorHarborSecret(obj) {
		return nil
	}
	return r.allWorkspaceRequests(ctx)
}

// operatorSecretChangedPredicate filters Secret events down to meaningful changes
// of the tenant-operator-secret. Creating it enables the Harbor integration and
// deleting it disables it again, so both fan out; updates only matter when the
// credentials themselves changed.
func operatorSecretChangedPredicate() predicate.Funcs {
	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			return workspace.IsOperatorHarborSecret(e.Object)
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			return workspace.IsOperatorHarborSecret(e.Object)
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			if !workspace.IsOperatorHarborSecret(e.ObjectNew) {
				return false
			}
			oldSecret, ok1 := e.ObjectOld.(*corev1.Secret)
			newSecret, ok2 := e.ObjectNew.(*corev1.Secret)
			if !ok1 || !ok2 {
				return false
			}
			return !maps.EqualFunc(oldSecret.Data, newSecret.Data, bytes.Equal)
		},
		GenericFunc: func(e event.GenericEvent) bool {
			return workspace.IsOperatorHarborSecret(e.Object)
		},
	}
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
func (r *WorkspaceReconciler) updateStatus(
	ctx context.Context,
	w *tenantv1alpha1.Workspace,
	missingHarborMembers []string,
	harborEnabled bool,
) error {
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

	// Update ConfigMap reference
	var configMap corev1.ConfigMap
	if err := r.Get(ctx, client.ObjectKey{Name: workspace.ConfigName, Namespace: w.Spec.Namespace}, &configMap); err == nil {
		newStatus.ConfigMapRef = &tenantv1alpha1.ResourceReference{
			Name:      workspace.ConfigName,
			Namespace: w.Spec.Namespace,
		}
	} else {
		newStatus.ConfigMapRef = nil
	}

	// Update ImagePullSecret reference
	if harborEnabled {
		newStatus.ImagePullSecretRef = &tenantv1alpha1.ResourceReference{
			Name:      workspace.ImagePullSecretName,
			Namespace: w.Spec.Namespace,
		}
	} else {
		newStatus.ImagePullSecretRef = nil
	}

	// Update HelmRepository reference
	if harborEnabled {
		newStatus.HelmRepositoryRef = &tenantv1alpha1.ResourceReference{
			Name:      workspace.HelmRepositoryName,
			Namespace: w.Spec.Namespace,
		}
	} else {
		newStatus.HelmRepositoryRef = nil
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
