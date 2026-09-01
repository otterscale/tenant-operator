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
	goerrors "errors"
	"net/http"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	tenantv1alpha1 "github.com/otterscale/tenant-operator/api/v1alpha1"
	"github.com/otterscale/tenant-operator/internal/harbor"
	"github.com/otterscale/tenant-operator/internal/labels"
	ws "github.com/otterscale/tenant-operator/internal/workspace"
)

var _ = Describe("Workspace Controller", func() {
	const (
		timeout   = time.Second * 10
		interval  = time.Millisecond * 250
		adminUser = "admin-user"
		viewUser  = "view-user"
	)

	var (
		ctx           context.Context
		reconciler    *WorkspaceReconciler
		harborFake    *fakeHarborClient
		workspace     *tenantv1alpha1.Workspace
		resourceName  string
		namespaceName string
	)

	// --- Helpers ---

	makeWorkspace := func(name, namespace string, mods ...func(*tenantv1alpha1.Workspace)) *tenantv1alpha1.Workspace {
		w := &tenantv1alpha1.Workspace{
			Name: name,
			Spec: tenantv1alpha1.WorkspaceSpec{
				Namespace: namespace,
				Members: []tenantv1alpha1.WorkspaceMember{
					{Role: tenantv1alpha1.MemberRoleAdmin, Subject: adminUser},
				},
			},
		}
		for _, mod := range mods {
			mod(w)
		}
		return w
	}

	executeReconcile := func() {
		nsName := types.NamespacedName{Name: resourceName}
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nsName})
		Expect(err).NotTo(HaveOccurred())
	}

	fullyReconcile := func() {
		executeReconcile()
	}

	fetchResource := func(obj client.Object, name, namespace string) {
		key := types.NamespacedName{Name: name, Namespace: namespace}
		Eventually(func() error {
			return k8sClient.Get(ctx, key, obj)
		}, timeout, interval).Should(Succeed())
	}

	// --- Lifecycle ---

	BeforeEach(func() {
		ctx = context.Background()
		resourceName = string(uuid.NewUUID())
		namespaceName = string(uuid.NewUUID())
		harborFake = &fakeHarborClient{}
		reconciler = &WorkspaceReconciler{
			Client:          k8sClient,
			Scheme:          k8sClient.Scheme(),
			Recorder:        events.NewFakeRecorder(100),
			NewHarborClient: newFakeHarborClient(harborFake),
		}
		workspace = makeWorkspace(resourceName, namespaceName)
	})

	JustBeforeEach(func() {
		Expect(k8sClient.Create(ctx, workspace)).To(Succeed())
	})

	AfterEach(func() {
		nsName := types.NamespacedName{Name: resourceName}
		if err := k8sClient.Get(ctx, nsName, workspace); err == nil {
			Expect(k8sClient.Delete(ctx, workspace)).To(Succeed())
			Eventually(func() bool {
				return errors.IsNotFound(k8sClient.Get(ctx, nsName, workspace))
			}, timeout, interval).Should(BeTrue())
		}
	})

	// --- Tests ---

	Context("Basic Reconciliation", func() {
		It("should fully provision the workspace resources", func() {
			fullyReconcile()

			By("Verifying the namespace")
			var ns corev1.Namespace
			fetchResource(&ns, namespaceName, "")
			Expect(ns.Labels).To(HaveKeyWithValue("app.kubernetes.io/managed-by", "tenant-operator"))

			By("Verifying the Admin RoleBinding")
			var rb rbacv1.RoleBinding
			fetchResource(&rb, ws.RoleBindingName+"-admin", namespaceName)
			Expect(rb.Subjects).To(ContainElement(WithTransform(func(s rbacv1.Subject) string { return s.Name }, Equal(adminUser))))

			By("Verifying status updates")
			fetchResource(workspace, resourceName, "")
			Expect(workspace.Status.NamespaceRef.Name).To(Equal(namespaceName))

			readyCond := apimeta.FindStatusCondition(workspace.Status.Conditions, "Ready")
			Expect(readyCond).NotTo(BeNil())
			Expect(readyCond.Status).To(Equal(metav1.ConditionTrue))
		})
	})

	Context("Harbor Integration", func() {
		It("should publish the Harbor refs once the resources exist", func() {
			fullyReconcile()

			var pullSecret corev1.Secret
			fetchResource(&pullSecret, ws.ImagePullSecretName, namespaceName)
			Expect(pullSecret.Type).To(Equal(corev1.SecretTypeDockerConfigJson))

			By("Listing the pull secret on the namespace's default ServiceAccount")
			var defaultSA corev1.ServiceAccount
			fetchResource(&defaultSA, "default", namespaceName)
			Expect(defaultSA.ImagePullSecrets).To(ContainElement(
				corev1.LocalObjectReference{Name: ws.ImagePullSecretName}))

			fetchResource(workspace, resourceName, "")
			Expect(workspace.Status.ImagePullSecretRef).NotTo(BeNil())
			Expect(workspace.Status.ImagePullSecretRef.Name).To(Equal(ws.ImagePullSecretName))
			Expect(workspace.Status.HelmRepositoryRef).NotTo(BeNil())
			Expect(workspace.Status.HelmRepositoryRef.Name).To(Equal(ws.HelmRepositoryName))
		})

		// The robot is created before the Secret is written, so a failure in
		// between leaves a robot with no recoverable credentials. Harbor reveals a
		// secret only when it sets one, so a fresh one is the only way back.
		It("should rebuild a missing image pull secret by refreshing the robot secret", func() {
			harborFake.robotExists = true

			fullyReconcile()

			Expect(harborFake.refreshes).To(Equal(1), "a missing Secret must trigger exactly one refresh")

			var pullSecret corev1.Secret
			fetchResource(&pullSecret, ws.ImagePullSecretName, namespaceName)
			Expect(pullSecret.Data).To(HaveKey(corev1.DockerConfigJsonKey))
			Expect(string(pullSecret.Data[corev1.DockerConfigJsonKey])).
				To(ContainSubstring("refreshed-robot-secret"), "the rebuilt Secret must carry the new credentials")

			fetchResource(workspace, resourceName, "")
			Expect(workspace.Status.ImagePullSecretRef).NotTo(BeNil())
		})

		// Refreshing invalidates the live secret, so it must only happen when there
		// is nothing to lose.
		It("should not refresh the robot secret while the image pull secret is intact", func() {
			fullyReconcile() // creates the robot and writes the Secret

			harborFake.robotExists = true
			executeReconcile()

			Expect(harborFake.refreshes).To(BeZero(), "an intact Secret must never be rotated out from under its users")

			var pullSecret corev1.Secret
			fetchResource(&pullSecret, ws.ImagePullSecretName, namespaceName)
			Expect(string(pullSecret.Data[corev1.DockerConfigJsonKey])).
				To(ContainSubstring("fake-robot-secret"), "the original credentials must survive")
		})

		// Harbor federates against the same OIDC provider as the cluster, so a
		// member's RBAC subject is already the name Harbor knows it by. Pinning the
		// value that reaches the Harbor client keeps that assumption checkable.
		It("should identify a member in Harbor by its RBAC subject", func() {
			fullyReconcile()

			Expect(harborFake.desiredMembers).To(ConsistOf(harbor.ProjectMember{
				Username: adminUser,
				RoleID:   harbor.RoleProjectAdmin,
			}))
		})

		It("should report pending Harbor members on the Ready condition", func() {
			harborFake.missingUsers = []string{adminUser}

			fullyReconcile()

			fetchResource(workspace, resourceName, "")
			readyCond := apimeta.FindStatusCondition(workspace.Status.Conditions, "Ready")
			Expect(readyCond).NotTo(BeNil())
			Expect(readyCond.Status).To(Equal(metav1.ConditionFalse))
			Expect(readyCond.Reason).To(Equal("HarborMembersPending"))
			Expect(readyCond.Message).To(ContainSubstring(adminUser))
		})
	})

	Context("Watch Setup", func() {
		It("should register all watches with the manager", func() {
			mgr, err := ctrl.NewManager(cfg, ctrl.Options{
				Scheme:         k8sClient.Scheme(),
				LeaderElection: false,
				Metrics:        metricsserver.Options{BindAddress: "0"},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect((&WorkspaceReconciler{
				Client:          mgr.GetClient(),
				Scheme:          mgr.GetScheme(),
				Recorder:        events.NewFakeRecorder(100),
				NewHarborClient: newFakeHarborClient(&fakeHarborClient{}),
			}).SetupWithManager(mgr)).To(Succeed())
		})

		// Flux is a prerequisite, not an optional integration, so a cluster
		// without its CRDs must be refused at startup with a message naming the
		// requirement — not left to fail once the informer starts.
		It("should refuse to start when a required CRD is absent", func() {
			mgr, err := ctrl.NewManager(cfg, ctrl.Options{
				Scheme:         k8sClient.Scheme(),
				LeaderElection: false,
				Metrics:        metricsserver.Options{BindAddress: "0"},
				MapperProvider: func(*rest.Config, *http.Client) (apimeta.RESTMapper, error) {
					// Serves nothing, so every kind reports a no-match.
					return apimeta.NewDefaultRESTMapper(nil), nil
				},
			})
			Expect(err).NotTo(HaveOccurred())

			err = (&WorkspaceReconciler{
				Client:          mgr.GetClient(),
				Scheme:          mgr.GetScheme(),
				Recorder:        events.NewFakeRecorder(100),
				NewHarborClient: newFakeHarborClient(&fakeHarborClient{}),
			}).SetupWithManager(mgr)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("are required by the tenant operator"))
		})
	})

	Context("Rancher Project", func() {
		const rancherProjectID = "c-m-abcde:p-vwxyz"

		// The Rancher Project ID is operator-wide configuration read from the
		// tenant-operator-config ConfigMap, not a Workspace spec field.
		BeforeEach(func() {
			Expect(client.IgnoreAlreadyExists(k8sClient.Create(ctx, &corev1.Namespace{
				Name: ws.OperatorNamespace,
			}))).To(Succeed())

			globalConfig := &corev1.ConfigMap{
				Name:      ws.OperatorConfigName,
				Namespace: ws.OperatorNamespace,
				Data:      map[string]string{ws.RancherProjectIDKey: rancherProjectID},
			}
			if err := k8sClient.Create(ctx, globalConfig); errors.IsAlreadyExists(err) {
				fetchResource(globalConfig, ws.OperatorConfigName, ws.OperatorNamespace)
				globalConfig.Data = map[string]string{ws.RancherProjectIDKey: rancherProjectID}
				Expect(k8sClient.Update(ctx, globalConfig)).To(Succeed())
			} else {
				Expect(err).NotTo(HaveOccurred())
			}
			DeferCleanup(func() {
				Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, globalConfig))).To(Succeed())
			})
		})

		It("should reconcile the namespace annotation from the global config", func() {
			fullyReconcile()

			var ns corev1.Namespace
			fetchResource(&ns, namespaceName, "")
			Expect(ns.Annotations).To(HaveKeyWithValue("field.cattle.io/projectId", rancherProjectID))

			ns.Annotations["field.cattle.io/projectId"] = "local:p-wrong"
			Expect(k8sClient.Update(ctx, &ns)).To(Succeed())
			executeReconcile()

			fetchResource(&ns, namespaceName, "")
			Expect(ns.Annotations).To(HaveKeyWithValue("field.cattle.io/projectId", rancherProjectID))
		})

		It("should enqueue every workspace when the global config changes", func() {
			globalConfig := &corev1.ConfigMap{}
			fetchResource(globalConfig, ws.OperatorConfigName, ws.OperatorNamespace)

			Expect(reconciler.enqueueAllWorkspacesIf(ws.IsOperatorConfig)(ctx, globalConfig)).To(ContainElement(
				reconcile.Request{Name: resourceName},
			))

			By("Ignoring ConfigMaps that are not the global config")
			Expect(reconciler.enqueueAllWorkspacesIf(ws.IsOperatorConfig)(ctx, &corev1.ConfigMap{
				Name: "unrelated-config", Namespace: namespaceName,
			})).To(BeEmpty())
		})
	})

	Context("Namespace Conflict Handling", func() {
		It("should set Ready=False with NamespaceConflict when namespace already exists", func() {
			By("Creating an existing namespace not owned by the workspace")
			Expect(k8sClient.Create(ctx, &corev1.Namespace{
				Name: namespaceName,
			})).To(Succeed())

			By("Reconciling - a namespace conflict is permanent, so no error and no requeue")
			nsName := types.NamespacedName{Name: resourceName}
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nsName})
			Expect(err).NotTo(HaveOccurred())

			By("Verifying the status condition is updated")
			fetchResource(workspace, resourceName, "")
			readyCond := apimeta.FindStatusCondition(workspace.Status.Conditions, "Ready")
			Expect(readyCond).NotTo(BeNil())
			Expect(readyCond.Status).To(Equal(metav1.ConditionFalse))
			Expect(readyCond.Reason).To(Equal("NamespaceConflict"))
		})
	})

	Context("Reconcile Error Classification", func() {
		// A recorder that has seen nothing, so assertions about one call's events
		// are not confused by the reconcile before it.
		freshRecorder := func() *events.FakeRecorder {
			recorder := events.NewFakeRecorder(10)
			reconciler.Recorder = recorder
			return recorder
		}

		// Every domain sync reads then writes, so a concurrent write surfaces as a
		// 409. The next attempt re-reads and succeeds — a retry, not a failure.
		It("should retry a conflicting write without touching status or events", func() {
			fullyReconcile()
			fetchResource(workspace, resourceName, "")
			Expect(apimeta.FindStatusCondition(workspace.Status.Conditions, "Ready").Status).
				To(Equal(metav1.ConditionTrue))

			recorder := freshRecorder()
			conflict := errors.NewConflict(
				schema.GroupResource{Resource: "resourcequotas"},
				ws.ResourceQuotaName,
				goerrors.New("the object has been modified"))

			result, err := reconciler.handleReconcileError(ctx, workspace, conflict)
			Expect(err).To(MatchError(conflict), "the conflict is handed back for the rate-limited retry")
			Expect(result.IsZero()).To(BeTrue())
			Expect(recorder.Events).To(BeEmpty(), "a routine conflict must not raise a Warning")

			fetchResource(workspace, resourceName, "")
			Expect(apimeta.FindStatusCondition(workspace.Status.Conditions, "Ready").Status).
				To(Equal(metav1.ConditionTrue), "Ready must survive a conflict untouched")
		})

		// EventRecorder.Eventf treats its note as a format string, and Harbor error
		// notes carry percent-encoded response bodies.
		It("should record an error note containing % verbatim", func() {
			fullyReconcile()

			recorder := freshRecorder()
			_, err := reconciler.handleReconcileError(ctx, workspace,
				goerrors.New(`ensuring Harbor project: unexpected status 400: {"message":"bad path /a%2Fb"}`))
			Expect(err).To(HaveOccurred())

			var note string
			Eventually(recorder.Events).Should(Receive(&note))
			Expect(note).To(ContainSubstring("/a%2Fb"))
			Expect(note).NotTo(ContainSubstring("%!"), "the note must not be re-expanded as a format string")
		})
	})

	Context("Resource Management", func() {
		BeforeEach(func() {
			workspace = makeWorkspace(resourceName, namespaceName, func(w *tenantv1alpha1.Workspace) {
				w.Spec.ResourceQuota = &corev1.ResourceQuotaSpec{
					Hard: corev1.ResourceList{corev1.ResourcePods: resource.MustParse("10")},
				}
				w.Spec.LimitRange = &corev1.LimitRangeSpec{
					Limits: []corev1.LimitRangeItem{{
						Type:    corev1.LimitTypeContainer,
						Default: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("500m")},
					}},
				}
			})
		})

		It("should manage ResourceQuota and LimitRange lifecycles", func() {
			fullyReconcile()

			By("Verifying creation")
			var quota corev1.ResourceQuota
			fetchResource(&quota, ws.ResourceQuotaName, namespaceName)
			Expect(quota.Spec.Hard[corev1.ResourcePods]).To(Equal(resource.MustParse("10")))

			var limit corev1.LimitRange
			fetchResource(&limit, ws.LimitRangeName, namespaceName)
			Expect(limit.Spec.Limits[0].Default[corev1.ResourceCPU]).To(Equal(resource.MustParse("500m")))

			By("Updating Spec to remove constraints")
			fetchResource(workspace, resourceName, "")
			workspace.Spec.ResourceQuota = nil
			workspace.Spec.LimitRange = nil
			Expect(k8sClient.Update(ctx, workspace)).To(Succeed())

			executeReconcile()

			By("Verifying deletion")
			Expect(errors.IsNotFound(k8sClient.Get(ctx, types.NamespacedName{Name: ws.ResourceQuotaName, Namespace: namespaceName}, &quota))).To(BeTrue())
			Expect(errors.IsNotFound(k8sClient.Get(ctx, types.NamespacedName{Name: ws.LimitRangeName, Namespace: namespaceName}, &limit))).To(BeTrue())
		})
	})

	Context("Network Isolation", func() {
		BeforeEach(func() {
			workspace = makeWorkspace(resourceName, namespaceName, func(w *tenantv1alpha1.Workspace) {
				w.Spec.NetworkIsolation = tenantv1alpha1.NetworkIsolationSpec{
					Enabled:           true,
					AllowedNamespaces: []string{"kube-system"},
				}
			})
		})

		It("should manage NetworkPolicy lifecycle", func() {
			fullyReconcile()

			By("Verifying NetworkPolicy creation")
			var netpol networkingv1.NetworkPolicy
			fetchResource(&netpol, ws.NetworkPolicyName, namespaceName)
			Expect(netpol.Spec.Ingress).NotTo(BeEmpty())

			By("Disabling NetworkIsolation")
			fetchResource(workspace, resourceName, "")
			workspace.Spec.NetworkIsolation = tenantv1alpha1.NetworkIsolationSpec{}
			Expect(k8sClient.Update(ctx, workspace)).To(Succeed())

			executeReconcile()

			By("Verifying NetworkPolicy deletion")
			Expect(errors.IsNotFound(k8sClient.Get(ctx, types.NamespacedName{Name: ws.NetworkPolicyName, Namespace: namespaceName}, &netpol))).To(BeTrue())
		})
	})

	Context("RBAC & Multi-Member Support", func() {
		BeforeEach(func() {
			workspace = makeWorkspace(resourceName, namespaceName, func(w *tenantv1alpha1.Workspace) {
				w.Spec.Members = []tenantv1alpha1.WorkspaceMember{
					{Role: tenantv1alpha1.MemberRoleAdmin, Subject: adminUser},
					{Role: tenantv1alpha1.MemberRoleView, Subject: viewUser},
				}
			})
		})

		It("should sync RoleBindings accurately", func() {
			fullyReconcile()

			By("Checking View RoleBinding")
			var viewBinding rbacv1.RoleBinding
			fetchResource(&viewBinding, ws.RoleBindingName+"-view", namespaceName)
			Expect(viewBinding.Subjects).To(ContainElement(WithTransform(func(s rbacv1.Subject) string { return s.Name }, Equal(viewUser))))

			By("Removing View Member")
			fetchResource(workspace, resourceName, "")
			workspace.Spec.Members = []tenantv1alpha1.WorkspaceMember{
				{Role: tenantv1alpha1.MemberRoleAdmin, Subject: adminUser},
			}
			Expect(k8sClient.Update(ctx, workspace)).To(Succeed())

			fullyReconcile()

			By("Verifying View RoleBinding is gone")
			Expect(errors.IsNotFound(k8sClient.Get(ctx, types.NamespacedName{Name: ws.RoleBindingName + "-view", Namespace: namespaceName}, &viewBinding))).To(BeTrue())
		})
	})

	Context("Validating Admission Policy (Ownership)", func() {
		createImpersonatedClient := func(user string) client.Client {
			cfgCopy := *cfg
			cfgCopy.Impersonate = rest.ImpersonationConfig{UserName: user, Groups: []string{"system:authenticated"}}
			c, err := client.New(&cfgCopy, client.Options{Scheme: k8sClient.Scheme()})
			Expect(err).NotTo(HaveOccurred())
			return c
		}

		It("should enforce admin-only modifications", func() {
			By("Allowing admin update")
			adminClient := createImpersonatedClient(adminUser)
			var latestWs tenantv1alpha1.Workspace
			fetchResource(&latestWs, resourceName, "")

			latestWs.Spec.NetworkIsolation.Enabled = true
			Expect(adminClient.Update(ctx, &latestWs)).To(Succeed())

			By("Denying non-admin update")
			viewClient := createImpersonatedClient(viewUser)
			fetchResource(&latestWs, resourceName, "")

			latestWs.Spec.NetworkIsolation.Enabled = false
			err := viewClient.Update(ctx, &latestWs)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("only users with the 'admin' role"))
		})
	})

	Context("Domain Helpers", func() {
		It("should generate correct labels", func() {
			wsLabels := ws.LabelsForWorkspace("workspace-name", "v1")
			Expect(wsLabels).To(HaveKeyWithValue(labels.Name, "workspace-name"))
			Expect(wsLabels).To(HaveKeyWithValue(labels.Version, "v1"))
			Expect(wsLabels).To(HaveKeyWithValue(labels.Component, "workspace"))
			Expect(wsLabels).To(HaveKeyWithValue(labels.PartOf, "otterscale-system"))
			Expect(wsLabels).To(HaveKeyWithValue(labels.ManagedBy, "tenant-operator"))
		})

		It("should check ownership correctly", func() {
			uid := types.UID("12345")
			refs := []metav1.OwnerReference{{UID: uid}}
			Expect(ws.IsOwned(refs, uid)).To(BeTrue())
			Expect(ws.IsOwned(refs, "other")).To(BeFalse())
		})
	})
})
