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
	"encoding/json"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	tenantv1alpha1 "github.com/otterscale/api/tenant/v1alpha1"
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
		workspace     *tenantv1alpha1.Workspace
		resourceName  string
		namespaceName string
	)

	// --- Helpers ---

	toRawExtension := func(obj any) *runtime.RawExtension {
		raw, err := json.Marshal(obj)
		Expect(err).NotTo(HaveOccurred())
		return &runtime.RawExtension{Raw: raw}
	}

	makeWorkspace := func(name, namespace string, mods ...func(*tenantv1alpha1.Workspace)) *tenantv1alpha1.Workspace {
		w := &tenantv1alpha1.Workspace{
			ObjectMeta: metav1.ObjectMeta{Name: name},
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
		executeReconcile() // provisions resources + updates status
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
		reconciler = &WorkspaceReconciler{
			Client:        k8sClient,
			Scheme:        k8sClient.Scheme(),
			Recorder:      events.NewFakeRecorder(100),
			istioDetector: &IstioDetector{}, // defaults to disabled (no Istio in envtest)
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

			readyCond := meta.FindStatusCondition(workspace.Status.Conditions, "Ready")
			Expect(readyCond).NotTo(BeNil())
			Expect(readyCond.Status).To(Equal(metav1.ConditionTrue))
		})
	})

	Context("Namespace Conflict Handling", func() {
		It("should set Ready=False with NamespaceConflict when namespace already exists", func() {
			By("Creating an existing namespace not owned by the workspace")
			Expect(k8sClient.Create(ctx, &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{Name: namespaceName},
			})).To(Succeed())

			By("Running reconciliation - namespace conflict is a permanent error: should NOT return error (no requeue)")
			nsName := types.NamespacedName{Name: resourceName}
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nsName})
			Expect(err).NotTo(HaveOccurred())

			By("Verifying the status condition is updated")
			fetchResource(workspace, resourceName, "")
			readyCond := meta.FindStatusCondition(workspace.Status.Conditions, "Ready")
			Expect(readyCond).NotTo(BeNil())
			Expect(readyCond.Status).To(Equal(metav1.ConditionFalse))
			Expect(readyCond.Reason).To(Equal("NamespaceConflict"))
		})
	})

	Context("Resource Management", func() {
		BeforeEach(func() {
			workspace = makeWorkspace(resourceName, namespaceName, func(w *tenantv1alpha1.Workspace) {
				w.Spec.ResourceQuota = toRawExtension(corev1.ResourceQuotaSpec{
					Hard: corev1.ResourceList{corev1.ResourcePods: resource.MustParse("10")},
				})
				w.Spec.LimitRange = toRawExtension(corev1.LimitRangeSpec{
					Limits: []corev1.LimitRangeItem{{
						Type:    corev1.LimitTypeContainer,
						Default: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("500m")},
					}},
				})
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

		It("should manage NetworkPolicy when Istio is disabled", func() {
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
			fetchResource(&latestWs, resourceName, "") // Refresh

			latestWs.Spec.NetworkIsolation.Enabled = false
			err := viewClient.Update(ctx, &latestWs)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("only users with the 'admin' role"))
		})
	})

	Context("Domain Helpers", func() {
		It("should generate correct labels", func() {
			wsLabels := ws.LabelsForWorkspace("workspace-name", "v1")
			Expect(wsLabels).To(HaveKeyWithValue(labels.Instance, "workspace-name"))
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
