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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"

	tenantv1alpha1 "github.com/otterscale/api/tenant/v1alpha1"
	rbacv1 "k8s.io/api/rbac/v1"
)

var _ = Describe("Workspace Controller - CEL Validation", func() {
	const (
		timeout  = time.Second * 10
		interval = time.Millisecond * 250
	)

	var (
		ctx          context.Context
		workspace    *tenantv1alpha1.Workspace
		resourceName string
	)

	BeforeEach(func() {
		ctx = context.Background()
		resourceName = string(uuid.NewUUID())
	})

	AfterEach(func() {
		if workspace != nil {
			nsName := types.NamespacedName{Name: resourceName}
			if err := k8sClient.Get(ctx, nsName, workspace); err == nil {
				Expect(k8sClient.Delete(ctx, workspace)).To(Succeed())
				Eventually(func() bool {
					return errors.IsNotFound(k8sClient.Get(ctx, nsName, workspace))
				}, timeout, interval).Should(BeTrue())
			}
		}
	})

	createImpersonatedClient := func(user string, groups []string) client.Client {
		cfgCopy := *cfg
		if groups == nil {
			groups = []string{"system:authenticated"}
		}
		cfgCopy.Impersonate = rest.ImpersonationConfig{UserName: user, Groups: groups}
		c, err := client.New(&cfgCopy, client.Options{Scheme: k8sClient.Scheme()})
		Expect(err).NotTo(HaveOccurred())
		return c
	}

	Context("Admission Policy - Create Authorization", func() {
		It("should allow creation when creator is listed as admin", func() {
			userClient := createImpersonatedClient("alice", nil)
			workspace = &tenantv1alpha1.Workspace{
				ObjectMeta: metav1.ObjectMeta{Name: resourceName},
				Spec: tenantv1alpha1.WorkspaceSpec{
					Namespace: string(uuid.NewUUID()),
					Members: []tenantv1alpha1.WorkspaceMember{
						{Role: tenantv1alpha1.MemberRoleAdmin, Subject: "alice"},
					},
				},
			}
			Expect(userClient.Create(ctx, workspace)).To(Succeed())
		})

		It("should deny creation when creator is not listed as admin", func() {
			userClient := createImpersonatedClient("mallory", nil)
			workspace = &tenantv1alpha1.Workspace{
				ObjectMeta: metav1.ObjectMeta{Name: resourceName},
				Spec: tenantv1alpha1.WorkspaceSpec{
					Namespace: string(uuid.NewUUID()),
					Members: []tenantv1alpha1.WorkspaceMember{
						{Role: tenantv1alpha1.MemberRoleAdmin, Subject: "someone-else"},
					},
				},
			}
			err := userClient.Create(ctx, workspace)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("workspace creator must be listed as a member with the 'admin' role"))
		})

		It("should deny creation when creator is listed as edit only", func() {
			userClient := createImpersonatedClient("bob", nil)
			workspace = &tenantv1alpha1.Workspace{
				ObjectMeta: metav1.ObjectMeta{Name: resourceName},
				Spec: tenantv1alpha1.WorkspaceSpec{
					Namespace: string(uuid.NewUUID()),
					Members: []tenantv1alpha1.WorkspaceMember{
						{Role: tenantv1alpha1.MemberRoleAdmin, Subject: "someone-else"},
						{Role: tenantv1alpha1.MemberRoleEdit, Subject: "bob"},
					},
				},
			}
			err := userClient.Create(ctx, workspace)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("workspace creator must be listed as a member with the 'admin' role"))
		})

		It("should allow system:masters to create workspace for others", func() {
			masterClient := createImpersonatedClient("cluster-admin", []string{"system:masters"})
			workspace = &tenantv1alpha1.Workspace{
				ObjectMeta: metav1.ObjectMeta{Name: resourceName},
				Spec: tenantv1alpha1.WorkspaceSpec{
					Namespace: string(uuid.NewUUID()),
					Members: []tenantv1alpha1.WorkspaceMember{
						{Role: tenantv1alpha1.MemberRoleAdmin, Subject: "someone-else"},
					},
				},
			}
			Expect(masterClient.Create(ctx, workspace)).To(Succeed())
		})

		It("should allow user bound to cluster-admin ClusterRole to create workspace for others", func() {
			binding := &rbacv1.ClusterRoleBinding{
				ObjectMeta: metav1.ObjectMeta{Name: "test-cluster-admin-create-" + resourceName},
				RoleRef: rbacv1.RoleRef{
					APIGroup: rbacv1.GroupName,
					Kind:     "ClusterRole",
					Name:     "cluster-admin",
				},
				Subjects: []rbacv1.Subject{
					{Kind: rbacv1.UserKind, Name: "cluster-admin-user", APIGroup: rbacv1.GroupName},
				},
			}
			Expect(k8sClient.Create(ctx, binding)).To(Succeed())
			defer func() { _ = k8sClient.Delete(ctx, binding) }()

			caClient := createImpersonatedClient("cluster-admin-user", nil)
			workspace = &tenantv1alpha1.Workspace{
				ObjectMeta: metav1.ObjectMeta{Name: resourceName},
				Spec: tenantv1alpha1.WorkspaceSpec{
					Namespace: string(uuid.NewUUID()),
					Members: []tenantv1alpha1.WorkspaceMember{
						{Role: tenantv1alpha1.MemberRoleAdmin, Subject: "someone-else"},
					},
				},
			}
			Expect(caClient.Create(ctx, workspace)).To(Succeed())
		})

		It("should deny creation when another workspace already uses the same namespace", func() {
			sharedNS := string(uuid.NewUUID())

			firstWS := &tenantv1alpha1.Workspace{
				ObjectMeta: metav1.ObjectMeta{Name: resourceName},
				Spec: tenantv1alpha1.WorkspaceSpec{
					Namespace: sharedNS,
					Members: []tenantv1alpha1.WorkspaceMember{
						{Role: tenantv1alpha1.MemberRoleAdmin, Subject: "alice"},
					},
				},
			}
			aliceClient := createImpersonatedClient("alice", nil)
			Expect(aliceClient.Create(ctx, firstWS)).To(Succeed())
			workspace = firstWS

			secondName := string(uuid.NewUUID())
			secondWS := &tenantv1alpha1.Workspace{
				ObjectMeta: metav1.ObjectMeta{Name: secondName},
				Spec: tenantv1alpha1.WorkspaceSpec{
					Namespace: sharedNS,
					Members: []tenantv1alpha1.WorkspaceMember{
						{Role: tenantv1alpha1.MemberRoleAdmin, Subject: "bob"},
					},
				},
			}
			bobClient := createImpersonatedClient("bob", nil)
			err := bobClient.Create(ctx, secondWS)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("is already used by workspace"))
		})
	})

	Context("Admission Policy - Update/Delete Authorization", func() {
		const controllerServiceAccount = "system:serviceaccount:otterscale-system:tenant-operator-controller-manager"

		BeforeEach(func() {
			workspace = &tenantv1alpha1.Workspace{
				ObjectMeta: metav1.ObjectMeta{Name: resourceName},
				Spec: tenantv1alpha1.WorkspaceSpec{
					Namespace: string(uuid.NewUUID()),
					Members: []tenantv1alpha1.WorkspaceMember{
						{Role: tenantv1alpha1.MemberRoleAdmin, Subject: "admin-user"},
					},
				},
			}
			Expect(k8sClient.Create(ctx, workspace)).To(Succeed())
		})

		It("should allow controller service account to update workspace", func() {
			saClient := createImpersonatedClient(controllerServiceAccount, nil)

			nsName := types.NamespacedName{Name: resourceName}
			var w tenantv1alpha1.Workspace
			Expect(k8sClient.Get(ctx, nsName, &w)).To(Succeed())

			w.Spec.NetworkIsolation.Enabled = true
			Expect(saClient.Update(ctx, &w)).To(Succeed())
		})

		It("should allow system:masters to update workspace", func() {
			masterClient := createImpersonatedClient("cluster-admin", []string{"system:masters"})

			nsName := types.NamespacedName{Name: resourceName}
			var w tenantv1alpha1.Workspace
			Expect(k8sClient.Get(ctx, nsName, &w)).To(Succeed())

			w.Spec.NetworkIsolation.Enabled = true
			Expect(masterClient.Update(ctx, &w)).To(Succeed())
		})

		It("should deny non-admin user updates", func() {
			nonAdminClient := createImpersonatedClient("random-user", []string{"system:authenticated"})

			nsName := types.NamespacedName{Name: resourceName}
			var w tenantv1alpha1.Workspace
			Expect(k8sClient.Get(ctx, nsName, &w)).To(Succeed())

			w.Spec.NetworkIsolation.Enabled = true
			err := nonAdminClient.Update(ctx, &w)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("only users with the 'admin' role"))
		})

		It("should allow admin user defined in workspace to update", func() {
			adminClient := createImpersonatedClient("admin-user", []string{"system:authenticated"})

			nsName := types.NamespacedName{Name: resourceName}
			var w tenantv1alpha1.Workspace
			Expect(k8sClient.Get(ctx, nsName, &w)).To(Succeed())

			w.Spec.NetworkIsolation.Enabled = true
			Expect(adminClient.Update(ctx, &w)).To(Succeed())
		})

		It("should allow user bound to cluster-admin ClusterRole to update workspace", func() {
			binding := &rbacv1.ClusterRoleBinding{
				ObjectMeta: metav1.ObjectMeta{Name: "test-cluster-admin-" + resourceName},
				RoleRef: rbacv1.RoleRef{
					APIGroup: rbacv1.GroupName,
					Kind:     "ClusterRole",
					Name:     "cluster-admin",
				},
				Subjects: []rbacv1.Subject{
					{Kind: rbacv1.UserKind, Name: "cluster-admin-user", APIGroup: rbacv1.GroupName},
				},
			}
			Expect(k8sClient.Create(ctx, binding)).To(Succeed())
			defer func() { _ = k8sClient.Delete(ctx, binding) }()

			caClient := createImpersonatedClient("cluster-admin-user", []string{"system:authenticated"})

			nsName := types.NamespacedName{Name: resourceName}
			var w tenantv1alpha1.Workspace
			Expect(k8sClient.Get(ctx, nsName, &w)).To(Succeed())

			w.Spec.NetworkIsolation.Enabled = true
			Expect(caClient.Update(ctx, &w)).To(Succeed())
		})

		It("should allow workspace admin to delete workspace", func() {
			adminClient := createImpersonatedClient("admin-user", []string{"system:authenticated"})

			nsName := types.NamespacedName{Name: resourceName}
			var w tenantv1alpha1.Workspace
			Expect(k8sClient.Get(ctx, nsName, &w)).To(Succeed())

			Expect(adminClient.Delete(ctx, &w)).To(Succeed())

			// Prevent AfterEach from trying to delete again
			workspace = nil
		})

		It("should deny non-admin user from deleting workspace", func() {
			nonAdminClient := createImpersonatedClient("random-user", []string{"system:authenticated"})

			nsName := types.NamespacedName{Name: resourceName}
			var w tenantv1alpha1.Workspace
			Expect(k8sClient.Get(ctx, nsName, &w)).To(Succeed())

			err := nonAdminClient.Delete(ctx, &w)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("only users with the 'admin' role"))
		})

		It("should allow system:masters to delete workspace", func() {
			masterClient := createImpersonatedClient("cluster-admin", []string{"system:masters"})

			nsName := types.NamespacedName{Name: resourceName}
			var w tenantv1alpha1.Workspace
			Expect(k8sClient.Get(ctx, nsName, &w)).To(Succeed())

			Expect(masterClient.Delete(ctx, &w)).To(Succeed())

			workspace = nil
		})
	})

	Context("CRD CEL Validations", func() {
		It("should reject a Workspace with no admin member", func() {
			workspace = &tenantv1alpha1.Workspace{
				ObjectMeta: metav1.ObjectMeta{Name: resourceName},
				Spec: tenantv1alpha1.WorkspaceSpec{
					Namespace: string(uuid.NewUUID()),
					Members: []tenantv1alpha1.WorkspaceMember{
						{Role: tenantv1alpha1.MemberRoleView, Subject: "view-user"},
					},
				},
			}
			err := k8sClient.Create(ctx, workspace)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("at least one workspace member must have role 'admin'"))
		})

		It("should reject a reserved namespace", func() {
			workspace = &tenantv1alpha1.Workspace{
				ObjectMeta: metav1.ObjectMeta{Name: resourceName},
				Spec: tenantv1alpha1.WorkspaceSpec{
					Namespace: "kube-system",
					Members: []tenantv1alpha1.WorkspaceMember{
						{Role: tenantv1alpha1.MemberRoleAdmin, Subject: "admin-user"},
					},
				},
			}
			err := k8sClient.Create(ctx, workspace)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("namespace is reserved and cannot be used for a workspace"))
		})

		It("should reject invalid allowedNamespaces entries", func() {
			workspace = &tenantv1alpha1.Workspace{
				ObjectMeta: metav1.ObjectMeta{Name: resourceName},
				Spec: tenantv1alpha1.WorkspaceSpec{
					Namespace: string(uuid.NewUUID()),
					Members: []tenantv1alpha1.WorkspaceMember{
						{Role: tenantv1alpha1.MemberRoleAdmin, Subject: "admin-user"},
					},
					NetworkIsolation: tenantv1alpha1.NetworkIsolationSpec{
						Enabled:           true,
						AllowedNamespaces: []string{"BAD_NAMESPACE"},
					},
				},
			}
			err := k8sClient.Create(ctx, workspace)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("spec.networkIsolation.allowedNamespaces[0]"))
			Expect(err.Error()).To(ContainSubstring("should match '^([a-z0-9]"))
		})
	})

})
