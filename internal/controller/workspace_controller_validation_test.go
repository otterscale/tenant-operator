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

	Context("Admission Policy - Service Account Exemption", func() {
		const controllerServiceAccount = "system:serviceaccount:otterscale-system:tenant-operator-controller-manager"

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
