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

package workspace_test

import (
	"context"
	"fmt"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	authenticationv1 "k8s.io/api/authentication/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/validate/content"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	tenantv1alpha1 "github.com/otterscale/tenant-operator/api/v1alpha1"

	"github.com/otterscale/tenant-operator/internal/workspace"
)

const testOperatorSA = "system:serviceaccount:test-system:test-controller-manager"

func newWorkspace(members []tenantv1alpha1.WorkspaceMember) *tenantv1alpha1.Workspace {
	return newWorkspaceWithName("test-workspace", "test-ns", members)
}

func newWorkspaceWithName(name, namespace string, members []tenantv1alpha1.WorkspaceMember) *tenantv1alpha1.Workspace {
	labels := make(map[string]string, len(members))
	for _, m := range members {
		labels[workspace.UserLabelPrefix+m.Subject] = "true"
	}
	return &tenantv1alpha1.Workspace{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: labels,
		},
		Spec: tenantv1alpha1.WorkspaceSpec{
			Namespace: namespace,
			Members:   members,
		},
	}
}

func newFakeReader(objs ...runtime.Object) client.Reader {
	s := runtime.NewScheme()
	_ = rbacv1.AddToScheme(s)
	_ = tenantv1alpha1.AddToScheme(s)
	return fake.NewClientBuilder().WithScheme(s).WithRuntimeObjects(objs...).Build()
}

// ---------------------------------------------------------------------------

var _ = Describe("OperatorServiceAccountIdentity", func() {
	It("should compose the canonical service account username", func() {
		got := workspace.OperatorServiceAccountIdentity("otterscale-system", "tenant-operator-controller-manager")
		Expect(got).To(Equal("system:serviceaccount:otterscale-system:tenant-operator-controller-manager"))
	})
})

// ---------------------------------------------------------------------------
// AuthorizeCreation
// ---------------------------------------------------------------------------

var _ = Describe("AuthorizeCreation", func() {
	var (
		ctx    context.Context
		reader client.Reader
		ws     *tenantv1alpha1.Workspace
	)

	Context("role and identity checks", func() {
		BeforeEach(func() {
			ctx = context.Background()
			clusterAdminBinding := &rbacv1.ClusterRoleBinding{
				ObjectMeta: metav1.ObjectMeta{Name: "cluster-admin-binding"},
				RoleRef: rbacv1.RoleRef{
					APIGroup: rbacv1.GroupName,
					Kind:     "ClusterRole",
					Name:     "cluster-admin",
				},
				Subjects: []rbacv1.Subject{
					{Kind: rbacv1.UserKind, Name: "super-admin"},
				},
			}
			reader = newFakeReader(clusterAdminBinding)
			ws = newWorkspace([]tenantv1alpha1.WorkspaceMember{
				{Role: tenantv1alpha1.MemberRoleAdmin, Subject: "alice"},
				{Role: tenantv1alpha1.MemberRoleEdit, Subject: "bob"},
			})
		})

		DescribeTable("should allow or deny based on caller identity",
			func(user authenticationv1.UserInfo, shouldSucceed bool) {
				err := workspace.AuthorizeCreation(ctx, reader, user, ws, testOperatorSA)
				if shouldSucceed {
					Expect(err).NotTo(HaveOccurred())
				} else {
					Expect(err).To(HaveOccurred())
				}
			},
			Entry("privileged group bypasses all checks",
				authenticationv1.UserInfo{Username: "any-user", Groups: []string{"system:masters"}}, true),
			Entry("operator SA bypasses all checks",
				authenticationv1.UserInfo{Username: testOperatorSA}, true),
			Entry("creator listed as admin is allowed",
				authenticationv1.UserInfo{Username: "alice"}, true),
			Entry("user bound to cluster-admin is allowed even without admin role",
				authenticationv1.UserInfo{Username: "super-admin"}, true),
			Entry("creator listed as edit only is denied",
				authenticationv1.UserInfo{Username: "bob"}, false),
			Entry("creator not listed at all is denied",
				authenticationv1.UserInfo{Username: "mallory"}, false),
			Entry("non-privileged group does not help",
				authenticationv1.UserInfo{Username: "mallory", Groups: []string{"system:authenticated"}}, false),
		)
	})

})

// ---------------------------------------------------------------------------
// AuthorizeModification
// ---------------------------------------------------------------------------

var _ = Describe("AuthorizeModification", func() {
	var (
		ctx    context.Context
		reader client.Reader
		ws     *tenantv1alpha1.Workspace
	)

	Context("role and identity checks", func() {
		BeforeEach(func() {
			ctx = context.Background()
			clusterAdminBinding := &rbacv1.ClusterRoleBinding{
				ObjectMeta: metav1.ObjectMeta{Name: "cluster-admin-binding"},
				RoleRef: rbacv1.RoleRef{
					APIGroup: rbacv1.GroupName,
					Kind:     "ClusterRole",
					Name:     "cluster-admin",
				},
				Subjects: []rbacv1.Subject{
					{Kind: rbacv1.UserKind, Name: "super-admin"},
					{Kind: rbacv1.GroupKind, Name: "ops-team"},
					{Kind: rbacv1.ServiceAccountKind, Name: "deployer", Namespace: "ci"},
				},
			}
			nonPrivilegedBinding := &rbacv1.ClusterRoleBinding{
				ObjectMeta: metav1.ObjectMeta{Name: "view-binding"},
				RoleRef: rbacv1.RoleRef{
					APIGroup: rbacv1.GroupName,
					Kind:     "ClusterRole",
					Name:     "view",
				},
				Subjects: []rbacv1.Subject{
					{Kind: rbacv1.UserKind, Name: "viewer"},
				},
			}
			reader = newFakeReader(clusterAdminBinding, nonPrivilegedBinding)
			ws = newWorkspace([]tenantv1alpha1.WorkspaceMember{
				{Role: tenantv1alpha1.MemberRoleAdmin, Subject: "alice"},
				{Role: tenantv1alpha1.MemberRoleEdit, Subject: "bob"},
				{Role: tenantv1alpha1.MemberRoleView, Subject: "charlie"},
			})
		})

		DescribeTable("should allow or deny based on caller identity",
			func(user authenticationv1.UserInfo, shouldSucceed bool) {
				err := workspace.AuthorizeModification(ctx, reader, user, ws, testOperatorSA)
				if shouldSucceed {
					Expect(err).NotTo(HaveOccurred())
				} else {
					Expect(err).To(HaveOccurred())
				}
			},
			// Privileged groups
			Entry("system:masters group is allowed",
				authenticationv1.UserInfo{Username: "any-user", Groups: []string{"system:masters"}}, true),
			Entry("kubeadm:cluster-admins group is allowed",
				authenticationv1.UserInfo{Username: "any-user", Groups: []string{"kubeadm:cluster-admins"}}, true),
			Entry("privileged group among other groups",
				authenticationv1.UserInfo{Username: "any-user", Groups: []string{"dev-team", "system:masters", "ops"}}, true),
			// Operator SA
			Entry("operator service account is allowed",
				authenticationv1.UserInfo{Username: testOperatorSA}, true),
			// Workspace admin
			Entry("workspace admin member is allowed",
				authenticationv1.UserInfo{Username: "alice"}, true),
			// Privileged ClusterRole via ClusterRoleBinding
			Entry("user bound to cluster-admin via User subject",
				authenticationv1.UserInfo{Username: "super-admin"}, true),
			Entry("user in group bound to cluster-admin via Group subject",
				authenticationv1.UserInfo{Username: "someone", Groups: []string{"ops-team"}}, true),
			Entry("service account bound to cluster-admin via ServiceAccount subject",
				authenticationv1.UserInfo{Username: "system:serviceaccount:ci:deployer"}, true),
			// Denied
			Entry("workspace edit member is denied",
				authenticationv1.UserInfo{Username: "bob"}, false),
			Entry("workspace view member is denied",
				authenticationv1.UserInfo{Username: "charlie"}, false),
			Entry("user bound to non-privileged role only is denied",
				authenticationv1.UserInfo{Username: "viewer"}, false),
			Entry("unknown user is denied",
				authenticationv1.UserInfo{Username: "mallory"}, false),
			Entry("empty username with no groups is denied",
				authenticationv1.UserInfo{}, false),
			Entry("non-privileged group is denied",
				authenticationv1.UserInfo{Username: "mallory", Groups: []string{"system:authenticated", "dev-team"}}, false),
		)
	})

	Context("with empty members", func() {
		BeforeEach(func() {
			ctx = context.Background()
			reader = newFakeReader()
			ws = newWorkspace(nil)
		})

		It("should deny a regular user", func() {
			err := workspace.AuthorizeModification(ctx, reader, authenticationv1.UserInfo{Username: "alice"}, ws, testOperatorSA)
			Expect(err).To(HaveOccurred())
		})

		It("should allow a privileged user", func() {
			err := workspace.AuthorizeModification(ctx, reader, authenticationv1.UserInfo{
				Username: "admin",
				Groups:   []string{"system:masters"},
			}, ws, testOperatorSA)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Context("RoleRef.Kind validation", func() {
		It("should ignore bindings whose RoleRef.Kind is not ClusterRole", func() {
			binding := &rbacv1.ClusterRoleBinding{
				ObjectMeta: metav1.ObjectMeta{Name: "role-not-clusterrole"},
				RoleRef: rbacv1.RoleRef{
					APIGroup: rbacv1.GroupName,
					Kind:     "Role",
					Name:     "cluster-admin",
				},
				Subjects: []rbacv1.Subject{
					{Kind: rbacv1.UserKind, Name: "tricky-user"},
				},
			}
			reader := newFakeReader(binding)
			ws := newWorkspace(nil)

			err := workspace.AuthorizeModification(context.Background(), reader, authenticationv1.UserInfo{Username: "tricky-user"}, ws, testOperatorSA)
			Expect(err).To(HaveOccurred())
		})
	})
})

// ---------------------------------------------------------------------------
// ValidateNamespaceUniqueness
// ---------------------------------------------------------------------------

var _ = Describe("ValidateNamespaceUniqueness", func() {
	var (
		ctx    context.Context
		reader client.Reader
	)

	BeforeEach(func() {
		ctx = context.Background()
	})

	It("should allow creation when no other workspace uses the namespace", func() {
		reader = newFakeReader(
			newWorkspaceWithName("ws-other", "ns-other", []tenantv1alpha1.WorkspaceMember{
				{Role: tenantv1alpha1.MemberRoleAdmin, Subject: "alice"},
			}),
		)
		ws := newWorkspaceWithName("ws-new", "ns-new", []tenantv1alpha1.WorkspaceMember{
			{Role: tenantv1alpha1.MemberRoleAdmin, Subject: "bob"},
		})

		err := workspace.ValidateNamespaceUniqueness(ctx, reader, ws)
		Expect(err).NotTo(HaveOccurred())
	})

	It("should deny creation when another workspace already uses the namespace", func() {
		reader = newFakeReader(
			newWorkspaceWithName("ws-existing", "shared-ns", []tenantv1alpha1.WorkspaceMember{
				{Role: tenantv1alpha1.MemberRoleAdmin, Subject: "alice"},
			}),
		)
		ws := newWorkspaceWithName("ws-conflict", "shared-ns", []tenantv1alpha1.WorkspaceMember{
			{Role: tenantv1alpha1.MemberRoleAdmin, Subject: "bob"},
		})

		err := workspace.ValidateNamespaceUniqueness(ctx, reader, ws)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring(`namespace "shared-ns" is already used by workspace "ws-existing"`))
	})

	It("should not conflict with itself (same name)", func() {
		reader = newFakeReader(
			newWorkspaceWithName("ws-self", "ns-self", []tenantv1alpha1.WorkspaceMember{
				{Role: tenantv1alpha1.MemberRoleAdmin, Subject: "alice"},
			}),
		)
		ws := newWorkspaceWithName("ws-self", "ns-self", []tenantv1alpha1.WorkspaceMember{
			{Role: tenantv1alpha1.MemberRoleAdmin, Subject: "alice"},
		})

		err := workspace.ValidateNamespaceUniqueness(ctx, reader, ws)
		Expect(err).NotTo(HaveOccurred())
	})

	It("should allow creation when no workspaces exist", func() {
		reader = newFakeReader()
		ws := newWorkspaceWithName("ws-first", "ns-first", []tenantv1alpha1.WorkspaceMember{
			{Role: tenantv1alpha1.MemberRoleAdmin, Subject: "alice"},
		})

		err := workspace.ValidateNamespaceUniqueness(ctx, reader, ws)
		Expect(err).NotTo(HaveOccurred())
	})
})

// ---------------------------------------------------------------------------
// ValidateWorkspaceName
// ---------------------------------------------------------------------------

var _ = Describe("ValidateWorkspaceName", func() {
	It("should allow a name shorter than the label value limit", func() {
		err := workspace.ValidateWorkspaceName(strings.Repeat("a", content.LabelValueMaxLength-1))
		Expect(err).NotTo(HaveOccurred())
	})

	It("should allow a name at the label value limit", func() {
		err := workspace.ValidateWorkspaceName(strings.Repeat("a", content.LabelValueMaxLength))
		Expect(err).NotTo(HaveOccurred())
	})

	It("should deny a name longer than the label value limit", func() {
		err := workspace.ValidateWorkspaceName(strings.Repeat("a", content.LabelValueMaxLength+1))
		Expect(err).To(MatchError(fmt.Sprintf(
			"workspace metadata.name must be no more than %d bytes", content.LabelValueMaxLength)))
	})
})
