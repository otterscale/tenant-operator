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
	"slices"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	authenticationv1 "k8s.io/api/authentication/v1"
	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/validate/content"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	tenantv1alpha1 "github.com/otterscale/tenant-operator/api/v1alpha1"

	"github.com/otterscale/tenant-operator/internal/workspace"
)

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

// newFakeClient builds a client that stands in for the cluster authorizer:
// a SubjectAccessReview is answered "allowed" when its user, or any of its
// groups, appears in clusterWide. Everything else behaves as the normal fake.
//
// The production check is a wildcard SubjectAccessReview, so the fake only has
// to decide who the authorizer would wave through — it does not model RBAC.
func newFakeClient(clusterWide []string, objs ...runtime.Object) client.Client {
	s := runtime.NewScheme()
	_ = corev1.AddToScheme(s)
	_ = authorizationv1.AddToScheme(s)
	_ = tenantv1alpha1.AddToScheme(s)

	allowed := func(review *authorizationv1.SubjectAccessReview) bool {
		if slices.Contains(clusterWide, review.Spec.User) {
			return true
		}
		return slices.ContainsFunc(review.Spec.Groups, func(g string) bool {
			return slices.Contains(clusterWide, g)
		})
	}

	return fake.NewClientBuilder().
		WithScheme(s).
		WithRuntimeObjects(objs...).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				review, ok := obj.(*authorizationv1.SubjectAccessReview)
				if !ok {
					return c.Create(ctx, obj, opts...)
				}
				review.Status.Allowed = allowed(review)
				return nil
			},
		}).
		Build()
}

// ---------------------------------------------------------------------------
// AuthorizeCreation
// ---------------------------------------------------------------------------

var _ = Describe("AuthorizeCreation", func() {
	var (
		ctx context.Context
		c   client.Client
		ws  *tenantv1alpha1.Workspace
	)

	Context("role and identity checks", func() {
		BeforeEach(func() {
			ctx = context.Background()
			c = newFakeClient([]string{"super-admin"})
			ws = newWorkspace([]tenantv1alpha1.WorkspaceMember{
				{Role: tenantv1alpha1.MemberRoleAdmin, Subject: "alice"},
				{Role: tenantv1alpha1.MemberRoleEdit, Subject: "bob"},
			})
		})

		DescribeTable("should allow or deny based on caller identity",
			func(user authenticationv1.UserInfo, shouldSucceed bool) {
				err := workspace.AuthorizeCreation(ctx, c, user, ws)
				if shouldSucceed {
					Expect(err).NotTo(HaveOccurred())
				} else {
					Expect(err).To(HaveOccurred())
				}
			},
			Entry("privileged group bypasses all checks",
				authenticationv1.UserInfo{Username: "any-user", Groups: []string{"system:masters"}}, true),
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
		ctx context.Context
		c   client.Client
		ws  *tenantv1alpha1.Workspace
	)

	Context("role and identity checks", func() {
		BeforeEach(func() {
			ctx = context.Background()
			// The authorizer grants cluster-wide access to a user, a group, and a
			// service account; "viewer" stands for a user it knows but does not
			// grant that access to.
			c = newFakeClient([]string{
				"super-admin",
				"ops-team",
				"system:serviceaccount:ci:deployer",
			})
			ws = newWorkspace([]tenantv1alpha1.WorkspaceMember{
				{Role: tenantv1alpha1.MemberRoleAdmin, Subject: "alice"},
				{Role: tenantv1alpha1.MemberRoleEdit, Subject: "bob"},
				{Role: tenantv1alpha1.MemberRoleView, Subject: "charlie"},
			})
		})

		DescribeTable("should allow or deny based on caller identity",
			func(user authenticationv1.UserInfo, shouldSucceed bool) {
				err := workspace.AuthorizeModification(ctx, c, user, ws)
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
			// Workspace admin
			Entry("workspace admin member is allowed",
				authenticationv1.UserInfo{Username: "alice"}, true),
			// Cluster-wide access, as answered by the authorizer
			Entry("user granted cluster-wide access is allowed",
				authenticationv1.UserInfo{Username: "super-admin"}, true),
			Entry("user whose group is granted cluster-wide access is allowed",
				authenticationv1.UserInfo{Username: "someone", Groups: []string{"ops-team"}}, true),
			Entry("service account granted cluster-wide access is allowed",
				authenticationv1.UserInfo{Username: "system:serviceaccount:ci:deployer"}, true),
			// Denied
			Entry("workspace edit member is denied",
				authenticationv1.UserInfo{Username: "bob"}, false),
			Entry("workspace view member is denied",
				authenticationv1.UserInfo{Username: "charlie"}, false),
			Entry("user without cluster-wide access is denied",
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
			c = newFakeClient(nil)
			ws = newWorkspace(nil)
		})

		It("should deny a regular user", func() {
			err := workspace.AuthorizeModification(ctx, c, authenticationv1.UserInfo{Username: "alice"}, ws)
			Expect(err).To(HaveOccurred())
		})

		It("should allow a privileged user", func() {
			err := workspace.AuthorizeModification(ctx, c, authenticationv1.UserInfo{
				Username: "admin",
				Groups:   []string{"system:masters"},
			}, ws)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	// The authorizer is the only source of truth now, so a caller it denies is
	// denied however the cluster's RBAC happens to be shaped.
	Context("authorizer failures", func() {
		It("should deny a user the authorizer does not grant cluster-wide access", func() {
			err := workspace.AuthorizeModification(context.Background(), newFakeClient([]string{"someone-else"}),
				authenticationv1.UserInfo{Username: "tricky-user"}, newWorkspace(nil))
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
		reader = newFakeClient(nil,
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
		reader = newFakeClient(nil,
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
		reader = newFakeClient(nil,
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
		reader = newFakeClient(nil)
		ws := newWorkspaceWithName("ws-first", "ns-first", []tenantv1alpha1.WorkspaceMember{
			{Role: tenantv1alpha1.MemberRoleAdmin, Subject: "alice"},
		})

		err := workspace.ValidateNamespaceUniqueness(ctx, reader, ws)
		Expect(err).NotTo(HaveOccurred())
	})
})

// ---------------------------------------------------------------------------
// ValidateNamespaceAvailable
// ---------------------------------------------------------------------------

var _ = Describe("ValidateNamespaceAvailable", func() {
	var ctx context.Context

	BeforeEach(func() {
		ctx = context.Background()
	})

	newNamespace := func(name string) *corev1.Namespace {
		return &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}
	}

	It("should allow a namespace nothing has created yet", func() {
		reader := newFakeClient(nil, newNamespace("someone-elses-ns"))
		ws := newWorkspaceWithName("ws-new", "ns-free", nil)

		Expect(workspace.ValidateNamespaceAvailable(ctx, reader, ws)).To(Succeed())
	})

	// Admitting this would produce a NamespaceConflictError on every reconcile,
	// with no way out: reconcile will not adopt the namespace and spec.namespace
	// is immutable. The rejection has to happen here or not at all.
	It("should deny a namespace that already exists", func() {
		reader := newFakeClient(nil, newNamespace("taken-ns"))
		ws := newWorkspaceWithName("ws-new", "taken-ns", nil)

		err := workspace.ValidateNamespaceAvailable(ctx, reader, ws)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring(`namespace "taken-ns" already exists`))
		Expect(err.Error()).To(ContainSubstring("spec.namespace"), "the message should say how to proceed")
	})

	It("should allow when the cluster has no namespaces at all", func() {
		reader := newFakeClient(nil)
		ws := newWorkspaceWithName("ws-first", "ns-first", nil)

		Expect(workspace.ValidateNamespaceAvailable(ctx, reader, ws)).To(Succeed())
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
