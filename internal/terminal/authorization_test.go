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

package terminal_test

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	authenticationv1 "k8s.io/api/authentication/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	consolev1alpha1 "github.com/otterscale/api/console/v1alpha1"

	"github.com/otterscale/tenant-operator/internal/terminal"
)

const (
	testOperatorSA = "system:serviceaccount:test-system:test-controller-manager"
	testSubject    = "792aa169-7f46-4018-82a8-9834bd4ab853"
)

func newTerminal(subject string) *consolev1alpha1.Terminal {
	return &consolev1alpha1.Terminal{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "term-" + subject[:8],
			Namespace: "console",
		},
		Spec: consolev1alpha1.TerminalSpec{Subject: subject},
	}
}

func newFakeReader(objs ...runtime.Object) client.Reader {
	s := runtime.NewScheme()
	_ = rbacv1.AddToScheme(s)
	_ = consolev1alpha1.AddToScheme(s)
	return fake.NewClientBuilder().WithScheme(s).WithRuntimeObjects(objs...).Build()
}

var _ = Describe("AuthorizeCreation", func() {
	var (
		reader client.Reader
		term   *consolev1alpha1.Terminal
	)

	BeforeEach(func() {
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
		term = newTerminal(testSubject)
	})

	DescribeTable("should allow or deny based on caller identity",
		func(user authenticationv1.UserInfo, shouldSucceed bool) {
			err := terminal.AuthorizeCreation(context.Background(), reader, user, term, testOperatorSA)
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
		Entry("caller identity matches spec.subject is allowed",
			authenticationv1.UserInfo{Username: testSubject}, true),
		Entry("user bound to cluster-admin is allowed even when identity differs",
			authenticationv1.UserInfo{Username: "super-admin"}, true),
		Entry("a different subject's identity is denied",
			authenticationv1.UserInfo{Username: "11111111-1111-1111-1111-111111111111"}, false),
		Entry("unauthenticated/empty username is denied",
			authenticationv1.UserInfo{}, false),
		Entry("non-privileged group does not help",
			authenticationv1.UserInfo{Username: "mallory", Groups: []string{"system:authenticated"}}, false),
	)
})

var _ = Describe("AuthorizeModification", func() {
	var (
		reader client.Reader
		term   *consolev1alpha1.Terminal
	)

	BeforeEach(func() {
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
		term = newTerminal(testSubject)
	})

	DescribeTable("should allow or deny based on caller identity",
		func(user authenticationv1.UserInfo, shouldSucceed bool) {
			err := terminal.AuthorizeModification(context.Background(), reader, user, term, testOperatorSA)
			if shouldSucceed {
				Expect(err).NotTo(HaveOccurred())
			} else {
				Expect(err).To(HaveOccurred())
			}
		},
		Entry("system:masters group is allowed",
			authenticationv1.UserInfo{Username: "any-user", Groups: []string{"system:masters"}}, true),
		Entry("kubeadm:cluster-admins group is allowed",
			authenticationv1.UserInfo{Username: "any-user", Groups: []string{"kubeadm:cluster-admins"}}, true),
		Entry("operator service account is allowed",
			authenticationv1.UserInfo{Username: testOperatorSA}, true),
		Entry("the Terminal's own subject is allowed",
			authenticationv1.UserInfo{Username: testSubject}, true),
		Entry("user bound to cluster-admin via User subject",
			authenticationv1.UserInfo{Username: "super-admin"}, true),
		Entry("user in group bound to cluster-admin via Group subject",
			authenticationv1.UserInfo{Username: "someone", Groups: []string{"ops-team"}}, true),
		Entry("service account bound to cluster-admin via ServiceAccount subject",
			authenticationv1.UserInfo{Username: "system:serviceaccount:ci:deployer"}, true),
		Entry("a different user (even a real UUID) is denied",
			authenticationv1.UserInfo{Username: "11111111-1111-1111-1111-111111111111"}, false),
		Entry("user bound to non-privileged role only is denied",
			authenticationv1.UserInfo{Username: "viewer"}, false),
		Entry("unknown user is denied",
			authenticationv1.UserInfo{Username: "mallory"}, false),
		Entry("empty username with no groups is denied",
			authenticationv1.UserInfo{}, false),
	)

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
			r := newFakeReader(binding)

			err := terminal.AuthorizeModification(context.Background(), r, authenticationv1.UserInfo{Username: "tricky-user"}, newTerminal(testSubject), testOperatorSA)
			Expect(err).To(HaveOccurred())
		})
	})
})
