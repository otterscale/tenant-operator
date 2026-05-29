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

package v1alpha1

import (
	"context"
	"maps"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	tenantv1alpha1 "github.com/otterscale/api/tenant/v1alpha1"
	"github.com/otterscale/tenant-operator/internal/workspace"
)

var _ = Describe("Workspace Webhook", func() {
	var (
		obj       *tenantv1alpha1.Workspace
		defaulter WorkspaceCustomDefaulter
	)

	BeforeEach(func() {
		obj = &tenantv1alpha1.Workspace{
			ObjectMeta: metav1.ObjectMeta{Name: "test-workspace"},
			Spec: tenantv1alpha1.WorkspaceSpec{
				Namespace: "test-ns",
				Members: []tenantv1alpha1.WorkspaceMember{
					{Role: tenantv1alpha1.MemberRoleAdmin, Subject: "admin-user"},
				},
			},
		}
		defaulter = WorkspaceCustomDefaulter{}
	})

	Context("Member Label Synchronization", func() {
		It("should mirror member subjects as labels on create", func() {
			obj.Spec.Members = []tenantv1alpha1.WorkspaceMember{
				{Role: tenantv1alpha1.MemberRoleAdmin, Subject: "admin-user"},
				{Role: tenantv1alpha1.MemberRoleView, Subject: "view-user"},
			}

			Expect(defaulter.Default(context.Background(), obj)).To(Succeed())

			Expect(obj.Labels).To(HaveKeyWithValue(workspace.UserLabelPrefix+"admin-user", "true"))
			Expect(obj.Labels).To(HaveKeyWithValue(workspace.UserLabelPrefix+"view-user", "true"))
		})

		It("should remove stale user labels when members are removed", func() {
			obj.Labels = map[string]string{
				workspace.UserLabelPrefix + "admin-user":   "true",
				workspace.UserLabelPrefix + "removed-user": "true",
			}
			obj.Spec.Members = []tenantv1alpha1.WorkspaceMember{
				{Role: tenantv1alpha1.MemberRoleAdmin, Subject: "admin-user"},
			}

			Expect(defaulter.Default(context.Background(), obj)).To(Succeed())

			Expect(obj.Labels).To(HaveKeyWithValue(workspace.UserLabelPrefix+"admin-user", "true"))
			Expect(obj.Labels).NotTo(HaveKey(workspace.UserLabelPrefix + "removed-user"))
		})

		It("should correctly sync labels when member list is completely replaced", func() {
			obj.Labels = map[string]string{
				workspace.UserLabelPrefix + "old-admin": "true",
				workspace.UserLabelPrefix + "old-view":  "true",
			}
			obj.Spec.Members = []tenantv1alpha1.WorkspaceMember{
				{Role: tenantv1alpha1.MemberRoleAdmin, Subject: "new-admin"},
				{Role: tenantv1alpha1.MemberRoleEdit, Subject: "new-editor"},
			}

			Expect(defaulter.Default(context.Background(), obj)).To(Succeed())

			Expect(obj.Labels).To(HaveKeyWithValue(workspace.UserLabelPrefix+"new-admin", "true"))
			Expect(obj.Labels).To(HaveKeyWithValue(workspace.UserLabelPrefix+"new-editor", "true"))
			Expect(obj.Labels).NotTo(HaveKey(workspace.UserLabelPrefix + "old-admin"))
			Expect(obj.Labels).NotTo(HaveKey(workspace.UserLabelPrefix + "old-view"))
		})

		It("should preserve non-user custom labels", func() {
			obj.Labels = map[string]string{
				"my-custom-label":                        "my-custom-value",
				"another-label":                          "another-value",
				workspace.UserLabelPrefix + "stale-user": "true",
			}
			obj.Spec.Members = []tenantv1alpha1.WorkspaceMember{
				{Role: tenantv1alpha1.MemberRoleAdmin, Subject: "admin-user"},
			}

			Expect(defaulter.Default(context.Background(), obj)).To(Succeed())

			Expect(obj.Labels).To(HaveKeyWithValue("my-custom-label", "my-custom-value"))
			Expect(obj.Labels).To(HaveKeyWithValue("another-label", "another-value"))
			Expect(obj.Labels).To(HaveKeyWithValue(workspace.UserLabelPrefix+"admin-user", "true"))
			Expect(obj.Labels).NotTo(HaveKey(workspace.UserLabelPrefix + "stale-user"))
		})

		It("should handle workspace with nil labels", func() {
			obj.Labels = nil
			obj.Spec.Members = []tenantv1alpha1.WorkspaceMember{
				{Role: tenantv1alpha1.MemberRoleAdmin, Subject: "admin-user"},
			}

			Expect(defaulter.Default(context.Background(), obj)).To(Succeed())

			Expect(obj.Labels).To(HaveKeyWithValue(workspace.UserLabelPrefix+"admin-user", "true"))
		})

		It("should handle members with special characters in their subject", func() {
			obj.Spec.Members = []tenantv1alpha1.WorkspaceMember{
				{Role: tenantv1alpha1.MemberRoleAdmin, Subject: "user.example.com"},
			}

			Expect(defaulter.Default(context.Background(), obj)).To(Succeed())

			Expect(obj.Labels).To(HaveKeyWithValue(workspace.UserLabelPrefix+"user.example.com", "true"))
		})

		It("should handle empty members slice without panic", func() {
			obj.Spec.Members = []tenantv1alpha1.WorkspaceMember{}

			Expect(defaulter.Default(context.Background(), obj)).To(Succeed())

			for k := range obj.Labels {
				Expect(k).NotTo(HavePrefix(workspace.UserLabelPrefix))
			}
		})

		It("should be idempotent across multiple invocations", func() {
			obj.Spec.Members = []tenantv1alpha1.WorkspaceMember{
				{Role: tenantv1alpha1.MemberRoleAdmin, Subject: "admin-user"},
				{Role: tenantv1alpha1.MemberRoleView, Subject: "view-user"},
			}

			Expect(defaulter.Default(context.Background(), obj)).To(Succeed())
			firstLabels := make(map[string]string)
			maps.Copy(firstLabels, obj.Labels)

			Expect(defaulter.Default(context.Background(), obj)).To(Succeed())
			Expect(obj.Labels).To(Equal(firstLabels))
		})

	})

	Context("Namespace Auto-Generation", func() {
		It("should auto-generate a 6-character namespace when empty", func() {
			obj.Spec.Namespace = ""

			Expect(defaulter.Default(context.Background(), obj)).To(Succeed())

			Expect(obj.Spec.Namespace).To(HaveLen(6))
			Expect(obj.Spec.Namespace).To(MatchRegexp(`^[a-z][a-z0-9]{5}$`))
		})

		It("should not overwrite namespace when explicitly provided", func() {
			obj.Spec.Namespace = "my-custom-ns"

			Expect(defaulter.Default(context.Background(), obj)).To(Succeed())

			Expect(obj.Spec.Namespace).To(Equal("my-custom-ns"))
		})

		It("should generate unique names on successive calls", func() {
			names := make(map[string]struct{}, 10)
			for range 10 {
				names[generateNamespaceName()] = struct{}{}
			}
			// With 6 random chars, 10 calls should produce at least 2 distinct values
			Expect(len(names)).To(BeNumerically(">=", 2))
		})
	})

})
