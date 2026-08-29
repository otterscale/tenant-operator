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
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	tenantv1alpha1 "github.com/otterscale/tenant-operator/api/v1alpha1"
	"github.com/otterscale/tenant-operator/internal/workspace"
)

// newNamespaceReader returns a reader holding exactly the named Namespaces, so
// a spec can say which generated names are already taken.
func newNamespaceReader(names ...string) client.Reader {
	s := runtime.NewScheme()
	Expect(corev1.AddToScheme(s)).To(Succeed())

	existing := make([]runtime.Object, 0, len(names))
	for _, name := range names {
		existing = append(existing, &corev1.Namespace{Name: name})
	}
	return fake.NewClientBuilder().WithScheme(s).WithRuntimeObjects(existing...).Build()
}

// rejectFirstReader reports the first n names it is asked about as taken and
// every later one as free, recording what it was asked. That makes the retry
// observable, in a name space too large to collide in on purpose.
type rejectFirstReader struct {
	client.Reader
	remaining int
	asked     []string
}

func (r *rejectFirstReader) Get(_ context.Context, key client.ObjectKey, _ client.Object, _ ...client.GetOption) error {
	r.asked = append(r.asked, key.Name)
	if r.remaining > 0 {
		r.remaining--
		return nil // found — the name is taken
	}
	return apierrors.NewNotFound(corev1.Resource("namespaces"), key.Name)
}

var _ = Describe("Workspace Webhook", func() {
	var (
		obj       *tenantv1alpha1.Workspace
		defaulter WorkspaceCustomDefaulter
	)

	BeforeEach(func() {
		obj = &tenantv1alpha1.Workspace{
			Name: "test-workspace",
			Spec: tenantv1alpha1.WorkspaceSpec{
				Namespace: "test-ns",
				Members: []tenantv1alpha1.WorkspaceMember{
					{Role: tenantv1alpha1.MemberRoleAdmin, Subject: "admin-user"},
				},
			},
		}
		defaulter = WorkspaceCustomDefaulter{Reader: newNamespaceReader()}
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
			Expect(len(names)).To(BeNumerically(">=", 2))
		})

		// A generated name landing on an existing Namespace would produce a
		// Workspace that can never reconcile and can never be renamed.
		It("should discard generated names an existing namespace already holds", func() {
			reader := &rejectFirstReader{remaining: 3}
			defaulter.Reader = reader
			obj.Spec.Namespace = ""

			Expect(defaulter.Default(context.Background(), obj)).To(Succeed())

			Expect(reader.asked).To(HaveLen(4), "should have kept looking past the three taken names")
			Expect(reader.asked[:3]).NotTo(ContainElement(obj.Spec.Namespace),
				"a name reported as taken must not be handed out")
			Expect(obj.Spec.Namespace).To(Equal(reader.asked[3]))
		})

		It("should fail admission rather than reuse a name when every candidate is taken", func() {
			reader := &rejectFirstReader{remaining: namespaceNameAttempts}
			defaulter.Reader = reader
			obj.Spec.Namespace = ""

			err := defaulter.Default(context.Background(), obj)
			Expect(err).To(MatchError(ContainSubstring("could not generate an unused namespace name")))
			Expect(reader.asked).To(HaveLen(namespaceNameAttempts), "the retry must be bounded")
			Expect(obj.Spec.Namespace).To(BeEmpty(), "a failed generation must not leave a name behind")
		})
	})

})
