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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	consolev1alpha1 "github.com/otterscale/api/console/v1alpha1"
	"github.com/otterscale/tenant-operator/internal/terminal"
)

var _ = Describe("Terminal Webhook", func() {
	var (
		obj       *consolev1alpha1.Terminal
		defaulter TerminalCustomDefaulter
	)

	BeforeEach(func() {
		obj = &consolev1alpha1.Terminal{
			ObjectMeta: metav1.ObjectMeta{Name: "term-792aa169", Namespace: "console"},
			Spec:       consolev1alpha1.TerminalSpec{Subject: "792aa169-7f46-4018-82a8-9834bd4ab853"},
		}
		defaulter = TerminalCustomDefaulter{}
	})

	Context("Subject Label Synchronization", func() {
		It("should set the subject label on create", func() {
			Expect(defaulter.Default(context.Background(), obj)).To(Succeed())

			Expect(obj.Labels).To(HaveKeyWithValue(terminal.SubjectLabel, "792aa169-7f46-4018-82a8-9834bd4ab853"))
		})

		It("should handle a Terminal with nil labels", func() {
			obj.Labels = nil

			Expect(defaulter.Default(context.Background(), obj)).To(Succeed())

			Expect(obj.Labels).To(HaveKeyWithValue(terminal.SubjectLabel, obj.Spec.Subject))
		})

		It("should preserve other existing labels", func() {
			obj.Labels = map[string]string{"my-custom-label": "my-custom-value"}

			Expect(defaulter.Default(context.Background(), obj)).To(Succeed())

			Expect(obj.Labels).To(HaveKeyWithValue("my-custom-label", "my-custom-value"))
			Expect(obj.Labels).To(HaveKeyWithValue(terminal.SubjectLabel, obj.Spec.Subject))
		})

		It("should overwrite a stale subject label", func() {
			obj.Labels = map[string]string{terminal.SubjectLabel: "stale-value"}

			Expect(defaulter.Default(context.Background(), obj)).To(Succeed())

			Expect(obj.Labels).To(HaveKeyWithValue(terminal.SubjectLabel, obj.Spec.Subject))
		})

		It("should be idempotent across multiple invocations", func() {
			Expect(defaulter.Default(context.Background(), obj)).To(Succeed())
			Expect(defaulter.Default(context.Background(), obj)).To(Succeed())

			Expect(obj.Labels).To(HaveLen(1))
			Expect(obj.Labels).To(HaveKeyWithValue(terminal.SubjectLabel, obj.Spec.Subject))
		})
	})
})
