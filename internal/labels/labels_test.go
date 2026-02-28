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

package labels_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/otterscale/tenant-operator/internal/labels"
)

var _ = Describe("Standard", func() {
	Describe("return value completeness", func() {
		It("contains exactly the expected labels when version is non-empty", func() {
			got := labels.Standard("my-app", "controller", "v1.2.3")

			Expect(got).To(Equal(map[string]string{
				labels.Name:      "my-app",
				labels.Component: "controller",
				labels.PartOf:    labels.System,
				labels.ManagedBy: labels.Operator,
				labels.Version:   "v1.2.3",
			}))
		})

		It("omits the version label when version is empty", func() {
			got := labels.Standard("my-app", "controller", "")

			Expect(got).To(Equal(map[string]string{
				labels.Name:      "my-app",
				labels.Component: "controller",
				labels.PartOf:    labels.System,
				labels.ManagedBy: labels.Operator,
			}))
			Expect(got).NotTo(HaveKey(labels.Version))
		})
	})

	Describe("variable arguments are passed through", func() {
		DescribeTable("name, component and version",
			func(name, component, version string) {
				got := labels.Standard(name, component, version)

				Expect(got).To(HaveKeyWithValue(labels.Name, name))
				Expect(got).To(HaveKeyWithValue(labels.Component, component))
				if version != "" {
					Expect(got).To(HaveKeyWithValue(labels.Version, version))
				} else {
					Expect(got).NotTo(HaveKey(labels.Version))
				}
			},
			Entry("typical release", "workspace", "webhook", "v0.1.0"),
			Entry("empty version omits label", "module", "controller", ""),
			Entry("all empty strings", "", "", ""),
		)
	})

	Describe("fixed labels are immutable regardless of arguments", func() {
		DescribeTable("PartOf and ManagedBy are always set to the operator identity",
			func(name, component, version string) {
				got := labels.Standard(name, component, version)

				Expect(got).To(HaveKeyWithValue(labels.PartOf, labels.System))
				Expect(got).To(HaveKeyWithValue(labels.ManagedBy, labels.Operator))
			},
			Entry("with version", "any", "any", "v1.0.0"),
			Entry("without version", "any", "any", ""),
			Entry("all empty", "", "", ""),
		)

		It("System constant equals otterscale-system", func() {
			Expect(labels.System).To(Equal("otterscale-system"))
		})

		It("Operator constant equals tenant-operator", func() {
			Expect(labels.Operator).To(Equal("tenant-operator"))
		})
	})
})
