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
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/client-go/rest"
)

var _ = Describe("IstioDetector", func() {
	Context("Initialization", func() {
		It("should default to disabled when Istio CRDs are not present", func() {
			detector := NewIstioDetector(&rest.Config{Host: "https://127.0.0.1:1"})
			Expect(detector.IsEnabled()).To(BeFalse())
		})
	})

	Context("State Management", func() {
		It("should report the correct state via IsEnabled", func() {
			detector := &IstioDetector{}

			By("Defaulting to false")
			Expect(detector.IsEnabled()).To(BeFalse())

			By("Setting to true via atomic store")
			detector.enabled.Store(true)
			Expect(detector.IsEnabled()).To(BeTrue())

			By("Setting back to false")
			detector.enabled.Store(false)
			Expect(detector.IsEnabled()).To(BeFalse())
		})

		It("should be safe for concurrent access", func() {
			detector := &IstioDetector{}
			done := make(chan struct{})

			go func() {
				defer close(done)
				for i := 0; i < 1000; i++ {
					detector.enabled.Store(true)
					detector.enabled.Store(false)
				}
			}()

			for i := 0; i < 1000; i++ {
				_ = detector.IsEnabled()
			}

			Eventually(done).Should(BeClosed())
		})
	})

	Context("Refresh", func() {
		It("should preserve previous state on unreachable API server", func() {
			// Create a detector with a cached DiscoveryClient pointing to an unreachable host.
			detector := NewIstioDetector(&rest.Config{Host: "https://127.0.0.1:1"})

			By("Manually enabling and then refreshing against unreachable server")
			detector.enabled.Store(true)
			detector.Refresh()

			By("State should be preserved (not flapped to false)")
			Expect(detector.IsEnabled()).To(BeTrue())
		})

		It("should be a no-op when dc is nil", func() {
			detector := &IstioDetector{}
			detector.enabled.Store(true)
			detector.Refresh()
			Expect(detector.IsEnabled()).To(BeTrue())
		})
	})
})
