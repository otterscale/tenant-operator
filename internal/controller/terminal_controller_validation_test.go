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
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"

	consolev1alpha1 "github.com/otterscale/api/console/v1alpha1"
	"github.com/otterscale/tenant-operator/internal/terminal"
)

var _ = Describe("Terminal Controller - CEL Validation and Authorization", func() {
	const (
		timeout  = time.Second * 10
		interval = time.Millisecond * 250
	)

	var (
		ctx     context.Context
		term    *consolev1alpha1.Terminal
		subject string
	)

	BeforeEach(func() {
		ctx = context.Background()
		subject = string(uuid.NewUUID())
	})

	AfterEach(func() {
		if term != nil {
			key := types.NamespacedName{Name: term.Name, Namespace: "console"}
			if err := k8sClient.Get(ctx, key, term); err == nil {
				Expect(k8sClient.Delete(ctx, term)).To(Succeed())
				Eventually(func() bool {
					return errors.IsNotFound(k8sClient.Get(ctx, key, term))
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

	newTerminal := func(s string) *consolev1alpha1.Terminal {
		return &consolev1alpha1.Terminal{
			ObjectMeta: metav1.ObjectMeta{Name: terminal.PodName(s), Namespace: "console"},
			Spec:       consolev1alpha1.TerminalSpec{Subject: s},
		}
	}

	Context("CEL Validation", func() {
		It("should deny creation when metadata.name doesn't match term-<subject prefix>", func() {
			userClient := createImpersonatedClient(subject, nil)
			term = &consolev1alpha1.Terminal{
				ObjectMeta: metav1.ObjectMeta{Name: "term-wrongname", Namespace: "console"},
				Spec:       consolev1alpha1.TerminalSpec{Subject: subject},
			}
			err := userClient.Create(ctx, term)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("metadata.name must be term-"))
		})

		It("should deny creation when subject is not a lowercase UUID", func() {
			userClient := createImpersonatedClient("not-a-uuid", nil)
			term = &consolev1alpha1.Terminal{
				ObjectMeta: metav1.ObjectMeta{Name: "term-notauuid", Namespace: "console"},
				Spec:       consolev1alpha1.TerminalSpec{Subject: "not-a-uuid"},
			}
			err := userClient.Create(ctx, term)
			Expect(err).To(HaveOccurred())
		})

		It("should deny updating spec.subject after creation (immutable)", func() {
			userClient := createImpersonatedClient(subject, nil)
			term = newTerminal(subject)
			Expect(userClient.Create(ctx, term)).To(Succeed())

			term.Spec.Subject = string(uuid.NewUUID())
			err := userClient.Update(ctx, term)
			Expect(err).To(HaveOccurred())
		})
	})

	Context("Admission Policy - Create Authorization", func() {
		It("should allow creation when caller's identity matches spec.subject", func() {
			userClient := createImpersonatedClient(subject, nil)
			term = newTerminal(subject)
			Expect(userClient.Create(ctx, term)).To(Succeed())
		})

		It("should deny creation when caller's identity does not match spec.subject", func() {
			userClient := createImpersonatedClient("mallory", nil)
			term = newTerminal(subject)
			err := userClient.Create(ctx, term)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("must match the identity of the user"))
		})

		It("should allow system:masters to create a Terminal for another subject", func() {
			masterClient := createImpersonatedClient("cluster-admin", []string{"system:masters"})
			term = newTerminal(subject)
			Expect(masterClient.Create(ctx, term)).To(Succeed())
		})

		It("should allow user bound to cluster-admin ClusterRole to create for another subject", func() {
			binding := &rbacv1.ClusterRoleBinding{
				ObjectMeta: metav1.ObjectMeta{Name: "test-cluster-admin-create-" + subject},
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
			term = newTerminal(subject)
			Expect(caClient.Create(ctx, term)).To(Succeed())
		})
	})

	Context("Admission Policy - Update/Delete Authorization", func() {
		const controllerServiceAccount = "system:serviceaccount:otterscale-system:tenant-operator-controller-manager"

		BeforeEach(func() {
			term = newTerminal(subject)
			Expect(k8sClient.Create(ctx, term)).To(Succeed())
		})

		It("should allow the controller service account to update the Terminal", func() {
			saClient := createImpersonatedClient(controllerServiceAccount, nil)

			var t consolev1alpha1.Terminal
			key := types.NamespacedName{Name: term.Name, Namespace: "console"}
			Expect(k8sClient.Get(ctx, key, &t)).To(Succeed())

			t.Spec.IdleTimeoutSeconds = 60
			Expect(saClient.Update(ctx, &t)).To(Succeed())
		})

		It("should deny a different user's update", func() {
			otherClient := createImpersonatedClient("mallory", nil)

			var t consolev1alpha1.Terminal
			key := types.NamespacedName{Name: term.Name, Namespace: "console"}
			Expect(k8sClient.Get(ctx, key, &t)).To(Succeed())

			t.Spec.IdleTimeoutSeconds = 60
			err := otherClient.Update(ctx, &t)
			Expect(err).To(HaveOccurred())
		})

		It("should allow the Terminal's own subject to update it", func() {
			ownerClient := createImpersonatedClient(subject, nil)

			var t consolev1alpha1.Terminal
			key := types.NamespacedName{Name: term.Name, Namespace: "console"}
			Expect(k8sClient.Get(ctx, key, &t)).To(Succeed())

			t.Spec.IdleTimeoutSeconds = 60
			Expect(ownerClient.Update(ctx, &t)).To(Succeed())
		})

		It("should allow the Terminal's own subject to delete it", func() {
			ownerClient := createImpersonatedClient(subject, nil)

			var t consolev1alpha1.Terminal
			key := types.NamespacedName{Name: term.Name, Namespace: "console"}
			Expect(k8sClient.Get(ctx, key, &t)).To(Succeed())

			Expect(ownerClient.Delete(ctx, &t)).To(Succeed())

			// Prevent AfterEach from trying to delete again.
			term = nil
		})

		It("should deny a different user's delete", func() {
			otherClient := createImpersonatedClient("mallory", nil)

			var t consolev1alpha1.Terminal
			key := types.NamespacedName{Name: term.Name, Namespace: "console"}
			Expect(k8sClient.Get(ctx, key, &t)).To(Succeed())

			err := otherClient.Delete(ctx, &t)
			Expect(err).To(HaveOccurred())
		})
	})
})
