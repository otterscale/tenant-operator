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
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	consolev1alpha1 "github.com/otterscale/api/console/v1alpha1"
	"github.com/otterscale/tenant-operator/internal/terminal"
)

var _ = Describe("Terminal Controller", func() {
	const (
		timeout  = time.Second * 10
		interval = time.Millisecond * 250
	)

	var (
		ctx        context.Context
		reconciler *TerminalReconciler
		term       *consolev1alpha1.Terminal
		subject    string
		podName    string
	)

	// --- Helpers ---

	executeReconcile := func() (reconcile.Result, error) {
		key := types.NamespacedName{Name: term.Name, Namespace: "console"}
		return reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
	}

	fetchTerminal := func() {
		key := types.NamespacedName{Name: term.Name, Namespace: "console"}
		Eventually(func() error {
			return k8sClient.Get(ctx, key, term)
		}, timeout, interval).Should(Succeed())
	}

	fetchPod := func() *corev1.Pod {
		var pod corev1.Pod
		key := types.NamespacedName{Name: podName, Namespace: "console"}
		Eventually(func() error {
			return k8sClient.Get(ctx, key, &pod)
		}, timeout, interval).Should(Succeed())
		return &pod
	}

	markPodReady := func() {
		pod := fetchPod()
		pod.Status.Phase = corev1.PodRunning
		pod.Status.Conditions = []corev1.PodCondition{
			{Type: corev1.PodReady, Status: corev1.ConditionTrue},
		}
		Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())
	}

	// --- Lifecycle ---

	BeforeEach(func() {
		ctx = context.Background()
		reconciler = &TerminalReconciler{
			Client:   k8sClient,
			Scheme:   k8sClient.Scheme(),
			Recorder: events.NewFakeRecorder(100),
		}
		subject = string(uuid.NewUUID())
		podName = terminal.PodName(subject)
		term = &consolev1alpha1.Terminal{
			ObjectMeta: metav1.ObjectMeta{
				Name:      terminal.PodName(subject),
				Namespace: "console",
			},
			Spec: consolev1alpha1.TerminalSpec{Subject: subject},
		}
	})

	JustBeforeEach(func() {
		Expect(k8sClient.Create(ctx, term)).To(Succeed())
	})

	AfterEach(func() {
		key := types.NamespacedName{Name: term.Name, Namespace: "console"}
		if err := k8sClient.Get(ctx, key, term); err == nil {
			Expect(k8sClient.Delete(ctx, term)).To(Succeed())
			Eventually(func() bool {
				return errors.IsNotFound(k8sClient.Get(ctx, key, term))
			}, timeout, interval).Should(BeTrue())
		}
	})

	Context("Pod creation", func() {
		It("should create the Pod and reflect Creating/Pending status before it's ready", func() {
			_, err := executeReconcile()
			Expect(err).NotTo(HaveOccurred())

			pod := fetchPod()
			Expect(pod.Labels).To(HaveKeyWithValue(terminal.SubjectLabel, subject))
			Expect(pod.OwnerReferences).To(HaveLen(1))
			Expect(pod.OwnerReferences[0].Name).To(Equal(term.Name))
			Expect(pod.OwnerReferences[0].Controller).NotTo(BeNil())
			Expect(*pod.OwnerReferences[0].Controller).To(BeTrue())

			fetchTerminal()
			Expect(term.Status.PodName).To(Equal(podName))
			Expect(term.Status.Phase).To(Equal("Creating"))
			Expect(term.Status.PodReady).To(BeFalse())

			By("reconciling again while the Pod is not yet Ready")
			_, err = executeReconcile()
			Expect(err).NotTo(HaveOccurred())
			fetchTerminal()
			Expect(term.Status.Phase).To(Equal("Pending"))
		})

		It("should mark status Ready once the Pod becomes Ready", func() {
			_, err := executeReconcile()
			Expect(err).NotTo(HaveOccurred())
			fetchPod()

			markPodReady()

			_, err = executeReconcile()
			Expect(err).NotTo(HaveOccurred())

			fetchTerminal()
			Expect(term.Status.Phase).To(Equal("Ready"))
			Expect(term.Status.PodReady).To(BeTrue())
			readyCond := findCondition(term.Status.Conditions, ConditionTypeReady)
			Expect(readyCond).NotTo(BeNil())
			Expect(readyCond.Status).To(Equal(metav1.ConditionTrue))
		})
	})

	Context("Pod self-healing", func() {
		It("should delete a terminated Pod and recreate a fresh one on the next reconcile", func() {
			_, err := executeReconcile()
			Expect(err).NotTo(HaveOccurred())
			firstPod := fetchPod()

			firstPod.Status.Phase = corev1.PodFailed
			Expect(k8sClient.Status().Update(ctx, firstPod)).To(Succeed())

			By("reconciling: the terminated Pod should be deleted")
			_, err = executeReconcile()
			Expect(err).NotTo(HaveOccurred())
			Eventually(func() bool {
				var p corev1.Pod
				key := types.NamespacedName{Name: podName, Namespace: "console"}
				return errors.IsNotFound(k8sClient.Get(ctx, key, &p))
			}, timeout, interval).Should(BeTrue())

			By("reconciling again: a fresh Pod should be created")
			_, err = executeReconcile()
			Expect(err).NotTo(HaveOccurred())
			secondPod := fetchPod()
			Expect(secondPod.UID).NotTo(Equal(firstPod.UID))
		})
	})

	Context("Idle garbage collection", func() {
		It("should not delete a freshly created Terminal", func() {
			result, err := executeReconcile()
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(BeNumerically(">", 0))

			key := types.NamespacedName{Name: term.Name, Namespace: "console"}
			Expect(k8sClient.Get(ctx, key, term)).To(Succeed())
		})

		It("should delete the Terminal once its idle timeout has elapsed", func() {
			_, err := executeReconcile()
			Expect(err).NotTo(HaveOccurred())

			fetchTerminal()
			past := metav1.NewTime(time.Now().Add(-2 * terminal.DefaultIdleTimeout))
			term.Status.LastActivityTime = &past
			Expect(k8sClient.Status().Update(ctx, term)).To(Succeed())

			_, err = executeReconcile()
			Expect(err).NotTo(HaveOccurred())

			key := types.NamespacedName{Name: term.Name, Namespace: "console"}
			Eventually(func() bool {
				return errors.IsNotFound(k8sClient.Get(ctx, key, term))
			}, timeout, interval).Should(BeTrue())
		})

		It("should fall back to creationTimestamp when lastActivityTime was never set", func() {
			// The Terminal was just created, so creationTimestamp is "now" —
			// well within terminal.DefaultIdleTimeout — regardless of
			// lastActivityTime being unset.
			result, err := executeReconcile()
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(BeNumerically("~", terminal.DefaultIdleTimeout, time.Minute))
		})
	})
})

func findCondition(conditions []metav1.Condition, condType string) *metav1.Condition {
	for i := range conditions {
		if conditions[i].Type == condType {
			return &conditions[i]
		}
	}
	return nil
}
