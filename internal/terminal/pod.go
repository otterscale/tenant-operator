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

package terminal

import (
	"context"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlutil "sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	consolev1alpha1 "github.com/otterscale/api/console/v1alpha1"
	"github.com/otterscale/tenant-operator/internal/labels"
)

const (
	// PodNamePrefix is prepended to the first 8 characters of a Terminal's
	// spec.subject to derive the name of its Pod. The CRD's own
	// x-kubernetes-validations rule enforces that a Terminal's own
	// metadata.name follows this exact same convention, so the Pod name is
	// always identical to the owning Terminal's name.
	PodNamePrefix = "term-"

	// SubjectLabel records the full (36-character) subject UUID on the Pod,
	// so the controller can tell apart a genuine "this Pod already belongs
	// to this Terminal" from an 8-character prefix collision between two
	// different Terminals' subjects.
	SubjectLabel = "console.otterscale.io/subject"

	terminalContainerName = "terminal"
	proxyContainerName    = "proxy"

	serviceAccountName           = "impersonation-proxy"
	imagePullSecretName          = "dhi-pull-secret"
	userKubeconfigConfigMapName  = "user-kubeconfig"
	proxyKubeconfigConfigMapName = "proxy-kubeconfig"

	proxyPort = 8001

	runAsUserAndGroup = 65532
)

// PodName returns the deterministic Pod name for a Terminal with the given
// subject: "term-" followed by the first 8 characters of the subject UUID.
func PodName(subject string) string {
	return PodNamePrefix + subject[:8]
}

// ReconcilePod ensures the Pod backing t exists in t's own namespace.
//
// Unlike most resources reconciled elsewhere in this operator, a Pod's
// containers/image/env are immutable once created, so this deliberately does
// NOT behave like ctrlutil.CreateOrUpdate: an existing Pod's spec is never
// touched. If the Pod already terminated (Phase Failed/Succeeded — it runs
// with restartPolicy: Never, so it never restarts on its own), it is deleted
// so the next reconcile creates a fresh one. If an existing Pod's
// SubjectLabel does not match t.Spec.Subject exactly, that Pod belongs to a
// different Terminal whose subject happens to share the same 8-character
// name prefix; ReconcilePod returns ErrSubjectCollision and leaves it alone.
//
// The returned Pod is nil when the Pod does not exist yet (either because it
// was never created, or because a terminated one was just deleted) — callers
// should treat a nil Pod as "not ready" and let the next reconcile (Owns()
// watch, or the caller's own requeue) pick up the newly created one.
func ReconcilePod(ctx context.Context, c client.Client, scheme *runtime.Scheme, t *consolev1alpha1.Terminal) (*corev1.Pod, error) {
	logger := log.FromContext(ctx)

	var pod corev1.Pod
	key := types.NamespacedName{Namespace: t.Namespace, Name: PodName(t.Spec.Subject)}
	err := c.Get(ctx, key, &pod)
	switch {
	case apierrors.IsNotFound(err):
		desired := buildPod(t)
		if err := ctrlutil.SetControllerReference(t, desired, scheme); err != nil {
			return nil, err
		}
		if err := c.Create(ctx, desired); err != nil {
			return nil, err
		}
		logger.Info("Pod created", "name", desired.Name)
		return nil, nil
	case err != nil:
		return nil, err
	}

	if pod.Labels[SubjectLabel] != t.Spec.Subject {
		return nil, &ErrSubjectCollision{PodName: pod.Name, PodSubject: pod.Labels[SubjectLabel], WantSubject: t.Spec.Subject}
	}

	if pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded {
		logger.Info("Pod terminated, deleting so it can be recreated", "name", pod.Name, "phase", pod.Status.Phase)
		if err := c.Delete(ctx, &pod); err != nil && !apierrors.IsNotFound(err) {
			return nil, err
		}
		return nil, nil
	}

	return &pod, nil
}

// ErrSubjectCollision indicates that the Pod name derived from two different
// Terminals' subjects collided (an 8-character UUID prefix collision). The
// controller surfaces this as status.phase=Failed rather than acting on
// either Pod, since automatically deleting someone else's live session would
// itself be a worse outcome than leaving it for a human to resolve.
type ErrSubjectCollision struct {
	PodName     string
	PodSubject  string
	WantSubject string
}

func (e *ErrSubjectCollision) Error() string {
	return "pod " + e.PodName + " belongs to subject " + e.PodSubject + ", not " + e.WantSubject + " (8-character name prefix collision)"
}

func buildPod(t *consolev1alpha1.Terminal) *corev1.Pod {
	image := t.Spec.Image
	if image == "" {
		image = DefaultImage
	}

	terminalResources := t.Spec.Resources.Terminal
	if isZeroResources(terminalResources) {
		terminalResources = defaultTerminalResources
	}
	proxyResources := t.Spec.Resources.Proxy
	if isZeroResources(proxyResources) {
		proxyResources = defaultProxyResources
	}

	podLabels := labels.Standard(PodName(t.Spec.Subject), "session", "")
	podLabels[SubjectLabel] = t.Spec.Subject

	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      PodName(t.Spec.Subject),
			Namespace: t.Namespace,
			Labels:    podLabels,
		},
		Spec: corev1.PodSpec{
			ServiceAccountName:           serviceAccountName,
			AutomountServiceAccountToken: ptr.To(false),
			ImagePullSecrets:             []corev1.LocalObjectReference{{Name: imagePullSecretName}},
			EnableServiceLinks:           ptr.To(false),
			RestartPolicy:                corev1.RestartPolicyNever,
			SecurityContext: &corev1.PodSecurityContext{
				RunAsNonRoot:   ptr.To(true),
				RunAsUser:      ptr.To(int64(runAsUserAndGroup)),
				RunAsGroup:     ptr.To(int64(runAsUserAndGroup)),
				FSGroup:        ptr.To(int64(runAsUserAndGroup)),
				SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
			},
			Containers: []corev1.Container{
				terminalContainer(image, terminalResources),
				proxyContainer(image, t.Spec.Subject, proxyResources),
			},
			Volumes: podVolumes(),
		},
	}
}

func terminalContainer(image string, resources corev1.ResourceRequirements) corev1.Container {
	return corev1.Container{
		Name:    terminalContainerName,
		Image:   image,
		Command: []string{"/bin/sh", "-c", "sleep infinity"},
		Env: []corev1.EnvVar{
			{Name: "KUBECONFIG", Value: "/etc/kubeconfig/config"},
			{Name: "HOME", Value: "/tmp/home"},
		},
		VolumeMounts: []corev1.VolumeMount{
			{Name: "kubeconfig", MountPath: "/etc/kubeconfig", ReadOnly: true},
			{Name: "tmp", MountPath: "/tmp"},
		},
		SecurityContext: &corev1.SecurityContext{
			AllowPrivilegeEscalation: ptr.To(false),
			ReadOnlyRootFilesystem:   ptr.To(true),
			Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
		},
		Resources: resources,
	}
}

func proxyContainer(image, subject string, resources corev1.ResourceRequirements) corev1.Container {
	return corev1.Container{
		Name:  proxyContainerName,
		Image: image,
		Command: []string{
			"/bin/sh", "-c",
			`exec kubectl proxy ` +
				`--kubeconfig=/etc/proxy-kubeconfig/config ` +
				`--address=127.0.0.1 ` +
				`--port=` + strconv.Itoa(proxyPort) + ` ` +
				`--as="$(USER_UUID)" ` +
				`--reject-paths='^/api/.*/pods/.*/(exec|attach|portforward)'`,
		},
		Env: []corev1.EnvVar{
			{Name: "USER_UUID", Value: subject},
		},
		VolumeMounts: []corev1.VolumeMount{
			{Name: "proxy-kubeconfig", MountPath: "/etc/proxy-kubeconfig", ReadOnly: true},
			{Name: "sa-token", MountPath: "/var/run/secrets/kubernetes.io/serviceaccount", ReadOnly: true},
			{Name: "proxy-tmp", MountPath: "/tmp"},
		},
		SecurityContext: &corev1.SecurityContext{
			AllowPrivilegeEscalation: ptr.To(false),
			ReadOnlyRootFilesystem:   ptr.To(true),
			Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
		},
		Resources: resources,
	}
}

func podVolumes() []corev1.Volume {
	return []corev1.Volume{
		{
			Name: "kubeconfig",
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: userKubeconfigConfigMapName},
				},
			},
		},
		{
			Name: "proxy-kubeconfig",
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: proxyKubeconfigConfigMapName},
				},
			},
		},
		{
			Name: "tmp",
			VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{SizeLimit: resourceQuantity("100Mi")},
			},
		},
		{
			Name: "proxy-tmp",
			VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{SizeLimit: resourceQuantity("10Mi")},
			},
		},
		{
			Name: "sa-token",
			VolumeSource: corev1.VolumeSource{
				Projected: &corev1.ProjectedVolumeSource{
					Sources: []corev1.VolumeProjection{
						{
							ServiceAccountToken: &corev1.ServiceAccountTokenProjection{
								Path:              "token",
								ExpirationSeconds: ptr.To(int64(3600)),
							},
						},
						{
							ConfigMap: &corev1.ConfigMapProjection{
								LocalObjectReference: corev1.LocalObjectReference{Name: "kube-root-ca.crt"},
								Items:                []corev1.KeyToPath{{Key: "ca.crt", Path: "ca.crt"}},
							},
						},
						{
							DownwardAPI: &corev1.DownwardAPIProjection{
								Items: []corev1.DownwardAPIVolumeFile{
									{Path: "namespace", FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"}},
								},
							},
						},
					},
				},
			},
		},
	}
}

func isZeroResources(r corev1.ResourceRequirements) bool {
	return len(r.Limits) == 0 && len(r.Requests) == 0 && len(r.Claims) == 0
}

func resourceQuantity(s string) *resource.Quantity {
	q := resource.MustParse(s)
	return &q
}
