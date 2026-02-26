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

package workspace

import (
	"context"

	istiosecurityv1 "istio.io/api/security/v1"
	istiotypev1beta1 "istio.io/api/type/v1beta1"
	istioapisecurityv1 "istio.io/client-go/pkg/apis/security/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlutil "sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	tenantv1alpha1 "github.com/otterscale/api/tenant/v1alpha1"
)

// ReconcileNetworkIsolation decides whether to use Istio or standard NetworkPolicy
// and reconciles all related resources.
func ReconcileNetworkIsolation(ctx context.Context, c client.Client, scheme *runtime.Scheme, w *tenantv1alpha1.Workspace, version string, istioEnabled bool) error {
	if err := reconcileNetworkPolicy(ctx, c, scheme, w, version, istioEnabled); err != nil {
		return err
	}
	if err := reconcilePeerAuthentication(ctx, c, scheme, w, version, istioEnabled); err != nil {
		return err
	}
	return reconcileAuthorizationPolicy(ctx, c, scheme, w, version, istioEnabled)
}

// reconcileNetworkPolicy creates K8s NetworkPolicies for environments without Istio.
func reconcileNetworkPolicy(ctx context.Context, c client.Client, scheme *runtime.Scheme, w *tenantv1alpha1.Workspace, version string, istioEnabled bool) error {
	policy := &networkingv1.NetworkPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      NetworkPolicyName,
			Namespace: w.Spec.Namespace,
		},
	}

	if !w.Spec.NetworkIsolation.Enabled || istioEnabled {
		return client.IgnoreNotFound(c.Delete(ctx, policy))
	}

	op, err := ctrlutil.CreateOrUpdate(ctx, c, policy, func() error {
		policy.Labels = LabelsForWorkspace(w.Name, version)

		// Rule 1: Allow traffic from within the same namespace
		ingressRules := []networkingv1.NetworkPolicyIngressRule{
			{
				From: []networkingv1.NetworkPolicyPeer{
					{
						PodSelector: &metav1.LabelSelector{}, // Empty selector means all pods in this namespace
					},
				},
			},
		}

		// Rule 2: Allow traffic from allowed namespaces
		for _, namespace := range w.Spec.NetworkIsolation.AllowedNamespaces {
			ingressRules = append(ingressRules, networkingv1.NetworkPolicyIngressRule{
				From: []networkingv1.NetworkPolicyPeer{
					{
						NamespaceSelector: &metav1.LabelSelector{
							MatchLabels: map[string]string{
								"kubernetes.io/metadata.name": namespace,
							},
						},
					},
				},
			})
		}

		policy.Spec = networkingv1.NetworkPolicySpec{
			PodSelector: metav1.LabelSelector{}, // Apply to all pods
			PolicyTypes: []networkingv1.PolicyType{
				networkingv1.PolicyTypeIngress,
			},
			Ingress: ingressRules,
		}
		return ctrlutil.SetControllerReference(w, policy, scheme)
	})
	if err != nil {
		return err
	}
	if op != ctrlutil.OperationResultNone {
		log.FromContext(ctx).Info("NetworkPolicy reconciled", "operation", op, "name", policy.Name)
	}
	return nil
}

// reconcilePeerAuthentication enables strict mTLS when network isolation is enabled in Istio.
func reconcilePeerAuthentication(ctx context.Context, c client.Client, scheme *runtime.Scheme, w *tenantv1alpha1.Workspace, version string, istioEnabled bool) error {
	peer := &istioapisecurityv1.PeerAuthentication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      PeerAuthenticationName,
			Namespace: w.Spec.Namespace,
		},
	}

	if !w.Spec.NetworkIsolation.Enabled || !istioEnabled {
		return IgnoreNoMatchNotFound(c.Delete(ctx, peer))
	}

	op, err := ctrlutil.CreateOrUpdate(ctx, c, peer, func() error {
		peer.Labels = LabelsForWorkspace(w.Name, version)
		peer.Spec = istiosecurityv1.PeerAuthentication{
			Selector: &istiotypev1beta1.WorkloadSelector{MatchLabels: map[string]string{}},
			Mtls: &istiosecurityv1.PeerAuthentication_MutualTLS{
				Mode: istiosecurityv1.PeerAuthentication_MutualTLS_STRICT,
			},
		}
		return ctrlutil.SetControllerReference(w, peer, scheme)
	})
	if err != nil {
		return err
	}
	if op != ctrlutil.OperationResultNone {
		log.FromContext(ctx).Info("PeerAuthentication reconciled", "operation", op, "name", peer.Name)
	}
	return nil
}

// reconcileAuthorizationPolicy creates Istio AuthorizationPolicies for network isolation.
func reconcileAuthorizationPolicy(ctx context.Context, c client.Client, scheme *runtime.Scheme, w *tenantv1alpha1.Workspace, version string, istioEnabled bool) error {
	policy := &istioapisecurityv1.AuthorizationPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      AuthorizationPolicyName,
			Namespace: w.Spec.Namespace,
		},
	}

	if !w.Spec.NetworkIsolation.Enabled || !istioEnabled {
		return IgnoreNoMatchNotFound(c.Delete(ctx, policy))
	}

	op, err := ctrlutil.CreateOrUpdate(ctx, c, policy, func() error {
		policy.Labels = LabelsForWorkspace(w.Name, version)

		allowedNamespaces := []string{w.Spec.Namespace} // Always allow traffic within the workspace
		allowedNamespaces = append(allowedNamespaces, w.Spec.NetworkIsolation.AllowedNamespaces...)

		policy.Spec = istiosecurityv1.AuthorizationPolicy{
			Selector: &istiotypev1beta1.WorkloadSelector{
				MatchLabels: map[string]string{}, // Apply to all workloads
			},
			Rules: []*istiosecurityv1.Rule{
				{
					From: []*istiosecurityv1.Rule_From{
						{
							Source: &istiosecurityv1.Source{
								Namespaces: allowedNamespaces,
							},
						},
					},
				},
			},
			Action: istiosecurityv1.AuthorizationPolicy_ALLOW,
		}
		return ctrlutil.SetControllerReference(w, policy, scheme)
	})
	if err != nil {
		return err
	}
	if op != ctrlutil.OperationResultNone {
		log.FromContext(ctx).Info("AuthorizationPolicy reconciled", "operation", op, "name", policy.Name)
	}
	return nil
}
