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
	"fmt"

	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlutil "sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	tenantv1alpha1 "github.com/otterscale/tenant-operator/api/v1alpha1"
)

// ReconcileNetworkIsolation ensures the NetworkPolicy matches the desired state.
// Enabled, it denies ingress except from the workspace's own namespace and any
// configured AllowedNamespaces; disabled, the NetworkPolicy is removed.
func ReconcileNetworkIsolation(ctx context.Context, c client.Client, scheme *runtime.Scheme, w *tenantv1alpha1.Workspace, version string) error {
	policy := &networkingv1.NetworkPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      NetworkPolicyName,
			Namespace: w.Spec.Namespace,
		},
	}

	if !w.Spec.NetworkIsolation.Enabled {
		if err := client.IgnoreNotFound(c.Delete(ctx, policy)); err != nil {
			return fmt.Errorf("deleting NetworkPolicy: %w", err)
		}
		return nil
	}

	op, err := ctrlutil.CreateOrUpdate(ctx, c, policy, func() error {
		policy.Labels = LabelsForWorkspace(w.Name, version)

		ingressRules := []networkingv1.NetworkPolicyIngressRule{
			{
				From: []networkingv1.NetworkPolicyPeer{
					{
						PodSelector: &metav1.LabelSelector{},
					},
				},
			},
		}

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
			PodSelector: metav1.LabelSelector{},
			PolicyTypes: []networkingv1.PolicyType{
				networkingv1.PolicyTypeIngress,
			},
			Ingress: ingressRules,
		}
		return ctrlutil.SetControllerReference(w, policy, scheme)
	})
	if err != nil {
		return fmt.Errorf("reconciling NetworkPolicy: %w", err)
	}
	if op != ctrlutil.OperationResultNone {
		log.FromContext(ctx).Info("NetworkPolicy reconciled", "operation", op, "name", policy.Name)
	}
	return nil
}
