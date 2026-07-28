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

package admissionpolicy

import (
	"maps"
	"os"
	"slices"
	"strings"
	"testing"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"
)

func TestWorkspaceFluxPolicies(t *testing.T) {
	t.Parallel()

	data, err := os.ReadFile("workspace_flux.yaml")
	if err != nil {
		t.Fatal(err)
	}

	wantPolicies := map[string]struct {
		group       string
		resource    string
		expressions []string
	}{
		"workspace-helmrelease.tenant.otterscale.io": {
			group:    "helm.toolkit.fluxcd.io",
			resource: "helmreleases",
			expressions: []string{
				"has(object.spec.serviceAccountName) && object.spec.serviceAccountName == 'workspace-reconciler'",
				"!has(object.spec.kubeConfig)",
				"!has(object.spec.targetNamespace) || object.spec.targetNamespace == object.metadata.namespace",
				"!has(object.spec.storageNamespace) || object.spec.storageNamespace == object.metadata.namespace",
				"!has(object.spec.chart) || !has(object.spec.chart.spec.sourceRef.namespace) || " +
					"object.spec.chart.spec.sourceRef.namespace == object.metadata.namespace",
				"!has(object.spec.chartRef) || !has(object.spec.chartRef.namespace) || " +
					"object.spec.chartRef.namespace == object.metadata.namespace",
				"!has(object.spec.dependsOn) || object.spec.dependsOn.all(ref, " +
					"!has(ref.namespace) || ref.namespace == object.metadata.namespace)",
			},
		},
		"workspace-kustomization.tenant.otterscale.io": {
			group:    "kustomize.toolkit.fluxcd.io",
			resource: "kustomizations",
			expressions: []string{
				"has(object.spec.serviceAccountName) && object.spec.serviceAccountName == 'workspace-reconciler'",
				"!has(object.spec.kubeConfig)",
				"!has(object.spec.targetNamespace) || object.spec.targetNamespace == object.metadata.namespace",
				"!has(object.spec.sourceRef.namespace) || object.spec.sourceRef.namespace == object.metadata.namespace",
				"!has(object.spec.dependsOn) || object.spec.dependsOn.all(ref, " +
					"!has(ref.namespace) || ref.namespace == object.metadata.namespace)",
			},
		},
	}

	policies := make(map[string]admissionregistrationv1.ValidatingAdmissionPolicy)
	bindings := make(map[string]admissionregistrationv1.ValidatingAdmissionPolicyBinding)
	for document := range strings.SplitSeq(string(data), "\n---\n") {
		var typeMeta metav1.TypeMeta
		if err := yaml.Unmarshal([]byte(document), &typeMeta); err != nil {
			t.Fatal(err)
		}
		switch typeMeta.Kind {
		case "ValidatingAdmissionPolicy":
			var policy admissionregistrationv1.ValidatingAdmissionPolicy
			if err := yaml.Unmarshal([]byte(document), &policy); err != nil {
				t.Fatal(err)
			}
			policies[policy.Name] = policy
		case "ValidatingAdmissionPolicyBinding":
			var binding admissionregistrationv1.ValidatingAdmissionPolicyBinding
			if err := yaml.Unmarshal([]byte(document), &binding); err != nil {
				t.Fatal(err)
			}
			bindings[binding.Name] = binding
		default:
			t.Fatalf("unexpected manifest kind %q", typeMeta.Kind)
		}
	}

	if len(policies) != len(wantPolicies) || len(bindings) != len(wantPolicies) {
		t.Fatalf("got %d policies and %d bindings, want %d of each", len(policies), len(bindings), len(wantPolicies))
	}

	for name, want := range wantPolicies {
		policy, ok := policies[name]
		if !ok {
			t.Errorf("policy %q not found", name)
			continue
		}
		assertWorkspacePolicy(t, &policy, want.group, want.resource, want.expressions)

		binding, ok := bindings[name]
		if !ok {
			t.Errorf("binding %q not found", name)
			continue
		}
		wantActions := []admissionregistrationv1.ValidationAction{admissionregistrationv1.Deny}
		if binding.Spec.PolicyName != name || !slices.Equal(binding.Spec.ValidationActions, wantActions) {
			t.Errorf("binding %q does not enforce policy with Deny: %#v", name, binding.Spec)
		}
	}
}

func assertWorkspacePolicy(
	t *testing.T,
	policy *admissionregistrationv1.ValidatingAdmissionPolicy,
	group string,
	resource string,
	expressions []string,
) {
	t.Helper()

	if policy.Spec.FailurePolicy == nil || *policy.Spec.FailurePolicy != admissionregistrationv1.Fail {
		t.Errorf("policy %q failurePolicy is not Fail", policy.Name)
	}
	constraints := policy.Spec.MatchConstraints
	if constraints == nil || constraints.NamespaceSelector == nil {
		t.Fatalf("policy %q has no namespaceSelector", policy.Name)
	}
	wantLabels := map[string]string{
		"app.kubernetes.io/component":  "workspace",
		"app.kubernetes.io/managed-by": "tenant-operator",
		"app.kubernetes.io/part-of":    "otterscale-system",
	}
	if !maps.Equal(constraints.NamespaceSelector.MatchLabels, wantLabels) {
		t.Errorf(
			"policy %q namespaceSelector = %#v, want %#v",
			policy.Name,
			constraints.NamespaceSelector.MatchLabels,
			wantLabels,
		)
	}
	if len(constraints.ResourceRules) != 1 {
		t.Fatalf("policy %q resource rules = %#v", policy.Name, constraints.ResourceRules)
	}
	rule := constraints.ResourceRules[0]
	groupsMatch := slices.Equal(rule.APIGroups, []string{group})
	versionsMatch := slices.Equal(rule.APIVersions, []string{"*"})
	resourcesMatch := slices.Equal(rule.Resources, []string{resource})
	if !groupsMatch || !versionsMatch || !resourcesMatch {
		t.Errorf("policy %q resource rule = %#v", policy.Name, rule)
	}
	wantOperations := []admissionregistrationv1.OperationType{
		admissionregistrationv1.Create,
		admissionregistrationv1.Update,
	}
	if !slices.Equal(rule.Operations, wantOperations) {
		t.Errorf("policy %q operations = %#v", policy.Name, rule.Operations)
	}

	gotExpressions := make([]string, 0, len(policy.Spec.Validations))
	for _, validation := range policy.Spec.Validations {
		gotExpressions = append(gotExpressions, validation.Expression)
	}
	if !slices.Equal(gotExpressions, expressions) {
		t.Errorf("policy %q expressions = %#v, want %#v", policy.Name, gotExpressions, expressions)
	}
}
