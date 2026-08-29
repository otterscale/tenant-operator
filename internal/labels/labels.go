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

// Package labels provides the Kubernetes recommended label constants and
// builders shared by all operator-managed resources.
//
// See: https://kubernetes.io/docs/concepts/overview/working-with-objects/common-labels/
package labels

const (
	// Name identifies the application name (Kubernetes Recommended Label).
	Name = "app.kubernetes.io/name"

	// Component identifies the component within the architecture (e.g. "module", "workspace").
	Component = "app.kubernetes.io/component"

	// PartOf identifies the higher-level application this resource belongs to.
	PartOf = "app.kubernetes.io/part-of"

	// ManagedBy identifies the tool/operator that manages the resource.
	ManagedBy = "app.kubernetes.io/managed-by"

	// Version identifies the current version of the application.
	Version = "app.kubernetes.io/version"

	// System is the fixed PartOf value shared across all OtterScale operators.
	System = "otterscale-system"

	// Operator is the ManagedBy value for this operator.
	Operator = "tenant-operator"
)

// Standard returns the base Kubernetes recommended labels for operator-managed
// resources; callers add their own domain-specific labels on top.
//
// An empty version omits the app.kubernetes.io/version label, which carries no
// meaning when blank.
func Standard(name, component, version string) map[string]string {
	m := map[string]string{
		Name:      name,
		Component: component,
		PartOf:    System,
		ManagedBy: Operator,
	}
	if version != "" {
		m[Version] = version
	}
	return m
}
