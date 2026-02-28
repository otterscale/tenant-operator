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
	"testing"

	"github.com/otterscale/tenant-operator/internal/labels"
)

func TestStandard(t *testing.T) {
	got := labels.Standard("my-app", "controller", "v1.2.3")

	want := map[string]string{
		"app.kubernetes.io/name":       "my-app",
		"app.kubernetes.io/component":  "controller",
		"app.kubernetes.io/part-of":    "otterscale-system",
		"app.kubernetes.io/managed-by": "tenant-operator",
		"app.kubernetes.io/version":    "v1.2.3",
	}

	if len(got) != len(want) {
		t.Errorf("Standard() returned %d labels, want %d", len(got), len(want))
	}

	for key, wantVal := range want {
		if gotVal, ok := got[key]; !ok {
			t.Errorf("Standard() missing label %q", key)
		} else if gotVal != wantVal {
			t.Errorf("Standard()[%q] = %q, want %q", key, gotVal, wantVal)
		}
	}
}

func TestStandard_AllArgsPassedThrough(t *testing.T) {
	cases := []struct {
		name      string
		component string
		version   string
	}{
		{"workspace", "webhook", "v0.1.0"},
		{"module", "controller", ""},
		{"", "", ""},
	}

	for _, tc := range cases {
		got := labels.Standard(tc.name, tc.component, tc.version)

		if got[labels.Name] != tc.name {
			t.Errorf("Standard(%q, ...) Name = %q, want %q", tc.name, got[labels.Name], tc.name)
		}
		if got[labels.Component] != tc.component {
			t.Errorf("Standard(..., %q, ...) Component = %q, want %q", tc.component, got[labels.Component], tc.component)
		}
		if got[labels.Version] != tc.version {
			t.Errorf("Standard(..., %q) Version = %q, want %q", tc.version, got[labels.Version], tc.version)
		}
	}
}

func TestStandard_FixedLabels(t *testing.T) {
	got := labels.Standard("any", "any", "any")

	if got[labels.PartOf] != "otterscale-system" {
		t.Errorf("Standard() PartOf = %q, want %q", got[labels.PartOf], "otterscale-system")
	}
	if got[labels.ManagedBy] != "tenant-operator" {
		t.Errorf("Standard() ManagedBy = %q, want %q", got[labels.ManagedBy], "tenant-operator")
	}
}
