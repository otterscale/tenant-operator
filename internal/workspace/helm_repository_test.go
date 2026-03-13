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
	"testing"
)

func TestBuildHelmRepositoryURL(t *testing.T) {
	tests := []struct {
		name         string
		harborURL    string
		projectName  string
		wantURL      string
		wantInsecure bool
		wantErr      bool
	}{
		{
			name:         "standard https URL",
			harborURL:    "https://harbor.example.com",
			projectName:  "my-workspace",
			wantURL:      "oci://harbor.example.com/my-workspace",
			wantInsecure: false,
		},
		{
			name:         "URL with port",
			harborURL:    "https://harbor.example.com:8443",
			projectName:  "test-ns",
			wantURL:      "oci://harbor.example.com:8443/test-ns",
			wantInsecure: false,
		},
		{
			name:         "http URL is insecure",
			harborURL:    "http://harbor.local",
			projectName:  "dev",
			wantURL:      "oci://harbor.local/dev",
			wantInsecure: true,
		},
		{
			name:         "http URL with port is insecure",
			harborURL:    "http://harbor.default.svc.cluster.local:80",
			projectName:  "my-ns",
			wantURL:      "oci://harbor.default.svc.cluster.local:80/my-ns",
			wantInsecure: true,
		},
		{
			name:        "invalid URL",
			harborURL:   "://invalid",
			projectName: "ns",
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotURL, gotInsecure, err := buildHelmRepositoryURL(tt.harborURL, tt.projectName)
			if (err != nil) != tt.wantErr {
				t.Errorf("buildHelmRepositoryURL() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotURL != tt.wantURL {
				t.Errorf("buildHelmRepositoryURL() URL = %q, want %q", gotURL, tt.wantURL)
			}
			if gotInsecure != tt.wantInsecure {
				t.Errorf("buildHelmRepositoryURL() insecure = %v, want %v", gotInsecure, tt.wantInsecure)
			}
		})
	}
}
