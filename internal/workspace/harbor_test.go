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
	"encoding/json"
	"testing"
)

func TestBuildDockerConfigJSON(t *testing.T) {
	data, err := buildDockerConfigJSON("https://harbor.example.com", "robot$workspace-test", "secret-token")
	if err != nil {
		t.Fatalf("buildDockerConfigJSON returned error: %v", err)
	}

	var config map[string]any
	if err := json.Unmarshal(data, &config); err != nil {
		t.Fatalf("failed to unmarshal docker config JSON: %v", err)
	}

	auths, ok := config["auths"].(map[string]any)
	if !ok {
		t.Fatal("missing or invalid 'auths' field")
	}

	entry, ok := auths["https://harbor.example.com"].(map[string]any)
	if !ok {
		t.Fatal("missing entry for harbor.example.com")
	}

	if entry["username"] != "robot$workspace-test" {
		t.Errorf("expected username=robot$workspace-test, got %v", entry["username"])
	}
	if entry["password"] != "secret-token" {
		t.Errorf("expected password=secret-token, got %v", entry["password"])
	}
	if _, ok := entry["auth"]; !ok {
		t.Error("missing 'auth' field in docker config entry")
	}
}
