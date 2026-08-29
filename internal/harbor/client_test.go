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

package harbor

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

const (
	projectsPath = "/api/v2.0/projects"
	membersPath  = "/api/v2.0/projects/test-project/members"
)

// --- EnsureProject tests ---

func TestEnsureProject_AlreadyExists(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead && r.URL.Path == projectsPath {
			w.WriteHeader(http.StatusOK)
			return
		}
		t.Errorf("unexpected request: %s %s", r.Method, r.URL)
	}))
	defer srv.Close()

	c := NewClient(srv.URL, "admin", "password")
	if err := c.EnsureProject(context.Background(), "test-project"); err != nil {
		t.Fatalf("EnsureProject returned error: %v", err)
	}
}

func TestEnsureProject_Created(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead && r.URL.Path == projectsPath {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if r.Method == http.MethodPost && r.URL.Path == projectsPath {
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("failed to decode request body: %v", err)
			}
			if body["project_name"] != "test-project" {
				t.Errorf("expected project_name=test-project, got %v", body["project_name"])
			}
			if body["public"] != false {
				t.Errorf("expected public=false, got %v", body["public"])
			}
			w.WriteHeader(http.StatusCreated)
			return
		}
		t.Errorf("unexpected request: %s %s", r.Method, r.URL)
	}))
	defer srv.Close()

	c := NewClient(srv.URL, "admin", "password")
	if err := c.EnsureProject(context.Background(), "test-project"); err != nil {
		t.Fatalf("EnsureProject returned error: %v", err)
	}
}

func TestEnsureProject_ConflictIsIdempotent(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if r.Method == http.MethodPost && r.URL.Path == projectsPath {
			w.WriteHeader(http.StatusConflict)
			return
		}
		t.Errorf("unexpected request: %s %s", r.Method, r.URL)
	}))
	defer srv.Close()

	c := NewClient(srv.URL, "admin", "password")
	if err := c.EnsureProject(context.Background(), "test-project"); err != nil {
		t.Fatalf("EnsureProject should treat 409 as success, got error: %v", err)
	}
}

func TestEnsureProject_ServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("internal error"))
	}))
	defer srv.Close()

	c := NewClient(srv.URL, "admin", "password")
	if err := c.EnsureProject(context.Background(), "test-project"); err == nil {
		t.Fatal("EnsureProject should return error on 500")
	}
}

// --- ReconcileProjectMembers tests ---

func TestReconcileProjectMembers_AddsNewMembers(t *testing.T) {
	var addedMembers []map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == membersPath {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte("[]"))
			return
		}
		if r.Method == http.MethodPost && r.URL.Path == membersPath {
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("failed to decode body: %v", err)
			}
			addedMembers = append(addedMembers, body)
			w.WriteHeader(http.StatusCreated)
			return
		}
		t.Errorf("unexpected request: %s %s", r.Method, r.URL)
	}))
	defer srv.Close()

	c := NewClient(srv.URL, "admin", "password")
	_, err := c.ReconcileProjectMembers(context.Background(), "test-project", []ProjectMember{
		{Username: "alice", RoleID: RoleProjectAdmin},
		{Username: "bob", RoleID: RoleDeveloper},
	})
	if err != nil {
		t.Fatalf("ReconcileProjectMembers returned error: %v", err)
	}
	if len(addedMembers) != 2 {
		t.Fatalf("expected 2 members added, got %d", len(addedMembers))
	}
}

func TestReconcileProjectMembers_UpdatesRole(t *testing.T) {
	updatedRoleID := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == membersPath {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode([]existingMember{
				{ID: 10, RoleID: RoleGuest, Username: "alice"},
			})
			return
		}
		if r.Method == http.MethodPut && r.URL.Path == membersPath+"/10" {
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("failed to decode body: %v", err)
			}
			updatedRoleID = int(body["role_id"].(float64))
			w.WriteHeader(http.StatusOK)
			return
		}
		t.Errorf("unexpected request: %s %s", r.Method, r.URL)
	}))
	defer srv.Close()

	c := NewClient(srv.URL, "admin", "password")
	_, err := c.ReconcileProjectMembers(context.Background(), "test-project", []ProjectMember{
		{Username: "alice", RoleID: RoleProjectAdmin},
	})
	if err != nil {
		t.Fatalf("ReconcileProjectMembers returned error: %v", err)
	}
	if updatedRoleID != RoleProjectAdmin {
		t.Errorf("expected role updated to %d, got %d", RoleProjectAdmin, updatedRoleID)
	}
}

func TestReconcileProjectMembers_RemovesStaleMember(t *testing.T) {
	deleted := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == membersPath {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode([]existingMember{
				{ID: 10, RoleID: RoleProjectAdmin, Username: "alice"},
				{ID: 20, RoleID: RoleDeveloper, Username: "removed-user"},
			})
			return
		}
		if r.Method == http.MethodDelete && r.URL.Path == membersPath+"/20" {
			deleted = true
			w.WriteHeader(http.StatusOK)
			return
		}
		t.Errorf("unexpected request: %s %s", r.Method, r.URL)
	}))
	defer srv.Close()

	c := NewClient(srv.URL, "admin", "password")
	_, err := c.ReconcileProjectMembers(context.Background(), "test-project", []ProjectMember{
		{Username: "alice", RoleID: RoleProjectAdmin},
	})
	if err != nil {
		t.Fatalf("ReconcileProjectMembers returned error: %v", err)
	}
	if !deleted {
		t.Error("expected stale member to be deleted")
	}
}

func TestReconcileProjectMembers_NoChanges(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == membersPath {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode([]existingMember{
				{ID: 10, RoleID: RoleProjectAdmin, Username: "alice"},
			})
			return
		}
		t.Errorf("unexpected request: %s %s (no mutations expected)", r.Method, r.URL)
	}))
	defer srv.Close()

	c := NewClient(srv.URL, "admin", "password")
	_, err := c.ReconcileProjectMembers(context.Background(), "test-project", []ProjectMember{
		{Username: "alice", RoleID: RoleProjectAdmin},
	})
	if err != nil {
		t.Fatalf("ReconcileProjectMembers returned error: %v", err)
	}
}

func TestReconcileProjectMembers_AddConflictIsIdempotent(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == membersPath {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte("[]"))
			return
		}
		if r.Method == http.MethodPost && r.URL.Path == membersPath {
			w.WriteHeader(http.StatusConflict)
			return
		}
		t.Errorf("unexpected request: %s %s", r.Method, r.URL)
	}))
	defer srv.Close()

	c := NewClient(srv.URL, "admin", "password")
	_, err := c.ReconcileProjectMembers(context.Background(), "test-project", []ProjectMember{
		{Username: "alice", RoleID: RoleProjectAdmin},
	})
	if err != nil {
		t.Fatalf("ReconcileProjectMembers should treat 409 as success, got: %v", err)
	}
}

func TestReconcileProjectMembers_SkipsMissingUser(t *testing.T) {
	var addedUsernames []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == membersPath {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte("[]"))
			return
		}
		if r.Method == http.MethodPost && r.URL.Path == membersPath {
			var body struct {
				MemberUser struct {
					Username string `json:"username"`
				} `json:"member_user"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("failed to decode body: %v", err)
			}
			if body.MemberUser.Username == "ghost" {
				w.WriteHeader(http.StatusNotFound)
				_, _ = w.Write([]byte(`{"errors":[{"code":"NOT_FOUND","message":"ghost not found"}]}`))
				return
			}
			addedUsernames = append(addedUsernames, body.MemberUser.Username)
			w.WriteHeader(http.StatusCreated)
			return
		}
		t.Errorf("unexpected request: %s %s", r.Method, r.URL)
	}))
	defer srv.Close()

	c := NewClient(srv.URL, "admin", "password")
	missing, err := c.ReconcileProjectMembers(context.Background(), "test-project", []ProjectMember{
		{Username: "alice", RoleID: RoleProjectAdmin},
		{Username: "ghost", RoleID: RoleDeveloper},
		{Username: "bob", RoleID: RoleDeveloper},
	})
	if err != nil {
		t.Fatalf("ReconcileProjectMembers should skip 404, got error: %v", err)
	}
	if len(missing) != 1 || missing[0] != "ghost" {
		t.Errorf("expected missing=[ghost], got %v", missing)
	}
	if len(addedUsernames) != 2 {
		t.Errorf("expected alice and bob added, got %v", addedUsernames)
	}
}

// --- EnsureRobot / RefreshRobotSecret tests ---

// robotTestHandler serves project lookup and robot API requests, with
// robotResponse standing in for what the robot list endpoint returns.
func robotTestHandler(t *testing.T, robotResponse []byte, postHandler http.HandlerFunc) http.HandlerFunc {
	return robotTestHandlerWithPatch(t, robotResponse, postHandler, nil)
}

// robotTestHandlerWithPatch additionally serves the per-robot PATCH that
// refreshes a secret.
func robotTestHandlerWithPatch(
	t *testing.T, robotResponse []byte, postHandler, patchHandler http.HandlerFunc,
) http.HandlerFunc {
	t.Helper()
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPatch && strings.HasPrefix(r.URL.Path, robotsPath+"/") && patchHandler != nil {
			patchHandler(w, r)
			return
		}
		if r.Method == http.MethodGet && r.URL.Path == projectsPath+"/test-ns" {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"project_id": 1, "name": "test-ns",
			})
			return
		}
		if r.Method == http.MethodGet && r.URL.Path == robotsPath {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(robotResponse)
			return
		}
		if r.Method == http.MethodPost && r.URL.Path == robotsPath && postHandler != nil {
			postHandler(w, r)
			return
		}
		t.Errorf("unexpected request: %s %s", r.Method, r.URL)
	}
}

func TestEnsureRobot_Created(t *testing.T) {
	postHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("failed to decode request body: %v", err)
		}
		if body["name"] != "workspace-test-ns" {
			t.Errorf("expected name=workspace-test-ns, got %v", body["name"])
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"name":   "robot$test-ns+workspace-test-ns",
			"secret": "generated-secret-token",
		})
	})
	srv := httptest.NewServer(robotTestHandler(t, []byte("[]"), postHandler))
	defer srv.Close()

	c := NewClient(srv.URL, "admin", "password")
	creds, err := c.EnsureRobot(context.Background(), "test-ns", "workspace-test-ns")
	if err != nil {
		t.Fatalf("EnsureRobot returned error: %v", err)
	}
	if creds == nil {
		t.Fatal("expected credentials from a robot this call created")
	}
	if creds.Name != "robot$test-ns+workspace-test-ns" {
		t.Errorf("expected Name=robot$test-ns+workspace-test-ns, got %s", creds.Name)
	}
	if creds.Secret != "generated-secret-token" {
		t.Errorf("expected Secret=generated-secret-token, got %s", creds.Secret)
	}
}

func TestEnsureRobot_AlreadyExists(t *testing.T) {
	existingRobots, _ := json.Marshal([]map[string]any{
		{"name": "robot$test-ns+workspace-test-ns", "id": 42},
	})
	srv := httptest.NewServer(robotTestHandler(t, existingRobots, nil))
	defer srv.Close()

	c := NewClient(srv.URL, "admin", "password")
	creds, err := c.EnsureRobot(context.Background(), "test-ns", "workspace-test-ns")
	if err != nil {
		t.Fatalf("EnsureRobot returned error: %v", err)
	}
	// Harbor reveals a robot secret only when it sets one, so nil credentials are
	// the signal the caller refreshes on.
	if creds != nil {
		t.Errorf("expected nil credentials for an existing robot, got %+v", creds)
	}
}

func TestEnsureRobot_ConflictIsIdempotent(t *testing.T) {
	postHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"errors":[{"code":"CONFLICT","message":"robot account already exists"}]}`))
	})
	srv := httptest.NewServer(robotTestHandler(t, []byte("[]"), postHandler))
	defer srv.Close()

	c := NewClient(srv.URL, "admin", "password")
	creds, err := c.EnsureRobot(context.Background(), "test-ns", "workspace-test-ns")
	if err != nil {
		t.Fatalf("EnsureRobot should treat 409 as success, got error: %v", err)
	}
	// The robot was created between the check and the create, so its secret went
	// to whoever won — indistinguishable from finding it already there.
	if creds != nil {
		t.Errorf("expected nil credentials for conflict, got %+v", creds)
	}
}

func TestEnsureRobot_ServerError(t *testing.T) {
	postHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("internal error"))
	})
	srv := httptest.NewServer(robotTestHandler(t, []byte("[]"), postHandler))
	defer srv.Close()

	c := NewClient(srv.URL, "admin", "password")
	_, err := c.EnsureRobot(context.Background(), "test-ns", "workspace-test-ns")
	if err == nil {
		t.Fatal("EnsureRobot should return error on 500")
	}
}

// existingRobotList is the robot Harbor reports for project test-ns.
func existingRobotList(t *testing.T) []byte {
	t.Helper()
	body, err := json.Marshal([]map[string]any{
		{"id": 42, "name": "robot$test-ns+workspace-test-ns"},
	})
	if err != nil {
		t.Fatalf("marshaling robot list: %v", err)
	}
	return body
}

func TestRefreshRobotSecret_ReturnsNewCredentials(t *testing.T) {
	var patchedPath string
	patchHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		patchedPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"secret": "brand-new-secret"})
	})
	srv := httptest.NewServer(robotTestHandlerWithPatch(t, existingRobotList(t), nil, patchHandler))
	defer srv.Close()

	c := NewClient(srv.URL, "admin", "password")
	creds, err := c.RefreshRobotSecret(context.Background(), "test-ns", "workspace-test-ns")
	if err != nil {
		t.Fatalf("RefreshRobotSecret returned error: %v", err)
	}

	// The robot is addressed by the id found in the list, not by its name.
	if want := robotsPath + "/42"; patchedPath != want {
		t.Errorf("patched %s, want %s", patchedPath, want)
	}
	if creds.Secret != "brand-new-secret" {
		t.Errorf("Secret = %q, want brand-new-secret", creds.Secret)
	}
	// The refresh response carries only the secret, so the name comes from the
	// robot Harbor matched.
	if creds.Name != "robot$test-ns+workspace-test-ns" {
		t.Errorf("Name = %q, want robot$test-ns+workspace-test-ns", creds.Name)
	}
}

func TestRefreshRobotSecret_MissingRobotIsAnError(t *testing.T) {
	srv := httptest.NewServer(robotTestHandlerWithPatch(t, []byte("[]"), nil, nil))
	defer srv.Close()

	c := NewClient(srv.URL, "admin", "password")
	_, err := c.RefreshRobotSecret(context.Background(), "test-ns", "workspace-test-ns")
	if err == nil {
		t.Fatal("refreshing a robot that does not exist should be an error")
	}
	if !strings.Contains(err.Error(), "does not exist") {
		t.Errorf("error = %q, want it to say the robot does not exist", err)
	}
}

// An empty secret would reach the image pull Secret and fail every pull with an
// authentication error far from here, so it is rejected at source.
func TestRefreshRobotSecret_EmptySecretIsAnError(t *testing.T) {
	patchHandler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"secret": ""})
	})
	srv := httptest.NewServer(robotTestHandlerWithPatch(t, existingRobotList(t), nil, patchHandler))
	defer srv.Close()

	c := NewClient(srv.URL, "admin", "password")
	_, err := c.RefreshRobotSecret(context.Background(), "test-ns", "workspace-test-ns")
	if err == nil {
		t.Fatal("an empty secret should be an error")
	}
}

func TestRefreshRobotSecret_ServerError(t *testing.T) {
	patchHandler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("internal error"))
	})
	srv := httptest.NewServer(robotTestHandlerWithPatch(t, existingRobotList(t), nil, patchHandler))
	defer srv.Close()

	c := NewClient(srv.URL, "admin", "password")
	_, err := c.RefreshRobotSecret(context.Background(), "test-ns", "workspace-test-ns")
	if err == nil {
		t.Fatal("RefreshRobotSecret should return error on 500")
	}
}

// --- Misc tests ---

func TestBasicAuthIsSent(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, pass, ok := r.BasicAuth()
		if !ok || user != "robot$admin" || pass != "secret" {
			t.Errorf("expected basic auth robot$admin:secret, got %s:%s (ok=%v)", user, pass, ok)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := NewClient(srv.URL, "robot$admin", "secret")
	_ = c.EnsureProject(context.Background(), "any")
}
