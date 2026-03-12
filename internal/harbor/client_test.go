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
	"testing"
)

const (
	projectsPath = "/api/v2.0/projects"
	robotsPath   = "/api/v2.0/robots"
)

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
		if r.Method == http.MethodPost {
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

func TestEnsureRobotAccount_Created(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == robotsPath {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte("[]"))
			return
		}
		if r.Method == http.MethodPost && r.URL.Path == robotsPath {
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
				"name":   "robot$workspace-test-ns",
				"secret": "generated-secret-token",
			})
			return
		}
		t.Errorf("unexpected request: %s %s", r.Method, r.URL)
	}))
	defer srv.Close()

	c := NewClient(srv.URL, "admin", "password")
	creds, created, err := c.EnsureRobotAccount(context.Background(), "test-ns", "workspace-test-ns")
	if err != nil {
		t.Fatalf("EnsureRobotAccount returned error: %v", err)
	}
	if !created {
		t.Error("expected created=true")
	}
	if creds.Name != "robot$workspace-test-ns" {
		t.Errorf("expected Name=robot$workspace-test-ns, got %s", creds.Name)
	}
	if creds.Secret != "generated-secret-token" {
		t.Errorf("expected Secret=generated-secret-token, got %s", creds.Secret)
	}
}

func TestEnsureRobotAccount_AlreadyExists(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == robotsPath {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode([]map[string]any{
				{"name": "robot$workspace-test-ns", "id": 42},
			})
			return
		}
		t.Errorf("unexpected request: %s %s", r.Method, r.URL)
	}))
	defer srv.Close()

	c := NewClient(srv.URL, "admin", "password")
	creds, created, err := c.EnsureRobotAccount(context.Background(), "test-ns", "workspace-test-ns")
	if err != nil {
		t.Fatalf("EnsureRobotAccount returned error: %v", err)
	}
	if created {
		t.Error("expected created=false for existing robot")
	}
	if creds != nil {
		t.Error("expected nil credentials for existing robot")
	}
}

func TestEnsureRobotAccount_ServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte("[]"))
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("internal error"))
	}))
	defer srv.Close()

	c := NewClient(srv.URL, "admin", "password")
	_, _, err := c.EnsureRobotAccount(context.Background(), "test-ns", "workspace-test-ns")
	if err == nil {
		t.Fatal("EnsureRobotAccount should return error on 500")
	}
}

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
