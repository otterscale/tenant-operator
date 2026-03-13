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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// Harbor project role IDs.
const (
	RoleProjectAdmin int = 1
	RoleDeveloper    int = 2
	RoleGuest        int = 3
)

// Client defines the operations needed for workspace Harbor integration.
type Client interface {
	// EnsureProject ensures a Harbor project with the given name exists.
	EnsureProject(ctx context.Context, projectName string) error

	// ReconcileProjectMembers synchronizes the Harbor project membership to match
	// the desired list of members. It adds missing members, updates roles that
	// have changed, and removes members that are no longer desired.
	ReconcileProjectMembers(ctx context.Context, projectName string, desired []ProjectMember) error

	// EnsureRobotAccount ensures a robot account exists for the given project.
	// Returns the credentials and whether the robot was newly created.
	// If the robot already exists, created is false and credentials are nil.
	EnsureRobotAccount(ctx context.Context, projectName string, robotName string) (creds *RobotCredentials, created bool, err error)
}

// ProjectMember represents a desired Harbor project member.
type ProjectMember struct {
	Username string
	RoleID   int
}

// RobotCredentials holds the credentials returned by Harbor for a robot account.
type RobotCredentials struct {
	Name   string // full robot name, e.g. robot$workspace-mynamespace
	Secret string // the token/password
}

// httpClient implements Client using Harbor v2.0 REST API.
type httpClient struct {
	baseURL    string
	username   string
	password   string
	httpClient *http.Client
}

// NewClient creates a new Harbor API client.
func NewClient(baseURL, username, password string) Client {
	return &httpClient{
		baseURL:  strings.TrimRight(baseURL, "/"),
		username: username,
		password: password,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// EnsureProject ensures a Harbor project with the given name exists.
// It is idempotent: if the project already exists, it returns nil.
func (c *httpClient) EnsureProject(ctx context.Context, projectName string) error {
	// Check if project already exists
	exists, err := c.projectExists(ctx, projectName)
	if err != nil {
		return fmt.Errorf("checking Harbor project existence: %w", err)
	}
	if exists {
		return nil
	}

	// Create project
	body := map[string]any{
		"project_name": projectName,
		"public":       false,
	}
	resp, err := c.doJSON(ctx, http.MethodPost, "/api/v2.0/projects", body)
	if err != nil {
		return fmt.Errorf("creating Harbor project: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	switch resp.StatusCode {
	case http.StatusCreated, http.StatusConflict:
		return nil
	default:
		return c.unexpectedStatus("creating Harbor project", resp)
	}
}

// existingMember represents a member returned by the Harbor API.
type existingMember struct {
	ID       int    `json:"id"`
	RoleID   int    `json:"role_id"`
	Username string `json:"entity_name"`
}

// ReconcileProjectMembers synchronizes the Harbor project membership to match
// the desired list. It lists current members, then adds/updates/removes as needed.
func (c *httpClient) ReconcileProjectMembers(ctx context.Context, projectName string, desired []ProjectMember) error {
	basePath := fmt.Sprintf("/api/v2.0/projects/%s/members", projectName)

	// List current members
	resp, err := c.do(ctx, http.MethodGet, basePath, nil)
	if err != nil {
		return fmt.Errorf("listing project members: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return c.unexpectedStatus("listing project members", resp)
	}

	var existing []existingMember
	if err := json.NewDecoder(resp.Body).Decode(&existing); err != nil {
		return fmt.Errorf("decoding project members: %w", err)
	}

	// Build lookup of existing members by username
	existingByName := make(map[string]existingMember, len(existing))
	for _, m := range existing {
		existingByName[m.Username] = m
	}

	// Build lookup of desired members by username
	desiredByName := make(map[string]ProjectMember, len(desired))
	for _, m := range desired {
		desiredByName[m.Username] = m
	}

	// Add or update members
	for _, d := range desired {
		if cur, ok := existingByName[d.Username]; ok {
			// Exists — update role if changed
			if cur.RoleID != d.RoleID {
				if err := c.updateProjectMember(ctx, basePath, cur.ID, d.RoleID); err != nil {
					return fmt.Errorf("updating member %s: %w", d.Username, err)
				}
			}
		} else {
			// New member — add
			if err := c.addProjectMember(ctx, basePath, d.Username, d.RoleID); err != nil {
				return fmt.Errorf("adding member %s: %w", d.Username, err)
			}
		}
	}

	// Remove members no longer in desired list
	for _, cur := range existing {
		if _, ok := desiredByName[cur.Username]; !ok {
			if err := c.deleteProjectMember(ctx, basePath, cur.ID); err != nil {
				return fmt.Errorf("removing member %s: %w", cur.Username, err)
			}
		}
	}

	return nil
}

// addProjectMember adds a user to the Harbor project with the given role.
func (c *httpClient) addProjectMember(ctx context.Context, basePath, username string, roleID int) error {
	body := map[string]any{
		"role_id": roleID,
		"member_user": map[string]string{
			"username": username,
		},
	}
	resp, err := c.doJSON(ctx, http.MethodPost, basePath, body)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	switch resp.StatusCode {
	case http.StatusCreated, http.StatusConflict:
		return nil
	default:
		return c.unexpectedStatus("adding project member", resp)
	}
}

// updateProjectMember updates the role of an existing project member.
func (c *httpClient) updateProjectMember(ctx context.Context, basePath string, memberID, roleID int) error {
	path := fmt.Sprintf("%s/%d", basePath, memberID)
	body := map[string]any{
		"role_id": roleID,
	}
	resp, err := c.doJSON(ctx, http.MethodPut, path, body)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return c.unexpectedStatus("updating project member role", resp)
	}
	return nil
}

// deleteProjectMember removes a member from the Harbor project.
func (c *httpClient) deleteProjectMember(ctx context.Context, basePath string, memberID int) error {
	path := fmt.Sprintf("%s/%d", basePath, memberID)
	resp, err := c.do(ctx, http.MethodDelete, path, nil)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return c.unexpectedStatus("removing project member", resp)
	}
	return nil
}

// EnsureRobotAccount ensures a robot account exists for the given project.
func (c *httpClient) EnsureRobotAccount(ctx context.Context, projectName string, robotName string) (*RobotCredentials, bool, error) {
	// Check if robot already exists
	exists, err := c.robotExists(ctx, robotName)
	if err != nil {
		return nil, false, fmt.Errorf("checking Harbor robot existence: %w", err)
	}
	if exists {
		return nil, false, nil
	}

	// Create robot account
	body := map[string]any{
		"name":     robotName,
		"duration": -1,
		"level":    "project",
		"permissions": []map[string]any{
			{
				"kind":      "project",
				"namespace": projectName,
				"access": []map[string]string{
					{"resource": "repository", "action": "pull"},
					{"resource": "repository", "action": "push"},
				},
			},
		},
	}

	resp, err := c.doJSON(ctx, http.MethodPost, "/api/v2.0/robots", body)
	if err != nil {
		return nil, false, fmt.Errorf("creating Harbor robot account: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	switch resp.StatusCode {
	case http.StatusCreated:
		// fall through to decode credentials
	case http.StatusConflict:
		// Robot was created between our check and create — idempotent success
		return nil, false, nil
	default:
		return nil, false, c.unexpectedStatus("creating Harbor robot account", resp)
	}

	var result struct {
		Name   string `json:"name"`
		Secret string `json:"secret"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, false, fmt.Errorf("decoding Harbor robot response: %w", err)
	}

	return &RobotCredentials{
		Name:   result.Name,
		Secret: result.Secret,
	}, true, nil
}

// projectExists checks whether a Harbor project with the given name exists.
func (c *httpClient) projectExists(ctx context.Context, projectName string) (bool, error) {
	resp, err := c.do(ctx, http.MethodHead, "/api/v2.0/projects?project_name="+projectName, nil)
	if err != nil {
		return false, err
	}
	defer func() { _ = resp.Body.Close() }()

	switch resp.StatusCode {
	case http.StatusOK:
		return true, nil
	case http.StatusNotFound:
		return false, nil
	default:
		return false, c.unexpectedStatus("checking Harbor project", resp)
	}
}

// robotExists checks whether a Harbor robot account with the given name prefix exists.
func (c *httpClient) robotExists(ctx context.Context, robotName string) (bool, error) {
	resp, err := c.do(ctx, http.MethodGet, "/api/v2.0/robots?q=name=~"+robotName, nil)
	if err != nil {
		return false, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return false, c.unexpectedStatus("listing Harbor robots", resp)
	}

	var robots []struct {
		Name string `json:"name"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&robots); err != nil {
		return false, fmt.Errorf("decoding Harbor robots response: %w", err)
	}

	for _, r := range robots {
		if r.Name == "robot$"+robotName {
			return true, nil
		}
	}
	return false, nil
}

// do executes an HTTP request against the Harbor API.
func (c *httpClient) do(ctx context.Context, method, path string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, body)
	if err != nil {
		return nil, err
	}
	req.SetBasicAuth(c.username, c.password)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	return c.httpClient.Do(req)
}

// doJSON marshals the body as JSON and executes the request.
func (c *httpClient) doJSON(ctx context.Context, method, path string, body any) (*http.Response, error) {
	data, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("marshaling request body: %w", err)
	}
	return c.do(ctx, method, path, bytes.NewReader(data))
}

// unexpectedStatus returns an error with the response status and body snippet.
func (c *httpClient) unexpectedStatus(operation string, resp *http.Response) error {
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
	return fmt.Errorf("%s: unexpected status %d: %s", operation, resp.StatusCode, string(body))
}
