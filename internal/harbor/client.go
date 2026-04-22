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
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// ErrUserNotFound is returned when Harbor reports the target user does not exist.
// Callers treat this as a skip signal rather than a hard failure, because the user
// may be provisioned asynchronously (e.g. on first OIDC login).
var ErrUserNotFound = errors.New("harbor user not found")

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
	//
	// Desired users that do not yet exist in Harbor (404 on add) are skipped and
	// returned in missingUsers so the caller can schedule a later retry. Skipping
	// one user does not abort the rest of the loop; an error is only returned for
	// unrecoverable failures.
	ReconcileProjectMembers(ctx context.Context, projectName string, desired []ProjectMember) (missingUsers []string, err error)

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
//
// If Harbor returns 404 when adding a user, the user is skipped and recorded in
// missingUsers — the rest of the reconcile loop continues. Harbor users are
// provisioned on first login, so missing users are expected to appear later.
func (c *httpClient) ReconcileProjectMembers(ctx context.Context, projectName string, desired []ProjectMember) ([]string, error) {
	basePath := fmt.Sprintf("/api/v2.0/projects/%s/members", projectName)

	// List current members
	resp, err := c.do(ctx, http.MethodGet, basePath, nil)
	if err != nil {
		return nil, fmt.Errorf("listing project members: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, c.unexpectedStatus("listing project members", resp)
	}

	var existing []existingMember
	if err := json.NewDecoder(resp.Body).Decode(&existing); err != nil {
		return nil, fmt.Errorf("decoding project members: %w", err)
	}

	// Build lookup of existing members by username. We will remove members from this
	// map as we find them in the desired list. The remaining members will be deleted.
	existingByName := make(map[string]existingMember, len(existing))
	for _, m := range existing {
		existingByName[m.Username] = m
	}

	var missing []string

	// Add or update members, and track which existing members are still desired.
	for _, d := range desired {
		if cur, ok := existingByName[d.Username]; ok {
			// Exists — update role if changed
			if cur.RoleID != d.RoleID {
				if err := c.updateProjectMember(ctx, basePath, cur.ID, d.RoleID); err != nil {
					return nil, fmt.Errorf("updating member %s: %w", d.Username, err)
				}
			}
			// Member is desired, so remove from the map of members to be deleted.
			delete(existingByName, d.Username)
		} else {
			// New member — add
			if err := c.addProjectMember(ctx, basePath, d.Username, d.RoleID); err != nil {
				if errors.Is(err, ErrUserNotFound) {
					missing = append(missing, d.Username)
					continue
				}
				return nil, fmt.Errorf("adding member %s: %w", d.Username, err)
			}
		}
	}

	// Remove members no longer in desired list (the ones left in the map).
	for _, cur := range existingByName {
		if err := c.deleteProjectMember(ctx, basePath, cur.ID); err != nil {
			return nil, fmt.Errorf("removing member %s: %w", cur.Username, err)
		}
	}

	return missing, nil
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
	case http.StatusNotFound:
		// Harbor returns 404 when the target user has not been provisioned yet
		// (users are created on first OIDC login). Surface a sentinel so callers
		// can skip-and-retry instead of failing the whole reconcile.
		return ErrUserNotFound
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
	exists, err := c.robotExists(ctx, projectName, robotName)
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

// getProjectID returns the numeric ID of a Harbor project by name.
func (c *httpClient) getProjectID(ctx context.Context, projectName string) (int, error) {
	resp, err := c.do(ctx, http.MethodGet, "/api/v2.0/projects/"+url.PathEscape(projectName), nil)
	if err != nil {
		return 0, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return 0, c.unexpectedStatus("getting Harbor project", resp)
	}

	var project struct {
		ProjectID int `json:"project_id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&project); err != nil {
		return 0, fmt.Errorf("decoding Harbor project response: %w", err)
	}
	return project.ProjectID, nil
}

// robotExists checks whether a Harbor robot account exists for the given project.
func (c *httpClient) robotExists(ctx context.Context, projectName, robotName string) (bool, error) {
	projectID, err := c.getProjectID(ctx, projectName)
	if err != nil {
		return false, err
	}

	// Build query: Level=project,ProjectID={id},name=~{projectName}+{robotName}
	q := fmt.Sprintf("Level=project,ProjectID=%d,name=~%s+%s", projectID, projectName, robotName)
	path := "/api/v2.0/robots?q=" + url.QueryEscape(url.QueryEscape(q)) // double escape for query parameter

	resp, err := c.do(ctx, http.MethodGet, path, nil)
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

	fullName := fmt.Sprintf("robot$%s+%s", projectName, robotName)
	for _, r := range robots {
		if r.Name == fullName {
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
