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

// ErrUserNotFound is returned when Harbor reports the target user does not
// exist. It is a skip signal, not a hard failure: Harbor provisions users
// asynchronously, on first OIDC login.
var ErrUserNotFound = errors.New("harbor user not found")

// Harbor project role IDs.
const (
	RoleProjectAdmin int = 1
	RoleDeveloper    int = 2
	RoleGuest        int = 3
)

// robotsPath is the collection endpoint for robot accounts. Robots are addressed
// cluster-wide even when they belong to a project, hence not under /projects/{name}.
const robotsPath = "/api/v2.0/robots"

// Client defines the operations needed for workspace Harbor integration.
type Client interface {
	// EnsureProject ensures a Harbor project with the given name exists.
	EnsureProject(ctx context.Context, projectName string) error

	// ReconcileProjectMembers synchronizes the Harbor project membership to match
	// desired: adding, re-roling and removing as needed.
	//
	// Users Harbor does not have yet (404 on add) are skipped and returned in
	// missingUsers for a later retry, without aborting the rest of the loop. An
	// error means an unrecoverable failure.
	ReconcileProjectMembers(ctx context.Context, projectName string, desired []ProjectMember) (missingUsers []string, err error)

	// EnsureRobot ensures the project's robot account exists.
	//
	// Credentials come back only when this call created the robot: Harbor reveals
	// a robot secret at the moment it sets one and never again. Nil credentials
	// mean "already there", not "failed" — callers needing credentials for such a
	// robot must ask RefreshRobotSecret.
	EnsureRobot(ctx context.Context, projectName string, robotName string) (*RobotCredentials, error)

	// RefreshRobotSecret has Harbor issue a new secret for an existing robot.
	// This invalidates the robot's previous secret, so it is for rebuilding lost
	// credentials rather than routine use. Refreshing a robot that does not exist
	// is an error.
	RefreshRobotSecret(ctx context.Context, projectName string, robotName string) (*RobotCredentials, error)
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

func (c *httpClient) EnsureProject(ctx context.Context, projectName string) error {
	exists, err := c.projectExists(ctx, projectName)
	if err != nil {
		return fmt.Errorf("checking Harbor project existence: %w", err)
	}
	if exists {
		return nil
	}

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

func (c *httpClient) ReconcileProjectMembers(ctx context.Context, projectName string, desired []ProjectMember) ([]string, error) {
	basePath := fmt.Sprintf("/api/v2.0/projects/%s/members", projectName)

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

	// Members are removed from this lookup as they are found in desired; whatever
	// is left over is no longer wanted and gets deleted.
	existingByName := make(map[string]existingMember, len(existing))
	for _, m := range existing {
		existingByName[m.Username] = m
	}

	var missing []string

	for _, d := range desired {
		if cur, ok := existingByName[d.Username]; ok {
			if cur.RoleID != d.RoleID {
				if err := c.updateProjectMember(ctx, basePath, cur.ID, d.RoleID); err != nil {
					return nil, fmt.Errorf("updating member %s: %w", d.Username, err)
				}
			}
			delete(existingByName, d.Username)
		} else {
			if err := c.addProjectMember(ctx, basePath, d.Username, d.RoleID); err != nil {
				if errors.Is(err, ErrUserNotFound) {
					missing = append(missing, d.Username)
					continue
				}
				return nil, fmt.Errorf("adding member %s: %w", d.Username, err)
			}
		}
	}

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
		// The user has not been provisioned yet — Harbor creates users on first
		// OIDC login. The sentinel lets callers skip-and-retry.
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

func (c *httpClient) EnsureRobot(ctx context.Context, projectName string, robotName string) (*RobotCredentials, error) {
	existing, err := c.findRobot(ctx, projectName, robotName)
	if err != nil {
		return nil, fmt.Errorf("checking Harbor robot existence: %w", err)
	}
	if existing != nil {
		return nil, nil
	}

	return c.createRobot(ctx, projectName, robotName)
}

// createRobot creates the project robot and returns the secret Harbor reveals
// once. A 409 means something else created it in the meantime, and that call
// got the secret, so there is none to report.
func (c *httpClient) createRobot(ctx context.Context, projectName string, robotName string) (*RobotCredentials, error) {
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

	resp, err := c.doJSON(ctx, http.MethodPost, robotsPath, body)
	if err != nil {
		return nil, fmt.Errorf("creating Harbor robot account: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	switch resp.StatusCode {
	case http.StatusCreated:
		// fall through to decode credentials
	case http.StatusConflict:
		return nil, nil
	default:
		return nil, c.unexpectedStatus("creating Harbor robot account", resp)
	}

	var result struct {
		Name   string `json:"name"`
		Secret string `json:"secret"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decoding Harbor robot response: %w", err)
	}
	// An empty secret would reach the image pull Secret and fail every pull with
	// an authentication error far from here, so it is rejected at source.
	if result.Secret == "" {
		return nil, fmt.Errorf("creating Harbor robot account: Harbor returned an empty secret")
	}

	return &RobotCredentials{
		Name:   result.Name,
		Secret: result.Secret,
	}, nil
}

// RefreshRobotSecret rebuilds a robot by deleting and re-creating it.
//
// Harbor's RefreshSec operation would be the direct route, but it is a PATCH on
// the robot and Harbor defines no robot:update permission at either scope — so
// no robot account can perform it, only an administrator. Deleting and creating
// stays within create/read/list/delete, which a scoped robot can hold.
//
// The cost is a window where the robot does not exist and pulls fail. It is
// bounded by these two calls, and this path only runs when the workspace's
// image pull Secret has gone missing, which already broke pulls.
func (c *httpClient) RefreshRobotSecret(ctx context.Context, projectName string, robotName string) (*RobotCredentials, error) {
	existing, err := c.findRobot(ctx, projectName, robotName)
	if err != nil {
		return nil, fmt.Errorf("finding Harbor robot to refresh: %w", err)
	}
	if existing == nil {
		return nil, fmt.Errorf("refreshing Harbor robot secret: robot %q does not exist in project %q",
			robotName, projectName)
	}

	if err := c.deleteRobot(ctx, existing.ID); err != nil {
		return nil, err
	}

	creds, err := c.createRobot(ctx, projectName, robotName)
	if err != nil {
		return nil, fmt.Errorf("re-creating Harbor robot after delete: %w", err)
	}
	if creds == nil {
		// A 409 right after a successful delete means another writer re-created
		// the robot and holds the only copy of its secret.
		return nil, fmt.Errorf("refreshing Harbor robot secret: robot %q in project %q was re-created concurrently",
			robotName, projectName)
	}
	return creds, nil
}

// deleteRobot removes a robot by ID. A robot that is already gone is not an
// error: the goal is that it no longer exists.
func (c *httpClient) deleteRobot(ctx context.Context, robotID int) error {
	path := fmt.Sprintf("%s/%d", robotsPath, robotID)

	resp, err := c.do(ctx, http.MethodDelete, path, nil)
	if err != nil {
		return fmt.Errorf("deleting Harbor robot account: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNotFound {
		return c.unexpectedStatus("deleting Harbor robot account", resp)
	}
	return nil
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

// robotRef identifies a robot account Harbor already holds. The numeric ID is
// what the per-robot endpoints are addressed by.
type robotRef struct {
	ID   int
	Name string // full name, e.g. robot$myproject+myrobot
}

// findRobot returns the project's robot account, or nil when Harbor has none.
//
// /api/v2.0/robots lists robots cluster-wide and pages them, so the query is
// narrowed server-side — otherwise the robot could sit on a later page and be
// reported missing. The exact-name comparison afterwards is what decides;
// Level/ProjectID only bound what has to be paged through.
func (c *httpClient) findRobot(ctx context.Context, projectName, robotName string) (*robotRef, error) {
	projectID, err := c.getProjectID(ctx, projectName)
	if err != nil {
		return nil, err
	}

	q := fmt.Sprintf("Level=project,ProjectID=%d,name=~%s+%s", projectID, projectName, robotName)
	path := robotsPath + "?q=" + url.QueryEscape(url.QueryEscape(q)) // double escape for query parameter

	resp, err := c.do(ctx, http.MethodGet, path, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, c.unexpectedStatus("listing Harbor robots", resp)
	}

	var robots []struct {
		ID   int    `json:"id"`
		Name string `json:"name"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&robots); err != nil {
		return nil, fmt.Errorf("decoding Harbor robots response: %w", err)
	}

	fullName := fmt.Sprintf("robot$%s+%s", projectName, robotName)
	for _, r := range robots {
		if r.Name == fullName {
			return &robotRef{ID: r.ID, Name: r.Name}, nil
		}
	}
	return nil, nil
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
