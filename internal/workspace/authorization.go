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
	"context"
	"fmt"
	"slices"

	authenticationv1 "k8s.io/api/authentication/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	tenantv1alpha1 "github.com/otterscale/api/tenant/v1alpha1"
)

// OperatorServiceAccountIdentity constructs the full Kubernetes service account
// identity string from the pod namespace and service account name.
// Example: OperatorServiceAccountIdentity("otterscale-system", "controller-manager")
// returns "system:serviceaccount:otterscale-system:controller-manager".
func OperatorServiceAccountIdentity(namespace, saName string) string {
	return "system:serviceaccount:" + namespace + ":" + saName
}

// privilegedGroups are Kubernetes groups that bypass all workspace-level
// authorization checks (cluster super-admins).
var privilegedGroups = []string{"system:masters", "kubeadm:cluster-admins"}

// privilegedClusterRoles are Kubernetes ClusterRoles whose bound subjects
// bypass all workspace-level authorization checks.
var privilegedClusterRoles = []string{"cluster-admin"}

// AuthorizeCreation checks whether the requesting user is allowed to create a
// Workspace. Non-privileged callers must list themselves as an admin member of the
// new Workspace and must not exceed the per-user workspace quota.
//
// maxPerUser controls the maximum number of workspaces a regular user may
// administer. A value of 0 disables quota enforcement.
//
// Privileged callers (group, operator SA, or cluster-admin ClusterRole holders)
// bypass both the self-admin requirement and the quota.
//
// Allowed callers (checked cheapest-first):
//   - Members of a privileged group (system:masters, kubeadm:cluster-admins)
//   - The operator's own ServiceAccount (operatorSA)
//   - A user who is listed as an "admin" member in the new workspace AND under quota
//   - A user bound to a privileged ClusterRole (e.g. cluster-admin) via ClusterRoleBinding
func AuthorizeCreation(ctx context.Context, reader client.Reader, userInfo authenticationv1.UserInfo, ws *tenantv1alpha1.Workspace, operatorSA string, maxPerUser int) error {
	if inPrivilegedGroup(userInfo) || userInfo.Username == operatorSA {
		return nil
	}

	if isWorkspaceAdmin(userInfo.Username, ws) {
		return enforceWorkspaceQuota(ctx, reader, userInfo.Username, maxPerUser)
	}

	// Not listed as admin — last resort: check for privileged ClusterRole.
	ok, err := hasPrivilegedClusterRole(ctx, reader, userInfo)
	if err != nil {
		return fmt.Errorf("failed to check ClusterRole bindings: %w", err)
	}
	if ok {
		return nil
	}

	return fmt.Errorf("workspace creator must be listed as a member with the 'admin' role")
}

// enforceWorkspaceQuota rejects the request if the user already administers
// maxPerUser or more workspaces. A maxPerUser of 0 disables the check.
//
// The quota counts workspaces where the user is a member with the "admin" role,
// using the user label index for an efficient initial filter followed by an
// in-memory role check.
func enforceWorkspaceQuota(ctx context.Context, reader client.Reader, username string, maxPerUser int) error {
	if maxPerUser <= 0 {
		return nil
	}

	var list tenantv1alpha1.WorkspaceList
	if err := reader.List(ctx, &list, client.MatchingLabels{
		UserLabelPrefix + username: "true",
	}); err != nil {
		return fmt.Errorf("failed to list user workspaces for quota check: %w", err)
	}

	adminCount := 0
	for i := range list.Items {
		if isWorkspaceAdmin(username, &list.Items[i]) {
			adminCount++
		}
	}

	if adminCount >= maxPerUser {
		return fmt.Errorf("user %q has reached the maximum number of workspaces (%d)", username, maxPerUser)
	}

	return nil
}

// AuthorizeModification checks whether the requesting user is allowed to
// update the given Workspace. The workspace parameter must be the **old**
// (pre-update) object so that a user cannot grant themselves admin and
// approve in the same request.
//
// reader is used to list ClusterRoleBindings for privileged ClusterRole checks.
// operatorSA is the full service account identity of the controller-manager
// (e.g. "system:serviceaccount:otterscale-system:tenant-operator-controller-manager").
// It is provided at startup so the operator works regardless of the namespace it is deployed in.
//
// Allowed callers (checked cheapest-first):
//   - Members of a privileged group (system:masters, kubeadm:cluster-admins)
//   - The operator's own ServiceAccount (operatorSA)
//   - A workspace member whose role is "admin" in the current (old) spec
//   - A user bound to a privileged ClusterRole (e.g. cluster-admin) via ClusterRoleBinding
func AuthorizeModification(ctx context.Context, reader client.Reader, userInfo authenticationv1.UserInfo, workspace *tenantv1alpha1.Workspace, operatorSA string) error {
	if inPrivilegedGroup(userInfo) {
		return nil
	}

	if userInfo.Username == operatorSA {
		return nil
	}

	if isWorkspaceAdmin(userInfo.Username, workspace) {
		return nil
	}

	ok, err := hasPrivilegedClusterRole(ctx, reader, userInfo)
	if err != nil {
		return fmt.Errorf("failed to check ClusterRole bindings: %w", err)
	}
	if ok {
		return nil
	}

	return fmt.Errorf("only users with the 'admin' role defined in this workspace can modify or delete it")
}

func hasPrivilegedClusterRole(ctx context.Context, reader client.Reader, userInfo authenticationv1.UserInfo) (bool, error) {
	var bindings rbacv1.ClusterRoleBindingList
	if err := reader.List(ctx, &bindings); err != nil {
		return false, err
	}

	for i := range bindings.Items {
		b := &bindings.Items[i]
		if b.RoleRef.Kind != "ClusterRole" || !slices.Contains(privilegedClusterRoles, b.RoleRef.Name) {
			continue
		}
		for _, subject := range b.Subjects {
			if matchesSubject(subject, userInfo) {
				return true, nil
			}
		}
	}
	return false, nil
}

func matchesSubject(subject rbacv1.Subject, userInfo authenticationv1.UserInfo) bool {
	switch subject.Kind {
	case rbacv1.UserKind:
		return subject.Name == userInfo.Username
	case rbacv1.ServiceAccountKind:
		sa := "system:serviceaccount:" + subject.Namespace + ":" + subject.Name
		return sa == userInfo.Username
	case rbacv1.GroupKind:
		return slices.Contains(userInfo.Groups, subject.Name)
	}
	return false
}

// inPrivilegedGroup returns true if the user belongs to any privileged group.
func inPrivilegedGroup(userInfo authenticationv1.UserInfo) bool {
	for _, g := range userInfo.Groups {
		if slices.Contains(privilegedGroups, g) {
			return true
		}
	}
	return false
}

// isWorkspaceAdmin returns true if username matches a member with role "admin".
func isWorkspaceAdmin(username string, workspace *tenantv1alpha1.Workspace) bool {
	for _, m := range workspace.Spec.Members {
		if m.Subject == username && m.Role == tenantv1alpha1.MemberRoleAdmin {
			return true
		}
	}
	return false
}
