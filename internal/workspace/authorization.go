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
	authorizationv1 "k8s.io/api/authorization/v1"
	"k8s.io/apimachinery/pkg/api/validate/content"
	"sigs.k8s.io/controller-runtime/pkg/client"

	tenantv1alpha1 "github.com/otterscale/tenant-operator/api/v1alpha1"
)

// privilegedGroups are Kubernetes groups that bypass all workspace-level
// authorization checks (cluster super-admins). They are answered locally, with
// no API call, so the common cluster-admin path stays cheap.
var privilegedGroups = []string{"system:masters", "kubeadm:cluster-admins"}

// AuthorizeCreation checks whether the requesting user is allowed to create a
// Workspace. Non-privileged callers must list themselves as an admin member of
// the new Workspace.
//
// Privileged callers (a privileged group, or cluster-wide access) bypass the
// self-admin requirement.
//
// Allowed callers (checked cheapest-first):
//   - Members of a privileged group (system:masters, kubeadm:cluster-admins)
//   - A user who is listed as an "admin" member in the new workspace
//   - A user the cluster authorizer grants cluster-wide access (see hasClusterWideAccess)
func AuthorizeCreation(ctx context.Context, c client.Client, userInfo authenticationv1.UserInfo, ws *tenantv1alpha1.Workspace) error {
	if inPrivilegedGroup(userInfo) {
		return nil
	}

	if isWorkspaceAdmin(userInfo.Username, ws) {
		return nil
	}

	// Not listed as admin — last resort: ask the cluster authorizer.
	ok, err := hasClusterWideAccess(ctx, c, userInfo)
	if err != nil {
		return fmt.Errorf("failed to check cluster-wide access: %w", err)
	}
	if ok {
		return nil
	}

	return fmt.Errorf("workspace creator must be listed as a member with the 'admin' role")
}

// AuthorizeModification checks whether the requesting user is allowed to
// update the given Workspace. The workspace parameter must be the **old**
// (pre-update) object so that a user cannot grant themselves admin and
// approve in the same request.
//
// c is used to submit a SubjectAccessReview for the cluster-wide access check.
//
// Allowed callers (checked cheapest-first):
//   - Members of a privileged group (system:masters, kubeadm:cluster-admins)
//   - A workspace member whose role is "admin" in the current (old) spec
//   - A user the cluster authorizer grants cluster-wide access (see hasClusterWideAccess)
//
// The operator itself needs no exemption here: it only ever patches the
// workspaces/status subresource, which this webhook does not intercept.
func AuthorizeModification(ctx context.Context, c client.Client, userInfo authenticationv1.UserInfo, workspace *tenantv1alpha1.Workspace) error {
	if inPrivilegedGroup(userInfo) {
		return nil
	}

	if isWorkspaceAdmin(userInfo.Username, workspace) {
		return nil
	}

	ok, err := hasClusterWideAccess(ctx, c, userInfo)
	if err != nil {
		return fmt.Errorf("failed to check cluster-wide access: %w", err)
	}
	if ok {
		return nil
	}

	return fmt.Errorf("only users with the 'admin' role defined in this workspace can modify or delete it")
}

// hasClusterWideAccess reports whether the cluster authorizer grants the user
// every verb on every resource in every group — the access cluster-admin
// carries, and the standard meaning of "cluster super-admin".
//
// This asks the API server rather than enumerating ClusterRoleBindings here.
// Doing it locally meant the operator held a list/watch on every
// ClusterRoleBinding in the cluster — an informer caching cluster-wide RBAC in
// the operator's memory to answer one yes/no question — and it recognised only
// a binding to the ClusterRole literally named "cluster-admin". A
// SubjectAccessReview is one request, needs no RBAC cache, and is answered by
// the same authorizer chain that will judge the request anyway, so an
// equivalently-powerful role under a different name is recognised too.
func hasClusterWideAccess(ctx context.Context, c client.Client, userInfo authenticationv1.UserInfo) (bool, error) {
	extra := make(map[string]authorizationv1.ExtraValue, len(userInfo.Extra))
	for key, value := range userInfo.Extra {
		extra[key] = authorizationv1.ExtraValue(value)
	}

	// RBAC matches a wildcard request only against a wildcard rule, so this
	// resolves to "is this subject bound to something as broad as cluster-admin".
	review := &authorizationv1.SubjectAccessReview{
		Spec: authorizationv1.SubjectAccessReviewSpec{
			User:   userInfo.Username,
			Groups: userInfo.Groups,
			UID:    userInfo.UID,
			Extra:  extra,
			ResourceAttributes: &authorizationv1.ResourceAttributes{
				Verb:     "*",
				Group:    "*",
				Resource: "*",
			},
		},
	}
	if err := c.Create(ctx, review); err != nil {
		return false, err
	}
	return review.Status.Allowed, nil
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

// ValidateNamespaceUniqueness ensures no other Workspace already claims the
// same target namespace. Without this check two Workspaces could reference the
// same namespace and the second would be permanently stuck at Ready=False.
func ValidateNamespaceUniqueness(ctx context.Context, reader client.Reader, ws *tenantv1alpha1.Workspace) error {
	var list tenantv1alpha1.WorkspaceList
	if err := reader.List(ctx, &list); err != nil {
		return fmt.Errorf("failed to list workspaces for namespace uniqueness check: %w", err)
	}
	for i := range list.Items {
		existing := &list.Items[i]
		if existing.Name != ws.Name && existing.Spec.Namespace == ws.Spec.Namespace {
			return fmt.Errorf("namespace %q is already used by workspace %q", ws.Spec.Namespace, existing.Name)
		}
	}
	return nil
}

// ValidateWorkspaceName ensures the name can be used as a Kubernetes label value.
func ValidateWorkspaceName(name string) error {
	if len(name) > content.LabelValueMaxLength {
		return fmt.Errorf("workspace metadata.name must be no more than %d bytes", content.LabelValueMaxLength)
	}
	return nil
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
