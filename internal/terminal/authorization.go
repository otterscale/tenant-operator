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

package terminal

import (
	"context"
	"fmt"
	"slices"

	authenticationv1 "k8s.io/api/authentication/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	consolev1alpha1 "github.com/otterscale/api/console/v1alpha1"
)

// privilegedGroups are Kubernetes groups that bypass all Terminal-level
// authorization checks (cluster super-admins).
var privilegedGroups = []string{"system:masters", "kubeadm:cluster-admins"}

// privilegedClusterRoles are Kubernetes ClusterRoles whose bound subjects
// bypass all Terminal-level authorization checks.
var privilegedClusterRoles = []string{"cluster-admin"}

// AuthorizeCreation checks whether the requesting user is allowed to create
// the given Terminal. RBAC (see the terminal-user Role bound to
// system:authenticated in config/console) already lets any logged-in user
// call the terminals API at all; this closes the gap RBAC alone can't:
// a caller may only create a Terminal for themselves.
//
// Allowed callers (checked cheapest-first):
//   - Members of a privileged group (system:masters, kubeadm:cluster-admins)
//   - The operator's own ServiceAccount (operatorSA)
//   - The user whose identity matches the new Terminal's spec.subject
//   - A user bound to a privileged ClusterRole (e.g. cluster-admin) via ClusterRoleBinding
func AuthorizeCreation(ctx context.Context, reader client.Reader, userInfo authenticationv1.UserInfo, t *consolev1alpha1.Terminal, operatorSA string) error {
	return authorize(ctx, reader, userInfo, t.Spec.Subject, operatorSA)
}

// AuthorizeModification checks whether the requesting user is allowed to
// update or delete the given Terminal. The terminal parameter must be the
// **old** (pre-update) object, consistent with how a caller cannot rewrite
// spec.subject to themselves and approve in the same request — though
// spec.subject is separately enforced immutable by the CRD itself.
//
// Allowed callers: same as AuthorizeCreation, matched against the old
// object's spec.subject.
func AuthorizeModification(ctx context.Context, reader client.Reader, userInfo authenticationv1.UserInfo, t *consolev1alpha1.Terminal, operatorSA string) error {
	return authorize(ctx, reader, userInfo, t.Spec.Subject, operatorSA)
}

func authorize(ctx context.Context, reader client.Reader, userInfo authenticationv1.UserInfo, subject, operatorSA string) error {
	if inPrivilegedGroup(userInfo) || userInfo.Username == operatorSA {
		return nil
	}

	if userInfo.Username == subject {
		return nil
	}

	ok, err := hasPrivilegedClusterRole(ctx, reader, userInfo)
	if err != nil {
		return fmt.Errorf("failed to check ClusterRole bindings: %w", err)
	}
	if ok {
		return nil
	}

	return fmt.Errorf("a Terminal's subject must match the identity of the user creating, updating, or deleting it")
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
