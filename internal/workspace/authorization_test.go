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
	"testing"

	authenticationv1 "k8s.io/api/authentication/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	tenantv1alpha1 "github.com/otterscale/api/tenant/v1alpha1"
)

// newWorkspace is a test helper that builds a Workspace with the given members.
func newWorkspace(members []tenantv1alpha1.WorkspaceMember) *tenantv1alpha1.Workspace {
	return &tenantv1alpha1.Workspace{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-workspace",
		},
		Spec: tenantv1alpha1.WorkspaceSpec{
			Namespace: "test-ns",
			Members:   members,
		},
	}
}

const testOperatorSA = "system:serviceaccount:test-system:test-controller-manager"

func newFakeReader(objs ...runtime.Object) client.Reader {
	s := runtime.NewScheme()
	_ = rbacv1.AddToScheme(s)
	return fake.NewClientBuilder().WithScheme(s).WithRuntimeObjects(objs...).Build()
}

func TestOperatorServiceAccountIdentity(t *testing.T) {
	got := OperatorServiceAccountIdentity("otterscale-system", "tenant-operator-controller-manager")
	want := "system:serviceaccount:otterscale-system:tenant-operator-controller-manager"
	if got != want {
		t.Errorf("OperatorServiceAccountIdentity() = %q, want %q", got, want)
	}
}

func TestAuthorizeModification(t *testing.T) {
	clusterAdminBinding := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "cluster-admin-binding"},
		RoleRef: rbacv1.RoleRef{
			APIGroup: rbacv1.GroupName,
			Kind:     "ClusterRole",
			Name:     "cluster-admin",
		},
		Subjects: []rbacv1.Subject{
			{Kind: rbacv1.UserKind, Name: "super-admin"},
			{Kind: rbacv1.GroupKind, Name: "ops-team"},
			{Kind: rbacv1.ServiceAccountKind, Name: "deployer", Namespace: "ci"},
		},
	}
	nonPrivilegedBinding := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "view-binding"},
		RoleRef: rbacv1.RoleRef{
			APIGroup: rbacv1.GroupName,
			Kind:     "ClusterRole",
			Name:     "view",
		},
		Subjects: []rbacv1.Subject{
			{Kind: rbacv1.UserKind, Name: "viewer"},
		},
	}

	reader := newFakeReader(clusterAdminBinding, nonPrivilegedBinding)
	ctx := context.Background()

	ws := newWorkspace([]tenantv1alpha1.WorkspaceMember{
		{Role: tenantv1alpha1.MemberRoleAdmin, Subject: "alice"},
		{Role: tenantv1alpha1.MemberRoleEdit, Subject: "bob"},
		{Role: tenantv1alpha1.MemberRoleView, Subject: "charlie"},
	})

	tests := []struct {
		name    string
		user    authenticationv1.UserInfo
		wantErr bool
	}{
		// Privileged groups
		{
			name:    "system:masters group is allowed",
			user:    authenticationv1.UserInfo{Username: "any-user", Groups: []string{"system:masters"}},
			wantErr: false,
		},
		{
			name:    "kubeadm:cluster-admins group is allowed",
			user:    authenticationv1.UserInfo{Username: "any-user", Groups: []string{"kubeadm:cluster-admins"}},
			wantErr: false,
		},
		{
			name:    "privileged group among other groups",
			user:    authenticationv1.UserInfo{Username: "any-user", Groups: []string{"dev-team", "system:masters", "ops"}},
			wantErr: false,
		},
		// Operator SA
		{
			name:    "operator service account is allowed",
			user:    authenticationv1.UserInfo{Username: testOperatorSA},
			wantErr: false,
		},
		// Workspace admin
		{
			name:    "workspace admin member is allowed",
			user:    authenticationv1.UserInfo{Username: "alice"},
			wantErr: false,
		},
		// Privileged ClusterRole (cluster-admin) via ClusterRoleBinding
		{
			name:    "user bound to cluster-admin via User subject",
			user:    authenticationv1.UserInfo{Username: "super-admin"},
			wantErr: false,
		},
		{
			name:    "user in group bound to cluster-admin via Group subject",
			user:    authenticationv1.UserInfo{Username: "someone", Groups: []string{"ops-team"}},
			wantErr: false,
		},
		{
			name:    "service account bound to cluster-admin via ServiceAccount subject",
			user:    authenticationv1.UserInfo{Username: "system:serviceaccount:ci:deployer"},
			wantErr: false,
		},
		// Denied
		{
			name:    "workspace edit member is denied",
			user:    authenticationv1.UserInfo{Username: "bob"},
			wantErr: true,
		},
		{
			name:    "workspace view member is denied",
			user:    authenticationv1.UserInfo{Username: "charlie"},
			wantErr: true,
		},
		{
			name:    "user bound to non-privileged role only is denied",
			user:    authenticationv1.UserInfo{Username: "viewer"},
			wantErr: true,
		},
		{
			name:    "unknown user is denied",
			user:    authenticationv1.UserInfo{Username: "mallory"},
			wantErr: true,
		},
		{
			name:    "empty username with no groups is denied",
			user:    authenticationv1.UserInfo{},
			wantErr: true,
		},
		{
			name:    "non-privileged group is denied",
			user:    authenticationv1.UserInfo{Username: "mallory", Groups: []string{"system:authenticated", "dev-team"}},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := AuthorizeModification(ctx, reader, tt.user, ws, testOperatorSA)
			if (err != nil) != tt.wantErr {
				t.Errorf("AuthorizeModification() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestAuthorizeModification_EmptyMembers(t *testing.T) {
	reader := newFakeReader()
	ctx := context.Background()
	ws := newWorkspace(nil)

	err := AuthorizeModification(ctx, reader, authenticationv1.UserInfo{Username: "alice"}, ws, testOperatorSA)
	if err == nil {
		t.Error("AuthorizeModification() expected error for user not in empty members list")
	}

	err = AuthorizeModification(ctx, reader, authenticationv1.UserInfo{Username: "admin", Groups: []string{"system:masters"}}, ws, testOperatorSA)
	if err != nil {
		t.Errorf("AuthorizeModification() unexpected error for privileged user: %v", err)
	}
}

func TestAuthorizeModification_IgnoresNonClusterRoleKind(t *testing.T) {
	binding := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "role-not-clusterrole"},
		RoleRef: rbacv1.RoleRef{
			APIGroup: rbacv1.GroupName,
			Kind:     "Role",
			Name:     "cluster-admin",
		},
		Subjects: []rbacv1.Subject{
			{Kind: rbacv1.UserKind, Name: "tricky-user"},
		},
	}
	reader := newFakeReader(binding)
	ws := newWorkspace(nil)

	err := AuthorizeModification(context.Background(), reader, authenticationv1.UserInfo{Username: "tricky-user"}, ws, testOperatorSA)
	if err == nil {
		t.Error("AuthorizeModification() should deny when RoleRef.Kind is not ClusterRole")
	}
}
