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
	"testing"

	authenticationv1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

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

func TestOperatorServiceAccountIdentity(t *testing.T) {
	got := OperatorServiceAccountIdentity("otterscale-system", "tenant-operator-controller-manager")
	want := "system:serviceaccount:otterscale-system:tenant-operator-controller-manager"
	if got != want {
		t.Errorf("OperatorServiceAccountIdentity() = %q, want %q", got, want)
	}
}

func TestAuthorizeModification(t *testing.T) {
	workspace := newWorkspace([]tenantv1alpha1.WorkspaceMember{
		{Role: tenantv1alpha1.MemberRoleAdmin, Subject: "alice"},
		{Role: tenantv1alpha1.MemberRoleEdit, Subject: "bob"},
		{Role: tenantv1alpha1.MemberRoleView, Subject: "charlie"},
	})

	tests := []struct {
		name    string
		user    authenticationv1.UserInfo
		wantErr bool
	}{
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
		{
			name:    "operator service account is allowed",
			user:    authenticationv1.UserInfo{Username: testOperatorSA},
			wantErr: false,
		},
		{
			name:    "workspace admin member is allowed",
			user:    authenticationv1.UserInfo{Username: "alice"},
			wantErr: false,
		},
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
			err := AuthorizeModification(tt.user, workspace, testOperatorSA)
			if (err != nil) != tt.wantErr {
				t.Errorf("AuthorizeModification() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestAuthorizeModification_EmptyMembers(t *testing.T) {
	workspace := newWorkspace(nil)

	// Even an empty member list should deny non-privileged users.
	err := AuthorizeModification(authenticationv1.UserInfo{Username: "alice"}, workspace, testOperatorSA)
	if err == nil {
		t.Error("AuthorizeModification() expected error for user not in empty members list")
	}

	// Privileged user should still pass.
	err = AuthorizeModification(authenticationv1.UserInfo{Username: "admin", Groups: []string{"system:masters"}}, workspace, testOperatorSA)
	if err != nil {
		t.Errorf("AuthorizeModification() unexpected error for privileged user: %v", err)
	}
}
