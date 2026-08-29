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

package controller

import (
	"context"
	"fmt"

	"github.com/otterscale/tenant-operator/internal/harbor"
)

// fakeHarborClient satisfies harbor.Client without talking to a Harbor server.
// The Harbor integration is a deployment prerequisite, so every reconcile in
// these tests goes through it; this keeps that path exercised while leaving the
// Harbor API itself covered by internal/harbor's own tests.
//
// The zero value reports every robot as newly created, so ReconcileHarbor takes
// its "write the image pull Secret" branch.
type fakeHarborClient struct {
	// missingUsers is returned from ReconcileProjectMembers so specs can drive
	// the HarborMembersPending path.
	missingUsers []string
	// robotExists makes EnsureRobot report the robot as pre-existing, so it
	// returns no credentials — the state that forces the caller to decide
	// between leaving the image pull Secret alone and refreshing the secret.
	robotExists bool
	// desiredMembers records what the last ReconcileProjectMembers was asked to
	// sync, so specs can assert on the identity a member is given in Harbor.
	desiredMembers []harbor.ProjectMember
	// refreshes counts RefreshRobotSecret calls. Refreshing invalidates the
	// live credentials, so specs assert on when it happens, not only that the
	// Secret ends up present.
	refreshes int
}

// robotFullName mirrors Harbor's naming for a project-level robot. The real
// client returns this form, so the fake must too — a fake that answered
// "robot$<robot>" would let a mismatch in the docker config username through.
func robotFullName(projectName, robotName string) string {
	return fmt.Sprintf("robot$%s+%s", projectName, robotName)
}

func (f *fakeHarborClient) EnsureProject(_ context.Context, _ string) error {
	return nil
}

func (f *fakeHarborClient) ReconcileProjectMembers(
	_ context.Context, _ string, desired []harbor.ProjectMember,
) ([]string, error) {
	f.desiredMembers = desired
	return f.missingUsers, nil
}

func (f *fakeHarborClient) EnsureRobot(
	_ context.Context, projectName, robotName string,
) (*harbor.RobotCredentials, error) {
	if f.robotExists {
		return nil, nil
	}
	return &harbor.RobotCredentials{
		Name:   robotFullName(projectName, robotName),
		Secret: "fake-robot-secret",
	}, nil
}

func (f *fakeHarborClient) RefreshRobotSecret(
	_ context.Context, projectName, robotName string,
) (*harbor.RobotCredentials, error) {
	f.refreshes++
	return &harbor.RobotCredentials{
		Name: robotFullName(projectName, robotName),
		// Distinct from the create-path secret so specs can tell which call the
		// Secret's contents came from.
		Secret: "refreshed-robot-secret",
	}, nil
}

// newFakeHarborClient returns the factory the reconciler uses to build its
// Harbor client, ignoring the credentials it is handed.
func newFakeHarborClient(fake *fakeHarborClient) func(string, string, string) harbor.Client {
	return func(_, _, _ string) harbor.Client {
		return fake
	}
}
