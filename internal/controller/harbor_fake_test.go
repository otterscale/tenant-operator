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
	// robotExists makes EnsureRobotAccount report the robot as pre-existing,
	// which suppresses image pull Secret creation.
	robotExists bool
}

func (f *fakeHarborClient) EnsureProject(_ context.Context, _ string) error {
	return nil
}

func (f *fakeHarborClient) ReconcileProjectMembers(
	_ context.Context, _ string, _ []harbor.ProjectMember,
) ([]string, error) {
	return f.missingUsers, nil
}

func (f *fakeHarborClient) EnsureRobotAccount(
	_ context.Context, _ string, robotName string,
) (*harbor.RobotCredentials, bool, error) {
	if f.robotExists {
		return nil, false, nil
	}
	return &harbor.RobotCredentials{
		Name:   "robot$" + robotName,
		Secret: "fake-robot-secret",
	}, true, nil
}

// newFakeHarborClient returns the factory the reconciler uses to build its
// Harbor client, ignoring the credentials it is handed.
func newFakeHarborClient(fake *fakeHarborClient) func(string, string, string) harbor.Client {
	return func(_, _, _ string) harbor.Client {
		return fake
	}
}
