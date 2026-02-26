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
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/otterscale/tenant-operator/internal/labels"
)

const (
	RoleBindingName         = "workspace-role-binding"
	ResourceQuotaName       = "workspace-resource-quota"
	LimitRangeName          = "workspace-limit-range"
	NetworkPolicyName       = "workspace-network-policy"
	PeerAuthenticationName  = "workspace-peer-authentication"
	AuthorizationPolicyName = "workspace-authorization-policy"

	UserLabelPrefix = "user.otterscale.io/"

	// ConditionTypeReady is the condition type that indicates whether all
	// workspace resources have been successfully reconciled.
	ConditionTypeReady = "Ready"
)

// LabelsForWorkspace returns a standard set of labels for resources managed by this operator.
func LabelsForWorkspace(workspace, version string) map[string]string {
	return labels.Standard(workspace, "workspace", version)
}

// IsOwned checks if the object is owned by the given UID to prevent adoption conflicts.
func IsOwned(refs []metav1.OwnerReference, uid types.UID) bool {
	for _, ref := range refs {
		if ref.UID == uid {
			return true
		}
	}
	return false
}

// IgnoreNoMatchNotFound ignores NoMatch errors and NotFound errors.
// This is useful when deleting Istio resources that may not exist in the cluster.
func IgnoreNoMatchNotFound(err error) error {
	if meta.IsNoMatchError(err) {
		return nil
	}
	return client.IgnoreNotFound(err)
}
