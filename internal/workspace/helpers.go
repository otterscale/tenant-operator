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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/otterscale/tenant-operator/internal/labels"
)

const (
	// OperatorNamespace is where the operator runs, and where it looks for the
	// operator-wide tenant-operator-config ConfigMap and tenant-operator-secret.
	OperatorNamespace = "otterscale-system"

	// ServiceAccountName is also the name of its RoleBinding, and is the value
	// cluster admission requires in a tenant HelmRelease.
	ServiceAccountName = "workspace-deployer"

	RoleBindingName          = "workspace-role-binding"
	ResourceQuotaName        = "workspace-resource-quota"
	LimitRangeName           = "workspace-limit-range"
	NetworkPolicyName        = "workspace-network-policy"
	ImagePullSecretName      = "workspace-image-pull-secret"
	HelmRepositoryName       = "workspace-helm-repository"
	HarborDefaultProjectName = "library"

	LabelFromHarbor = "tenant.otterscale.io/from-harbor"
	LabelInternal   = "tenant.otterscale.io/internal"

	UserLabelPrefix = "user.otterscale.io/"

	// clusterRoleKind is the RoleRef.Kind naming a ClusterRole. rbac/v1 exports
	// constants for subject kinds but not for this one.
	clusterRoleKind = "ClusterRole"

	// ConditionTypeReady reports whether all workspace resources reconciled.
	ConditionTypeReady = "Ready"
)

// LabelsForWorkspace returns the standard labels for operator-managed resources.
func LabelsForWorkspace(workspace, version string) map[string]string {
	return labels.Standard(workspace, "workspace", version)
}

// IsOwned reports whether the given UID appears in refs, guarding against
// adopting resources that belong to someone else.
func IsOwned(refs []metav1.OwnerReference, uid types.UID) bool {
	for _, ref := range refs {
		if ref.UID == uid {
			return true
		}
	}
	return false
}
