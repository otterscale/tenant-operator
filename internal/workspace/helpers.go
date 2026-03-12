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
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	tenantv1alpha1 "github.com/otterscale/api/tenant/v1alpha1"
	"github.com/otterscale/tenant-operator/internal/labels"
)

const (
	RoleBindingName   = "workspace-role-binding"
	ResourceQuotaName = "workspace-resource-quota"
	LimitRangeName    = "workspace-limit-range"
	NetworkPolicyName = "workspace-network-policy"

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

func SyncMemberLabels(ws *tenantv1alpha1.Workspace) bool {
	if ws == nil {
		return false
	}

	labelsMap := ws.GetLabels()
	if labelsMap == nil {
		labelsMap = make(map[string]string)
	}

	desired := make(map[string]struct{}, len(ws.Spec.Members))
	for _, m := range ws.Spec.Members {
		desired[UserLabelPrefix+m.Subject] = struct{}{}
	}

	changed := false
	for k := range labelsMap {
		if strings.HasPrefix(k, UserLabelPrefix) {
			if _, ok := desired[k]; !ok {
				delete(labelsMap, k)
				changed = true
			}
		}
	}

	for k := range desired {
		if labelsMap[k] != "true" {
			labelsMap[k] = "true"
			changed = true
		}
	}

	if changed {
		ws.SetLabels(labelsMap)
	}
	return changed
}
