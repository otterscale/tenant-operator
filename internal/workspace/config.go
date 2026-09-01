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

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// OperatorConfigName is the operator-wide ConfigMap, in OperatorNamespace,
// holding settings that cannot be discovered from the cluster (see
// RancherProjectIDKey). It is never copied into a workspace, and unlike the
// Harbor credentials Secret it is optional.
const OperatorConfigName = "tenant-operator-config"

// operatorConfigData returns the data of the tenant-operator-config ConfigMap.
// A missing ConfigMap yields a nil map, so callers treat "not configured" and
// "configured with nothing" identically.
func operatorConfigData(ctx context.Context, c client.Client) (map[string]string, error) {
	var cm corev1.ConfigMap
	key := types.NamespacedName{Name: OperatorConfigName, Namespace: OperatorNamespace}
	if err := c.Get(ctx, key, &cm); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("reading operator ConfigMap %s: %w", key, err)
	}
	return cm.Data, nil
}

// IsOperatorConfig reports whether obj is the operator-wide ConfigMap.
func IsOperatorConfig(obj client.Object) bool {
	if obj == nil {
		return false
	}
	return obj.GetName() == OperatorConfigName &&
		obj.GetNamespace() == OperatorNamespace
}
