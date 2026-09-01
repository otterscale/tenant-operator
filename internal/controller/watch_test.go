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
	"testing"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/event"

	ws "github.com/otterscale/tenant-operator/internal/workspace"
)

func operatorConfigMap(data map[string]string) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		Name:      ws.OperatorConfigName,
		Namespace: ws.OperatorNamespace,
		Data:      data,
	}
}

func TestOperatorConfigChangedPredicate(t *testing.T) {
	t.Parallel()

	otherConfig := &corev1.ConfigMap{
		Name: "unrelated-config", Namespace: "workspace-a",
		Data: map[string]string{ws.RancherProjectIDKey: "c-m-abcde:p-vwxyz"},
	}
	p := operatorConfigChangedPredicate()

	t.Run("create", func(t *testing.T) {
		if !p.Create(event.CreateEvent{Object: operatorConfigMap(nil)}) {
			t.Error("global config create was filtered out")
		}
		if p.Create(event.CreateEvent{Object: otherConfig}) {
			t.Error("unrelated ConfigMap create was let through")
		}
	})

	t.Run("delete", func(t *testing.T) {
		if !p.Delete(event.DeleteEvent{Object: operatorConfigMap(nil)}) {
			t.Error("global config delete was filtered out")
		}
		if p.Delete(event.DeleteEvent{Object: otherConfig}) {
			t.Error("unrelated ConfigMap delete was let through")
		}
	})

	t.Run("update on changed data", func(t *testing.T) {
		old := operatorConfigMap(map[string]string{ws.RancherProjectIDKey: "c-m-abcde:p-vwxyz"})
		updated := operatorConfigMap(map[string]string{ws.RancherProjectIDKey: "local:p-other"})
		if !p.Update(event.UpdateEvent{ObjectOld: old, ObjectNew: updated}) {
			t.Error("changed data was filtered out")
		}
	})

	t.Run("update with unchanged data", func(t *testing.T) {
		data := map[string]string{ws.RancherProjectIDKey: "c-m-abcde:p-vwxyz"}
		old := operatorConfigMap(data)
		updated := operatorConfigMap(map[string]string{ws.RancherProjectIDKey: "c-m-abcde:p-vwxyz"})
		updated.Labels = map[string]string{"unrelated": "churn"}
		if p.Update(event.UpdateEvent{ObjectOld: old, ObjectNew: updated}) {
			t.Error("metadata-only update was let through")
		}
	})

	t.Run("update on an unrelated ConfigMap", func(t *testing.T) {
		updated := otherConfig.DeepCopy()
		updated.Data = map[string]string{ws.RancherProjectIDKey: "local:p-other"}
		if p.Update(event.UpdateEvent{ObjectOld: otherConfig, ObjectNew: updated}) {
			t.Error("unrelated ConfigMap update was let through")
		}
	})
}

func operatorSecret() *corev1.Secret {
	return &corev1.Secret{
		Name:      ws.OperatorSecretName,
		Namespace: ws.OperatorNamespace,
		Data: map[string][]byte{
			ws.HarborURLKey:         []byte("https://harbor.example.com"),
			ws.HarborRobotNameKey:   []byte("robot$otterscale"),
			ws.HarborRobotSecretKey: []byte("secret"),
		},
	}
}

func TestOperatorSecretChangedPredicate(t *testing.T) {
	t.Parallel()

	otherSecret := &corev1.Secret{
		Name: ws.ImagePullSecretName, Namespace: "workspace-a",
		Data: map[string][]byte{ws.HarborURLKey: []byte("https://harbor.example.com")},
	}
	p := operatorSecretChangedPredicate()

	t.Run("create and delete", func(t *testing.T) {
		if !p.Create(event.CreateEvent{Object: operatorSecret()}) {
			t.Error("operator secret create was filtered out")
		}
		if p.Create(event.CreateEvent{Object: otherSecret}) {
			t.Error("unrelated Secret create was let through")
		}
		if !p.Delete(event.DeleteEvent{Object: operatorSecret()}) {
			t.Error("operator secret delete was filtered out")
		}
		if p.Delete(event.DeleteEvent{Object: otherSecret}) {
			t.Error("unrelated Secret delete was let through")
		}
	})

	t.Run("update on rotated credentials", func(t *testing.T) {
		old := operatorSecret()
		updated := operatorSecret()
		updated.Data[ws.HarborRobotSecretKey] = []byte("rotated")
		if !p.Update(event.UpdateEvent{ObjectOld: old, ObjectNew: updated}) {
			t.Error("rotated credentials were filtered out")
		}
	})

	// A plain map comparison on []byte values would report every update as a
	// change, fanning out on metadata churn.
	t.Run("update with equal data in distinct byte slices", func(t *testing.T) {
		old := operatorSecret()
		updated := operatorSecret()
		updated.Labels = map[string]string{"unrelated": "churn"}
		if p.Update(event.UpdateEvent{ObjectOld: old, ObjectNew: updated}) {
			t.Error("metadata-only update was let through")
		}
	})

	t.Run("update on an unrelated Secret", func(t *testing.T) {
		updated := otherSecret.DeepCopy()
		updated.Data = map[string][]byte{ws.HarborURLKey: []byte("https://other.example.com")}
		if p.Update(event.UpdateEvent{ObjectOld: otherSecret, ObjectNew: updated}) {
			t.Error("unrelated Secret update was let through")
		}
	})
}
