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

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestResolveClusterIP(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)

	tests := []struct {
		name    string
		objects []metav1.Object
		wantIP  string
		wantErr bool
	}{
		{
			name: "valid controlPlaneEndpoint with ip:port",
			objects: []metav1.Object{
				&corev1.ConfigMap{
					ObjectMeta: metav1.ObjectMeta{Name: "kubeadm-config", Namespace: "kube-system"},
					Data: map[string]string{
						"ClusterConfiguration": "controlPlaneEndpoint: 10.102.197.202:8443\n",
					},
				},
			},
			wantIP: "10.102.197.202",
		},
		{
			name: "controlPlaneEndpoint without port",
			objects: []metav1.Object{
				&corev1.ConfigMap{
					ObjectMeta: metav1.ObjectMeta{Name: "kubeadm-config", Namespace: "kube-system"},
					Data: map[string]string{
						"ClusterConfiguration": "controlPlaneEndpoint: 10.0.0.1\n",
					},
				},
			},
			wantIP: "10.0.0.1",
		},
		{
			name:    "kubeadm-config not found",
			objects: []metav1.Object{},
			wantErr: true,
		},
		{
			name: "missing ClusterConfiguration key",
			objects: []metav1.Object{
				&corev1.ConfigMap{
					ObjectMeta: metav1.ObjectMeta{Name: "kubeadm-config", Namespace: "kube-system"},
					Data:       map[string]string{"other": "data"},
				},
			},
			wantErr: true,
		},
		{
			name: "empty controlPlaneEndpoint",
			objects: []metav1.Object{
				&corev1.ConfigMap{
					ObjectMeta: metav1.ObjectMeta{Name: "kubeadm-config", Namespace: "kube-system"},
					Data: map[string]string{
						"ClusterConfiguration": "kubernetesVersion: v1.35.3\n",
					},
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			objs := make([]runtime.Object, len(tt.objects))
			for i, o := range tt.objects {
				objs[i] = o.(runtime.Object)
			}
			c := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(objs...).Build()

			got, err := resolveClusterIP(context.Background(), c)
			if (err != nil) != tt.wantErr {
				t.Errorf("resolveClusterIP() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.wantIP {
				t.Errorf("resolveClusterIP() = %q, want %q", got, tt.wantIP)
			}
		})
	}
}

func TestResolveServiceNodePort(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)

	tests := []struct {
		name     string
		objects  []metav1.Object
		ns       string
		svcName  string
		portName string
		wantPort int32
		wantErr  bool
	}{
		{
			name: "valid service with named nodeport",
			objects: []metav1.Object{
				&corev1.Service{
					ObjectMeta: metav1.ObjectMeta{Name: "my-svc", Namespace: "my-ns"},
					Spec: corev1.ServiceSpec{
						Type: corev1.ServiceTypeNodePort,
						Ports: []corev1.ServicePort{
							{Name: "http", NodePort: 30000},
							{Name: "grpc", NodePort: 30001},
						},
					},
				},
			},
			ns:       "my-ns",
			svcName:  "my-svc",
			portName: "grpc",
			wantPort: 30001,
		},
		{
			name:     "service not found",
			objects:  []metav1.Object{},
			ns:       "my-ns",
			svcName:  "missing-svc",
			portName: "http",
			wantErr:  true,
		},
		{
			name: "port name not found",
			objects: []metav1.Object{
				&corev1.Service{
					ObjectMeta: metav1.ObjectMeta{Name: "my-svc", Namespace: "my-ns"},
					Spec: corev1.ServiceSpec{
						Ports: []corev1.ServicePort{
							{Name: "http", NodePort: 30000},
						},
					},
				},
			},
			ns:       "my-ns",
			svcName:  "my-svc",
			portName: "missing",
			wantErr:  true,
		},
		{
			name: "nodeport is zero",
			objects: []metav1.Object{
				&corev1.Service{
					ObjectMeta: metav1.ObjectMeta{Name: "my-svc", Namespace: "my-ns"},
					Spec: corev1.ServiceSpec{
						Ports: []corev1.ServicePort{
							{Name: "http", NodePort: 0},
						},
					},
				},
			},
			ns:       "my-ns",
			svcName:  "my-svc",
			portName: "http",
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			objs := make([]runtime.Object, len(tt.objects))
			for i, o := range tt.objects {
				objs[i] = o.(runtime.Object)
			}
			c := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(objs...).Build()

			got, err := resolveServiceNodePort(context.Background(), c, tt.ns, tt.svcName, tt.portName)
			if (err != nil) != tt.wantErr {
				t.Errorf("resolveServiceNodePort() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.wantPort {
				t.Errorf("resolveServiceNodePort() = %d, want %d", got, tt.wantPort)
			}
		})
	}
}
