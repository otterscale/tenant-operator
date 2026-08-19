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

package terminal

import (
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// Fallbacks applied whenever a Terminal leaves the corresponding spec field
// empty. The image and resource values mirror the original hand-written
// web-terminal manifest this operator replaces.
const (
	// DefaultImage is used for both the "terminal" and "proxy" containers
	// when Terminal.Spec.Image is empty.
	DefaultImage = "dhi.io/kubectl:1-compat"

	// DefaultIdleTimeout is how long a Terminal may stay inactive before the
	// controller garbage-collects it, used when the Terminal's own
	// spec.idleTimeoutSeconds is zero. It must stay greater than zero:
	// spec.idleTimeoutSeconds==0 means "use this default", so a zero default
	// would make every such Terminal look idle the instant it's created.
	DefaultIdleTimeout = 30 * time.Minute
)

var (
	// defaultTerminalResources is used for the "terminal" container when
	// Terminal.Spec.Resources.Terminal is empty.
	defaultTerminalResources = corev1.ResourceRequirements{
		Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("50m"), corev1.ResourceMemory: resource.MustParse("64Mi")},
		Limits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("500m"), corev1.ResourceMemory: resource.MustParse("256Mi")},
	}

	// defaultProxyResources is used for the "proxy" container when
	// Terminal.Spec.Resources.Proxy is empty.
	defaultProxyResources = corev1.ResourceRequirements{
		Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("50m"), corev1.ResourceMemory: resource.MustParse("64Mi")},
		Limits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("200m"), corev1.ResourceMemory: resource.MustParse("128Mi")},
	}
)
