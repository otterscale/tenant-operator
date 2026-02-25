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
	"sync/atomic"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/rest"
)

// IstioDetector provides a thread-safe way to check whether Istio CRDs are available
// in the cluster. It uses an atomic boolean that is refreshed via the Discovery API
// whenever a relevant CRD event is observed.
//
// The DiscoveryClient is cached to avoid creating a new HTTP client on every Refresh.
type IstioDetector struct {
	dc      *discovery.DiscoveryClient
	enabled atomic.Bool
}

// NewIstioDetector creates an IstioDetector and performs an initial availability check.
// If the DiscoveryClient cannot be created (e.g., nil config in tests), the detector
// defaults to disabled and Refresh becomes a no-op.
func NewIstioDetector(config *rest.Config) *IstioDetector {
	d := &IstioDetector{}
	if config != nil {
		dc, err := discovery.NewDiscoveryClientForConfig(config)
		if err == nil {
			d.dc = dc
		}
	}
	d.Refresh()
	return d
}

// IsEnabled returns whether Istio CRDs are currently available in the cluster.
func (d *IstioDetector) IsEnabled() bool {
	return d.enabled.Load()
}

// Refresh re-checks Istio CRD availability via the Discovery API and updates the flag.
// On error the previous state is preserved; this is intentional to avoid flapping
// caused by transient API-server issues.
func (d *IstioDetector) Refresh() {
	if d.dc == nil {
		return
	}
	supported, definitive := isResourceSupported(d.dc, "security.istio.io/v1")
	if definitive {
		d.enabled.Store(supported)
	}
	// Non-definitive result (transient error): keep previous state to avoid flapping.
}

// isResourceSupported checks if a CRD group version exists in the cluster.
// It returns whether the group is supported and whether the answer is definitive.
// When the API server responds with a status code (e.g. 404 Not Found), the result
// is definitive. Transport-level errors (connection refused, timeout) are transient
// and the caller should preserve the previous state.
func isResourceSupported(dc *discovery.DiscoveryClient, groupVersion string) (supported bool, definitive bool) {
	_, err := dc.ServerResourcesForGroupVersion(groupVersion)
	if err == nil {
		return true, true
	}
	// If the API server responded with a HTTP status error, the determination is
	// definitive: the group version does not exist.
	if apierrors.IsNotFound(err) || apierrors.ReasonForError(err) != "" {
		return false, true
	}
	// Transport-level error (e.g. connection refused, timeout) — not definitive.
	return false, false
}
