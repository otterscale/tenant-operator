# Third-party CRDs for envtest

CRDs owned by other projects that the controller test suite needs served.
`SetupWithManager` refuses to start unless the cluster serves both of these, so
without them every controller spec fails.

`make verify-test-crds` regenerates both from the modules pinned in `go.mod` and
fails if what is committed here differs. It runs in the Tests workflow, so a
dependency bump that leaves these behind turns that PR red instead of silently
testing against an older schema.

Refresh them with the steps below after bumping the corresponding module — or
after bumping `CONTROLLER_TOOLS_VERSION`, since the generated CRD records the
controller-gen version that produced it.

## Gateway API

Published as manifests by the upstream module, so copy rather than generate:

```sh
GW=$(go list -m -f '{{.Dir}}' sigs.k8s.io/gateway-api)
cp "$GW/config/crd/standard/gateway.networking.k8s.io_gateways.yaml" test/crd/
chmod u+w test/crd/gateway.networking.k8s.io_gateways.yaml   # module cache is read-only
```

## Flux source-controller

Ships API types but no manifests, so the CRD is generated from the pinned
version:

```sh
make controller-gen
FLUX=$(go list -m -f '{{.Dir}}' github.com/fluxcd/source-controller/api)
./bin/controller-gen crd \
  paths="$FLUX/v1/..." \
  output:crd:artifacts:config=test/crd
# Generates every source-controller CRD; keep only helmrepositories and delete
# the rest.
```

`config/crd/bases/` is for this project's own generated CRDs and is written by
`make manifests` — do not put third-party manifests there.
