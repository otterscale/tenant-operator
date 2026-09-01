# Third-party CRDs for envtest

CRDs owned by other projects that the controller test suite needs served.
`SetupWithManager` refuses to start unless the cluster serves them, so without
them every controller spec fails.

`make verify-test-crds` regenerates them from the modules pinned in `go.mod` and
fails if what is committed here differs. It runs in the Tests workflow, so a
dependency bump that leaves these behind turns that PR red instead of silently
testing against an older schema.

Refresh them with the steps below after bumping the corresponding module — or
after bumping `CONTROLLER_TOOLS_VERSION`, since the generated CRD records the
controller-gen version that produced it.

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
