# OtterScale Operator Template

[![Tests](https://github.com/otterscale/operator-template/actions/workflows/test.yml/badge.svg)](https://github.com/otterscale/operator-template/actions/workflows/test.yml)
[![Lint](https://github.com/otterscale/operator-template/actions/workflows/lint.yml/badge.svg)](https://github.com/otterscale/operator-template/actions/workflows/lint.yml)
[![codecov](https://codecov.io/gh/otterscale/operator-template/branch/main/graph/badge.svg)](https://codecov.io/gh/otterscale/operator-template)
[![Go Report Card](https://goreportcard.com/badge/github.com/otterscale/operator-template)](https://goreportcard.com/report/github.com/otterscale/operator-template)
[![Release](https://img.shields.io/github/v/release/otterscale/operator-template)](https://github.com/otterscale/operator-template/releases/latest)
[![License](https://img.shields.io/github/license/otterscale/operator-template)](LICENSE)

A GitHub repository template for building Kubernetes operators that reconcile [OtterScale](https://github.com/otterscale/api) custom resources. Scaffolded with [Kubebuilder](https://book.kubebuilder.io) v4.

## Quick Start

Click **"Use this template"** on GitHub to create a new repository, then clone it locally.

### Add an API Controller

Use `kubebuilder create api` with the external API flags to scaffold a controller for an OtterScale resource:

```bash
kubebuilder create api \
  --group addons --version v1alpha1 --kind Module \
  --controller=true --resource=false \
  --external-api-path=github.com/otterscale/api/addons/v1alpha1 \
  --external-api-domain=otterscale.io \
  --external-api-module=github.com/otterscale/api
```

| Flag                    | Purpose                                                      |
| ----------------------- | ------------------------------------------------------------ |
| `--controller=true`     | Generate a controller for reconciliation logic               |
| `--resource=false`      | Skip CRD generation (the CRD is defined in the external API) |
| `--external-api-path`   | Go import path of the external API types                     |
| `--external-api-domain` | API group domain (produces `addons.otterscale.io`)           |
| `--external-api-module` | Go module that provides the types                            |

This scaffolds:

- `internal/controller/module_controller.go` — reconciliation logic
- `internal/controller/module_controller_test.go` — test skeleton
- Registration in `cmd/main.go`

Repeat for additional resources by changing `--group`, `--version`, and `--kind`.

### After Scaffolding

```bash
go mod tidy
make manifests generate
```

## Versioning

The `cmd/main.go` file declares a `version` variable (defaults to `"devel"`). This value is injected at build time via `-ldflags`:

```go
var version = "devel"
```

Both the `Makefile` and `Dockerfile` automatically pass `VERSION` (derived from `git describe --tags --always`) through `-ldflags "-X main.version=$(VERSION)"`. If you add version-dependent logic (e.g. logging, health endpoints, or user-agent strings), make sure to reference `version` from `cmd/main.go`.

> **Note:** When customizing the build process or adding a new build target, remember to include `-ldflags "-X main.version=$(VERSION)"` so the binary embeds the correct version.

## Development

### Prerequisites

- Go 1.26+
- Docker 17.03+
- kubectl v1.11.3+
- Access to a Kubernetes cluster

### Run Locally

```bash
make run
```

### Run Tests

```bash
make test
```

### Lint

```bash
make lint      # check
make lint-fix  # auto-fix
```

## Deployment

### Build & Push Image

```bash
export IMG=<registry>/<project>:tag
make docker-build docker-push IMG=$IMG
```

### Deploy to Cluster

```bash
make deploy IMG=$IMG
```

### Undeploy

```bash
make undeploy
```

## CI / CD

This template includes GitHub Actions workflows out of the box:

| Workflow        | Trigger               | Description                                                |
| --------------- | --------------------- | ---------------------------------------------------------- |
| **Lint**        | push, PR              | Runs `golangci-lint`                                       |
| **Tests**       | push, PR              | Runs `make test` (unit tests via envtest)                  |
| **E2E Tests**   | push, PR              | Runs end-to-end tests on a Kind cluster                    |
| **Publish**     | release published     | Builds & pushes image to `ghcr.io`, uploads `install.yaml` |
| **Auto Update** | weekly (Tue) / manual | Checks for Kubebuilder scaffold updates                    |

## Distribution

### YAML Bundle

```bash
make build-installer IMG=<registry>/<project>:tag
```

Users install with:

```bash
kubectl apply -f https://raw.githubusercontent.com/<org>/<repo>/<tag>/dist/install.yaml
```

### Helm Chart

```bash
kubebuilder edit --plugins=helm/v2-alpha
```

A chart will be generated under `dist/chart/`.

## License

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
