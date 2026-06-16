# OtterScale Tenant Operator

[![Tests](https://github.com/otterscale/tenant-operator/actions/workflows/test.yml/badge.svg)](https://github.com/otterscale/tenant-operator/actions/workflows/test.yml)
[![Lint](https://github.com/otterscale/tenant-operator/actions/workflows/lint.yml/badge.svg)](https://github.com/otterscale/tenant-operator/actions/workflows/lint.yml)
[![codecov](https://codecov.io/gh/otterscale/tenant-operator/branch/main/graph/badge.svg)](https://codecov.io/gh/otterscale/tenant-operator)
[![Go Report Card](https://goreportcard.com/badge/github.com/otterscale/tenant-operator)](https://goreportcard.com/report/github.com/otterscale/tenant-operator)
[![Release](https://img.shields.io/github/v/release/otterscale/tenant-operator)](https://github.com/otterscale/tenant-operator/releases/latest)
[![License](https://img.shields.io/github/license/otterscale/tenant-operator)](LICENSE)

**A Kubernetes operator that turns a single `Workspace` resource into a fully isolated multi-tenant environment.**

The OtterScale Tenant Operator reconciles the `Workspace` custom resource (`tenant.otterscale.io/v1alpha1`) and provisions everything a tenant needs: a dedicated namespace, RBAC bindings, resource quotas, network isolation, and optional image-registry access. The controller stays thin — it fetches the desired state, syncs each concern, and lets Kubernetes garbage collection clean up owned resources when a workspace is removed.

## What It Manages

For every `Workspace`, the operator reconciles:

- **Namespace** — created with Pod Security Standards labels applied.
- **RBAC** — RoleBindings for admin, edit, and view roles, grouped by member.
- **Resource constraints** — ResourceQuota and LimitRange when specified.
- **Network isolation** — default-deny NetworkPolicies with allow rules for the workspace and approved namespaces.
- **Harbor integration** _(optional)_ — projects, robot accounts, image-pull secrets, and a Flux `HelmRepository` pointing at the registry.
- **Configuration** — a workspace-level ConfigMap for service-discovery endpoint overrides.

Admission webhooks default and validate `Workspace` resources — auto-generating namespace names, syncing members to labels, and enforcing creation authorization.

## Built With

Go and [kubebuilder](https://book.kubebuilder.io/) on top of `controller-runtime`. The `Workspace` API is defined in [otterscale/api](https://github.com/otterscale/api).

## Documentation

Installation, the full `Workspace` spec, and operational guides will be published in the project documentation.

## Ecosystem

The Tenant Operator is one component of the OtterScale platform; the `Workspace` CRD it reconciles is defined in [otterscale/api](https://github.com/otterscale/api). See the [otterscale](https://github.com/otterscale/otterscale) repository for an overview of the full project and its components.

## Contributing

Contributions are welcome. A contribution guide (`CONTRIBUTING.md`) will follow; until then, please open an issue or a pull request to get involved.

## License

<<<<<<< HEAD
=======
This project is licensed under the [Apache License 2.0](LICENSE).

[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2Fotterscale%2Ftenant-operator.svg?type=large&issueType=license)](https://app.fossa.com/projects/git%2Bgithub.com%2Fotterscale%2Ftenant-operator?ref=badge_large&issueType=license)
>>>>>>> tmp-original-16-06-26-02-32
