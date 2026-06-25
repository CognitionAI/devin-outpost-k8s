# Development

## Prerequisites

- Rust (pinned in `rust-toolchain.toml`)
- A Kubernetes cluster + `kubectl` for running against (kind/minikube fine for the
  control loop; real worker pods need the not-yet-published worker image)

## Build & check

```bash
cargo check
cargo clippy --all-targets -- -D warnings
cargo fmt --all --check
```

## Regenerate the CRD

The CRD YAML/JSON is generated from the Rust types — never hand-edit it.

```bash
cargo run --bin crdgen > charts/outposts-operator/files/crd-outpostpool.json
```

(`crdgen` prints the CRD; the Helm chart renders it via `templates/crd.yaml`.)

## Run locally

```bash
export RUST_LOG=info,outposts_operator=debug
cargo run --bin outposts-operator
# metrics/health on http://127.0.0.1:8080
```

Apply the CRD and an example pool:

```bash
cargo run --bin crdgen | kubectl apply -f -
kubectl create secret generic my-pool-token --from-literal=token=<PAT>
kubectl apply -f examples/outpostpool.yaml
```

## Helm

```bash
helm lint charts/outposts-operator
helm template charts/outposts-operator
helm template charts/outposts-operator --set defaultPool.enabled=true --set defaultPool.poolId=pool_x
```

## Layout

```
src/
  opbeta/      upstream /opbeta/outposts client + wire types
  crd/         OutpostPool custom resource
  controller/  reconcile loop + worker Pod builder
  snapshot/    SnapshotProvider trait + noop/gke/filesystem
  metrics.rs   Prometheus + health endpoints
  telemetry.rs tracing setup
  config.rs    operator config (env)
  bin/crdgen.rs  CRD generator
charts/        Helm chart
docs/          this documentation
examples/      sample OutpostPool
```

## Scaffold status

Behavioural code paths return `Error::NotImplemented` and are marked with
`TODO`. The project compiles, generates the CRD, and serves metrics; the
reconcile/claim/worker/snapshot logic is intentionally unimplemented.
