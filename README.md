# outposts-kubernetes

A Kubernetes operator that runs **Devin Outposts** ("Bring Your Own Box")
workers on any certified Kubernetes cluster (GKE, EKS, ...).

The Devin control plane exposes a Kubernetes-shaped, account-scoped queue API
(`/opbeta/outposts`, "Outposts Beta") listing the Devin sessions that should be
running. This operator consumes that queue: it claims sessions and runs a Devin
worker `Pod` for each — the cluster-native replacement for the single-host
`devin worker` CLI.

> **Status: scaffold.** Types, skeletons, CRD, Helm chart, and docs only. The
> reconcile/claim/worker/snapshot logic is **not implemented yet** (stubs return
> `Error::NotImplemented`). The project compiles, generates its CRD, and serves
> metrics.
>
> **Testing is blocked** on a lightweight devin-CLI worker image, which is not
> published yet; `worker.image` values are placeholders.

## Features (planned)

- One operator, many pools via the `OutpostPool` CRD (`outposts.cognition.ai/v1alpha1`)
- Per-pool `nodeSelector`, pod resource requests/limits, runtime class
  (gVisor / Kata), and GKE Autopilot annotations/labels
- Resume handling via `resumePolicy`: `StartFresh` | `GkeSnapshot` |
  `FilesystemSnapshot` (GKE [pod snapshots] default off)
- Helm chart with an optional one-line default pool
- Prometheus `/metrics` + health endpoints

## Quick start

```bash
# Install operator + CRD, and a default pool in one shot:
helm install outposts charts/outposts-operator \
  --set defaultPool.enabled=true \
  --set defaultPool.poolId=pool_xxx \
  --set defaultPool.token.value=<PAT>

# Or install the operator and manage pools yourself:
helm install outposts charts/outposts-operator
kubectl create secret generic my-pool-token --from-literal=token=<PAT>
kubectl apply -f examples/outpostpool.yaml
```

The PAT needs the `UseOutpostsMachine` account permission.

## Documentation

- [Architecture](docs/ARCHITECTURE.md)
- [Upstream API contract](docs/API_CONTRACT.md)
- [Snapshots & resume](docs/SNAPSHOTS.md)
- [Observability](docs/OBSERVABILITY.md)
- [Development](docs/DEVELOPMENT.md)

## License

[MIT](LICENSE)

[pod snapshots]: https://docs.cloud.google.com/kubernetes-engine/docs/concepts/pod-snapshots
