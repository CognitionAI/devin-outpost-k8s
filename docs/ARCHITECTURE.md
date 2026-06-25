# Architecture

> **Status: scaffold.** This documents the intended design. Behavioural logic is
> stubbed in code (`Error::NotImplemented`).

## What this is

The Devin control plane exposes a Kubernetes-shaped, account-scoped **queue API**
under `/opbeta/outposts` ("Outposts Beta"). The API lists Devin sessions that
*should* be running; it is the responsibility of whoever implements the API to
actually run a Devin worker for each session, however they can.

`outposts-operator` is one such implementer: a Kubernetes operator that runs
Devin Outposts ("Bring Your Own Box") workers as `Pod`s on any certified
Kubernetes cluster (GKE, EKS, ...). It is the cluster-native replacement for the
single-host `devin worker` CLI
(`devin-webapp/apps/chisel/chisel/src/worker/`).

## Components

```
                        +---------------------------+
   /opbeta/outposts <-- |     OutpostsClient        |  (src/opbeta)
   (devin-webapp)       |  list / watch / claim     |
                        +------------+--------------+
                                     |
   OutpostPool CR  --->  +-----------v--------------+      owns
   (src/crd)            |   Controller / reconcile  | ---------------> Pods
                        |   (src/controller)        |   (one per claimed
                        +-----------+--------------+      session)
                                     |
                        +-----------v--------------+
                        |   SnapshotProvider        |  (src/snapshot)
                        |   noop / gke / filesystem |
                        +---------------------------+

   Metrics/health (src/metrics) :8080  /metrics /healthz /readyz
```

- **`opbeta`** — typed client + models for the upstream queue API. Mirrors the
  reference worker's wire types so the contract stays in lockstep.
- **`crd`** — the `OutpostPool` custom resource (`outposts.cognition.ai/v1alpha1`).
  One CR binds one upstream pool (pool ID + account PAT) to a worker `Pod`
  template. **One operator deployment reconciles many `OutpostPool`s.**
- **`controller`** — a `kube::runtime::Controller` over `OutpostPool` (and, once
  implemented, owned worker `Pod`s). Per pool it lists/watches the queue, claims
  up to `maxConcurrentSessions` pending sessions, creates an owned worker `Pod`
  per claim, renews claims before their deadline, and tears down pods as sessions
  terminate.
- **`snapshot`** — a cloud-portable `SnapshotProvider` trait selected by the
  pool's `resumePolicy`. Backs `resume`-kind sessions and (optionally) idle cost
  savings.
- **`metrics` / `telemetry`** — Prometheus `/metrics` + health endpoints and
  tracing setup.

## Reconcile flow (intended)

1. List/watch `GET /opbeta/outposts/devins?pool=<id>` (resumable opaque cursor,
   at-least-once delivery).
2. For each `pending` session, up to `maxConcurrentSessions`:
   `POST .../devins/{id}/claim` (atomic CAS, `409` => someone else won, skip).
   The claim returns a short-lived `connect_token` + `gateway_url`.
3. Create an owned worker `Pod` from the pool's `worker` template, injecting the
   gateway/connect/session env (see [API_CONTRACT.md](./API_CONTRACT.md)).
4. Re-claim with the same `acceptor_id` before `claim_deadline` (TTL ~5 min) to
   renew while the session runs.
5. On `session_status=terminated`, delete the pod and
   `POST .../devins/{id}/release`.

## The worker image

The per-session pod runs a lightweight image bundling the `devin` CLI/worker.
**No such image is published yet** — `image:` values are placeholders
(`ghcr.io/usacognition/devin-worker:latest`). Building and publishing that image
is tracked separately (it should become part of the normal devin CLI publish
flow). End-to-end testing of this operator is blocked until it exists.

## Resume & snapshots

`resumePolicy` selects how `kind=resume` sessions are served:

| Policy               | Provider                     | Portability |
|----------------------|------------------------------|-------------|
| `StartFresh` (default) | `NoopSnapshotProvider`       | all clusters |
| `GkeSnapshot`        | `GkeSnapshotProvider`        | GKE only |
| `FilesystemSnapshot` | `FilesystemSnapshotProvider` | CSI VolumeSnapshot |

See [SNAPSHOTS.md](./SNAPSHOTS.md).

## Design decisions

- **CRD over pure config.** Pools are first-class objects so one operator can
  serve many pools with independent tokens/runtime/snapshot settings, and so
  status/observability is Kubernetes-native. The Helm chart can still deploy a
  single default pool for the easy path (`defaultPool.enabled=true`).
- **Account-scoped tokens via `Secret`.** Each pool references a `Secret`; the
  operator never holds long-lived credentials in config.
- **Portable observability.** Plain Prometheus/OpenMetrics endpoint, scrapeable
  by Prometheus, GKE Managed Prometheus, etc. Optional `ServiceMonitor`.
- **k8s-openapi pinned to the earliest supported API surface** (`v1_32`) for the
  widest cluster compatibility.
