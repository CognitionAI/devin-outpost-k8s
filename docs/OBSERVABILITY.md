# Observability

The operator exposes an HTTP server (default `:8080`, `metrics.service.port`):

| Path | Purpose |
|---|---|
| `/metrics` | Prometheus / OpenMetrics exposition |
| `/healthz` | Liveness probe |
| `/readyz`  | Readiness probe |

## Metrics

All metrics use the `outposts_` prefix and (where relevant) a `pool_id` label.
Defined in `src/metrics.rs`:

| Metric | Type | Meaning |
|---|---|---|
| `outposts_reconciliations_total` | counter | reconcile invocations per pool |
| `outposts_reconcile_failures_total` | counter | reconcile failures per pool |
| `outposts_sessions_claimed_total` | counter | sessions claimed per pool |
| `outposts_claim_conflicts_total` | counter | claim races lost (HTTP 409) per pool |
| `outposts_active_workers` | gauge | worker pods currently running per pool |

> Counters increment once the reconcile logic is implemented; the registry and
> endpoint are live today.

## Scraping

- **Prometheus Operator:** set `metrics.serviceMonitor.enabled=true` to ship a
  `ServiceMonitor`.
- **GKE Managed Prometheus / others:** scrape the `*-metrics` `Service` on the
  `metrics` port directly, or add an equivalent `PodMonitoring`.

## Logging

Structured logging via `tracing`. `LOG_FORMAT=json` emits JSON; `RUST_LOG`
controls filtering (default `info,outposts_operator=debug`).
