# Upstream API contract (`/opbeta/outposts`)

Source of truth in `devin-webapp`:
- `apps/webserver/app/routers/opbeta/outposts_router.py` (server)
- `apps/chisel/chisel/src/worker/` (reference Rust worker)

This operator must treat that contract as **immutable** — it is a cross-repo
downstream API. The types here (`src/opbeta/types.rs`) mirror it.

## Auth

Bearer **personal access token (PAT)** with the `UseOutpostsMachine` account
permission. The whole surface is account-scoped (a pool spans all orgs in the
account) and gated behind the `outposts-enabled` Unleash flag.

## Endpoints

| Method & path | Purpose |
|---|---|
| `GET /opbeta/outposts/devins` | List queued sessions. `?watch=true` streams `MODIFIED`/`DELETED` SSE events with a resumable opaque `cursor` (at-least-once delivery). |
| `POST /opbeta/outposts/devins/{id}/claim` | Atomic compare-and-set claim. Returns `connect_token` + `gateway_url`. Re-claiming with the same `acceptor_id` **renews** (TTL ~5 min). `409` on conflict. |
| `POST /opbeta/outposts/devins/{id}/release` | Return a session to the queue. |
| `GET/POST/DELETE /opbeta/outposts/pools` | Pool CRUD. |

## Object shape (`OutpostDevin`)

```jsonc
{
  "metadata": { "session_id": "...", "pool_id": "...", "created_at": 0, "updated_at": 0 },
  "spec": {
    "kind": "new | resume",
    "platform": "linux",
    "remote_binary_sha": "abc123 | null"   // sha-pinned devin-remote; null => worker default
  },
  "status": {
    "phase": "pending | claimed",
    "acceptor_id": "... | null",
    "claim_deadline": 0,                    // unix seconds
    "session_status": "pending | running | suspended | terminated",
    "connect_token": "... | null",          // only from a successful claim
    "gateway_url": "wss://... | null"        // only from a successful claim
  }
}
```

Unknown enum values are tolerated (deserialize to `Unknown`) so server-side
additions never break the operator.

> The reference worker currently implements only `kind=new`; `resume` is in the
> contract but not yet served end-to-end.

## Worker / connect-back contract

The claimed worker runs `devin-remote` in **connect-back mode**: it dials the
gateway's public leg with the short-lived connect token, and the brain attaches
over the gateway's internal leg. The container env mirrors
`apps/chisel/chisel/src/worker/runner.rs`:

| Env var | Source |
|---|---|
| `DEVIN_OUTPOST_GATEWAY_URL` | `status.gateway_url` from the claim |
| `DEVIN_OUTPOST_CONNECT_TOKEN` | `status.connect_token` from the claim |
| `DEVIN_OUTPOST_SESSION_ID` | `metadata.session_id` |
| `DEVIN_REMOTE_STATE_DIR`, `DEVIN_REMOTE_AUTH_TOKEN`, `DEVIN_PTY_BRIDGE_PORT` | worker-managed |

Plus a small passthrough whitelist (`PATH`, `HOME`, `USER`, `LOGNAME`, `LANG`,
`TZ`, `TMPDIR`). See `src/controller/pod.rs` for where these are injected.
