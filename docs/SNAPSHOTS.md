# Snapshots & resume

> **Status: scaffold.** Provider methods return `Error::NotImplemented`.

`kind=resume` sessions need their prior state restored. How that happens is
selected per pool via `spec.resumePolicy`, backed by a `SnapshotProvider`
(`src/snapshot/`).

## Policies

### `StartFresh` (default)
`NoopSnapshotProvider`. No state is persisted; resumes start from scratch. Fully
portable; the right choice until snapshotting is implemented.

### `GkeSnapshot`
`GkeSnapshotProvider`, backed by [GKE Pod snapshots][gke]. Intended flow: on
`session_status=suspended` snapshot the worker pod; on a `kind=resume` for the
session, restore it. GKE only. Honors `spec.snapshot` (`enabled`, `ttlSeconds`).

### `FilesystemSnapshot`
`FilesystemSnapshotProvider`, backed by a CSI `VolumeSnapshot` (or a retained
PVC). Portable fallback for non-GKE clusters: persist the worker's state dir and
re-mount it on resume.

## Snapshot policy

`spec.snapshot` is **disabled by default**:

```yaml
snapshot:
  enabled: false
  ttlSeconds: null   # null => keep until the session terminates
```

When enabled, `ttlSeconds` bounds how long a snapshot is retained before GC.

## Open questions (for the implementation phase)

- Is the primary goal to back the `resume` session kind, to suspend/restore idle
  worker pods for cost savings, or both? (Initial intent: back `resume`.)
- What exactly constitutes "session state" for the filesystem provider — which
  directories must be captured for a faithful resume?
- GKE pod-snapshot quotas / restore latency and how they bound `maxConcurrentSessions`.

[gke]: https://docs.cloud.google.com/kubernetes-engine/docs/concepts/pod-snapshots
