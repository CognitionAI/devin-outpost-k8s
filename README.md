# Devin Outposts for Kubernetes

A Kubernetes operator that runs **Devin Outposts** ("Bring Your Own Box")
workers on any certified Kubernetes cluster (GKE, EKS, ...).

> **Note:** Outposts is not covered by the same support as the rest of the Devin
> product. If you plan to deploy this, contact your account team for more
> information.

## Quick start

```bash
# Install operator + CRD, and a default pool in one shot:
helm install outposts charts/devin-outposts-k8s \
  --set defaultPool.enabled=true \
  --set defaultPool.poolId=pool_xxx \
  --set defaultPool.token.value=<SVC_ACT_TOKEN>

# Or install the operator and manage pools yourself:
helm install outposts charts/devin-outposts-k8s
kubectl create secret generic my-pool-token --from-literal=token=<PAT>
kubectl apply -f examples/outpostpool.yaml
```

The operator authenticates to the Outposts API as a **service account user**.
Its token looks like `cog_abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz`.

## Creating a service account user

Outposts must be enabled on your account by Cognition first — contact your
account team. Once enabled, in the Devin web app:

1. **Create a role** with Outposts access. Under Settings → Roles, add an
   Enterprise role and enable **Use outpost machine** (`UseOutpostsMachine`)
   under *Outpost permissions*. Also enable **Manage outpost pools**
   (`ManageOutpostsOrchestrator`) if this account will create/delete pools.
2. **Provision the service user.** Under Settings → Devin API → *Service users*,
   click **Provision service user**, give it a name (e.g. `outposts-operator`),
   assign the role from step 1, and set an expiration.
3. **Copy the token.** The `cog_...` token is shown **only once** at creation —
   copy it now; it can't be retrieved later. Use it as the `<SVC_ACT_TOKEN>` /
   `<PAT>` above.
4. **Create a pool.** Under Settings → Outpost pools, click **Create pool** and
   note its `pool_id` for the operator config.

## Documentation

This project puts all of its documentation into the Rustdoc (inline comments in
the source code). To read the docs, run
`cargo doc --document-private-items --open`.

`CR-soon nikhil: When this is published to crates.io, put the docs.rs link here.`

## License

[MIT](LICENSE)
