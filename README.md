# Devin Outposts for Kubernetes

A Kubernetes operator that runs **Devin Outposts** ("Bring Your Own Box")
workers on any certified Kubernetes cluster (GKE, EKS, ...).

> **Note:** This operator is **not** covered by the same support as the rest of the
> Devin product. If you plan to deploy this, contact your account team for more
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

1. **Create an outpost**. Under Settings → Environment → Outposts, add an
   outpost. You will receive a service account user token.
2. **Copy the token.** The `cog_...` token is shown **only once** at creation —
   copy it now; it can't be retrieved later. Use it as the `<SVC_ACT_TOKEN>` /
   `<PAT>` above.

## Code Documentation

This project puts all of its documentation into the Rustdoc (inline comments in
the source code). To read the docs, run `cargo doc --document-private-items --open`.

## License

[MIT](LICENSE)
