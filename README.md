# Devin Outposts for Kubernetes

A Kubernetes operator that runs **Devin Outposts** ("Bring Your Own Box")
workers on any certified Kubernetes cluster (GKE, EKS, ...).

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

The service account needs the `Outposts` permission. A service account token looks like `cog_abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz`.

## Documentation

This project puts all of its documentation into the Rustdoc (inline comments in
the source code). To read the docs, run
`cargo doc --document-private-items --open`.

`CR-soon nikhil: When this is published to crates.io, put the docs.rs link here.`

## License

[MIT](LICENSE)
