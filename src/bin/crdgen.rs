//! Generate the `OutpostPool` CRD and print it to stdout as JSON.
//!
//! JSON is valid YAML, and `kubectl apply` / Helm accept it directly, so the
//! generated file can live next to the chart's YAML templates:
//!
//! ```sh
//! cargo run --bin crdgen > charts/devin-outposts-k8s/crds/outpostpool.yaml
//! ```

use devin_outposts_k8s::crd::OutpostPool;
use kube::CustomResourceExt;

fn main() -> anyhow::Result<()> {
    let crd = OutpostPool::crd();
    println!("{}", serde_json::to_string_pretty(&crd)?);
    Ok(())
}
