//! Tracing/log initialization.

use tracing_subscriber::prelude::*;
use tracing_subscriber::{EnvFilter, Registry};

/// Initialize the global tracing subscriber.
///
/// Reads `RUST_LOG` for filtering and emits JSON logs when `LOG_FORMAT=json`,
/// otherwise a human-friendly format.
pub fn init() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,devin_outposts_k8s=debug"));

    let json = std::env::var("LOG_FORMAT").as_deref() == Ok("json");

    let registry = Registry::default().with(filter);
    if json {
        registry
            .with(tracing_subscriber::fmt::layer().json())
            .init();
    } else {
        registry.with(tracing_subscriber::fmt::layer()).init();
    }
}
