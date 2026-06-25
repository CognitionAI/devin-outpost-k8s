//! # outposts-operator
//!
//! A Kubernetes operator that runs **Devin Outposts** ("Bring Your Own Box")
//! workers on any certified Kubernetes cluster (GKE, EKS, ...).
//!
//! The Devin control plane exposes a deliberately Kubernetes-shaped, account
//! scoped queue API under `/opbeta/outposts` (list + watch + claim/release).
//! Each item in the queue (`OutpostDevin`) describes a Devin session that
//! *should* be running somewhere. This operator is one possible implementer of
//! that contract: it watches the queue, claims pending sessions, and runs the
//! `devin` worker for each one as a Kubernetes `Pod`.
//!
//! ## Crate layout
//!
//! - [`opbeta`]   — typed client + models for the upstream `/opbeta` queue API.
//! - [`crd`]      — the [`crd::OutpostPool`] custom resource (operator config).
//! - [`controller`] — the reconcile loop and per-session `Pod` template builder.
//! - [`snapshot`] — the cloud-portable snapshot/restore provider abstraction
//!   used to back `resume` sessions (GKE pod snapshots, filesystem, no-op).
//! - [`metrics`] / [`telemetry`] — Prometheus metrics and tracing setup.
//! - [`config`]   — process-level runtime configuration.
//! - [`error`]    — the crate error type.
//!
//! > **Status: scaffold.** Types and module boundaries are defined here, but the
//! > behavioural logic is intentionally stubbed (`Error::NotImplemented` /
//! > `todo!()`). See `docs/ARCHITECTURE.md` for the intended design.

// This is a scaffold: many items are defined ahead of the code that will use
// them. Remove this once the controller logic is implemented.
#![allow(dead_code)]

pub mod config;
pub mod controller;
pub mod crd;
pub mod error;
pub mod metrics;
pub mod opbeta;
pub mod snapshot;
pub mod telemetry;

pub use error::{Error, Result};

/// API group used by all custom resources owned by this operator.
pub const API_GROUP: &str = "outposts.cognition.ai";

/// API version currently served for [`crd::OutpostPool`].
pub const API_VERSION: &str = "v1alpha1";

/// Default manager/field-manager name used for server-side apply.
pub const MANAGER_NAME: &str = "outposts-operator";
