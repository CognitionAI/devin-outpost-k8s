//! # devin-outposts-k8s
//!
//! A Kubernetes operator that runs **Devin Outposts** ("Bring Your Own Box")
//! workers on Kubernetes (GKE, EKS, kubeadm, ...).
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
//! - [`api`]         — typed client + models for the upstream queue API
//! - [`crd`]         — the [`crd::OutpostPool`] custom resource
//! - [`controller`]  — the reconcile loop and per-session `Pod` template builder
//! - [`snapshot`]    — the cloud-portable snapshot/restore provider abstraction
//!   used to back `resume` sessions (GKE pod snapshots, filesystem, no-op).
//! - [`elector`]     — Lease-based leader election
//! - [`metrics`] / [`telemetry`] — Prometheus metrics and tracing setup.
//! - [`config`]      — process-level runtime configuration
//! - [`error`]       — the crate error type

pub mod api;
pub mod config;
pub mod controller;
pub mod crd;
pub mod elector;
pub mod error;
pub mod metrics;
pub mod snapshot;
pub mod telemetry;

pub use error::{Error, Result};

/// API group used by all custom resources owned by this operator.
pub const API_GROUP: &str = "outposts.cognition.com";

/// API version currently served for [`crd::OutpostPool`].
pub const API_VERSION: &str = "v1alpha1";

/// Default manager/field-manager name used for server-side apply.
pub const MANAGER_NAME: &str = "devin-outposts-k8s";
