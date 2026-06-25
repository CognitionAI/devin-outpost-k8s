//! Custom resource definitions owned by the operator.
//!
//! Currently a single namespaced resource, [`OutpostPool`], which binds one
//! account-scoped Outposts queue to a worker `Pod` template. One operator
//! deployment reconciles many `OutpostPool` resources.

mod outpost_pool;

pub use outpost_pool::*;
