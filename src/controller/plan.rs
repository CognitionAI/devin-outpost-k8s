//! Pure decision logic for one pool reconcile pass.
//!
//! [`plan`] maps the observed state — the upstream queue's sessions plus the
//! worker pods that exist in the cluster — to the actions the reconciler
//! should take, without performing any I/O itself. Keeping this pure makes
//! the whole session ↔ pod lifecycle mapping (see [`crate::controller`])
//! unit-testable without a cluster or a queue server.

use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::Pod;

use crate::api::{OutpostDevin, Phase, SessionStatus};

/// Observed inputs to [`plan`].
#[derive(Debug)]
pub struct Observed<'a> {
    /// Every (non-tombstoned) queue item for the pool.
    pub sessions: &'a [OutpostDevin],
    /// Existing worker pods, keyed by session ID.
    pub pods_by_session: &'a BTreeMap<String, Pod>,
    /// Worker pods owned by this pool that match no current queue item.
    pub orphan_pods: &'a [String],
    /// Our acceptor identity.
    pub acceptor_id: &'a str,
    /// The pool's `maxConcurrentSessions`.
    pub max_concurrent: u32,
    /// Give up on a worker restarted more than this many times.
    pub restart_limit: u32,
    /// Renew claims whose deadline is within this margin (seconds).
    pub renew_margin_secs: i64,
    /// Current unix time (seconds); injected for testability.
    pub now: i64,
}

/// One action the reconciler should take. Actions are independent; the
/// executor applies them best-effort and the next reconcile converges
/// whatever failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    /// Claim this pending session and start a worker for it.
    Claim { session_id: String },
    /// We hold the claim but no worker pod exists (fresh claim, operator
    /// restart, or a replaced pod): re-claim for a fresh connect token and
    /// create the token secret + pod.
    StartWorker { session_id: String },
    /// Renew our claim before its deadline.
    Renew { session_id: String },
    /// The session suspended: snapshot per the pool's resume policy, then
    /// tear down the pod + secret and release the claim.
    Suspend { session_id: String },
    /// The session terminated: release the claim, delete the pod + secret,
    /// and delete snapshot artifacts.
    Terminate { session_id: String },
    /// The worker exceeded the restart limit: release the claim and tear the
    /// pod down so the session isn't held hostage by a broken node/image.
    GiveUp { session_id: String, restarts: u32 },
    /// The worker pod completed while its session is still live: delete it;
    /// the next pass recreates it via [`Action::StartWorker`].
    ReplaceSucceededPod { session_id: String },
    /// Delete a pod that no longer maps to any live claim of ours (session
    /// gone from the queue, or claimed by another worker).
    DeleteOrphanPod { pod_name: String },
}

/// Total restarts across the pod's containers (the kubelet restarts the
/// worker in place under `restartPolicy: OnFailure`).
pub fn pod_restart_count(pod: &Pod) -> u32 {
    pod.status
        .as_ref()
        .and_then(|s| s.container_statuses.as_ref())
        .into_iter()
        .flatten()
        .map(|cs| cs.restart_count.max(0) as u32)
        .sum()
}

fn pod_succeeded(pod: &Pod) -> bool {
    pod.status
        .as_ref()
        .and_then(|s| s.phase.as_deref())
        .is_some_and(|phase| phase == "Succeeded")
}

/// Compute the actions for one reconcile pass.
pub fn plan(observed: &Observed<'_>) -> Vec<Action> {
    let mut actions = Vec::new();

    let ours = |session: &&OutpostDevin| {
        session.status.phase == Phase::Claimed
            && session.status.acceptor_id.as_deref() == Some(observed.acceptor_id)
    };

    let mut active_ours: u32 = 0;
    for session in observed.sessions.iter().filter(ours) {
        let session_id = session.metadata.session_id.clone();
        let pod = observed.pods_by_session.get(&session_id);
        match session.status.session_status {
            SessionStatus::Terminated => {
                actions.push(Action::Terminate { session_id });
                continue;
            }
            SessionStatus::Suspended => {
                actions.push(Action::Suspend { session_id });
                continue;
            }
            SessionStatus::Pending | SessionStatus::Running | SessionStatus::Unknown => {}
        }
        active_ours += 1;
        match pod {
            None => actions.push(Action::StartWorker { session_id }),
            Some(pod) if pod_succeeded(pod) => {
                actions.push(Action::ReplaceSucceededPod { session_id })
            }
            Some(pod) => {
                let restarts = pod_restart_count(pod);
                if restarts > observed.restart_limit {
                    actions.push(Action::GiveUp {
                        session_id,
                        restarts,
                    });
                    active_ours -= 1;
                } else if session
                    .status
                    .claim_deadline
                    .is_none_or(|deadline| deadline - observed.now < observed.renew_margin_secs)
                {
                    actions.push(Action::Renew { session_id });
                }
            }
        }
    }

    // Stale pods for sessions another worker claimed (e.g. our claim expired
    // during operator downtime and someone else picked the session up).
    for session in observed.sessions.iter() {
        if session.status.phase == Phase::Claimed
            && session.status.acceptor_id.as_deref() != Some(observed.acceptor_id)
            && let Some(pod) = observed.pods_by_session.get(&session.metadata.session_id)
        {
            actions.push(Action::DeleteOrphanPod {
                pod_name: pod.metadata.name.clone().unwrap_or_default(),
            });
        }
    }

    // Claim pending sessions, oldest first, up to the concurrency limit.
    // Only rows whose session awaits serving: a released suspended/terminated
    // session leaves a phase=pending row behind until the upstream sweeper
    // tombstones it, and re-claiming one would flap suspend→release→claim
    // forever.
    let mut pending: Vec<&OutpostDevin> = observed
        .sessions
        .iter()
        .filter(|s| {
            s.status.phase == Phase::Pending && s.status.session_status == SessionStatus::Pending
        })
        .collect();
    pending.sort_by(|a, b| {
        (a.metadata.created_at, &a.metadata.session_id)
            .cmp(&(b.metadata.created_at, &b.metadata.session_id))
    });
    let slots = observed.max_concurrent.saturating_sub(active_ours);
    actions.extend(pending.iter().take(slots as usize).map(|s| Action::Claim {
        session_id: s.metadata.session_id.clone(),
    }));

    actions.extend(
        observed
            .orphan_pods
            .iter()
            .map(|pod_name| Action::DeleteOrphanPod {
                pod_name: pod_name.clone(),
            }),
    );

    actions
}

/// The earliest claim deadline among our claims (unix seconds), for
/// scheduling the next reconcile ahead of expiry.
pub fn next_claim_deadline(sessions: &[OutpostDevin], acceptor_id: &str) -> Option<i64> {
    sessions
        .iter()
        .filter(|s| {
            s.status.phase == Phase::Claimed
                && s.status.acceptor_id.as_deref() == Some(acceptor_id)
                && s.status.session_status != SessionStatus::Terminated
        })
        .filter_map(|s| s.status.claim_deadline)
        .min()
}

#[cfg(test)]
mod tests {
    use k8s_openapi::api::core::v1::{ContainerStatus, PodStatus};

    use crate::api::{Kind, Metadata, Spec, Status};

    use super::*;

    const US: &str = "acceptor-us";

    fn session(id: &str, created_at: i64, phase: Phase, status: SessionStatus) -> OutpostDevin {
        OutpostDevin {
            metadata: Metadata {
                session_id: id.to_string(),
                pool_id: "pool_test".to_string(),
                created_at: Some(created_at),
                updated_at: Some(created_at),
            },
            spec: Spec {
                kind: Kind::New,
                platform: "linux".to_string(),
                remote_binary_sha: None,
            },
            status: Status {
                phase,
                acceptor_id: (phase == Phase::Claimed).then(|| US.to_string()),
                claim_deadline: (phase == Phase::Claimed).then_some(1_000),
                session_status: status,
                connect_token: None,
                gateway_url: None,
            },
        }
    }

    fn pod(phase: &str, restarts: i32) -> Pod {
        Pod {
            status: Some(PodStatus {
                phase: Some(phase.to_string()),
                container_statuses: Some(vec![ContainerStatus {
                    restart_count: restarts,
                    ..Default::default()
                }]),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn observed<'a>(
        sessions: &'a [OutpostDevin],
        pods: &'a BTreeMap<String, Pod>,
        orphans: &'a [String],
    ) -> Observed<'a> {
        Observed {
            sessions,
            pods_by_session: pods,
            orphan_pods: orphans,
            acceptor_id: US,
            max_concurrent: 2,
            restart_limit: 3,
            renew_margin_secs: 60,
            now: 0,
        }
    }

    #[test]
    fn claims_pending_oldest_first_up_to_the_limit() {
        let sessions = vec![
            session("late", 30, Phase::Pending, SessionStatus::Pending),
            session("early", 10, Phase::Pending, SessionStatus::Pending),
            session("mid", 20, Phase::Pending, SessionStatus::Pending),
        ];
        let pods = BTreeMap::new();
        let actions = plan(&observed(&sessions, &pods, &[]));
        assert_eq!(
            actions,
            vec![
                Action::Claim {
                    session_id: "early".into()
                },
                Action::Claim {
                    session_id: "mid".into()
                },
            ]
        );
    }

    #[test]
    fn running_claims_consume_concurrency_slots() {
        let sessions = vec![
            session("running-1", 1, Phase::Claimed, SessionStatus::Running),
            session("running-2", 2, Phase::Claimed, SessionStatus::Running),
            session("pending", 3, Phase::Pending, SessionStatus::Pending),
        ];
        let pods = BTreeMap::from([
            ("running-1".to_string(), pod("Running", 0)),
            ("running-2".to_string(), pod("Running", 0)),
        ]);
        let mut obs = observed(&sessions, &pods, &[]);
        // Deadlines (1000) are far beyond now + margin, so no renewals — and
        // both slots are taken, so no claims.
        obs.now = 0;
        assert_eq!(plan(&obs), vec![]);
    }

    #[test]
    fn renews_claims_near_their_deadline() {
        let sessions = vec![session("s", 1, Phase::Claimed, SessionStatus::Running)];
        let pods = BTreeMap::from([("s".to_string(), pod("Running", 0))]);
        let mut obs = observed(&sessions, &pods, &[]);
        obs.now = 950; // deadline 1000, margin 60 => renew
        assert_eq!(
            plan(&obs),
            vec![Action::Renew {
                session_id: "s".into()
            }]
        );
    }

    #[test]
    fn starts_a_worker_when_our_claim_has_no_pod() {
        let sessions = vec![session("s", 1, Phase::Claimed, SessionStatus::Pending)];
        let pods = BTreeMap::new();
        assert_eq!(
            plan(&observed(&sessions, &pods, &[])),
            vec![Action::StartWorker {
                session_id: "s".into()
            }]
        );
    }

    #[test]
    fn terminated_and_suspended_sessions_tear_down() {
        let sessions = vec![
            session("dead", 1, Phase::Claimed, SessionStatus::Terminated),
            session("asleep", 2, Phase::Claimed, SessionStatus::Suspended),
        ];
        let pods = BTreeMap::new();
        assert_eq!(
            plan(&observed(&sessions, &pods, &[])),
            vec![
                Action::Terminate {
                    session_id: "dead".into()
                },
                Action::Suspend {
                    session_id: "asleep".into()
                },
            ]
        );
    }

    #[test]
    fn gives_up_past_the_restart_limit_and_frees_the_slot() {
        let sessions = vec![
            session("crashy", 1, Phase::Claimed, SessionStatus::Running),
            session("waiting", 2, Phase::Pending, SessionStatus::Pending),
        ];
        let pods = BTreeMap::from([("crashy".to_string(), pod("Running", 4))]);
        let actions = plan(&observed(&sessions, &pods, &[]));
        assert!(actions.contains(&Action::GiveUp {
            session_id: "crashy".into(),
            restarts: 4
        }));
        // The freed slot is claimable in the same pass.
        assert!(actions.contains(&Action::Claim {
            session_id: "waiting".into()
        }));
    }

    #[test]
    fn replaces_succeeded_pods_of_live_sessions() {
        let sessions = vec![session("s", 1, Phase::Claimed, SessionStatus::Running)];
        let pods = BTreeMap::from([("s".to_string(), pod("Succeeded", 0))]);
        assert_eq!(
            plan(&observed(&sessions, &pods, &[])),
            vec![Action::ReplaceSucceededPod {
                session_id: "s".into()
            }]
        );
    }

    #[test]
    fn deletes_pods_for_sessions_claimed_elsewhere_and_orphans() {
        let mut stolen = session("stolen", 1, Phase::Claimed, SessionStatus::Pending);
        stolen.status.acceptor_id = Some("someone-else".to_string());
        let sessions = vec![stolen];
        let mut pod_for_stolen = pod("Running", 0);
        pod_for_stolen.metadata.name = Some("pod-stolen".to_string());
        let pods = BTreeMap::from([("stolen".to_string(), pod_for_stolen)]);
        let orphans = vec!["pod-orphan".to_string()];
        let actions = plan(&observed(&sessions, &pods, &orphans));
        assert_eq!(
            actions,
            vec![
                Action::DeleteOrphanPod {
                    pod_name: "pod-stolen".into()
                },
                Action::DeleteOrphanPod {
                    pod_name: "pod-orphan".into()
                },
            ]
        );
    }

    #[test]
    fn released_terminal_pending_rows_are_not_claimed() {
        let sessions = vec![
            session("dead", 1, Phase::Pending, SessionStatus::Terminated),
            session("asleep", 2, Phase::Pending, SessionStatus::Suspended),
            session("live", 3, Phase::Pending, SessionStatus::Running),
        ];
        let pods = BTreeMap::new();
        assert_eq!(plan(&observed(&sessions, &pods, &[])), vec![]);
    }

    #[test]
    fn next_deadline_ignores_other_acceptors_and_terminated() {
        let mut ours = session("ours", 1, Phase::Claimed, SessionStatus::Running);
        ours.status.claim_deadline = Some(500);
        let mut theirs = session("theirs", 2, Phase::Claimed, SessionStatus::Running);
        theirs.status.acceptor_id = Some("someone-else".to_string());
        theirs.status.claim_deadline = Some(100);
        let mut dead = session("dead", 3, Phase::Claimed, SessionStatus::Terminated);
        dead.status.claim_deadline = Some(50);
        let sessions = vec![ours, theirs, dead];
        assert_eq!(next_claim_deadline(&sessions, US), Some(500));
    }
}
