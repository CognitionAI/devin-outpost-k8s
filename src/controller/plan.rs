//! Pure decision logic for one pool reconcile pass.
//!
//! [`plan`] maps the observed state — the upstream queue's sessions plus the
//! worker pods that exist in the cluster — to the actions the reconciler
//! should take, without performing any I/O itself. Keeping this pure makes
//! the whole session ↔ pod lifecycle mapping (see [`crate::controller`])
//! unit-testable without a cluster or a queue server.

use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::Pod;

use crate::api::{OutpostDevin, Phase, SessionStatus, StartupFailure, StartupFailureReason};

use super::pod::effective_worker_image;

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
    /// Report a worker pod still unschedulable or still failing to pull an
    /// image after this long (seconds); see
    /// [`crate::config::OperatorConfig::worker_startup_timeout`].
    pub startup_timeout_secs: i64,
    /// Renew claims whose deadline is within this margin (seconds).
    pub renew_margin_secs: i64,
    /// Name of the worker container within the pod (`worker.containerName`).
    pub container_name: &'a str,
    /// Pool-level image fallback (worker.overrides.image or the operator
    /// default); session and build images take precedence.
    pub desired_image: &'a str,
    pub build_image: Option<&'a str>,
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
    /// The worker pod cannot start (image pull or scheduling never completed,
    /// see [`pod_startup_failure`]): report it upstream, which takes the
    /// attempt out of the queue for good, then tear the pod down. Releasing
    /// instead would hand the same unpullable image to the next claim. The
    /// claim stays counted against `maxConcurrentSessions` and renewed
    /// ([`Action::Renew`] is planned alongside) until the server has
    /// acknowledged the failure: a report that fails must not free the slot
    /// or let the lease lapse back into the queue.
    StartupFailed {
        session_id: String,
        failure: StartupFailure,
    },
    /// The worker pod completed while its session is still live: delete it;
    /// the next pass recreates it via [`Action::StartWorker`].
    ReplaceSucceededPod { session_id: String },
    /// The worker is crash-looping on an image that no longer matches what
    /// the operator would create (the pool/default image changed since the
    /// pod was built): delete it so the next pass recreates it with the
    /// current spec instead of looping toward [`Action::GiveUp`] on the
    /// stale one. Healthy workers are left alone — a live session must not
    /// be restarted by an image rollout.
    ReplaceDriftedPod { session_id: String },
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

/// Waiting reasons the kubelet retries with backoff; given up on only after
/// the startup timeout. `ImageInspectError` is a failed runtime call, which
/// can be transient, not a verdict on the image.
const IMAGE_PULL_RETRYING_REASONS: [&str; 3] =
    ["ErrImagePull", "ImagePullBackOff", "ImageInspectError"];
/// Waiting reasons no retry fixes.
const IMAGE_PULL_FATAL_REASONS: [&str; 2] = ["InvalidImageName", "ErrImageNeverPull"];

/// Why the pod will not start on its own, if that is already certain: an
/// image (of an init or worker container) that cannot be pulled, or an image
/// pull or scheduling still pending `startup_timeout_secs` after the pod was
/// created. None of these restart the container, so [`pod_restart_count`]
/// never sees them. This covers initial startup only: once the worker
/// container (`container_name`; sidecars don't count) has run, a pull
/// failure on a later restart is not judged here, since the pod's age says
/// nothing about how long that pull has been failing.
pub fn pod_startup_failure(
    pod: &Pod,
    container_name: &str,
    now: i64,
    startup_timeout_secs: i64,
) -> Option<StartupFailure> {
    let status = pod.status.as_ref()?;
    let worker_ran = status
        .container_statuses
        .iter()
        .flatten()
        .filter(|cs| cs.name == container_name)
        .any(|cs| {
            cs.restart_count > 0
                || cs
                    .state
                    .as_ref()
                    .is_some_and(|s| s.running.is_some() || s.terminated.is_some())
                || cs
                    .last_state
                    .as_ref()
                    .is_some_and(|s| s.terminated.is_some())
        });
    if worker_ran {
        return None;
    }
    let timed_out = pod
        .metadata
        .creation_timestamp
        .as_ref()
        .is_some_and(|created| now - created.0.as_second() >= startup_timeout_secs);
    let waiting = status
        .init_container_statuses
        .iter()
        .flatten()
        .chain(status.container_statuses.iter().flatten())
        .filter_map(|cs| {
            let waiting = cs.state.as_ref()?.waiting.as_ref()?;
            Some((cs, waiting.reason.as_deref()?, waiting.message.as_deref()))
        });
    for (cs, reason, message) in waiting {
        let failure = |kind| StartupFailure {
            reason: kind,
            image: Some(cs.image.clone()).filter(|image| !image.is_empty()),
            message: match message {
                Some(message) => format!("{reason}: {message}"),
                None => reason.to_string(),
            },
        };
        if IMAGE_PULL_FATAL_REASONS.contains(&reason) {
            return Some(failure(StartupFailureReason::ImagePullError));
        }
        if timed_out && IMAGE_PULL_RETRYING_REASONS.contains(&reason) {
            return Some(failure(StartupFailureReason::ImagePullTimeout));
        }
    }
    if timed_out && status.phase.as_deref() == Some("Pending") {
        let unscheduled = status
            .conditions
            .iter()
            .flatten()
            .find(|c| c.type_ == "PodScheduled" && c.status == "False")?;
        return Some(StartupFailure {
            reason: StartupFailureReason::SchedulingTimeout,
            image: None,
            message: match (&unscheduled.reason, &unscheduled.message) {
                (Some(reason), Some(message)) => format!("{reason}: {message}"),
                (reason, message) => reason
                    .clone()
                    .or_else(|| message.clone())
                    .unwrap_or_else(|| "Unschedulable".to_string()),
            },
        });
    }
    None
}

fn pod_succeeded(pod: &Pod) -> bool {
    pod.status
        .as_ref()
        .and_then(|s| s.phase.as_deref())
        .is_some_and(|phase| phase == "Succeeded")
}

/// Whether the pod is failing to come up: a container sits in
/// `CrashLoopBackOff`, or has restarted and still isn't ready.
fn pod_crash_looping(pod: &Pod) -> bool {
    pod.status
        .as_ref()
        .and_then(|s| s.container_statuses.as_ref())
        .into_iter()
        .flatten()
        .any(|cs| {
            let backoff = cs
                .state
                .as_ref()
                .and_then(|s| s.waiting.as_ref())
                .and_then(|w| w.reason.as_deref())
                == Some("CrashLoopBackOff");
            backoff || (cs.restart_count > 0 && !cs.ready)
        })
}

fn worker_image<'a>(pod: &'a Pod, container_name: &str) -> Option<&'a str> {
    pod.spec
        .as_ref()?
        .containers
        .iter()
        .find(|c| c.name == container_name)?
        .image
        .as_deref()
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
                if let Some(failure) = pod_startup_failure(
                    pod,
                    observed.container_name,
                    observed.now,
                    observed.startup_timeout_secs,
                ) {
                    if session
                        .status
                        .claim_deadline
                        .is_none_or(|deadline| deadline - observed.now < observed.renew_margin_secs)
                    {
                        actions.push(Action::Renew {
                            session_id: session_id.clone(),
                        });
                    }
                    actions.push(Action::StartupFailed {
                        session_id,
                        failure,
                    });
                } else if restarts > observed.restart_limit {
                    actions.push(Action::GiveUp {
                        session_id,
                        restarts,
                    });
                    active_ours -= 1;
                } else if pod_crash_looping(pod)
                    && worker_image(pod, observed.container_name)
                        != Some(effective_worker_image(
                            session,
                            observed.desired_image,
                            observed.build_image,
                        ))
                {
                    actions.push(Action::ReplaceDriftedPod { session_id });
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
    // A row can be pending with a *running* session (the brain enqueues the
    // row at session start, before any worker claims it), but terminal
    // states are excluded: a released suspended/terminated session leaves a
    // phase=pending row behind until the upstream sweeper tombstones it, and
    // re-claiming one would flap suspend→release→claim forever.
    let mut pending: Vec<&OutpostDevin> = observed
        .sessions
        .iter()
        .filter(|s| {
            s.status.phase == Phase::Pending
                && !matches!(
                    s.status.session_status,
                    SessionStatus::Terminated | SessionStatus::Suspended
                )
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
                outpost_id: "pool_test".to_string(),
                created_at: Some(created_at),
                updated_at: Some(created_at),
            },
            spec: Spec {
                kind: Kind::New,
                platform: "linux".to_string(),
                remote_binary_sha: None,
                network_policy: Default::default(),
                image: None,
                image_build: false,
                image_target: None,
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
            startup_timeout_secs: STARTUP_TIMEOUT,
            renew_margin_secs: 60,
            container_name: "devin-worker",
            desired_image: "img:current",
            build_image: None,
            now: NOW,
        }
    }

    const NOW: i64 = 0;
    const STARTUP_TIMEOUT: i64 = 600;

    fn created_at(unix_secs: i64) -> k8s_openapi::apimachinery::pkg::apis::meta::v1::Time {
        k8s_openapi::apimachinery::pkg::apis::meta::v1::Time(
            k8s_openapi::jiff::Timestamp::from_second(unix_secs).unwrap(),
        )
    }

    /// A pod created `age_secs` before [`NOW`] whose `container` (an init
    /// container when `init`) waits with `reason`.
    fn waiting_pod(age_secs: i64, init: bool, reason: &str, image: &str) -> Pod {
        use k8s_openapi::api::core::v1::{ContainerState, ContainerStateWaiting};

        let status = ContainerStatus {
            name: "devin-worker".to_string(),
            image: image.to_string(),
            state: Some(ContainerState {
                waiting: Some(ContainerStateWaiting {
                    reason: Some(reason.to_string()),
                    message: Some("rpc error: manifest unknown".to_string()),
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        let mut pod = pod("Pending", 0);
        pod.metadata.creation_timestamp = Some(created_at(NOW - age_secs));
        let pod_status = pod.status.as_mut().unwrap();
        if init {
            pod_status.init_container_statuses = Some(vec![status]);
        } else {
            pod_status.container_statuses = Some(vec![status]);
        }
        pod
    }

    fn pod_with_image(phase: &str, restarts: i32, ready: bool, image: &str) -> Pod {
        let mut pod = pod(phase, restarts);
        pod.status
            .as_mut()
            .unwrap()
            .container_statuses
            .as_mut()
            .unwrap()[0]
            .ready = ready;
        pod.spec = Some(k8s_openapi::api::core::v1::PodSpec {
            containers: vec![k8s_openapi::api::core::v1::Container {
                name: "devin-worker".to_string(),
                image: Some(image.to_string()),
                ..Default::default()
            }],
            ..Default::default()
        });
        pod
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
    fn reports_image_pulls_still_failing_past_the_startup_timeout() {
        let sessions = vec![
            session("stuck", 1, Phase::Claimed, SessionStatus::Running),
            session("healthy", 1, Phase::Claimed, SessionStatus::Running),
            session("waiting", 2, Phase::Pending, SessionStatus::Pending),
        ];
        for init in [false, true] {
            let pods = BTreeMap::from([
                (
                    "stuck".to_string(),
                    waiting_pod(STARTUP_TIMEOUT, init, "ImagePullBackOff", "img:missing"),
                ),
                ("healthy".to_string(), pod("Running", 0)),
            ]);
            let actions = plan(&observed(&sessions, &pods, &[]));
            assert!(
                actions.contains(&Action::StartupFailed {
                    session_id: "stuck".into(),
                    failure: StartupFailure {
                        reason: StartupFailureReason::ImagePullTimeout,
                        image: Some("img:missing".into()),
                        message: "ImagePullBackOff: rpc error: manifest unknown".into(),
                    },
                }),
                "init={init}: {actions:?}"
            );
            // The slot is not freed until the server has acknowledged the
            // failure: with "stuck" still counted the pool is full, so
            // "waiting" is not claimed in the same pass.
            assert!(!actions.contains(&Action::Claim {
                session_id: "waiting".into()
            }));
        }
    }

    #[test]
    fn keeps_renewing_a_claim_whose_startup_failure_is_being_reported() {
        let mut stuck = session("stuck", 1, Phase::Claimed, SessionStatus::Running);
        stuck.status.claim_deadline = Some(NOW + 30);
        let sessions = vec![stuck];
        let pods = BTreeMap::from([(
            "stuck".to_string(),
            waiting_pod(STARTUP_TIMEOUT, false, "ImagePullBackOff", "img:missing"),
        )]);
        let actions = plan(&observed(&sessions, &pods, &[]));
        // Renew comes first so a failed report cannot let the lease lapse
        // and put the session back in the queue.
        assert!(matches!(
            &actions[..],
            [Action::Renew { session_id: renewed }, Action::StartupFailed { session_id, .. }]
                if renewed == "stuck" && session_id == "stuck"
        ));
    }

    #[test]
    fn a_pull_failure_after_the_worker_ran_is_not_a_startup_failure() {
        use k8s_openapi::api::core::v1::{ContainerState, ContainerStateTerminated};

        let sessions = vec![session("old", 1, Phase::Claimed, SessionStatus::Running)];
        let mut pod = waiting_pod(
            STARTUP_TIMEOUT * 10,
            false,
            "ImagePullBackOff",
            "img:current",
        );
        pod.spec = pod_with_image("Running", 1, false, "img:current").spec;
        let cs = &mut pod
            .status
            .as_mut()
            .unwrap()
            .container_statuses
            .as_mut()
            .unwrap()[0];
        cs.restart_count = 1;
        cs.last_state = Some(ContainerState {
            terminated: Some(ContainerStateTerminated {
                exit_code: 1,
                ..Default::default()
            }),
            ..Default::default()
        });
        // Not judged as a startup failure: the pod's age is not how long
        // this pull has failed.
        assert_eq!(
            plan(&observed(&sessions, &pods_of("old", pod), &[])),
            vec![]
        );
    }

    #[test]
    fn a_running_sidecar_does_not_hide_a_worker_that_never_started() {
        use k8s_openapi::api::core::v1::{ContainerState, ContainerStateRunning};

        let sessions = vec![session("s", 1, Phase::Claimed, SessionStatus::Running)];
        let mut pod = waiting_pod(STARTUP_TIMEOUT, false, "ImagePullBackOff", "img:missing");
        pod.status
            .as_mut()
            .unwrap()
            .container_statuses
            .as_mut()
            .unwrap()
            .push(ContainerStatus {
                name: "log-shipper".to_string(),
                state: Some(ContainerState {
                    running: Some(ContainerStateRunning::default()),
                    ..Default::default()
                }),
                ..Default::default()
            });
        assert!(matches!(
            &plan(&observed(&sessions, &pods_of("s", pod), &[]))[..],
            [Action::StartupFailed { session_id, failure }]
                if session_id == "s" && failure.reason == StartupFailureReason::ImagePullTimeout
        ));
    }

    #[test]
    fn image_inspect_errors_get_the_retry_window() {
        let sessions = vec![session("s", 1, Phase::Claimed, SessionStatus::Running)];
        let fresh = waiting_pod(1, false, "ImageInspectError", "img:x");
        assert_eq!(
            plan(&observed(&sessions, &pods_of("s", fresh), &[])),
            vec![]
        );
        let stale = waiting_pod(STARTUP_TIMEOUT, false, "ImageInspectError", "img:x");
        assert!(matches!(
            &plan(&observed(&sessions, &pods_of("s", stale), &[]))[..],
            [Action::StartupFailed { failure, .. }]
                if failure.reason == StartupFailureReason::ImagePullTimeout
        ));
    }

    fn pods_of(session_id: &str, pod: Pod) -> BTreeMap<String, Pod> {
        BTreeMap::from([(session_id.to_string(), pod)])
    }

    #[test]
    fn leaves_an_image_pull_alone_within_the_startup_timeout() {
        let sessions = vec![session(
            "pulling",
            1,
            Phase::Claimed,
            SessionStatus::Running,
        )];
        let pods = BTreeMap::from([(
            "pulling".to_string(),
            waiting_pod(STARTUP_TIMEOUT - 1, false, "ErrImagePull", "img:slow"),
        )]);
        // Nothing to do yet (the claim is far from its deadline): the pod
        // is left to the kubelet's pull retries.
        assert_eq!(plan(&observed(&sessions, &pods, &[])), vec![]);
    }

    #[test]
    fn reports_unpullable_images_without_waiting() {
        let sessions = vec![session("bad", 1, Phase::Claimed, SessionStatus::Running)];
        let pods = BTreeMap::from([(
            "bad".to_string(),
            waiting_pod(1, true, "InvalidImageName", "IMG:Bad"),
        )]);
        let actions = plan(&observed(&sessions, &pods, &[]));
        assert!(matches!(
            &actions[..],
            [Action::StartupFailed { session_id, failure }]
                if session_id == "bad" && failure.reason == StartupFailureReason::ImagePullError
        ));
    }

    #[test]
    fn reports_pods_still_unschedulable_past_the_startup_timeout() {
        use k8s_openapi::api::core::v1::PodCondition;

        let sessions = vec![session(
            "nowhere",
            1,
            Phase::Claimed,
            SessionStatus::Running,
        )];
        let mut pod = pod("Pending", 0);
        pod.metadata.creation_timestamp = Some(created_at(NOW - STARTUP_TIMEOUT));
        let status = pod.status.as_mut().unwrap();
        status.container_statuses = None;
        status.conditions = Some(vec![PodCondition {
            type_: "PodScheduled".into(),
            status: "False".into(),
            reason: Some("Unschedulable".into()),
            message: Some("0/3 nodes are available".into()),
            ..Default::default()
        }]);
        let pods = BTreeMap::from([("nowhere".to_string(), pod)]);
        assert_eq!(
            plan(&observed(&sessions, &pods, &[])),
            vec![Action::StartupFailed {
                session_id: "nowhere".into(),
                failure: StartupFailure {
                    reason: StartupFailureReason::SchedulingTimeout,
                    image: None,
                    message: "Unschedulable: 0/3 nodes are available".into(),
                },
            }]
        );
    }

    #[test]
    fn replaces_crash_looping_pods_whose_image_drifted() {
        let sessions = vec![session("s", 1, Phase::Claimed, SessionStatus::Running)];
        let pods = BTreeMap::from([(
            "s".to_string(),
            pod_with_image("Running", 2, false, "img:stale"),
        )]);
        assert_eq!(
            plan(&observed(&sessions, &pods, &[])),
            vec![Action::ReplaceDriftedPod {
                session_id: "s".into()
            }]
        );
    }

    #[test]
    fn crash_looping_pods_on_the_current_image_are_left_to_the_restart_limit() {
        for (image, image_build, build_base, expected) in [
            (None, false, None, "img:current"),
            (Some("img:session"), false, None, "img:session"),
            (None, true, Some("img:build"), "img:build"),
            (Some("img:session"), true, Some("img:build"), "img:session"),
            (None, false, Some("img:build"), "img:current"),
        ] {
            let mut s = session("s", 1, Phase::Claimed, SessionStatus::Running);
            s.spec.image = image.map(str::to_string);
            s.spec.image_build = image_build;
            let sessions = vec![s];
            for restarts in 1..=4 {
                let pods = BTreeMap::from([(
                    "s".to_string(),
                    pod_with_image("Running", restarts, false, expected),
                )]);
                let mut obs = observed(&sessions, &pods, &[]);
                obs.build_image = build_base;
                // Deadline 1000, now 0, margin 60 => no renewal either.
                let actions = plan(&obs);
                if restarts <= 3 {
                    assert_eq!(actions, vec![], "case {expected} restarts {restarts}");
                } else {
                    assert_eq!(
                        actions,
                        vec![Action::GiveUp {
                            session_id: "s".into(),
                            restarts: 4
                        }],
                        "case {expected}"
                    );
                }
            }
        }
    }

    #[test]
    fn healthy_pods_are_not_replaced_on_image_drift() {
        let sessions = vec![session("s", 1, Phase::Claimed, SessionStatus::Running)];
        let pods = BTreeMap::from([(
            "s".to_string(),
            pod_with_image("Running", 0, true, "img:stale"),
        )]);
        assert_eq!(plan(&observed(&sessions, &pods, &[])), vec![]);
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
            // The brain enqueues at session start, so an unclaimed fresh
            // row's session can already be running.
            session("live", 3, Phase::Pending, SessionStatus::Running),
        ];
        let pods = BTreeMap::new();
        assert_eq!(
            plan(&observed(&sessions, &pods, &[])),
            vec![Action::Claim {
                session_id: "live".into()
            }]
        );
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
