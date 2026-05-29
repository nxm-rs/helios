//! Consensus supervisor — observes "tip advanced" reports from the
//! consensus client and drives [`HealthStatus::Stalled`] /
//! [`ConsensusStatus`] on the shared [`VerificationStatus`].
//!
//! The supervisor lives in helios-core but is driven by the
//! network-specific consensus client (helios-ethereum,
//! helios-opstack). Each successful slot advance is reported via
//! [`SupervisorHandle::report_advance`]; a periodic check task in
//! helios-core compares "now" against "last advance time" and trips
//! `Stalled` when the configured threshold is exceeded.
//!
//! ## Wiring against a helios-ethereum / helios-opstack consensus client
//!
//! After constructing the consensus client, spawn the supervisor and
//! a small adapter that translates the client's "finalized block
//! advanced" watch into `report_advance` calls. Example shape (the
//! exact `finalized_block_recv()` method comes from the consensus
//! client's `Consensus<Block>` impl):
//!
//! ```ignore
//! let supervisor = spawn_supervisor(status.clone(), StallPolicy::default());
//! let mut block_rx = consensus.finalized_block_recv().unwrap();
//! let sup = supervisor.clone();
//! tokio::spawn(async move {
//!     while block_rx.changed().await.is_ok() {
//!         if let Some(block) = block_rx.borrow().clone() {
//!             sup.report_advance(block.header.number);
//!         }
//!     }
//! });
//! ```

use std::sync::Arc;
use std::time::{Duration, Instant};

use helios_common::network_spec::NetworkSpec;
use parking_lot::Mutex;

use crate::provider::event::{ConsensusStatus, HealthStatus};
use crate::provider::status::VerificationStatus;

/// Stall thresholds and backoff schedule for the consensus supervisor.
/// Defaults match the design issue's numbers:
///
/// - **96 s** head-age threshold before flipping `Stalled`
/// - **3** consecutive advance failures the supervisor will tolerate
///   (reported via [`SupervisorHandle::report_failure`]) before
///   recommending a restart
/// - **3** restart attempts before the supervisor escalates
/// - **5 s / 15 s / 45 s** exponential backoff between restart attempts
/// - **2 s** check interval for the stall-check timer (granularity of
///   the threshold check; smaller = faster taint detection, larger =
///   fewer wakeups)
#[derive(Debug, Clone, Copy)]
pub struct StallPolicy {
    pub head_age_threshold: Duration,
    pub advance_failure_threshold: u32,
    pub restart_attempt_limit: u32,
    pub restart_backoff: [Duration; 3],
    pub check_interval: Duration,
}

impl Default for StallPolicy {
    fn default() -> Self {
        Self {
            head_age_threshold: Duration::from_secs(96),
            advance_failure_threshold: 3,
            restart_attempt_limit: 3,
            restart_backoff: [
                Duration::from_secs(5),
                Duration::from_secs(15),
                Duration::from_secs(45),
            ],
            check_interval: Duration::from_secs(2),
        }
    }
}

/// Handle to the running supervisor task. Cheap to clone — shares
/// inner state via `Arc<Mutex<_>>`. Network-specific consensus clients
/// hold this and call [`Self::report_advance`] on every successful
/// tip advance, [`Self::report_failure`] on each advance failure.
#[derive(Clone)]
pub struct SupervisorHandle<N: NetworkSpec> {
    inner: Arc<Inner<N>>,
}

struct Inner<N: NetworkSpec> {
    last_advance: Mutex<Instant>,
    last_tip: Mutex<u64>,
    consecutive_failures: Mutex<u32>,
    restart_attempts: Mutex<u32>,
    status: VerificationStatus<N>,
    policy: StallPolicy,
}

impl<N: NetworkSpec> SupervisorHandle<N> {
    /// Report a successful slot advance. Clears any active `Stalled`
    /// state, resets the consecutive-failure counter, and refreshes
    /// `consensus_status()` with the new tip.
    pub fn report_advance(&self, tip: u64) {
        let now = Instant::now();
        *self.inner.last_advance.lock() = now;
        *self.inner.last_tip.lock() = tip;
        *self.inner.consecutive_failures.lock() = 0;
        *self.inner.restart_attempts.lock() = 0;

        self.inner.status._set_consensus_status(ConsensusStatus {
            tip,
            head_age: Duration::ZERO,
            ..Default::default()
        });

        // Clear Stalled (and only Stalled — Tainted stays sticky).
        let current = self.inner.status.health().borrow().clone();
        if matches!(current, HealthStatus::Stalled { .. }) {
            self.inner.status._set_health(HealthStatus::Healthy);
        }
    }

    /// Report a failed advance attempt (network error, decode error,
    /// etc.). When the consecutive-failure count exceeds
    /// [`StallPolicy::advance_failure_threshold`], the supervisor
    /// flips `Stalled` early — the periodic check task can still flip
    /// it based on head-age too, but failure-reporting lets the
    /// embedder taint faster than the timer.
    pub fn report_failure(&self) {
        let mut failures = self.inner.consecutive_failures.lock();
        *failures += 1;
        if *failures >= self.inner.policy.advance_failure_threshold {
            drop(failures);
            self.flip_stalled();
        }
    }

    fn flip_stalled(&self) {
        let already = matches!(
            *self.inner.status.health().borrow(),
            HealthStatus::Stalled { .. }
        );
        if !already {
            self.inner.status._set_health(HealthStatus::Stalled {
                since: *self.inner.last_advance.lock(),
                restart_attempts: *self.inner.restart_attempts.lock(),
            });
        }
    }
}

/// Spawn the supervisor task. Returns a handle the network-specific
/// consensus client uses to report advances and failures.
///
/// The spawned task runs a `check_interval`-cadence timer that
/// inspects "now - last_advance" and flips `Stalled` if the
/// `head_age_threshold` is exceeded. The timer does not auto-restart
/// the consensus client — recovery is reported via
/// [`SupervisorHandle::report_advance`] when the next advance lands.
pub fn spawn_supervisor<N: NetworkSpec>(
    status: VerificationStatus<N>,
    policy: StallPolicy,
) -> SupervisorHandle<N> {
    let inner = Arc::new(Inner {
        last_advance: Mutex::new(Instant::now()),
        last_tip: Mutex::new(0),
        consecutive_failures: Mutex::new(0),
        restart_attempts: Mutex::new(0),
        status,
        policy,
    });
    let handle = SupervisorHandle { inner };

    let watch_handle = handle.clone();
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(watch_handle.inner.policy.check_interval);
        // `interval` fires immediately on first poll; skip that.
        tick.tick().await;
        loop {
            tick.tick().await;
            let last = *watch_handle.inner.last_advance.lock();
            let age = last.elapsed();
            if age > watch_handle.inner.policy.head_age_threshold {
                watch_handle.flip_stalled();
            }
        }
    });

    handle
}
