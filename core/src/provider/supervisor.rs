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

use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use helios_common::network_spec::NetworkSpec;
use parking_lot::Mutex;

use crate::provider::event::HealthStatus;
use crate::provider::status::VerificationStatus;

/// Stall thresholds for the consensus supervisor.
///
/// Defaults match the design issue's numbers: **96 s** head-age
/// threshold before flipping `Stalled`, **3** consecutive advance
/// failures the supervisor will tolerate (reported via
/// [`SupervisorHandle::report_failure`]) before flipping early, **2 s**
/// check interval for the stall-check timer (granularity of the
/// threshold check; smaller = faster taint detection, larger = fewer
/// wakeups).
///
/// The restart-attempt limit and exponential-backoff schedule from the
/// original design ship in a follow-up once the consensus-side restart
/// API is wired; see <https://github.com/nxm-rs/helios/issues/31>.
#[derive(Debug, Clone, Copy)]
pub struct StallPolicy {
    pub head_age_threshold: Duration,
    pub advance_failure_threshold: u32,
    pub check_interval: Duration,
}

impl Default for StallPolicy {
    fn default() -> Self {
        Self {
            head_age_threshold: Duration::from_secs(96),
            advance_failure_threshold: 3,
            check_interval: Duration::from_secs(2),
        }
    }
}

/// Handle to the running supervisor task. Cheap to clone — shares
/// inner state via `Arc<Inner>`. Network-specific consensus clients
/// hold this and call [`Self::report_advance`] on every successful tip
/// advance, [`Self::report_failure`] on each advance failure.
///
/// Dropping every clone of the handle is the supervisor's shutdown
/// signal: the periodic check task holds only a `Weak<Inner>` and
/// terminates on the next tick when the strong count reaches zero.
#[derive(Clone)]
pub struct SupervisorHandle<N: NetworkSpec> {
    inner: Arc<Inner<N>>,
}

struct Inner<N: NetworkSpec> {
    last_advance: Mutex<Instant>,
    consecutive_failures: Mutex<u32>,
    status: VerificationStatus<N>,
    policy: StallPolicy,
}

impl<N: NetworkSpec> SupervisorHandle<N> {
    /// Report a successful slot advance. Clears any active `Stalled`
    /// state, resets the consecutive-failure counter, and refreshes
    /// `consensus_status()` with the new tip via `send_modify` so
    /// other fields (checkpoint, is_synced) set elsewhere are not
    /// reset by the struct-update path that previously defaulted them.
    pub fn report_advance(&self, tip: u64) {
        let now = Instant::now();
        *self.inner.last_advance.lock() = now;
        *self.inner.consecutive_failures.lock() = 0;

        self.inner.status._modify_consensus_status(|c| {
            c.tip = tip;
            c.head_age = Duration::ZERO;
        });

        // Clear Stalled (and only Stalled — Tainted stays sticky via
        // _set_health's guard).
        let current = self.inner.status.health().borrow().clone();
        if matches!(current, HealthStatus::Stalled) {
            self.inner.status._set_health(HealthStatus::Healthy);
        }
    }

    /// Report a failed advance attempt (network error, decode error,
    /// etc.). When the consecutive-failure count exceeds
    /// [`StallPolicy::advance_failure_threshold`], the supervisor flips
    /// `Stalled` early — the periodic check task can still flip it
    /// based on head-age too, but failure-reporting lets the embedder
    /// taint faster than the timer.
    pub fn report_failure(&self) {
        let mut failures = self.inner.consecutive_failures.lock();
        *failures += 1;
        if *failures >= self.inner.policy.advance_failure_threshold {
            drop(failures);
            self.flip_stalled();
        }
    }

    fn flip_stalled(&self) {
        // `_set_health` itself refuses to overwrite Tainted (sticky
        // taint), so we don't need a separate guard here — every other
        // transition to Stalled is allowed, including Stalled →
        // Stalled which is a no-op via `send_if_modified` semantics in
        // _set_health.
        self.inner.status._set_health(HealthStatus::Stalled);
    }
}

/// Spawn the supervisor task. Returns a handle the network-specific
/// consensus client uses to report advances and failures.
///
/// The spawned task runs a `check_interval`-cadence timer that
/// inspects "now - last_advance" and flips `Stalled` if the
/// `head_age_threshold` is exceeded. The task holds only a
/// `Weak<Inner>` — when every [`SupervisorHandle`] clone is dropped,
/// the next tick fails to upgrade and the task exits, so there's no
/// leak per `spawn_supervisor` call.
pub fn spawn_supervisor<N: NetworkSpec>(
    status: VerificationStatus<N>,
    policy: StallPolicy,
) -> SupervisorHandle<N> {
    let inner = Arc::new(Inner {
        last_advance: Mutex::new(Instant::now()),
        consecutive_failures: Mutex::new(0),
        status,
        policy,
    });
    let handle = SupervisorHandle {
        inner: inner.clone(),
    };

    // wasm32 has no tokio runtime / timer driver. Embedders on that
    // target call SupervisorHandle::report_* directly and don't get
    // the periodic stall check.
    #[cfg(not(target_family = "wasm"))]
    {
        let weak = Arc::downgrade(&inner);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(policy.check_interval);
            // `interval` fires immediately on first poll; skip that.
            tick.tick().await;
            loop {
                tick.tick().await;
                let Some(inner) = Weak::upgrade(&weak) else {
                    // All external handles dropped — exit cleanly.
                    break;
                };
                let last = *inner.last_advance.lock();
                let age = last.elapsed();
                // Refresh head_age every tick so observers reading
                // consensus_status() see a current value alongside
                // health(). Only wake receivers when the rounded-to-
                // seconds age changes — sub-second jitter every
                // check_interval would spam every observer otherwise.
                inner.status._modify_consensus_status_if(|c| {
                    if c.head_age.as_secs() == age.as_secs() {
                        return false;
                    }
                    c.head_age = age;
                    true
                });
                if age > inner.policy.head_age_threshold {
                    inner.status._set_health(HealthStatus::Stalled);
                }
            }
        });
    }

    handle
}
