//! [`VerificationStatus`] — the public handle for observing and gating on
//! verification activity.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use helios_common::network_spec::NetworkSpec;
use parking_lot::Mutex;
use tokio::sync::{broadcast, mpsc, watch};

use crate::provider::error::{FailureInfo, VerificationError};
use crate::provider::event::{
    ConsensusStatus, HealthStatus, SecurityEvent, VerificationCounts, VerificationEvent,
    VerifiedSnapshot,
};

/// `security_events` uses producer-backpressure — a small buffer is
/// intentional so a slow consumer makes the verifier task await rather
/// than racing ahead.
const SECURITY_EVENT_BUF: usize = 64;
/// `events_verbose` uses drop-oldest — the buffer can be larger because
/// dropping is acceptable for informational events.
const VERBOSE_EVENT_BUF: usize = 1024;

/// Handle for observing and gating on the verification activity of a
/// [`super::VerifiedHeliosProvider`].
///
/// Cheap to clone — internally just an `Arc<Inner>`.
#[derive(Clone)]
pub struct VerificationStatus<N: NetworkSpec> {
    inner: Arc<Inner<N>>,
}

pub(crate) struct Inner<N: NetworkSpec> {
    counts_tx: watch::Sender<VerificationCounts>,
    counts_rx: watch::Receiver<VerificationCounts>,

    health_tx: watch::Sender<HealthStatus>,
    health_rx: watch::Receiver<HealthStatus>,

    consensus_tx: watch::Sender<ConsensusStatus>,
    consensus_rx: watch::Receiver<ConsensusStatus>,

    security_tx: mpsc::Sender<SecurityEvent>,
    security_rx: Mutex<Option<mpsc::Receiver<SecurityEvent>>>,

    verbose_tx: broadcast::Sender<VerificationEvent<N>>,

    next_id: AtomicU64,
    /// Per-request settlement channels. The `watch::Sender` lives here
    /// until the request settles; barrier waiters subscribe via the
    /// sender so they observe the outcome even if they raced the settle.
    pending: Mutex<HashMap<u64, watch::Sender<Option<RequestOutcome>>>>,
}

/// Per-request settlement outcome. Carried on the watch channel held in
/// [`Inner::pending`] so [`VerificationStatus::barrier`] can classify
/// each snapshot id when it settles.
#[derive(Clone)]
enum RequestOutcome {
    Verified,
    Failed(FailureInfo),
    /// The [`PendingHandle`] was dropped without explicit resolution.
    /// Treated as a no-op by `barrier` — neither verified nor a failure.
    Cancelled,
}

impl<N: NetworkSpec> VerificationStatus<N> {
    /// Construct a fresh, healthy status handle. Provider builders call
    /// this to allocate the channels that the verifier tasks publish to.
    pub fn new() -> Self {
        let (counts_tx, counts_rx) = watch::channel(VerificationCounts::default());
        let (health_tx, health_rx) = watch::channel(HealthStatus::default());
        let (consensus_tx, consensus_rx) = watch::channel(ConsensusStatus::default());
        let (security_tx, security_rx) = mpsc::channel(SECURITY_EVENT_BUF);
        let (verbose_tx, _) = broadcast::channel(VERBOSE_EVENT_BUF);

        Self {
            inner: Arc::new(Inner {
                counts_tx,
                counts_rx,
                health_tx,
                health_rx,
                consensus_tx,
                consensus_rx,
                security_tx,
                security_rx: Mutex::new(Some(security_rx)),
                verbose_tx,
                next_id: AtomicU64::new(0),
                pending: Mutex::new(HashMap::new()),
            }),
        }
    }

    /// Latest-value snapshot of the verification counters.
    ///
    /// `Receiver::changed` resumes on every counter update; for a UI
    /// rendering at 60 fps the recommended pattern is to debounce
    /// (`sleep(16ms)` after each `changed().await`) to coalesce bursts.
    pub fn counts(&self) -> watch::Receiver<VerificationCounts> {
        self.inner.counts_rx.clone()
    }

    /// Sticky terminal-state of the provider. `Tainted` and `Stalled`
    /// survive late subscribers — joining after the event observes the
    /// current state immediately.
    ///
    /// This is the load-bearing security signal: `HealthStatus::Tainted`
    /// is flipped synchronously on the first mismatch, *before* the
    /// security_events queue, so the trust state cannot be lost
    /// regardless of event-stream backpressure.
    pub fn health(&self) -> watch::Receiver<HealthStatus> {
        self.inner.health_rx.clone()
    }

    /// Consensus client state — tip, head age, checkpoint. Separated from
    /// `counts()` because consensus advances on every slot regardless of
    /// verification activity; the wallet's "head 18 s old" indicator
    /// shouldn't be coupled to the verification counters.
    pub fn consensus_status(&self) -> watch::Receiver<ConsensusStatus> {
        self.inner.consensus_rx.clone()
    }

    /// Producer-backpressured event channel for `Mismatch` / `Failed`.
    ///
    /// Only one consumer can hold the receiver at a time — security events
    /// are critical and we don't want multiple subscribers racing. The
    /// recommended pattern is for the embedding application to take it
    /// once at startup and fan-out internally if multiple consumers need
    /// it.
    ///
    /// Returns `None` if the receiver has already been taken.
    pub fn take_security_events(&self) -> Option<mpsc::Receiver<SecurityEvent>> {
        self.inner.security_rx.lock().take()
    }

    /// Drop-oldest broadcast for informational events (Verified, Skipped,
    /// Dropped). `RecvError::Lagged(n)` should be translated by callers
    /// into the synthetic `VerificationEvent::Dropped { count: n }`
    /// payload when forwarded across a language boundary.
    pub fn events_verbose(&self) -> broadcast::Receiver<VerificationEvent<N>> {
        self.inner.verbose_tx.subscribe()
    }

    /// Sign-gating barrier: capture the request-ids currently in `pending`
    /// state at this moment, and resolve when every one of them has
    /// settled (Verified, Mismatched, or Failed).
    ///
    /// **Calls landing after barrier creation are not waited for** —
    /// otherwise a chatty UI keeps the barrier open forever. The returned
    /// `VerifiedSnapshot` carries the consensus tip at which the wait
    /// succeeded; signing code can refuse if the snapshot is stale by the
    /// time signing finishes.
    ///
    /// Use [`Self::barrier_with_timeout`] in production — an unresponsive
    /// consensus client could otherwise leave this future pending
    /// indefinitely.
    pub async fn barrier(&self) -> Result<VerifiedSnapshot, VerificationError> {
        // Atomic snapshot: the lock serialises us against `_settle`, so
        // every id in `receivers` either is still pending now (we'll get
        // notified) or has already settled and pushed its outcome
        // through the `watch::Sender` we subscribed to.
        let receivers: Vec<watch::Receiver<Option<RequestOutcome>>> = {
            let pending = self.inner.pending.lock();
            pending.values().map(watch::Sender::subscribe).collect()
        };

        let mut failures = Vec::new();

        for mut rx in receivers {
            loop {
                if let Some(outcome) = rx.borrow().clone() {
                    match outcome {
                        RequestOutcome::Verified | RequestOutcome::Cancelled => {}
                        RequestOutcome::Failed(info) => failures.push(info),
                    }
                    break;
                }
                if rx.changed().await.is_err() {
                    break;
                }
            }
        }

        if !failures.is_empty() {
            return Err(VerificationError::Failed { calls: failures });
        }

        let consensus = self.inner.consensus_rx.borrow().clone();
        Ok(VerifiedSnapshot {
            consensus_tip: consensus.tip,
            head_age: consensus.head_age,
            verified_at: Instant::now(),
        })
    }

    /// Time-bounded variant of [`Self::barrier`].
    pub async fn barrier_with_timeout(
        &self,
        timeout: Duration,
    ) -> Result<VerifiedSnapshot, VerificationError> {
        match tokio::time::timeout(timeout, self.barrier()).await {
            Ok(r) => r,
            Err(_) => Err(VerificationError::Timeout {
                still_pending: self.inner.pending.lock().len(),
            }),
        }
    }

    /// Clear taint after a mismatch has been observed.
    pub fn acknowledge_mismatch(&self) {
        self.inner
            .health_tx
            .send_modify(|s| *s = HealthStatus::default());
    }

    /// Allocate a request id, register a settlement channel, and bump the
    /// pending counter. The returned [`PendingHandle`] must be resolved by
    /// calling one of `record_verified` / `record_mismatch` /
    /// `record_failed`; dropping it without resolution counts as
    /// `Cancelled` (the slot is released, but no outcome counter ticks).
    pub(crate) fn _bump_pending(&self) -> PendingHandle<N> {
        let id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, _) = watch::channel(None);
        self.inner.pending.lock().insert(id, tx);
        self.inner.counts_tx.send_modify(|c| {
            c.pending = c.pending.saturating_add(1);
            c.last_change_at = Some(Instant::now());
        });
        PendingHandle {
            id,
            status: self.clone(),
            resolved: false,
        }
    }

    fn settle(&self, id: u64, outcome: RequestOutcome) {
        if let Some(tx) = self.inner.pending.lock().remove(&id) {
            let _ = tx.send(Some(outcome));
        }
    }

    /// Emit an informational event on the verbose stream, constructed
    /// lazily — the closure runs only when there's at least one
    /// subscriber. Used for `Verified` events whose payload requires
    /// cloning large network types.
    pub(crate) fn _emit_verbose_with<F>(&self, make: F)
    where
        F: FnOnce() -> VerificationEvent<N>,
    {
        if self.inner.verbose_tx.receiver_count() > 0 {
            let _ = self.inner.verbose_tx.send(make());
        }
    }

    /// Producer side: update consensus status. Called by the consensus
    /// supervisor on each tip advance. Public so the supervisor (which
    /// lives in network-specific crates like `helios-ethereum`) can drive
    /// it; not part of the consumer-facing surface.
    #[doc(hidden)]
    pub fn _set_consensus_status(&self, status: ConsensusStatus) {
        let _ = self.inner.consensus_tx.send(status);
    }
}

impl<N: NetworkSpec> Default for VerificationStatus<N> {
    fn default() -> Self {
        Self::new()
    }
}

/// RAII handle returned by [`VerificationStatus::_bump_pending`].
///
/// Must be resolved via [`Self::record_verified`], [`Self::record_mismatch`],
/// or [`Self::record_failed`]. Dropping without resolution releases the
/// pending slot but does not tick any outcome counter — used for
/// cancelled-by-caller paths (e.g. future dropped mid-RPC).
#[must_use = "PendingHandle should be explicitly resolved or dropped"]
pub(crate) struct PendingHandle<N: NetworkSpec> {
    id: u64,
    status: VerificationStatus<N>,
    resolved: bool,
}

impl<N: NetworkSpec> PendingHandle<N> {
    pub(crate) fn record_verified(mut self) {
        self.resolved = true;
        self.status.inner.counts_tx.send_modify(|c| {
            c.pending = c.pending.saturating_sub(1);
            c.verified = c.verified.saturating_add(1);
            c.last_change_at = Some(Instant::now());
        });
        self.status.settle(self.id, RequestOutcome::Verified);
    }

    pub(crate) async fn record_failed(mut self, info: FailureInfo) {
        self.resolved = true;
        self.status.inner.counts_tx.send_modify(|c| {
            c.pending = c.pending.saturating_sub(1);
            c.failed = c.failed.saturating_add(1);
            c.last_change_at = Some(Instant::now());
        });
        self.status
            .settle(self.id, RequestOutcome::Failed(info.clone()));
        let _ = self
            .status
            .inner
            .security_tx
            .send(SecurityEvent::Failed(info))
            .await;
    }
}

impl<N: NetworkSpec> Drop for PendingHandle<N> {
    fn drop(&mut self) {
        if !self.resolved {
            self.status.inner.counts_tx.send_modify(|c| {
                c.pending = c.pending.saturating_sub(1);
                c.last_change_at = Some(Instant::now());
            });
            self.status.settle(self.id, RequestOutcome::Cancelled);
        }
    }
}
