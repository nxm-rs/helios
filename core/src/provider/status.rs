//! [`VerificationStatus`] — the public handle for observing and gating on
//! verification activity.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use helios_common::network_spec::NetworkSpec;
use parking_lot::Mutex;
use tokio::sync::{broadcast, mpsc, watch};

use crate::provider::error::{FailureInfo, MismatchInfo, VerificationError};
use crate::provider::event::{
    ConsensusStatus, HealthStatus, SecurityEvent, VerificationCounts, VerificationEvent,
    VerifiedSnapshot,
};

const SECURITY_EVENT_BUF: usize = 64;
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
    // Each `watch` channel keeps both ends in `Inner`. The Receiver is
    // retained to guarantee `Sender::send` succeeds even across windows
    // where all external subscribers have temporarily dropped — without
    // a held receiver, `tokio::sync::watch::Sender::send` returns Err
    // and the producer-side update is silently lost. A drive-by
    // "cleanup unused field" refactor would re-introduce that bug.
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
    pending: Mutex<HashMap<u64, watch::Sender<Option<RequestOutcome>>>>,
}

/// Per-request settlement outcome. Carried on the watch channel held in
/// [`Inner::pending`] so [`VerificationStatus::barrier`] can classify
/// each snapshot id when it settles.
#[derive(Clone)]
pub(crate) enum RequestOutcome {
    Verified,
    Mismatched(MismatchInfo),
    Failed(FailureInfo),
    /// The [`PendingHandle`] was dropped without explicit resolution.
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
    /// rendering at frame rate the recommended pattern is to debounce
    /// after each `changed().await` to coalesce bursts.
    pub fn counts(&self) -> watch::Receiver<VerificationCounts> {
        self.inner.counts_rx.clone()
    }

    /// Sticky terminal-state of the provider. `Stalled` survives late
    /// subscribers — joining after the transition observes the current
    /// state immediately.
    pub fn health(&self) -> watch::Receiver<HealthStatus> {
        self.inner.health_rx.clone()
    }

    /// Consensus client state — tip, head age, checkpoint. Separated from
    /// `counts()` because consensus advances on every slot regardless of
    /// verification activity; a "head 18 s old" indicator shouldn't be
    /// coupled to the verification counters.
    pub fn consensus_status(&self) -> watch::Receiver<ConsensusStatus> {
        self.inner.consensus_rx.clone()
    }

    /// Bounded `mpsc::Receiver<SecurityEvent>`. Phase 1 publishes via
    /// `try_send`, so events past the buffer capacity are dropped rather
    /// than blocking the producer. Take it once at startup and fan out
    /// internally if multiple consumers need it.
    ///
    /// Returns `None` if the receiver has already been taken.
    pub fn take_security_events(&self) -> Option<mpsc::Receiver<SecurityEvent>> {
        self.inner.security_rx.lock().take()
    }

    /// Drop-oldest broadcast for informational events. Subscribe **before**
    /// issuing verified calls if you need to capture early events — events
    /// emitted while no subscribers are attached are dropped silently
    /// (the broadcast channel only synthesises `Lagged(n)` for receivers
    /// that exist and fall behind). Callers should translate
    /// `RecvError::Lagged(n)` into a synthetic
    /// [`VerificationEvent::Dropped`] when forwarding across a language
    /// boundary.
    pub fn events_verbose(&self) -> broadcast::Receiver<VerificationEvent<N>> {
        self.inner.verbose_tx.subscribe()
    }

    /// Sign-gating barrier: capture the request-ids currently in `pending`
    /// state at this moment, and resolve when every one of them has
    /// settled. Calls landing after barrier creation are not waited for.
    /// Refuses immediately with [`VerificationError::Tainted`] if the
    /// provider is currently tainted.
    ///
    /// Use [`Self::barrier_with_timeout`] in production — an unresponsive
    /// helios path could otherwise leave this future pending indefinitely.
    ///
    /// See also [`Self::scope`] for a per-screen barrier that only waits
    /// for calls made within the scope's lifetime.
    pub async fn barrier(&self) -> Result<VerifiedSnapshot, VerificationError> {
        self.barrier_over_receivers(self.snapshot_receivers()).await
    }

    /// Time-bounded variant of [`Self::barrier`]. On timeout, reports the
    /// number of the barrier's snapshot ids that hadn't settled yet — not
    /// the unrelated global pending count.
    pub async fn barrier_with_timeout(
        &self,
        timeout: Duration,
    ) -> Result<VerifiedSnapshot, VerificationError> {
        self.barrier_with_timeout_over_receivers(self.snapshot_receivers(), timeout)
            .await
    }

    /// Open a new [`Scope`] for sign-gating one logical UI screen or
    /// workflow. The scope captures the current request-id counter; its
    /// own `barrier()` filters the pending registry to ids allocated
    /// after this point, so unrelated background calls in flight at
    /// signing time don't block the gate.
    pub fn scope(&self) -> Scope<N> {
        Scope {
            status: self.clone(),
            start_id: self.inner.next_id.load(Ordering::Relaxed),
        }
    }

    pub(crate) async fn barrier_over_receivers(
        &self,
        receivers: Vec<watch::Receiver<Option<RequestOutcome>>>,
    ) -> Result<VerifiedSnapshot, VerificationError> {
        if self.is_tainted() {
            return Err(VerificationError::Tainted);
        }
        let mut mismatches = Vec::new();
        let mut failures = Vec::new();
        let mut settled = 0usize;
        Self::drain_receivers(receivers, &mut mismatches, &mut failures, &mut settled).await;
        self.finish_barrier(mismatches, failures)
    }

    pub(crate) async fn barrier_with_timeout_over_receivers(
        &self,
        receivers: Vec<watch::Receiver<Option<RequestOutcome>>>,
        timeout: Duration,
    ) -> Result<VerifiedSnapshot, VerificationError> {
        if self.is_tainted() {
            return Err(VerificationError::Tainted);
        }
        let snapshot_len = receivers.len();
        let mut mismatches = Vec::new();
        let mut failures = Vec::new();
        let mut settled = 0usize;

        let drained = tokio::time::timeout(
            timeout,
            Self::drain_receivers(receivers, &mut mismatches, &mut failures, &mut settled),
        )
        .await
        .is_ok();

        if !drained {
            return Err(VerificationError::Timeout {
                still_pending: snapshot_len.saturating_sub(settled),
            });
        }
        self.finish_barrier(mismatches, failures)
    }

    fn is_tainted(&self) -> bool {
        matches!(*self.inner.health_rx.borrow(), HealthStatus::Tainted { .. })
    }

    fn snapshot_receivers(&self) -> Vec<watch::Receiver<Option<RequestOutcome>>> {
        // Hold the lock for the snapshot only — `_settle` is serialised
        // against us, so every id either is still pending now (we'll get
        // notified) or has already pushed its outcome through the
        // `watch::Sender` we subscribed to.
        let pending = self.inner.pending.lock();
        pending.values().map(watch::Sender::subscribe).collect()
    }

    /// Snapshot the pending registry filtered to ids in `[start, end)`.
    /// Used by [`Scope`] to barrier only on calls made after scope
    /// creation.
    pub(crate) fn snapshot_receivers_in_range(
        &self,
        start: u64,
        end: u64,
    ) -> Vec<watch::Receiver<Option<RequestOutcome>>> {
        let pending = self.inner.pending.lock();
        pending
            .iter()
            .filter(|(id, _)| **id >= start && **id < end)
            .map(|(_, tx)| tx.subscribe())
            .collect()
    }

    pub(crate) fn next_id_snapshot(&self) -> u64 {
        self.inner.next_id.load(Ordering::Relaxed)
    }

    async fn drain_receivers(
        receivers: Vec<watch::Receiver<Option<RequestOutcome>>>,
        mismatches: &mut Vec<MismatchInfo>,
        failures: &mut Vec<FailureInfo>,
        settled: &mut usize,
    ) {
        for mut rx in receivers {
            loop {
                if let Some(outcome) = rx.borrow().clone() {
                    match outcome {
                        RequestOutcome::Verified | RequestOutcome::Cancelled => {}
                        RequestOutcome::Mismatched(info) => mismatches.push(info),
                        RequestOutcome::Failed(info) => failures.push(info),
                    }
                    *settled += 1;
                    break;
                }
                if rx.changed().await.is_err() {
                    // Sender dropped without a settle — treat as cancelled.
                    *settled += 1;
                    break;
                }
            }
        }
    }

    fn finish_barrier(
        &self,
        mismatches: Vec<MismatchInfo>,
        failures: Vec<FailureInfo>,
    ) -> Result<VerifiedSnapshot, VerificationError> {
        if !mismatches.is_empty() {
            return Err(VerificationError::Mismatched { calls: mismatches });
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

    /// Allocate a request id, register a settlement channel, and bump the
    /// pending counter. The returned [`PendingHandle`] must be resolved by
    /// calling [`PendingHandle::record_verified`],
    /// [`PendingHandle::record_mismatch`], or
    /// [`PendingHandle::record_failed`]; dropping it without resolution
    /// counts as `Cancelled` — the slot is released but no outcome
    /// counter ticks.
    ///
    /// Id allocation and the pending-map insert are atomic together: any
    /// reader that locks `pending` either sees the bumped `next_id` and
    /// the matching entry, or neither. Required for the snapshot
    /// semantics that downstream barriers / scopes rely on.
    pub(crate) fn _bump_pending(&self) -> PendingHandle<N> {
        let (tx, _) = watch::channel(None);
        let id = {
            let mut pending = self.inner.pending.lock();
            let id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
            pending.insert(id, tx);
            id
        };
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
        F: FnOnce() -> Option<VerificationEvent<N>>,
    {
        if self.inner.verbose_tx.receiver_count() > 0 {
            if let Some(event) = make() {
                let _ = self.inner.verbose_tx.send(event);
            }
        }
    }

    /// Producer side: update consensus status. Called by the consensus
    /// supervisor on each tip advance.
    #[doc(hidden)]
    pub fn _set_consensus_status(&self, status: ConsensusStatus) {
        let _ = self.inner.consensus_tx.send(status);
    }

    /// Producer side: set health status. Called by the consensus
    /// supervisor when stall thresholds trip or recover.
    ///
    /// **Sticky-taint guard:** `_set_health` refuses to overwrite a
    /// `Tainted` state with anything *except* another `Tainted`. So a
    /// stall recovery (`Healthy`) while the provider is already tainted
    /// preserves the trust signal — embedders that read `health()` for
    /// sign-gating cannot have the taint silently cleared by a
    /// concurrent supervisor write. Use
    /// [`Self::acknowledge_mismatch`] to explicitly clear taint.
    #[doc(hidden)]
    pub fn _set_health(&self, health: HealthStatus) {
        self.inner.health_tx.send_if_modified(|current| {
            // Sticky taint: refuse to overwrite Tainted with anything
            // other than Tainted. Any → Tainted, Tainted → Tainted (a
            // later mismatch) are allowed.
            if matches!(current, HealthStatus::Tainted { .. })
                && !matches!(health, HealthStatus::Tainted { .. })
            {
                return false;
            }
            *current = health;
            true
        });
    }

    /// Clear taint after a mismatch has been observed by the embedder.
    /// Guarded: only transitions `Tainted -> Healthy`; calling this in
    /// any other state (e.g. `Stalled`) is a no-op so a "clear taint"
    /// button cannot accidentally wipe a stall.
    pub fn acknowledge_mismatch(&self) {
        let cleared = self.inner.health_tx.send_if_modified(|s| {
            if matches!(s, HealthStatus::Tainted { .. }) {
                *s = HealthStatus::Healthy;
                true
            } else {
                false
            }
        });
        if cleared {
            let _ = self
                .inner
                .security_tx
                .try_send(SecurityEvent::MismatchAcknowledged { at: Instant::now() });
        }
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

    /// Resolves the handle as a mismatch.
    ///
    /// **Load-bearing security invariant:** `HealthStatus::Tainted` is
    /// flipped *synchronously*, before the security event is published.
    /// Sign-gating that reads `health()` sees the taint immediately,
    /// regardless of `security_events` backpressure or consumer
    /// scheduling. The taint signal cannot be lost.
    pub(crate) fn record_mismatch(mut self, info: MismatchInfo) {
        self.resolved = true;
        // Flip Tainted FIRST. This is what `health()` consumers observe
        // for sign-gating, and it must precede the security event push
        // so backpressure on the event channel cannot delay the trust
        // signal.
        self.status.inner.health_tx.send_modify(|s| {
            *s = HealthStatus::Tainted {
                first_mismatch: Box::new(info.clone()),
            };
        });
        self.status.inner.counts_tx.send_modify(|c| {
            c.pending = c.pending.saturating_sub(1);
            c.mismatched = c.mismatched.saturating_add(1);
            c.last_change_at = Some(Instant::now());
        });
        self.status
            .settle(self.id, RequestOutcome::Mismatched(info.clone()));
        // Synchronous try_send: if the buffer is full, the diagnostic is
        // dropped. The trust signal is already visible on `health()`.
        let _ = self
            .status
            .inner
            .security_tx
            .try_send(SecurityEvent::Mismatch(info));
    }

    /// Resolves the handle and publishes a [`SecurityEvent::Failed`].
    /// Synchronous so the publish is not cancellable — if the surrounding
    /// future is dropped, the event still reaches the channel (or is
    /// dropped immediately on a full channel; never silently lost to
    /// cancellation between the counter update and the publish).
    pub(crate) fn record_failed(mut self, info: FailureInfo) {
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
            .try_send(SecurityEvent::Failed(info));
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

/// Per-scope sign-gating barrier. Returned by [`VerificationStatus::scope`].
///
/// Captures the request-id counter at creation time. The scope's
/// [`Self::barrier`] / [`Self::barrier_with_timeout`] await only calls
/// whose ids fall in `[start_id, current_next_id)` at barrier call
/// time, so unrelated background calls in flight elsewhere in the
/// process don't block the gate.
///
/// `Tainted` is **not** scope-local — it's the sticky trust signal for
/// the entire provider, so a mismatch on any in-flight call (even one
/// outside this scope) refuses every barrier. That's intentional:
/// signing on a tainted provider is unsafe regardless of which screen
/// observed the mismatch.
///
/// Cheap to clone — just an `Arc<Inner>` + a `u64`.
#[derive(Clone)]
pub struct Scope<N: NetworkSpec> {
    status: VerificationStatus<N>,
    start_id: u64,
}

impl<N: NetworkSpec> Scope<N> {
    /// Wait for every call made within this scope to settle.
    ///
    /// "Within this scope" means: an id allocated by `_bump_pending`
    /// after [`VerificationStatus::scope`] was called and before this
    /// `barrier()` call. Ids that landed and settled inside the window
    /// before barrier() are not awaited (they're already done) but
    /// their `Tainted` flag — if any was a mismatch — still refuses
    /// the barrier via the sticky `health()` check.
    pub async fn barrier(&self) -> Result<VerifiedSnapshot, VerificationError> {
        let end_id = self.status.next_id_snapshot();
        let receivers = self
            .status
            .snapshot_receivers_in_range(self.start_id, end_id);
        self.status.barrier_over_receivers(receivers).await
    }

    /// Time-bounded variant of [`Self::barrier`]. On timeout, reports
    /// the number of scope ids that hadn't settled yet.
    pub async fn barrier_with_timeout(
        &self,
        timeout: std::time::Duration,
    ) -> Result<VerifiedSnapshot, VerificationError> {
        let end_id = self.status.next_id_snapshot();
        let receivers = self
            .status
            .snapshot_receivers_in_range(self.start_id, end_id);
        self.status
            .barrier_with_timeout_over_receivers(receivers, timeout)
            .await
    }

    /// Returns the parent [`VerificationStatus`] handle so consumers
    /// can subscribe to `health()` / `counts()` / etc. without holding
    /// a separate handle.
    pub fn verification_status(&self) -> &VerificationStatus<N> {
        &self.status
    }
}
