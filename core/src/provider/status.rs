//! [`VerificationStatus`] — the public handle for observing and gating on
//! verification activity.

use std::sync::Arc;
use std::time::Duration;

use helios_common::network_spec::NetworkSpec;
use tokio::sync::{broadcast, mpsc, watch, Mutex};

use crate::provider::error::{MismatchInfo, VerificationError};
use crate::provider::event::{
    ConsensusStatus, HealthStatus, SecurityEvent, VerificationCounts, VerificationEvent,
    VerifiedSnapshot,
};

/// Default buffer sizes for the verification channels.
///
/// `security_events` uses producer-backpressure — a small buffer is
/// intentional so a slow consumer makes the verifier task await rather
/// than racing ahead.
///
/// `events_verbose` uses drop-oldest — the buffer can be larger because
/// dropping is acceptable for informational events.
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
    counts_tx: watch::Sender<VerificationCounts>,
    counts_rx: watch::Receiver<VerificationCounts>,

    health_tx: watch::Sender<HealthStatus>,
    health_rx: watch::Receiver<HealthStatus>,

    consensus_tx: watch::Sender<ConsensusStatus>,
    consensus_rx: watch::Receiver<ConsensusStatus>,

    security_tx: mpsc::Sender<SecurityEvent>,
    security_rx: Mutex<Option<mpsc::Receiver<SecurityEvent>>>,

    verbose_tx: broadcast::Sender<VerificationEvent<N>>,
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
    pub async fn take_security_events(&self) -> Option<mpsc::Receiver<SecurityEvent>> {
        self.inner.security_rx.lock().await.take()
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
        todo!()
    }

    /// Time-bounded variant of [`Self::barrier`].
    pub async fn barrier_with_timeout(
        &self,
        _timeout: Duration,
    ) -> Result<VerifiedSnapshot, VerificationError> {
        todo!()
    }

    /// Clear taint after a mismatch has been observed.
    pub fn acknowledge_mismatch(&self) {
        self.inner
            .health_tx
            .send_modify(|s| *s = HealthStatus::default());
    }

    /// Producer side: bump pending count when a verification is
    /// dispatched.
    pub(crate) fn _bump_pending(&self) {
        self.inner.counts_tx.send_modify(|c| {
            c.pending = c.pending.saturating_add(1);
            c.last_change_at = Some(std::time::Instant::now());
        });
    }

    /// Producer side: record a verified result.
    pub(crate) fn _record_verified(&self) {
        self.inner.counts_tx.send_modify(|c| {
            c.pending = c.pending.saturating_sub(1);
            c.verified = c.verified.saturating_add(1);
            c.last_change_at = Some(std::time::Instant::now());
        });
    }

    /// Producer side: record a mismatched result. Flips `HealthStatus`
    /// synchronously *before* pushing the diagnostic detail to
    /// security_events, so the trust signal is observable on `health()`
    /// regardless of event-stream backpressure.
    pub(crate) async fn _record_mismatch(&self, info: MismatchInfo) {
        // Synchronous: taint is visible immediately on `health()`.
        self.inner.health_tx.send_modify(|s| {
            *s = HealthStatus::Tainted {
                first_mismatch: Box::new(info.clone()),
            };
        });
        self.inner.counts_tx.send_modify(|c| {
            c.pending = c.pending.saturating_sub(1);
            c.mismatched = c.mismatched.saturating_add(1);
            c.last_change_at = Some(std::time::Instant::now());
        });
        // Best-effort: the channel is bounded with backpressure, but a
        // capacity miss here is acceptable — `health()` already carries
        // the trust signal.
        let _ = self.inner.security_tx.send(SecurityEvent::Mismatch(info)).await;
    }

    /// Producer side: record a non-trust failure (transport error,
    /// proof-decoding error, etc.).
    pub(crate) async fn _record_failed(&self, info: crate::provider::error::FailureInfo) {
        self.inner.counts_tx.send_modify(|c| {
            c.pending = c.pending.saturating_sub(1);
            c.failed = c.failed.saturating_add(1);
            c.last_change_at = Some(std::time::Instant::now());
        });
        let _ = self.inner.security_tx.send(SecurityEvent::Failed(info)).await;
    }

    /// Producer side: emit an informational event on the verbose stream.
    /// Lossy by design; if no subscribers, this is a no-op.
    pub(crate) fn _emit_verbose(&self, event: VerificationEvent<N>) {
        let _ = self.inner.verbose_tx.send(event);
    }

    /// Like [`Self::_emit_verbose`], but the event is constructed lazily —
    /// the closure runs only when there's at least one subscriber. Used
    /// for `Verified` events whose payload requires cloning large network
    /// types.
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
