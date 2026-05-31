//! [`HeliosProviderBuilder`] — construct a routing-aware
//! [`alloy::providers::DynProvider<N>`] backed by helios.

use std::sync::Arc;

use alloy::providers::{DynProvider, RootProvider};
use helios_common::network_spec::NetworkSpec;

use crate::client::api::HeliosApi;
use crate::provider::error::MismatchInfo;
use crate::provider::event::HealthStatus;
use crate::provider::optimistic::OptimisticHeliosProvider;
use crate::provider::persistence::{spawn_taint_persistence, TaintConfig};
use crate::provider::status::VerificationStatus;
use crate::provider::verified::VerifiedHeliosProvider;

/// How a [`DynProvider<N>`] constructed via [`HeliosProviderBuilder`]
/// routes each read call.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Routing {
    /// Every helios-backable read awaits the verified path before
    /// returning. The strictest, slowest routing. Default for
    /// sign-gating use cases.
    #[default]
    VerifiedOnly,
    /// Each call returns the unverified RPC value immediately and
    /// spawns a background verifier task. Mismatches taint the provider
    /// via the shared [`VerificationStatus`].
    OptimisticThenVerified,
    /// Each call returns the unverified RPC value with no background
    /// verification. The shared [`VerificationStatus`] is still
    /// constructed (so the embedder can issue ad-hoc verified calls
    /// against a sibling [`VerifiedHeliosProvider`] if they keep one),
    /// but this routing emits no events on its own.
    RpcThenVerified,
}

/// Builder for a helios-backed [`DynProvider<N>`]. Pick a [`Routing`]
/// and call [`Self::build`] (or [`Self::build_with_status`] when you
/// want the [`VerificationStatus`] handle alongside the provider).
pub struct HeliosProviderBuilder<N: NetworkSpec> {
    helios: Arc<dyn HeliosApi<N>>,
    root: RootProvider<N>,
    status: Option<VerificationStatus<N>>,
    routing: Routing,
    taint_config: Option<TaintConfig>,
}

impl<N: NetworkSpec> HeliosProviderBuilder<N> {
    /// Start a new builder. Both the helios api and the alloy
    /// [`RootProvider<N>`] over the same execution RPC are required up
    /// front so the builder can wire either routing.
    pub fn new(helios: Arc<dyn HeliosApi<N>>, root: RootProvider<N>) -> Self {
        Self {
            helios,
            root,
            status: None,
            routing: Routing::default(),
            taint_config: None,
        }
    }

    /// Select the routing. Defaults to [`Routing::VerifiedOnly`].
    pub fn routing(mut self, routing: Routing) -> Self {
        self.routing = routing;
        self
    }

    /// Use a caller-provided [`VerificationStatus`] handle. Useful when
    /// the embedder wants to share one handle across multiple providers
    /// (e.g., a verified provider for sign-gating + an optimistic
    /// provider for browsing in the same wallet session).
    pub fn status(mut self, status: VerificationStatus<N>) -> Self {
        self.status = Some(status);
        self
    }

    /// Configure how verifier taint persists across process restarts.
    /// Defaults to [`TaintConfig::PerSession`] (no persistence).
    ///
    /// When set to a persisting variant ([`TaintConfig::DataDir`] or
    /// [`TaintConfig::Custom`]), [`Self::build`] / [`Self::build_with_status`]:
    ///
    /// 1. Load any existing mismatch from the store and pre-flip
    ///    `health()` to [`HealthStatus::Tainted`] *before* returning,
    ///    so the very first caller cannot race a verified read past a
    ///    persisted taint.
    /// 2. Spawn a background task that subscribes to `health()` and
    ///    writes to the store on each Tainted transition / clears it
    ///    when `acknowledge_mismatch` is called. File I/O happens on
    ///    `spawn_blocking` so the runtime isn't blocked.
    pub fn taint_config(mut self, config: TaintConfig) -> Self {
        self.taint_config = Some(config);
        self
    }

    /// Build the [`DynProvider<N>`] selected by [`Routing`]. The
    /// underlying [`VerificationStatus`] is constructed if not provided
    /// but is not returned — use [`Self::build_with_status`] when you
    /// need it.
    pub fn build(self) -> DynProvider<N> {
        self.build_with_status().0
    }

    /// Build the [`DynProvider<N>`] and return the
    /// [`VerificationStatus`] alongside.
    pub fn build_with_status(self) -> (DynProvider<N>, VerificationStatus<N>) {
        let status = self.status.unwrap_or_default();

        if let Some(store) = self.taint_config.and_then(TaintConfig::into_store) {
            match store.load() {
                Ok(Some(info)) => {
                    status._set_health(HealthStatus::Tainted {
                        first_mismatch: Box::new(info),
                    });
                }
                Ok(None) => {}
                Err(err) => {
                    // Fail closed: a corrupt / unreadable persisted record
                    // is treated as taint of unknown origin, not as "no
                    // taint". The synthetic MismatchInfo records the load
                    // failure so the embedder can surface it via the
                    // usual health() / acknowledge_mismatch flow.
                    tracing::warn!(
                        error = %err,
                        "taint store load failed; pre-flipping Tainted",
                    );
                    let info = MismatchInfo::now(
                        "<persistence load>",
                        "load error",
                        err.to_string(),
                    );
                    status._set_health(HealthStatus::Tainted {
                        first_mismatch: Box::new(info),
                    });
                }
            }
            spawn_taint_persistence(&status, store);
        }

        let provider = match self.routing {
            Routing::VerifiedOnly => DynProvider::new(VerifiedHeliosProvider::from_parts(
                self.helios,
                self.root,
                status.clone(),
            )),
            Routing::OptimisticThenVerified => DynProvider::new(
                OptimisticHeliosProvider::from_parts(self.helios, self.root, status.clone()),
            ),
            // Wrap RootProvider in RpcOnlyHeliosProvider so the
            // returned provider holds a clone of the VerificationStatus.
            // Without the clone, callers using build() (which drops the
            // returned status) would lose the status sender as soon as
            // build() returns — breaking any background subscriber (e.g.
            // the taint-persistence task added in a later PR).
            Routing::RpcThenVerified => {
                DynProvider::new(RpcOnlyHeliosProvider::new(self.root, status.clone()))
            }
        };
        (provider, status)
    }
}

/// Thin wrapper around [`RootProvider<N>`] that holds a
/// [`VerificationStatus<N>`] clone alongside the inner provider.
/// Implements [`Provider<N>`] by delegating to the inner root, so the
/// dispatch behaviour is identical to a bare `RootProvider<N>` — the
/// only difference is that the held status clone keeps the
/// `VerificationStatus<N>` sender alive across `build()`'s drop of the
/// returned tuple's `.1`.
#[derive(Clone)]
struct RpcOnlyHeliosProvider<N: NetworkSpec> {
    root: RootProvider<N>,
    _status: VerificationStatus<N>,
}

impl<N: NetworkSpec> RpcOnlyHeliosProvider<N> {
    fn new(root: RootProvider<N>, status: VerificationStatus<N>) -> Self {
        Self {
            root,
            _status: status,
        }
    }
}

impl<N: NetworkSpec> alloy::providers::Provider<N> for RpcOnlyHeliosProvider<N> {
    fn root(&self) -> &RootProvider<N> {
        &self.root
    }
}
