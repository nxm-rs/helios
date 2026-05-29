//! [`HeliosProviderBuilder`] — construct a routing-aware
//! [`alloy::providers::DynProvider<N>`] backed by helios.

use std::sync::Arc;

use alloy::providers::{DynProvider, RootProvider};
use helios_common::network_spec::NetworkSpec;

use crate::client::api::HeliosApi;
use crate::provider::optimistic::OptimisticHeliosProvider;
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
        let provider = match self.routing {
            Routing::VerifiedOnly => DynProvider::new(VerifiedHeliosProvider::from_parts(
                self.helios,
                self.root,
                status.clone(),
            )),
            Routing::OptimisticThenVerified => DynProvider::new(
                OptimisticHeliosProvider::from_parts(self.helios, self.root, status.clone()),
            ),
            Routing::RpcThenVerified => DynProvider::new(self.root),
        };
        (provider, status)
    }
}
