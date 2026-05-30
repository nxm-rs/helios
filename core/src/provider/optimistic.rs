//! [`OptimisticHeliosProvider`] — returns unverified RPC values
//! immediately and fans out background verification.
//!
//! For every overridden read method, the optimistic provider:
//! 1. issues the unverified call against [`RootProvider<N>`] and returns
//!    the value to the caller as soon as it arrives,
//! 2. spawns a background task that issues the verified-path call
//!    against [`HeliosApi<N>`] and compares the two responses,
//! 3. on a verified-vs-unverified mismatch, flips
//!    [`HealthStatus::Tainted`] *synchronously* before publishing the
//!    diagnostic [`SecurityEvent::Mismatch`] — see the load-bearing
//!    invariant documented in [`super`].
//!
//! Phase 2a overrides only `get_balance` as a proof of concept; the
//! remaining read methods land in 2b and follow the same pattern.

use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Instant;

use alloy::eips::BlockId;
use alloy::primitives::{Address, U256};
use alloy::providers::{Provider, ProviderCall, RootProvider, RpcWithBlock};
use helios_common::network_spec::NetworkSpec;

use crate::client::api::HeliosApi;
use crate::provider::error::{FailureInfo, MismatchInfo};
use crate::provider::status::VerificationStatus;

/// Optimistic-first helios provider. Returns the unverified RPC value
/// immediately and verifies in the background.
///
/// Cheap to clone — internally just an `Arc<Inner<N>>`.
///
/// Share the [`VerificationStatus<N>`] with a sibling
/// [`super::VerifiedHeliosProvider<N>`] when an embedder wants both:
/// the optimistic provider drives unverified rendering and verification
/// fan-out, while the verified provider is reserved for sign-gated
/// reads (balance/nonce immediately before signing).
#[derive(Clone)]
pub struct OptimisticHeliosProvider<N: NetworkSpec> {
    inner: Arc<Inner<N>>,
}

pub(crate) struct Inner<N: NetworkSpec> {
    helios: Arc<dyn HeliosApi<N>>,
    root: RootProvider<N>,
    status: VerificationStatus<N>,
    _network: PhantomData<N>,
}

impl<N: NetworkSpec> OptimisticHeliosProvider<N> {
    /// Construct from a pre-built [`HeliosApi`] impl, an alloy
    /// [`RootProvider<N>`] over the same execution RPC, and a shared
    /// [`VerificationStatus<N>`] handle.
    pub fn from_parts(
        helios: Arc<dyn HeliosApi<N>>,
        root: RootProvider<N>,
        status: VerificationStatus<N>,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                helios,
                root,
                status,
                _network: PhantomData,
            }),
        }
    }

    /// Returns the [`VerificationStatus`] handle for observing and
    /// gating on verification activity.
    pub fn verification_status(&self) -> &VerificationStatus<N> {
        &self.inner.status
    }
}

impl<N: NetworkSpec> Provider<N> for OptimisticHeliosProvider<N> {
    fn root(&self) -> &RootProvider<N> {
        &self.inner.root
    }

    fn get_balance(&self, address: Address) -> RpcWithBlock<Address, U256, U256> {
        let provider = self.clone();
        RpcWithBlock::new_provider(move |block_id| {
            let provider = provider.clone();
            ProviderCall::BoxedFuture(Box::pin(async move {
                let unverified = provider
                    .inner
                    .root
                    .get_balance(address)
                    .block_id(block_id)
                    .await?;
                provider.spawn_verifier_get_balance(address, block_id, unverified);
                Ok(unverified)
            }))
        })
    }
}

impl<N: NetworkSpec> OptimisticHeliosProvider<N> {
    /// Spawn a background verifier for one `get_balance` call. Compares
    /// the verified result against `unverified`; on divergence records a
    /// mismatch (which flips `HealthStatus::Tainted` synchronously
    /// before publishing the diagnostic event).
    ///
    /// The helios call is wrapped in `catch_unwind` so a panic in the
    /// verifier (proof-decoding bug, arithmetic overflow, fuzzy input)
    /// surfaces as `record_failed` with a descriptive `FailureInfo`
    /// rather than silently leaving counters at "pending → drop →
    /// Cancelled" — which would let a verifier-side bug hide behind
    /// the noise of normal operation.
    fn spawn_verifier_get_balance(&self, address: Address, block_id: BlockId, unverified: U256) {
        use futures::future::FutureExt;
        let handle = self.inner.status._bump_pending();
        let helios = self.inner.helios.clone();
        #[cfg(not(target_arch = "wasm32"))]
        let run = tokio::spawn;
        #[cfg(target_arch = "wasm32")]
        let run = wasm_bindgen_futures::spawn_local;
        run(async move {
            let result = std::panic::AssertUnwindSafe(helios.get_balance(address, block_id))
                .catch_unwind()
                .await;
            match result {
                Ok(Ok(verified)) if verified == unverified => {
                    handle.record_verified();
                }
                Ok(Ok(verified)) => {
                    let info = MismatchInfo {
                        method: "eth_getBalance",
                        unverified: format!("{unverified:#x}").into_boxed_str(),
                        verified: format!("{verified:#x}").into_boxed_str(),
                        at: Instant::now(),
                    };
                    handle.record_mismatch(info);
                }
                Ok(Err(err)) => {
                    let info = FailureInfo {
                        method: "eth_getBalance",
                        error: err.to_string().into_boxed_str(),
                        at: Instant::now(),
                    };
                    handle.record_failed(info);
                }
                Err(panic) => {
                    let msg = panic_message(panic);
                    let info = FailureInfo {
                        method: "eth_getBalance",
                        error: format!("verifier panicked: {msg}").into_boxed_str(),
                        at: Instant::now(),
                    };
                    handle.record_failed(info);
                }
            }
        });
    }
}

/// Extract a string message from a [`std::panic::catch_unwind`] payload.
fn panic_message(panic: Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = panic.downcast_ref::<&'static str>() {
        (*s).to_string()
    } else if let Some(s) = panic.downcast_ref::<String>() {
        s.clone()
    } else {
        "<non-string panic payload>".to_string()
    }
}
