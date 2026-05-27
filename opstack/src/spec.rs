use std::{collections::HashMap, sync::Arc};

use alloy::{
    consensus::{proofs::calculate_transaction_root, Receipt, ReceiptWithBloom, TxType},
    eips::{BlockId, Encodable2718},
    primitives::Address,
    rpc::types::{state::StateOverride, Log},
};

use async_trait::async_trait;
use helios_common::{
    execution_provider::ExecutionProvider,
    fork_schedule::ForkSchedule,
    network_spec::NetworkSpec,
    types::{Account, EvmError},
};
use op_alloy_consensus::{
    OpDepositReceipt, OpDepositReceiptWithBloom, OpReceipt, OpTxEnvelope, OpTxType,
    OpTypedTransaction,
};
use op_alloy_network::{
    BuildResult, Network, NetworkTransactionBuilder, NetworkWallet, TransactionBuilderError,
};
use op_alloy_rpc_types::{OpTransactionRequest, Transaction};
use op_revm::OpHaltReason;
use revm::context::result::ExecutionResult;

use crate::evm::OpStackEvm;

#[derive(Clone, Copy, Debug)]
pub struct OpStack;

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl NetworkSpec for OpStack {
    type HaltReason = OpHaltReason;

    fn encode_receipt(receipt: &Self::ReceiptResponse) -> Vec<u8> {
        // op-alloy 2.0 reshaped the receipt envelope: what used to be
        // `OpReceiptEnvelope<Log>` is now `ReceiptWithBloom<OpReceipt<Log>>`.
        // The bloom moved from the envelope variant into the outer
        // `ReceiptWithBloom`, and the inner `OpReceipt` enum holds a
        // bare `Receipt<Log>` per variant (and `OpDepositReceipt<Log>` for
        // the deposit variant).
        let receipt_with_bloom = &receipt.inner.inner;
        let bloom = receipt_with_bloom.logs_bloom;
        let op_receipt = &receipt_with_bloom.receipt;
        let tx_type = op_receipt.tx_type();
        let logs = op_receipt
            .as_receipt()
            .logs
            .iter()
            .map(|l| l.inner.clone())
            .collect::<Vec<_>>();

        let raw_encoded = match op_receipt {
            OpReceipt::Legacy(inner)
            | OpReceipt::Eip2930(inner)
            | OpReceipt::Eip1559(inner)
            | OpReceipt::Eip7702(inner)
            | OpReceipt::PostExec(inner) => {
                let r = Receipt {
                    status: inner.status,
                    cumulative_gas_used: inner.cumulative_gas_used,
                    logs,
                };
                let rwb = ReceiptWithBloom::new(r, bloom);
                alloy::rlp::encode(rwb)
            }
            OpReceipt::Deposit(inner) => {
                let r = Receipt {
                    status: inner.inner.status,
                    cumulative_gas_used: inner.inner.cumulative_gas_used,
                    logs,
                };

                let r = OpDepositReceipt {
                    inner: r,
                    deposit_nonce: inner.deposit_nonce,
                    deposit_receipt_version: inner.deposit_receipt_version,
                };

                let rwb = OpDepositReceiptWithBloom::new(r, bloom);
                alloy::rlp::encode(rwb)
            }
        };

        match tx_type {
            OpTxType::Legacy => raw_encoded,
            _ => [vec![tx_type as u8], raw_encoded].concat(),
        }
    }

    fn encode_transaction(tx: &Self::TransactionResponse) -> Vec<u8> {
        tx.inner.inner.encoded_2718()
    }

    fn is_hash_valid(block: &Self::BlockResponse) -> bool {
        if block.header.hash_slow() != block.header.hash {
            return false;
        }

        if let Some(txs) = block.transactions.as_transactions() {
            let txs_root = calculate_transaction_root(
                &txs.iter()
                    .map(|t| t.clone().inner.inner)
                    .collect::<Vec<_>>(),
            );
            if txs_root != block.header.transactions_root {
                return false;
            }
        }

        if let Some(withdrawals) = &block.withdrawals {
            if !withdrawals.0.is_empty() {
                return false;
            }
            // TODO: handle L2ToL1MessagePasser storage root check
        }

        true
    }

    fn receipt_contains(list: &[Self::ReceiptResponse], elem: &Self::ReceiptResponse) -> bool {
        for receipt in list {
            if receipt == elem {
                return true;
            }
        }

        false
    }

    fn receipt_logs(receipt: &Self::ReceiptResponse) -> Vec<Log> {
        receipt.inner.inner.receipt.as_receipt().logs.clone()
    }

    async fn transact<E: ExecutionProvider<Self>>(
        tx: &Self::TransactionRequest,
        validate_tx: bool,
        execution: Arc<E>,
        chain_id: u64,
        fork_schedule: ForkSchedule,
        block_id: BlockId,
        state_overrides: Option<StateOverride>,
    ) -> Result<(ExecutionResult<Self::HaltReason>, HashMap<Address, Account>), EvmError> {
        let mut evm = OpStackEvm::new(execution, chain_id, fork_schedule, block_id);

        evm.transact_inner(tx, validate_tx, state_overrides).await
    }
}

impl Network for OpStack {
    type TxType = op_alloy_consensus::OpTxType;
    type TxEnvelope = OpTxEnvelope;
    type UnsignedTx = OpTypedTransaction;
    type ReceiptEnvelope = op_alloy_consensus::OpReceiptEnvelope;
    type Header = alloy::consensus::Header;
    type TransactionRequest = OpTransactionRequest;
    type TransactionResponse = Transaction;
    type ReceiptResponse = op_alloy_rpc_types::OpTransactionReceipt;
    type HeaderResponse = alloy::rpc::types::Header;
    type BlockResponse = alloy::rpc::types::Block<Self::TransactionResponse, Self::HeaderResponse>;
}

// alloy 2.0 split `TransactionBuilder` into a base trait (getters and
// setters, provided by `op-alloy-rpc-types` for `OpTransactionRequest`
// already) and `NetworkTransactionBuilder<N>` (build, submit, type
// selection). Because helios's `OpStack` is a distinct type from
// op-alloy's `Optimism`, we still need our own `NetworkTransactionBuilder<OpStack>`
// impl, but only for the build half. Bodies are identical in shape to
// `op_alloy_network::Optimism`'s impl.
impl NetworkTransactionBuilder<OpStack> for OpTransactionRequest {
    fn can_submit(&self) -> bool {
        // from must be set; everything else may be filled in by the
        // RPC server on submission.
        self.as_ref().from.is_some()
    }

    fn can_build(&self) -> bool {
        let req = self.as_ref();
        let common = req.gas.is_some() && req.nonce.is_some();

        let legacy = req.gas_price.is_some();
        let eip2930 = legacy && req.access_list.is_some();
        let eip1559 = req.max_fee_per_gas.is_some() && req.max_priority_fee_per_gas.is_some();
        let eip4844 = eip1559 && req.sidecar.is_some() && req.to.is_some();
        let eip7702 = eip1559 && req.authorization_list.is_some();
        common && (legacy || eip2930 || eip1559 || eip4844 || eip7702)
    }

    fn complete_type(&self, ty: OpTxType) -> Result<(), Vec<&'static str>> {
        match ty {
            // Synthetic / non-user-buildable receipt types.
            OpTxType::Deposit => Err(vec!["not implemented for deposit tx"]),
            OpTxType::PostExec => Err(vec!["not implemented for post-exec tx"]),
            OpTxType::Legacy => self.as_ref().complete_legacy(),
            OpTxType::Eip2930 => self.as_ref().complete_2930(),
            OpTxType::Eip1559 => self.as_ref().complete_1559(),
            OpTxType::Eip7702 => self.as_ref().complete_7702(),
        }
    }

    #[doc(alias = "output_transaction_type")]
    fn output_tx_type(&self) -> OpTxType {
        match self.as_ref().preferred_type() {
            TxType::Eip1559 | TxType::Eip4844 => OpTxType::Eip1559,
            TxType::Eip2930 => OpTxType::Eip2930,
            TxType::Eip7702 => OpTxType::Eip7702,
            TxType::Legacy => OpTxType::Legacy,
        }
    }

    #[doc(alias = "output_transaction_type_checked")]
    fn output_tx_type_checked(&self) -> Option<OpTxType> {
        self.as_ref().buildable_type().map(|tx_ty| match tx_ty {
            TxType::Eip1559 | TxType::Eip4844 => OpTxType::Eip1559,
            TxType::Eip2930 => OpTxType::Eip2930,
            TxType::Eip7702 => OpTxType::Eip7702,
            TxType::Legacy => OpTxType::Legacy,
        })
    }

    fn prep_for_submission(&mut self) {
        let req = self.as_mut();
        req.transaction_type = Some(req.preferred_type() as u8);
        req.trim_conflicting_keys();
        req.populate_blob_hashes();
    }

    fn build_unsigned(self) -> BuildResult<OpTypedTransaction, OpStack> {
        if let Err((tx_type, missing)) = self.as_ref().missing_keys() {
            let tx_type = OpTxType::try_from(tx_type as u8).unwrap();
            return Err(
                TransactionBuilderError::InvalidTransactionRequest(tx_type, missing)
                    .into_unbuilt(self),
            );
        }
        Ok(self.build_typed_tx().expect("checked by missing_keys"))
    }

    async fn build<W: NetworkWallet<OpStack>>(
        self,
        wallet: &W,
    ) -> Result<<OpStack as Network>::TxEnvelope, TransactionBuilderError<OpStack>> {
        Ok(wallet.sign_request(self).await?)
    }
}
