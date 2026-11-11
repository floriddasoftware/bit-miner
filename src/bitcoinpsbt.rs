
use std::{
    collections::HashSet,
    error::Error,
    io::{Error as IoError, ErrorKind},
};

use bitcoin::{
    OutPoint,
    Transaction,
    TxOut,
    Txid,
    psbt::Psbt,
};

use crate::{
    bitcoinreceipt::SettlementReceipt,
    broadcast::FinalizedSettlement,
};

pub type BitcoinPsbtResult<T> =
    Result<T, Box<dyn Error + Send + Sync>>;

// -----------------------------------------------------------------------------
// Previous output resolution
// -----------------------------------------------------------------------------

/// The exact previous output identified by an input OutPoint.
///
/// The resolver supplies the TxOut corresponding to the identified
/// transaction ID and output index.
///
/// Only witness_utxo is used during PSBT construction.
pub struct ResolvedPrevout {
    pub outpoint: OutPoint,
    pub output: TxOut,
}

pub trait PrevoutResolver {
    fn resolve(
        &self,
        outpoint: &OutPoint,
    ) -> BitcoinPsbtResult<ResolvedPrevout>;
}

// -----------------------------------------------------------------------------
// Signed transaction
// -----------------------------------------------------------------------------

pub struct SignedBitcoinTransaction {
    pub receipt: Option<SettlementReceipt>,
    pub transaction: Transaction,
}

impl SignedBitcoinTransaction {
    #[inline(always)]
    pub fn txid(&self) -> Txid {
        self.transaction.compute_txid()
    }

    #[inline(always)]
    pub fn into_transaction(self) -> Transaction {
        self.transaction
    }
}

// -----------------------------------------------------------------------------
// Bitcoin PSBT
// -----------------------------------------------------------------------------

pub struct BitcoinPsbt {
    pub receipt: Option<SettlementReceipt>,
    pub psbt: Psbt,
    pub unsigned_txid: Txid,
}

impl BitcoinPsbt {
    /// Constructs a PSBT from the exact finalized settlement transaction
    /// and its resolved previous outputs.
    ///
    /// The resolved collection must correspond one-to-one with the
    /// unsigned transaction inputs, in their original transaction order.
    ///
    /// Each previous output is attached exclusively through witness_utxo.
    /// No previous transaction is fetched, reconstructed, or attached.
    #[inline]
    pub fn from_settlement(
        finalized: FinalizedSettlement,
        resolved_prevouts: Vec<ResolvedPrevout>,
    ) -> BitcoinPsbtResult<Self> {
        let FinalizedSettlement {
            receipt,
            transaction,
        } = finalized;

        if transaction.input.len() != resolved_prevouts.len() {
            return Err(IoError::new(
                ErrorKind::InvalidData,
                format!(
                    "resolved prevout count does not match settlement inputs: inputs={} resolved={}",
                    transaction.input.len(),
                    resolved_prevouts.len(),
                ),
            )
            .into());
        }

        let mut seen = HashSet::with_capacity(
            transaction.input.len(),
        );

        // Validate exact outpoint identity and transaction order.
        for (index, (tx_input, prevout)) in transaction
            .input
            .iter()
            .zip(resolved_prevouts.iter())
            .enumerate()
        {
            if tx_input.previous_output != prevout.outpoint {
                return Err(IoError::new(
                    ErrorKind::InvalidData,
                    format!(
                        "resolved prevout order mismatch at input {}: expected={} resolved={}",
                        index,
                        tx_input.previous_output,
                        prevout.outpoint,
                    ),
                )
                .into());
            }

            if !seen.insert(prevout.outpoint) {
                return Err(IoError::new(
                    ErrorKind::InvalidData,
                    format!(
                        "duplicate settlement input outpoint: {}",
                        prevout.outpoint,
                    ),
                )
                .into());
            }
        }

        // Preserve the exact finalized transaction.
        // Inputs and outputs are not reconstructed or replaced.
        let unsigned_txid = transaction.compute_txid();

        let mut psbt = Psbt::from_unsigned_tx(transaction)?;

        // Attach the exact resolved previous output to its
        // corresponding input using witness_utxo exclusively.
        for (psbt_input, prevout) in psbt
            .inputs
            .iter_mut()
            .zip(resolved_prevouts.into_iter())
        {
            psbt_input.witness_utxo = Some(prevout.output);
        }

        Ok(Self {
            receipt,
            psbt,
            unsigned_txid,
        })
    }

    #[inline(always)]
    pub fn unsigned_txid(&self) -> Txid {
        self.unsigned_txid
    }
}
