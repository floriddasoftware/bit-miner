
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

/// The previous output spent by one input of the unsigned settlement.
///
/// `outpoint` identifies the exact input being resolved.
/// `output` supplies the UTXO data required by PSBT construction.
/// `non_witness_utxo` is retained when the resolver has the complete
/// previous transaction, allowing legacy inputs to be signed safely.
pub struct ResolvedPrevout {
    pub outpoint: OutPoint,
    pub output: TxOut,
    pub non_witness_utxo: Option<Transaction>,
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

            if let Some(previous_transaction) =
                &prevout.non_witness_utxo
            {
                let previous_txid =
                    previous_transaction.compute_txid();

                if previous_txid != prevout.outpoint.txid {
                    return Err(IoError::new(
                        ErrorKind::InvalidData,
                        format!(
                            "previous transaction txid mismatch at input {}: expected={} resolved={}",
                            index,
                            prevout.outpoint.txid,
                            previous_txid,
                        ),
                    )
                    .into());
                }

                let previous_output = previous_transaction
                    .output
                    .get(prevout.outpoint.vout as usize)
                    .ok_or_else(|| {
                        IoError::new(
                            ErrorKind::InvalidData,
                            format!(
                                "previous output vout does not exist at input {}: txid={} vout={}",
                                index,
                                prevout.outpoint.txid,
                                prevout.outpoint.vout,
                            ),
                        )
                    })?;

                if previous_output != &prevout.output {
                    return Err(IoError::new(
                        ErrorKind::InvalidData,
                        format!(
                            "resolved previous output does not match previous transaction at input {}",
                            index,
                        ),
                    )
                    .into());
                }
            }
        }

        // Build the PSBT from the original unsigned transaction.
        // Its inputs are never reconstructed or replaced.
        let unsigned_txid = transaction.compute_txid();

        let mut psbt = Psbt::from_unsigned_tx(transaction)?;

        // Attach each resolved previous output to its corresponding
        // PSBT input without changing unsigned transaction input order.
        for (psbt_input, prevout) in psbt
            .inputs
            .iter_mut()
            .zip(resolved_prevouts.into_iter())
        {
            psbt_input.witness_utxo = Some(prevout.output);

            if let Some(previous_transaction) =
                prevout.non_witness_utxo
            {
                psbt_input.non_witness_utxo =
                    Some(previous_transaction);
            }
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
