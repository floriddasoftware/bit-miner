
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

use quantom_value::{QuantPerm, Retain};

use crate::{
    bitcoinreceipt::SettlementReceipt,
    broadcast::{
        FinalizedSettlement,
        WirePacket,
    },
    qpoutputsbitcoininput::{
        BitcoinInput,
        SigningError,
    },
};

pub type BitcoinPsbtResult<T> =
    Result<T, Box<dyn Error + Send + Sync>>;

// -----------------------------------------------------------------------------
// Previous output resolution
// -----------------------------------------------------------------------------

/// The exact previous output identified by an input OutPoint.
///
/// The submission and commitment identify the parent QTM event
/// responsible for producing this output.
pub struct ResolvedPrevout {
    pub outpoint: OutPoint,
    pub output: TxOut,
    pub submission: u64,
    pub commitment: [u8; 32],
}

pub trait PrevoutResolver {
    /// Resolve an input against its actual parent submission.
    ///
    /// The resolver must use both values to locate the parent
    /// output in the specified substrate activation.
    fn resolve(
        &self,
        outpoint: &OutPoint,
        submission: u64,
        commitment: [u8; 32],
    ) -> BitcoinPsbtResult<ResolvedPrevout>;

    /// Resolve directly from the parent's persisted wire packet.
    ///
    /// The packet supplies the exact outpoint and commitment.
    /// The caller supplies the submission that owns that packet.
    #[inline]
    fn resolve_packet(
        &self,
        packet: &WirePacket,
        submission: u64,
    ) -> BitcoinPsbtResult<ResolvedPrevout> {
        let resolved = self.resolve(
            &packet.outpoint,
            submission,
            packet.commitment,
        )?;

        if resolved.outpoint != packet.outpoint {
            return Err(IoError::new(
                ErrorKind::InvalidData,
                format!(
                    "resolved parent outpoint mismatch: packet={} resolved={}",
                    packet.outpoint,
                    resolved.outpoint,
                ),
            )
            .into());
        }

        if resolved.submission != submission {
            return Err(IoError::new(
                ErrorKind::InvalidData,
                format!(
                    "resolved parent submission mismatch: expected={} resolved={}",
                    submission,
                    resolved.submission,
                ),
            )
            .into());
        }

        if resolved.commitment != packet.commitment {
            return Err(IoError::new(
                ErrorKind::InvalidData,
                format!(
                    "resolved parent commitment mismatch: submission={}",
                    submission,
                ),
            )
            .into());
        }

        if resolved.output.value.to_sat() != packet.value {
            return Err(IoError::new(
                ErrorKind::InvalidData,
                format!(
                    "resolved parent value mismatch: outpoint={} packet={} resolved={}",
                    packet.outpoint,
                    packet.value,
                    resolved.output.value.to_sat(),
                ),
            )
            .into());
        }

        if resolved.output.script_pubkey != packet.script_pubkey {
            return Err(IoError::new(
                ErrorKind::InvalidData,
                format!(
                    "resolved parent script mismatch: outpoint={}",
                    packet.outpoint,
                ),
            )
            .into());
        }

        Ok(resolved)
    }
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
    /// Resolved prevouts must correspond one-to-one with the unsigned
    /// transaction inputs in their original transaction order.
    ///
    /// Each previous output is attached exclusively through witness_utxo.
    #[inline]
    pub fn from_settlement(
        finalized: &FinalizedSettlement,
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
        }

        let unsigned_txid = transaction.compute_txid();

        // Preserve the exact finalized transaction.
        let mut psbt = Psbt::from_unsigned_tx(transaction)?;

        // Attach only the exact resolved previous outputs.
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

    /// Signs one PSBT input using the BitcoinInput signing implementation.
    ///
    /// The PSBT's unsigned transaction and witness_utxo provide the
    /// transaction and exact previous-output data used by sign().
    #[inline]
    pub fn sign_input(
        &mut self,
        input: &mut BitcoinInput,
        manifold: &QuantPerm,
        private_key: [u8; 32],
        input_index: usize,
    ) -> Result<Retain, SigningError> {
        let psbt_input = self
            .psbt
            .inputs
            .get(input_index)
            .ok_or(SigningError::InputIndexOutOfRange {
                index: input_index,
                input_count: self.psbt.inputs.len(),
            })?;

        let previous_output = psbt_input
            .witness_utxo
            .as_ref()
            .ok_or(SigningError::PreviousOutputMismatch {
                index: input_index,
            })?;

        let transaction = self.psbt.unsigned_tx.clone();

        let retained = input.sign(
            manifold,
            private_key,
            &transaction,
            input_index,
            previous_output.value.to_sat(),
            &previous_output.script_pubkey,
        )?;

        self.psbt.inputs[input_index].final_script_witness =
            Some(input.witness.clone());

        Ok(retained)
    }

    #[inline(always)]
    pub fn unsigned_txid(&self) -> Txid {
        self.unsigned_txid
    }
}
