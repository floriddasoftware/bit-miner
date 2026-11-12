
use std::{
    collections::HashSet,
    error::Error,
    io::{Error as IoError, ErrorKind},
};

use bitcoin::{
    psbt::Psbt,
    OutPoint,
    Transaction,
    TxOut,
    Txid,
};

use crate::{
    broadcast::{
        FinalizedSettlement,
        WirePacket,
    },
};

pub type BitcoinPsbtResult<T> =
    Result<T, Box<dyn Error + Send + Sync>>;

pub struct ResolvedPrevout {
    pub outpoint: OutPoint,
    pub output: TxOut,
    pub submission: u64,
    pub commitment: [u8; 32],
}

pub trait PrevoutResolver {
    fn resolve(
        &self,
        outpoint: &OutPoint,
        submission: u64,
        commitment: [u8; 32],
    ) -> BitcoinPsbtResult<ResolvedPrevout>;

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

        let invalid = |message: String| {
            IoError::new(ErrorKind::InvalidData, message)
        };

        if resolved.outpoint != packet.outpoint {
            return Err(invalid(format!(
                "parent outpoint mismatch: expected={} resolved={}",
                packet.outpoint, resolved.outpoint,
            )).into());
        }

        if resolved.submission != submission {
            return Err(invalid(format!(
                "parent submission mismatch: expected={} resolved={}",
                submission, resolved.submission,
            )).into());
        }

        if resolved.commitment != packet.commitment {
            return Err(invalid(format!(
                "parent commitment mismatch: submission={}",
                submission,
            )).into());
        }

        if resolved.output.value.to_sat() != packet.value {
            return Err(invalid(format!(
                "parent value mismatch: outpoint={} expected={} resolved={}",
                packet.outpoint,
                packet.value,
                resolved.output.value.to_sat(),
            )).into());
        }

        if resolved.output.script_pubkey != packet.script_pubkey {
            return Err(invalid(format!(
                "parent script mismatch: outpoint={}",
                packet.outpoint,
            )).into());
        }

        Ok(resolved)
    }
}

pub struct BitcoinPsbt {
    pub psbt: Psbt,
    pub unsigned_txid: Txid,
}

impl BitcoinPsbt {
    #[inline]
    pub fn from_settlement(
        finalized: &FinalizedSettlement,
        resolved_prevouts: Vec<ResolvedPrevout>,
    ) -> BitcoinPsbtResult<Self> {
        let transaction = &finalized.transaction;

        if transaction.input.len() != resolved_prevouts.len() {
            return Err(IoError::new(
                ErrorKind::InvalidData,
                format!(
                    "prevout count mismatch: inputs={} resolved={}",
                    transaction.input.len(),
                    resolved_prevouts.len(),
                ),
            ).into());
        }

        let mut seen = HashSet::with_capacity(
            transaction.input.len(),
        );

        for (index, (input, prevout)) in transaction
            .input
            .iter()
            .zip(&resolved_prevouts)
            .enumerate()
        {
            if input.previous_output != prevout.outpoint {
                return Err(IoError::new(
                    ErrorKind::InvalidData,
                    format!(
                        "prevout order mismatch at input {}: expected={} resolved={}",
                        index,
                        input.previous_output,
                        prevout.outpoint,
                    ),
                ).into());
            }

            if !seen.insert(prevout.outpoint) {
                return Err(IoError::new(
                    ErrorKind::InvalidData,
                    format!(
                        "duplicate input outpoint: {}",
                        prevout.outpoint,
                    ),
                ).into());
            }
        }

        let unsigned_txid = transaction.compute_txid();
        let mut psbt = Psbt::from_unsigned_tx(transaction.clone())?;

        for (input, prevout) in psbt
            .inputs
            .iter_mut()
            .zip(resolved_prevouts)
        {
            input.witness_utxo = Some(prevout.output);
        }

        Ok(Self {
            psbt,
            unsigned_txid,
        })
    }

    #[inline(always)]
    pub fn unsigned_txid(&self) -> Txid {
        self.unsigned_txid
    }
}
