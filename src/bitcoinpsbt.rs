
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

type BitcoinPsbtResult<T> =
    Result<T, Box<dyn Error + Send + Sync>>;

// -----------------------------------------------------------------------------
// Previous output resolution
// -----------------------------------------------------------------------------

pub struct ResolvedPrevout {
    pub output: TxOut,
    pub previous_transaction: Option<Transaction>,
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
    // -------------------------------------------------------------------------
    // Construct PSBT from the existing settlement transaction
    // -------------------------------------------------------------------------

    pub fn from_settlement<R: PrevoutResolver>(
        settlement: FinalizedSettlement,
        resolver: &R,
    ) -> BitcoinPsbtResult<Self> {
        let FinalizedSettlement {
            receipt,
            transaction,
        } = settlement;

        if transaction.input.is_empty() {
            return Err(IoError::new(
                ErrorKind::InvalidInput,
                "cannot prepare PSBT without transaction inputs",
            ).into());
        }

        let mut seen = HashSet::new();

        for input in &transaction.input {
            if input.previous_output.is_null() {
                return Err(IoError::new(
                    ErrorKind::InvalidInput,
                    "coinbase inputs cannot be prepared as ordinary PSBT inputs",
                ).into());
            }

            if !input.script_sig.is_empty()
                || !input.witness.is_empty()
            {
                return Err(IoError::new(
                    ErrorKind::InvalidInput,
                    "settlement transaction must be unsigned before PSBT preparation",
                ).into());
            }

            if !seen.insert(input.previous_output) {
                return Err(IoError::new(
                    ErrorKind::InvalidData,
                    format!(
                        "duplicate transaction input: {}",
                        input.previous_output,
                    ),
                ).into());
            }
        }

        let unsigned_txid = transaction.compute_txid();

        let mut psbt = Psbt::from_unsigned_tx(transaction)?;

        for (index, txin) in
            psbt.unsigned_tx.input.iter().enumerate()
        {
            let outpoint = txin.previous_output;
            let resolved = resolver.resolve(&outpoint)?;

            // Validate an optional full previous transaction.
            if let Some(previous_transaction) =
                &resolved.previous_transaction
            {
                if previous_transaction.compute_txid()
                    != outpoint.txid
                {
                    return Err(IoError::new(
                        ErrorKind::InvalidData,
                        format!(
                            "previous transaction txid mismatch for input {}",
                            index,
                        ),
                    ).into());
                }

                let previous_output = previous_transaction
                    .output
                    .get(outpoint.vout as usize)
                    .ok_or_else(|| {
                        IoError::new(
                            ErrorKind::InvalidData,
                            format!(
                                "previous transaction has no vout {}",
                                outpoint.vout,
                            ),
                        )
                    })?;

                if previous_output != &resolved.output {
                    return Err(IoError::new(
                        ErrorKind::InvalidData,
                        format!(
                            "resolved previous output mismatch for input {}",
                            index,
                        ),
                    ).into());
                }
            } else if !resolved.output.script_pubkey
                .is_witness_program()
            {
                return Err(IoError::new(
                    ErrorKind::InvalidData,
                    format!(
                        "non-witness input {} requires its full previous transaction",
                        index,
                    ),
                ).into());
            }

            let psbt_input = &mut psbt.inputs[index];

            psbt_input.witness_utxo =
                Some(resolved.output);

            if let Some(previous_transaction) =
                resolved.previous_transaction
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

    // -------------------------------------------------------------------------
    // Accept the externally signed PSBT
    // -------------------------------------------------------------------------

    pub fn accept_signed_psbt(
        &mut self,
        signed: Psbt,
    ) -> BitcoinPsbtResult<()> {
        if signed.unsigned_tx != self.psbt.unsigned_tx {
            return Err(IoError::new(
                ErrorKind::InvalidData,
                "signed PSBT does not match the original settlement transaction",
            ).into());
        }

        self.psbt = signed;

        Ok(())
    }

    // -------------------------------------------------------------------------
    // Extract the finalized Bitcoin transaction
    // -------------------------------------------------------------------------

    pub fn finalize(
        self,
    ) -> BitcoinPsbtResult<SignedBitcoinTransaction> {
        let transaction = self
            .psbt
            .extract_tx()
            .map_err(|error| {
                IoError::new(
                    ErrorKind::InvalidData,
                    format!(
                        "PSBT transaction extraction failed: {}",
                        error,
                    ),
                )
            })?;

        Ok(SignedBitcoinTransaction {
            receipt: self.receipt,
            transaction,
        })
    }

    #[inline(always)]
    pub fn unsigned_txid(&self) -> Txid {
        self.unsigned_txid
    }

    #[inline(always)]
    pub fn into_psbt(self) -> Psbt {
        self.psbt
    }
}
