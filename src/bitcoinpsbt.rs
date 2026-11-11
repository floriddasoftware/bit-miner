use std::{
    collections::HashSet,
    error::Error,
    io::{Error as IoError, ErrorKind},
};

use bitcoin::{
    OutPoint,
    Transaction,
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
    pub input: TxIn,
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