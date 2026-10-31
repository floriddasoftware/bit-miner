
use bitcoin::Transaction;

use crate::bitcoinpayload::BitcoinPayload;
use crate::bitcoinreceipt::SettlementReceipt;
use crate::broadcast::FinalizedSettlement;
use crate::observebitcointransaction::ObservedBitcoinTransaction;
use crate::qpeventreceipt::EventReceipt;
use crate::sharedtypes::SettlementState;

use qp_hd::qp44::{PQ44Event, QP44Event};

use std::{
    error::Error,
    fmt,
    io,
};

#[derive(Debug)]
pub enum SettlementError {
    Backend(String),
    Unsupported(&'static str),
    Io(io::Error),
}

impl fmt::Display for SettlementError {
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        match self {
            Self::Backend(msg) => write!(f, "{msg}"),
            Self::Unsupported(msg) => write!(f, "{msg}"),
            Self::Io(error) => write!(f, "{error}"),
        }
    }
}

impl Error for SettlementError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<io::Error> for SettlementError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

pub struct SettlementEngine;

impl SettlementEngine {
    #[inline(always)]
    fn settle(
        receipt: EventReceipt,
    ) -> Result<SettlementReceipt, SettlementError> {
        let EventReceipt {
            event,
            locked,
            ..
        } = receipt;

        let script_pubkey =
            crate::bit_block::derive_script_pubkey(event.qtm.coordinate)
                .map_err(|error| SettlementError::Backend(error.to_string()))?;

        Ok(SettlementReceipt::new(
            Self::balance(event),
            SettlementState::Success,
            locked,
            script_pubkey,
        ))
    }

    #[inline(always)]
    pub fn balance(event: QP44Event) -> PQ44Event {
        let activation = event
            .heritage
            .state
            .activations()
            .saturating_sub(1);

        PQ44Event::balance_until(
            event.heritage,
            &event.qtm,
            event.depth,
            activation,
        )
    }

    #[inline(always)]
    pub fn evict(
        &self,
        receipt: EventReceipt,
    ) -> Result<SettlementReceipt, SettlementError> {
        Self::settle(receipt)
    }

    #[inline(always)]
    pub fn new(
        _substrate: impl Into<String>,
    ) -> Self {
        Self
    }

    #[inline(always)]
    pub fn confirmation_report(
        &self,
        txid: bitcoin::Txid,
        block_height: u64,
        confirmations: u64,
    ) {
        println!();
        println!("==============================================");
        println!("        BITCOIN CONFIRMATION UPDATE");
        println!("==============================================");
        println!("Chain        : Bitcoin");
        println!("TXID         : {}", txid);
        println!("Block Height : {}", block_height);
        println!(
            "Confirmations: {}",
            confirmations
        );
        println!(
            "Finality     : {}",
            if confirmations >= 6 {
                "CONFIRMED"
            } else {
                "WAITING"
            }
        );
        println!("==============================================");
        println!();
    }

    #[inline(always)]
    pub fn process(
        &self,
        receipt: EventReceipt,
        bitcoin: BitcoinPayload,
        observed: ObservedBitcoinTransaction,
    ) -> Result<FinalizedSettlement, SettlementError> {
        let receipt = Self::settle(receipt)?;

        let transaction = Self::materialize(
            &receipt,
            bitcoin,
            observed,
        )?;

        Ok(FinalizedSettlement::new(
            Some(receipt),
            transaction,
        ))
    }

    #[inline(always)]
    pub fn proceed(
        &self,
        receipt: SettlementReceipt,
        bitcoin: BitcoinPayload,
        observed: ObservedBitcoinTransaction,
    ) -> Result<FinalizedSettlement, SettlementError> {
        let transaction = Self::materialize(
            &receipt,
            bitcoin,
            observed,
        )?;

        Ok(FinalizedSettlement::new(
            Some(receipt),
            transaction,
        ))
    }

    #[inline(always)]
    pub fn coinbase(
        &self,
        transaction: Transaction,
    ) -> FinalizedSettlement {
        FinalizedSettlement::new(None, transaction)
    }

    #[inline(always)]
    fn materialize(
        receipt: &SettlementReceipt,
        bitcoin: BitcoinPayload,
        observed: ObservedBitcoinTransaction,
    ) -> Result<Transaction, SettlementError> {
        match receipt.state {
            SettlementState::Success => {}
            SettlementState::Pending => {
                return Err(SettlementError::Unsupported(
                    "cannot materialize a pending settlement",
                ));
            }
            SettlementState::Failed => {
                return Err(SettlementError::Unsupported(
                    "cannot materialize a failed settlement",
                ));
            }
        }

        if bitcoin.outputs.is_empty() {
            return Err(SettlementError::Unsupported(
                "cannot materialize a transaction without outputs",
            ));
        }

        if bitcoin.inputs.is_empty() {
            return Err(SettlementError::Unsupported(
                "cannot materialize a transaction without inputs",
            ));
        }

        let observed_transaction =
            ObservedBitcoinTransaction::new_transact(
                &receipt.event.heritage.state,
                bitcoin,
                observed.source,
            )
            .map_err(|error| {
                SettlementError::Backend(error.to_string())
            })?;

        Ok(observed_transaction.transaction)
    }
}
