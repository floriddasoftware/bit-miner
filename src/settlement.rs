
use std::{
    error::Error,
    fmt,
    io,
};

use bitcoin::Transaction;
use qp_hd::{PQ44Event, QP44Event};
use quantom_value::QuantPerm;
use qtm_graph::graph::{
    frame::Direction,
    scale::Scale,
};

use crate::{
    bitcoinpayload::BitcoinPayload,
    bitcoinreceipt::SettlementReceipt,
    broadcast::FinalizedSettlement,
    observebitcointransaction::ObservedBitcoinTransaction,
    qpeventreceipt::EventReceipt,
    sharedtypes::SettlementState,
};

#[derive(Debug)]
pub enum SettlementError {
    Backend(String),
    Unsupported(&'static str),
    Io(io::Error),
}

impl fmt::Display for SettlementError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Backend(message) => write!(f, "{message}"),
            Self::Unsupported(message) => write!(f, "Unsupported: {message}"),
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
        let EventReceipt { event, locked, .. } = receipt;

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
    pub fn new(_substrate: impl Into<String>) -> Self {
        Self
    }

    #[inline(always)]
    pub fn confirmation_report(
        &self,
        txid: bitcoin::Txid,
        block_height: u64,
        confirmations: u64,
    ) {
        println!(
            "\n==============================================\n\
             BITCOIN CONFIRMATION UPDATE\n\
             ==============================================\n\
             Chain        : Bitcoin\n\
             TXID         : {txid}\n\
             Block Height : {block_height}\n\
             Confirmations: {confirmations}\n\
             Finality     : {}\n\
             ==============================================\n",
            if confirmations >= 6 {
                "CONFIRMED"
            } else {
                "WAITING"
            }
        );
    }

    #[inline(always)]
    pub fn process(
        &self,
        receipt: EventReceipt,
        bitcoin: BitcoinPayload,
        observed: ObservedBitcoinTransaction,
        submission: usize,
    ) -> Result<(FinalizedSettlement, BitcoinPayload), SettlementError> {
        let receipt = Self::settle(receipt)?;

        let SettlementReceipt {
            event,
            state,
            locked,
            script_pubkey,
        } = receipt;

        let PQ44Event {
            heritage,
            qtm,
            depth,
        } = event;

        let direction = Direction::from_heritage(heritage, Scale::Global);
        let manifold = &direction.heritage().state;

        let (transaction, payload) = Self::materialize(
            manifold,
            bitcoin,
            observed,
            &direction,
            submission,
        )?;

        let event = PQ44Event {
            heritage: direction.into_heritage(),
            qtm,
            depth,
        };

        let receipt = SettlementReceipt::new(
            event,
            state,
            locked,
            script_pubkey,
        );

        Ok((
            FinalizedSettlement::new(Some(receipt), transaction),
            payload,
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
        manifold: &QuantPerm,
        bitcoin: BitcoinPayload,
        observed: ObservedBitcoinTransaction,
        direction: &Direction,
        submission: usize,
    ) -> Result<(Transaction, BitcoinPayload), SettlementError> {
        if bitcoin.inputs.is_empty() {
            return Err(SettlementError::Unsupported(
                "cannot materialize a transaction without inputs",
            ));
        }

        let (materialized, payload) =
            ObservedBitcoinTransaction::new_transact(
                manifold,
                bitcoin,
                observed.source,
                direction,
                submission,
            )
            .map_err(|error| SettlementError::Backend(error.to_string()))?;

        Ok((materialized.transaction, payload))
    }
}
