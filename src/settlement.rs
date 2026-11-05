
use bitcoin::Transaction;
use quantom_value::QuantPerm;

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
                .map_err(|error| {
                    SettlementError::Backend(error.to_string())
                })?;

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
        println!("Confirmations: {}", confirmations);
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
    mut bitcoin: BitcoinPayload,
    observed: ObservedBitcoinTransaction,
) -> Result<FinalizedSettlement, SettlementError> {
    let manifold = &receipt.event.heritage.state;
    let private_key: [u8; 32] = receipt.event.qtm.coordinate;

    // Every observed parent output must have exactly one child input.
    if bitcoin.inputs.len() != observed.transaction.output.len() {
        return Err(SettlementError::Backend(format!(
            "Bitcoin payload input count does not match observed parent output count: payload={} parent={}",
            bitcoin.inputs.len(),
            observed.transaction.output.len(),
        )));
    }

    // Construct the child transaction only after topology validation.
    let mut transaction = Self::materialize(
        manifold,
        bitcoin.clone(),
        observed.clone(),
    )?;

    // Validate and sign each child input against its corresponding parent output.
    for (index, input) in bitcoin.inputs.iter_mut().enumerate() {
        if input.previous_output.txid != observed.txid {
            return Err(SettlementError::Backend(format!(
                "input {} references unexpected parent transaction",
                index,
            )));
        }

        if input.previous_output.vout != index as u32 {
            return Err(SettlementError::Backend(format!(
                "input ordering mismatch at index {}: expected vout={} actual vout={}",
                index,
                index,
                input.previous_output.vout,
            )));
        }

        let previous_output = observed
            .transaction
            .output
            .get(input.previous_output.vout as usize)
            .ok_or_else(|| {
                SettlementError::Backend(format!(
                    "previous output not found for input {}: vout={}",
                    index,
                    input.previous_output.vout,
                ))
            })?;

        input
            .sign(
                manifold,
                private_key,
                &transaction,
                index,
                previous_output.value.to_sat(),
                &previous_output.script_pubkey,
            )
            .map_err(|error| {
                SettlementError::Backend(error.to_string())
            })?;

        transaction.input[index].witness = input.witness.clone();
    }

    let receipt = Self::settle(receipt)?;

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
        let manifold = &receipt.event.heritage.state;

        let transaction = Self::materialize(
            manifold,
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
        manifold: &QuantPerm,
        bitcoin: BitcoinPayload,
        observed: ObservedBitcoinTransaction,
    ) -> Result<Transaction, SettlementError> {
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
                manifold,
                bitcoin,
                observed.source,
            )
            .map_err(|error| {
                SettlementError::Backend(error.to_string())
            })?;

        Ok(observed_transaction.transaction)
    }
}
