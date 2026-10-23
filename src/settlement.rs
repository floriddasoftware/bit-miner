use bitcoin::{
    Amount,
    ScriptBuf,
    Sequence,
    Transaction,
    TxIn,
    TxOut,
    Version,
    absolute::LockTime,
};
use crate::bit_block::{
    derive_p2wpkh_witness,
    derive_script_pubkey,
};
#[derive(Debug)]
pub enum SettlementError {
    Backend(String),
    Unsupported(&'static str),
}
impl std::fmt::Display for SettlementError {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        match self {
            Self::Backend(msg) =>
                write!(f, "{msg}"),
            Self::Unsupported(msg) =>
                write!(f, "{msg}"),
        }
    }
}
impl std::error::Error for SettlementError {}
pub struct SettlementEngine;
impl SettlementEngine {
    // -------------------------------------------------------------------------
    // Settlement
    //
    // QP44Event
    //     ↓
    // balance
    //     ↓
    // PQ44Event
    //
    // The Bitcoin stealth topology is derived from the original QP44 event
    // coordinate at the settlement boundary.
    // -------------------------------------------------------------------------
    #[inline(always)]
    fn settle(
        receipt: EventReceipt,
    ) -> SettlementReceipt {
        let EventReceipt {
            event,
            state: _,
            locked,
            ..
        } = receipt;
        let script_pubkey =
            derive_script_pubkey(
                &event.qtm.coordinate,
            );
        SettlementReceipt::new(
            Self::balance(event),
            SettlementState::Success,
            locked,
            script_pubkey,
        )
    }
    // -------------------------------------------------------------------------
    // QP44 balance
    // -------------------------------------------------------------------------
    #[inline(always)]
    pub fn balance(
        event: QP44Event,
    ) -> PQ44Event {
        let activations =
            event
                .heritage
                .state
                .activations();
        PQ44Event::balance_until(
            event.heritage,
            &event.qtm,
            activations.saturating_sub(1),
        )
    }
    // -------------------------------------------------------------------------
    // Eviction / settlement
    // -------------------------------------------------------------------------
    #[inline(always)]
    pub fn evict(
        &self,
        receipt: EventReceipt,
    ) -> SettlementReceipt {
        Self::settle(receipt)
    }
    #[inline(always)]
    pub fn new(
        _substrate: impl Into<String>,
    ) -> Self {
        Self
    }
    // -------------------------------------------------------------------------
    // Bitcoin confirmation reporting
    // -------------------------------------------------------------------------
    #[inline(always)]
    pub fn confirmation_report(
        &self,
        txid: bitcoin::Txid,
        block_height: u64,
        confirmations: u64,
    ) {
        println!();
        println!("==============================================");
        println!("        🔗 BITCOIN CONFIRMATION UPDATE");
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
    // -------------------------------------------------------------------------
    // Process
    // -------------------------------------------------------------------------
    #[inline(always)]
    pub fn process(
        &self,
        receipt: EventReceipt,
        bitcoin: BitcoinPayload,
        parent: bitcoin::Transaction,
    ) -> Result<FinalizedSettlement, SettlementError> {
        let receipt =
            Self::settle(receipt);
        let transaction =
            Self::build_transaction(
                &receipt,
                bitcoin,
            )?;
        let vout =
            transaction
                .output
                .len()
                .saturating_sub(1)
                as u32;
        Ok(
            FinalizedSettlement::new(
                Some(receipt),
                parent,
                transaction,
                vout,
            )
        )
    }
    // -------------------------------------------------------------------------
    // Continue an existing settlement.
    // -------------------------------------------------------------------------
    #[inline(always)]
    pub fn proceed(
        &self,
        receipt: SettlementReceipt,
        bitcoin: BitcoinPayload,
        parent: bitcoin::Transaction,
    ) -> Result<FinalizedSettlement, SettlementError> {
        let transaction =
            Self::build_transaction(
                &receipt,
                bitcoin,
            )?;
        let vout =
            transaction
                .output
                .len()
                .saturating_sub(1)
                as u32;
        Ok(
            FinalizedSettlement::new(
                Some(receipt),
                parent,
                transaction,
                vout,
            )
        )
    }
    // -------------------------------------------------------------------------
    // Coinbase
    // -------------------------------------------------------------------------
    #[inline(always)]
    pub fn coinbase(
        &self,
        transaction: bitcoin::Transaction,
    ) -> FinalizedSettlement {
        FinalizedSettlement::new(
            None,
            transaction.clone(),
            transaction,
            0,
        )
    }
    // -------------------------------------------------------------------------
    // Bitcoin transaction construction
    // -------------------------------------------------------------------------
    #[inline(always)]
    fn build_transaction(
        receipt: &SettlementReceipt,
        bitcoin: BitcoinPayload,
    ) -> Result<Transaction, SettlementError> {
        match receipt.state {
            SettlementState::Success => {}
            SettlementState::Pending =>
                Err(
                    SettlementError::Unsupported(
                        "cannot build transaction from pending settlement",
                    )
                )?,
            SettlementState::Failed =>
                Err(
                    SettlementError::Unsupported(
                        "cannot build transaction from failed settlement",
                    )
                )?,
        }
        let manifold =
            &receipt
                .event
                .heritage
                .state;
        let fee =
            bitcoin
                .fee_mass(manifold)
                as u64;
        let last =
            bitcoin
                .outputs
                .len()
                .saturating_sub(1);
        // ---------------------------------------------------------------------
        // Unsigned transaction.
        // ---------------------------------------------------------------------
        let inputs =
            bitcoin
                .inputs
                .iter()
                .map(|input| {
                    TxIn {
                        previous_output:
                            input.previous_output,
                        script_sig:
                            input.script_sig.clone(),
                        sequence:
                            Sequence::MAX,
                        witness:
                            input.witness.clone(),
                    }
                })
                .collect::<Vec<_>>();
        let mut outputs =
            bitcoin
                .outputs
                .into_iter()
                .enumerate()
                .map(|(index, output)| {
                    let value =
                        if index == last {
                            output
                                .value
                                .saturating_sub(fee)
                        } else {
                            output.value
                        };
                    TxOut {
                        value:
                            Amount::from_sat(
                                value,
                            ),
                        script_pubkey:
                            output.script_pubkey,
                    }
                })
                .collect::<Vec<_>>();
        // ---------------------------------------------------------------------
        // The settlement output mirrors the QP44 stealth topology.
        //
        // QP44 coordinate
        //      ↓
        // derive_script_pubkey()
        //      ↓
        // P2WPKH scriptPubKey
        // ---------------------------------------------------------------------
        if let Some(output) =
            outputs.last_mut()
        {
            output.script_pubkey =
                receipt
                    .script_pubkey
                    .clone();
        }
        let mut transaction =
            Transaction {
                version:
                    Version(1),
                lock_time:
                    LockTime::ZERO,
                input:
                    inputs,
                output:
                    outputs,
            };
        // ---------------------------------------------------------------------
        // Sign the inputs after the complete transaction exists.
        //
        // derive_p2wpkh_witness() is the canonical signing helper from
        // bit_block.rs.
        //
        // The coordinate supplied here MUST be the coordinate controlling
        // the spent UTXO, not the newly created settlement coordinate.
        // ---------------------------------------------------------------------
        for (index, input) in bitcoin.inputs.iter().enumerate() {
            let coordinate =
                input
                    .coordinate
                    .ok_or(
                        SettlementError::Unsupported(
                            "P2WPKH input has no owning coordinate",
                        )
                    )?;
            transaction.input[index].witness =
                derive_p2wpkh_witness(
                    &coordinate,
                    &transaction,
                    index,
                    input.value,
                );
        }
        Ok(transaction)
    }
}

//The key correction from the previous version is that there is no duplicate derive_public_key, p2wpkh_script, or p2wpkh_witness implementation in SettlementEngine.

//The topology now has one canonical realization point:

//QP44Event
//   │
//   │ event.qtm.coordinate
//   ▼
//derive_script_pubkey()
//   │
//   ▼
//P2WPKH scriptPubKey
//   │
//   ▼
//SettlementReceipt
//   │
//   ▼
//Bitcoin settlement output

//And the spend path separately uses the existing canonical signer:

//spent UTXO
//   │
//   │ owning coordinate
//   ▼
//derive_p2wpkh_witness()
//   │
//   ▼
//Bitcoin input witness

//One compile-time dependency remains intentional: the BitcoinInput type needs coordinate and value for a real P2WPKH spend. If those fields aren’t currently present in your actual BitcoinInput, don’t duplicate signing logic to work around it—the correct update is to carry the UTXO’s ownership topology into BitcoinInput.