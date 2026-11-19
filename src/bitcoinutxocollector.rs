
use bitcoin::{
    OutPoint,
    ScriptBuf,
    Sequence,
    Witness,
};

use qtm_graph::graph::frame::Depth;
use quantom_value::QuantPerm;

use crate::{
    miner::Miner,
    observebitcointransaction::ObservedBitcoinTransaction,
    qpoutputsbitcoininput::BitcoinInput,
    settlement::SettlementError,
};

pub struct BitcoinUTXOCollector;

impl BitcoinUTXOCollector {
    #[inline(always)]
    pub fn new() -> Self {
        Self
    }

    /// Capture spendable outpoints from an observed transaction as BitcoinInputs.
    /// Downstream architecture is responsible for constructing new outputs.
    #[inline(always)]
    pub fn collect(
        &mut self,
        observed: &ObservedBitcoinTransaction,
        manifold: &QuantPerm,
    ) -> Result<(Vec<BitcoinInput>, u128, Depth), SettlementError> {
        let transaction = &observed.transaction;

        if transaction.compute_txid() != observed.txid {
            return Err(SettlementError::Backend(
                "observed transaction txid does not match its transaction".into(),
            ));
        }

        let mut depth = Depth::new("qp/payload");

        let spendable = transaction
            .output
            .iter()
            .enumerate()
            .filter(|(_, output)| {
                output.value.to_sat() > 0
                    && !output.script_pubkey.is_op_return()
            })
            .collect::<Vec<_>>();

        if spendable.is_empty() {
            return Err(SettlementError::Unsupported(
                "observed transaction has no spendable outputs",
            ));
        }

        let inputs = spendable
            .iter()
            .enumerate()
            .map(|(position, (parent_vout, output))| {
                let previous_output = OutPoint {
                    txid: observed.txid,
                    vout: *parent_vout as u32,
                };

                let value = output.value.to_sat();

                let (retained, input) = BitcoinInput::new(
                    manifold,
                    previous_output,
                    value,
                    ScriptBuf::new(),
                    Witness::new(),
                    Sequence::MAX,
                );

                depth.insert("input", position, retained);
                input
            })
            .collect::<Vec<BitcoinInput>>();

        let retained_mass = Miner::deep(manifold, &depth).mass;

        Ok((inputs, retained_mass, depth))
    }
}
