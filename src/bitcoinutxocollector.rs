
use bitcoin::{
    OutPoint,
    ScriptBuf,
    Sequence,
    Witness,
};

use qtm_graph::graph::frame::{
    Depth,
    Direction,
};

use crate::{
    bitcoinoutputqpinput::BitcoinOutput,
    bitcoinpayload::BitcoinPayload,
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

    #[inline(always)]
    pub fn collect(
        &mut self,
        observed: &ObservedBitcoinTransaction,
        manifold: &quantom_value::QuantPerm,
    ) -> Result<(BitcoinPayload, u128), SettlementError> {
        let transaction = &observed.transaction;

        if transaction.compute_txid() != observed.txid {
            return Err(SettlementError::Backend(
                "observed transaction txid does not match its transaction"
                    .into(),
            ));
        }

        let mut depth = Depth::new("qp/payload");

        let spendable_outputs = transaction
            .output
            .iter()
            .enumerate()
            .filter(|(_, output)| {
                output.value.to_sat() > 0
                    && !output.script_pubkey.is_op_return()
            })
            .collect::<Vec<_>>();

        if spendable_outputs.is_empty() {
            return Err(SettlementError::Unsupported(
                "observed transaction has no spendable outputs",
            ));
        }

        // The settlement is a child transaction.
        // Its inputs reference the observed parent's outputs,
        // not the inputs previously consumed by the parent.
        let inputs = spendable_outputs
            .iter()
            .enumerate()
            .map(|(position, (parent_vout, _))| {
                let previous_output = OutPoint {
                    txid: observed.txid,
                    vout: *parent_vout as u32,
                };

                let (retained, input) = BitcoinInput::new(
                    manifold,
                    previous_output,
                    ScriptBuf::new(),
                    Witness::new(),
                    Sequence::MAX,
                );

                depth.insert(
                    "input",
                    position,
                    retained,
                );

                input
            })
            .collect::<Vec<BitcoinInput>>();

        let output_offset = inputs.len();

        // Materialize the child transaction's outputs in
        // contiguous vout order.
        let outputs = spendable_outputs
            .iter()
            .enumerate()
            .map(|(position, (_, txout))| {
                let (output, retained) = BitcoinOutput::new(
                    manifold,
                    position as u32,
                    txout.value.to_sat(),
                    txout.script_pubkey.clone(),
                );

                depth.insert(
                    "output",
                    output_offset + position,
                    retained,
                );

                output
            })
            .collect::<Vec<BitcoinOutput>>();

        let retained_mass =
            Direction::realize_depth(manifold, &depth).mass;

        let payload = BitcoinPayload::new(
            inputs,
            outputs,
        );

        Ok((payload, retained_mass))
    }
}
