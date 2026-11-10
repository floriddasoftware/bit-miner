
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
        let mut depth = Depth::new("qp/payload");

        let inputs = transaction
            .input
            .iter()
            .enumerate()
            .map(|(position, txin)| {
                let (retained, input) = BitcoinInput::new(
                    manifold,
                    txin.previous_output,
                    txin.script_sig.clone(),
                    txin.witness.clone(),
                    txin.sequence,
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

        let outputs = transaction
            .output
            .iter()
            .enumerate()
            .map(|(position, txout)| {
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
