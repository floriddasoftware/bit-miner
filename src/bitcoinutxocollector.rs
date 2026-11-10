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

        let inputs = transaction
            .input
            .iter()
            .map(|txin| {
                BitcoinInput::new(
                    manifold,
                    txin.previous_output,
                    txin.script_sig.clone(),
                    txin.witness.clone(),
                    txin.sequence,
                )
                .0
            })
            .collect::<Vec<BitcoinInput>>();

        let outputs = transaction
            .output
            .iter()
            .enumerate()
            .map(|(vout, txout)| {
                BitcoinOutput::new(
                    manifold,
                    vout as u32,
                    txout.value.to_sat(),
                    txout.script_pubkey.clone(),
                )
                .1
            })
            .collect::<Vec<BitcoinOutput>>();

        let payload = BitcoinPayload::new(inputs, outputs);
        let retained = payload.replay_mass(manifold);

        Ok((payload, retained))
    }
}