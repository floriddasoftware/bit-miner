use quantom_value::Retain;

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
    ) -> Result<(BitcoinPayload, Vec<Retain>), SettlementError> {
        let transaction = &observed.transaction;
        let mut retained = Vec::new();

        let outputs = transaction
            .output
            .iter()
            .enumerate()
            .map(|(vout, output)| {
                let (bitcoin, retain) = BitcoinOutput::new(
                    manifold,
                    vout as u32,
                    output.value.to_sat(),
                    output.script_pubkey.clone(),
                );

                retained.push(retain);
                bitcoin
            })
            .collect::<Vec<BitcoinOutput>>();

        let inputs = transaction
            .output
            .iter()
            .enumerate()
            .map(|(vout, _)| {
                let (input, retain) = BitcoinInput::new(
                    manifold,
                    bitcoin::OutPoint {
                        txid: observed.txid,
                        vout: vout as u32,
                    },
                    bitcoin::ScriptBuf::new(),
                    bitcoin::Witness::default(),
                    bitcoin::Sequence::MAX,
                );

                retained.push(retain);
                input
            })
            .collect::<Vec<BitcoinInput>>();

        for (index, input) in inputs.iter().enumerate() {
            let expected = bitcoin::OutPoint {
                txid: observed.txid,
                vout: index as u32,
            };

            assert_eq!(
                input.previous_output,
                expected,
                "QP44 execution topology changed at vout={}: \
                 expected={}:{} payload={}:{}",
                index,
                expected.txid,
                expected.vout,
                input.previous_output.txid,
                input.previous_output.vout,
            );
        }

        Ok((BitcoinPayload::new(inputs, outputs), retained))
    }
}