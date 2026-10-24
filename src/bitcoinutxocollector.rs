use qp_hd::{
    qp44::QP44Event,
};
use crate::observebitcointransaction::ObserveditcoinTransaction;


pub struct BitcoinUTXOCollector {
    pub payload: BitcoinPayload,
}

pub struct BitcoinUTXOCollector {
    pub payload: BitcoinPayload,
}

impl BitcoinUTXOCollector {
    pub fn new(payload: BitcoinPayload) -> Self {
        Self { payload }
    }

    #[inline(always)]
    pub fn collect(
        &mut self,
        observed: &ObservedBitcoinTransaction,
        event: &EventReceipt,
    ) -> Result<(BitcoinPayload, Vec<Retain>), SettlementError> {
        let tx = &observed.transaction;
        let manifold = &event.event.heritage.state;
        let mut retained = Vec::new();

        let outputs = tx
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
            .collect::<Vec<_>>();

        let inputs = tx
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
            .collect::<Vec<_>>();

        // Preserve the existing QP44 execution topology.
        for (index, input) in inputs.iter().enumerate() {
            let expected = bitcoin::OutPoint {
                txid: observed.txid,
                vout: index as u32,
            };

            assert_eq!(
                input.previous_output,
                expected,
                "QP44 execution topology changed at vout={}: expected={}:{} payload={}:{}",
                index,
                expected.txid,
                expected.vout,
                input.previous_output.txid,
                input.previous_output.vout,
            );
        }

        let payload = BitcoinPayload::new(observed.txid, inputs, outputs);
        self.payload = payload;

        Ok((self.payload.clone(), retained))
    }
}