use crate::{
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
        .output
        .iter()
        .enumerate()
        .map(|(vout, _)| {
            BitcoinInput::new(
                manifold,
                bitcoin::OutPoint {
                    txid: observed.txid,
                    vout: vout as u32,
                },
                bitcoin::ScriptBuf::new(),
                bitcoin::Witness::default(),
                bitcoin::Sequence::MAX,
            )
            .0
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
    let payload = BitcoinPayload::new(inputs, Vec::new());
    let retained = payload.replay_mass(manifold);
    Ok((payload, retained))
}

}