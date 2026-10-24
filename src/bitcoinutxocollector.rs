use qp_hd::{
    qp44::QP44Event,
};
use crate::observebitcointransaction::ObserveditcoinTransaction;


pub struct BitcoinUTXOCollector;
    
    impl BitcoinUTXOCollector {
    #[inline(always)]
    pub fn new() -> Self {
        Self
    }

    #[inline(always)]
    pub fn collect<F>(
        &mut self,
        substrate: &str,
        transactions: &[ObservedBitcoinTransaction],
        mut submit: F,
    ) -> Vec<QP44Event>
    where
        F: FnMut(u128) -> EventReceipt,
    {
        let mut pending = Vec::new();

        for observed in transactions {
            println!(
                "[UTXO] substrate={} txid={} outputs={}",
                substrate,
                observed.txid,
                observed.outputs.len(),
            );

            for (position, output) in observed.outputs.iter().enumerate() {
                let utxo = BitcoinUTXO {
                    outpoint: bitcoin::OutPoint {
                        txid: observed.txid,
                        vout: output.vout,
                    },
                    value: output.value,
                    confirmations: 0,
                    coinbase: false,
                    script_pubkey: output.script_pubkey.clone(),
                    state: UTXOState::Unspent,
                };

                let Payload {
                    realized,
                    bitcoin,
                    ..
                } = utxo.payload();

                let receipt = submit(realized);

                pending.push(PendingQPEvent {
                    receipt,
                    bitcoin,
                    position,
                });
            }
        }

        pending
    }
}