use bitcoin::Txid;
use quantom_value::QuantPerm;
use qp_hd::protocolvalue::Qtm;

use crate::bitcoinpayload::BitcoinPayload;
use crate::observestream::{
BitcoinObservation,
BitcoinObservationSource,
};

type ObservationResult =
Result<T, Box<dyn std::error::Error + Send + Sync>>;

// —————————————————————————–
// Canonical observed Bitcoin transaction.
// Can originate from:
//   - network observation
//   - Bitcoin Core getblocktemplate()
//
// Preserves the decoded transaction together with all template metadata.
// NOTE: getblocktemplate() does NOT provide referenced UTXOs.
// —————————————————————————–
#[derive(Clone, Debug)]
pub struct ObservedBitcoinTransaction {
pub observation: Option,
pub source: BitcoinObservationSource,
pub txid: Txid,
pub transaction: bitcoin::Transaction,
pub coinbase: bool,
/// Fee from block template; None if only network observed.
pub fee: Option,
/// Weight from block template; None if only network observed.
pub weight: Option,
/// Zero-based indexes into template tx list (Bitcoin Core supplies 1-based).
pub depends: Option<Vec>,
/// Original zero-based position in template; None if only network observed.
pub template_index: Option,
}

impl ObservedBitcoinTransaction {
#[inline(always)]
pub fn is_coinbase(transaction: &bitcoin::Transaction) -> bool {
transaction.input.len() == 1
&& transaction.input[0].previous_output.is_null()
}

#[inline(always)]
pub fn coinbase(&self) -> bool {
    self.coinbase
}
#[inline(always)]
pub fn from_transaction(
    transaction: bitcoin::Transaction,
    source: BitcoinObservationSource,
) -> Self {
    let txid = transaction.compute_txid();
    let coinbase = Self::is_coinbase(&transaction);
    Self {
        observation: None,
        source,
        txid,
        transaction,
        coinbase,
        fee: None,
        weight: None,
        depends: None,
        template_index: None,
    }
}
#[inline(always)]
pub fn new_transact(
    manifold: &QuantPerm,
    mut payload: BitcoinPayload,
    source: BitcoinObservationSource,
) -> ObservationResult<Self> {
    let coordinate = Qtm::derive_coordinate(manifold);
    let script =
        crate::bit_block::derive_script_pubkey(coordinate)?;
    let fee = u64::try_from(payload.fee_mass(manifold))
        .unwrap_or(u64::MAX);
    if let Some(output) = payload.outputs.last_mut() {
        output.value = output.value.saturating_sub(fee);
    }
    // This constructs an unsigned transaction template. The scriptSig
    // and witness remain empty until the transaction is actually signed.
    let inputs = payload
        .inputs
        .into_iter()
        .map(|input| bitcoin::TxIn {
            previous_output: input.previous_output,
            script_sig: bitcoin::ScriptBuf::new(),
            sequence: bitcoin::Sequence::MAX,
            witness: bitcoin::Witness::new(),
        })
        .collect();
    // Apply the derived locking script to outputs created by this
    // constructor. The source payload's output scripts are intentionally
    // replaced here.
    let outputs = payload
        .outputs
        .into_iter()
        .map(|output| bitcoin::TxOut {
            value: bitcoin::Amount::from_sat(output.value),
            script_pubkey: script.clone(),
        })
        .collect();
    let transaction = bitcoin::Transaction {
        version: bitcoin::transaction::Version(1),
        lock_time: bitcoin::absolute::LockTime::ZERO,
        input: inputs,
        output: outputs,
    };
    let mut observed = Self::from_transaction(transaction, source);
    observed.fee = Some(fee);
    Ok(observed)
}
#[inline(always)]
pub fn new_transaction(
    manifold: &QuantPerm,
    payload: BitcoinPayload,
    source: BitcoinObservationSource,
) -> ObservationResult<Self> {
    Self::new_transact(manifold, payload, source)
}

}