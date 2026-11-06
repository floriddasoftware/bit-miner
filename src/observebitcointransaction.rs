use bitcoin::Txid;
use quantom_value::QuantPerm;
use qp_hd::protocolvalue::Qtm;

use crate::bitcoinpayload::{BitcoinPayload, PayloadComponent};
use crate::observestream::{
BitcoinObservation,
BitcoinObservationSource,
};

type ObservationResult =
Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// Canonical observed Bitcoin transaction.
///
/// Transactions can originate from network observation or Bitcoin Core
/// getblocktemplate(). Template metadata is retained when available.
///
/// NOTE: getblocktemplate() does not provide referenced UTXOs.
#[derive(Clone, Debug)]
pub struct ObservedBitcoinTransaction {
pub observation: Option,
pub source: BitcoinObservationSource,
pub txid: Txid,
pub transaction: bitcoin::Transaction,
pub coinbase: bool,
/// Fee from the block template; None if only network-observed.
pub fee: Option,
/// Weight from the block template; None if only network-observed.
pub weight: Option,
/// Zero-based indexes into the template transaction list.
/// Bitcoin Core supplies these indexes as one-based values.
pub depends: Option<Vec>,
/// Original zero-based position in the template.
/// None if only network-observed.
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
    let retained_input = payload
        .inputs
        .iter()
        .enumerate()
        .fold(0u128, |total, (index, input)| {
            let retained = input.retained(manifold).mass;
            eprintln!(
                "[bitcoin-retain] input index={} retained={}",
                index, retained
            );
            total.saturating_add(retained)
        });
    eprintln!(
        "[bitcoin-retain] input_total={}",
        retained_input
    );
    let fee = u64::try_from(payload.fee_mass(manifold))
        .map_err(|error| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("Bitcoin fee exceeds supported range: {error}"),
            )
        })?;
    let settlement_value = u64::try_from(retained_input)
        .map_err(|error| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "Input retained mass exceeds Bitcoin value range: {error}"
                ),
            )
        })?
        .checked_sub(fee)
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Settlement value is insufficient for fee",
            )
        })?;
    eprintln!(
        "[bitcoin-retain] fee={} settlement_value={}",
        fee, settlement_value
    );
    if let Some(output) = payload.outputs.last_mut() {
        output.value = settlement_value;
        output.assign(manifold, coordinate)?;
    }
    let retained_output = payload
        .outputs
        .iter()
        .enumerate()
        .fold(0u128, |total, (index, output)| {
            let retained = output.retained(manifold).mass;
            eprintln!(
                "[bitcoin-retain] output index={} value={} retained={}",
                index, output.value, retained
            );
            total.saturating_add(retained)
        });
    eprintln!(
        "[bitcoin-retain] output_total={}",
        retained_output
    );
    let replay_retained = payload.replay_mass(manifold);
    eprintln!(
        "[bitcoin-retain] replay_retained={}",
        replay_retained
    );
    let inputs = payload
        .inputs
        .into_iter()
        .map(|input| bitcoin::TxIn {
            previous_output: input.previous_output,
            script_sig: input.script_sig,
            sequence: input.sequence,
            witness: input.witness,
        })
        .collect();
    let outputs = payload
        .outputs
        .into_iter()
        .map(|output| bitcoin::TxOut {
            value: bitcoin::Amount::from_sat(output.value),
            script_pubkey: output.script_pubkey,
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