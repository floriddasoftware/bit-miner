
use bitcoin::{Amount, Txid};
use quantom_value::QuantPerm;
use qp_hd::protocolvalue::Qtm;

use crate::bitcoinpayload::{
    BitcoinPayload,
    PayloadComponent,
};
use crate::observestream::{
    BitcoinObservation,
    BitcoinObservationSource,
};

type ObservationResult<T> =
    Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// Canonical observed Bitcoin transaction.
///
/// Transactions can originate from network observation or Bitcoin Core
/// getblocktemplate(). Template metadata is retained when available.
///
/// NOTE: getblocktemplate() does not provide referenced UTXOs.
#[derive(Clone, Debug)]
pub struct ObservedBitcoinTransaction {
    pub observation: Option<BitcoinObservation>,
    pub source: BitcoinObservationSource,
    pub txid: Txid,
    pub transaction: bitcoin::Transaction,
    pub coinbase: bool,
    /// Fee quantity derived from the retained payload.
    pub fee: Option<u64>,
    /// Weight from the block template; None if unavailable.
    pub weight: Option<u64>,
    /// Zero-based indexes into the template transaction list.
    /// Bitcoin Core supplies these indexes as one-based values.
    pub depends: Option<Vec<usize>>,
    /// Original zero-based position in the template.
    /// None if unavailable.
    pub template_index: Option<usize>,
}

impl ObservedBitcoinTransaction {
    #[inline(always)]
    pub fn is_coinbase(
        transaction: &bitcoin::Transaction,
    ) -> bool {
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
        previous_transaction: &bitcoin::Transaction,
        source: BitcoinObservationSource,
    ) -> ObservationResult<Self> {
        let coordinate: [u8; 32] =
            Qtm::derive_coordinate(manifold);

        let fee_mass = payload.fee_mass(manifold);

        let fee = u64::try_from(fee_mass).map_err(|error| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "Retained fee quantity exceeds supported range: {error}"
                ),
            )
        })?;

        let output = payload
            .outputs
            .iter_mut()
            .find(|output| output.vout == 0)
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Settlement spread output vout=0 not found",
                )
            })?;

        let original_value = output.value;

        output.value = original_value.checked_sub(fee).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "Insufficient settlement output value: value={original_value} sat, fee={fee} sat",
                ),
            )
        })?;

        eprintln!(
            "[bitcoin-retain] fee_sat={} settlement_value_sat={}->{}",
            fee,
            original_value,
            output.value,
        );

        let (
            assigned_output,
            _assigned_input,
            assigned_input_retained,
        ) = output.assign(
            manifold,
            coordinate,
            previous_transaction,
        )?;

        eprintln!(
            "[bitcoin-retain] assigned_output retained_mass={}",
            assigned_output.mass,
        );

        eprintln!(
            "[bitcoin-retain] assigned_input retained_mass={}",
            assigned_input_retained.mass,
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
                value: Amount::from_sat(output.value),
                script_pubkey: output.script_pubkey,
            })
            .collect();

        let transaction = bitcoin::Transaction {
            version: bitcoin::transaction::Version(1),
            lock_time: bitcoin::absolute::LockTime::ZERO,
            input: inputs,
            output: outputs,
        };

        let mut observed = Self::from_transaction(
            transaction,
            source,
        );

        observed.fee = Some(fee);

        Ok(observed)
    }

    #[inline(always)]
    pub fn new_transaction(
        manifold: &QuantPerm,
        payload: BitcoinPayload,
        previous_transaction: &bitcoin::Transaction,
        source: BitcoinObservationSource,
    ) -> ObservationResult<Self> {
        Self::new_transact(
            manifold,
            payload,
            previous_transaction,
            source,
        )
    }
}