
use bitcoin::{
    Amount,
    ScriptBuf,
    TxIn,
    Txid,
    Witness,
};
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

#[derive(Clone, Debug)]
pub struct ObservedBitcoinTransaction {
    pub observation: Option<BitcoinObservation>,
    pub source: BitcoinObservationSource,
    pub txid: Txid,
    pub transaction: bitcoin::Transaction,
    pub coinbase: bool,
    pub fee: Option<u64>,
    pub weight: Option<u64>,
    pub depends: Option<Vec<usize>>,
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
        source: BitcoinObservationSource,
    ) -> ObservationResult<Self> {
        let coordinate: [u8; 32] =
            Qtm::derive_coordinate(manifold);

        let fee = u64::try_from(payload.fee_mass(manifold))
            .map_err(|error| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!(
                        "Retained fee quantity exceeds supported range: {error}"
                    ),
                )
            })?;

        // Deduct the settlement fee once from output vout=0.
        let settlement_output = payload
            .outputs
            .iter_mut()
            .find(|output| output.vout == 0)
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Settlement spread output vout=0 not found",
                )
            })?;

        let original_value = settlement_output.value;

        settlement_output.value = original_value
            .checked_sub(fee)
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!(
                        "Insufficient settlement output value: value={original_value} sat, fee={fee} sat",
                    ),
                )
            })?;

        let settlement_value = settlement_output.value;

        // Assign every payload output independently.
        // Assignment establishes the new output script and retained value.
        payload.outputs.sort_by_key(|output| output.vout);

        let mut assigned_outputs =
            Vec::with_capacity(payload.outputs.len());

        for (position, output) in
            payload.outputs.iter_mut().enumerate()
        {
            if output.vout as usize != position {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "Incomplete output template: expected vout={}, found vout={}",
                        position,
                        output.vout,
                    ),
                )
                .into());
            }

            let original_script = output.script_pubkey.clone();

            let retained = output.assign(
                manifold,
                coordinate,
            )?;

            eprintln!(
                "[bitcoin-retain] vout={} value_sat={} assigned_retained_mass={} previous_script_bytes={} assigned_script_bytes={}",
                output.vout,
                output.value,
                retained.mass,
                original_script.len(),
                output.script_pubkey.len(),
            );

            assigned_outputs.push(retained);
        }

        eprintln!(
            "[bitcoin-retain] fee_sat={} settlement_value_sat={}->{} assigned_outputs={}",
            fee,
            original_value,
            settlement_value,
            assigned_outputs.len(),
        );

        // Materialize the supplied input outpoints.
        // Signing data is intentionally empty at transaction creation.
        // Preserve each input's outpoint and sequence.
        let inputs: Vec<TxIn> = payload
            .inputs
            .into_iter()
            .map(|input| {
                TxIn {
                    previous_output: input.previous_output,
                    script_sig: ScriptBuf::new(),
                    sequence: input.sequence,
                    witness: Witness::new(),
                }
            })
            .collect();

        // Materialize assigned outputs in vout order.
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
        source: BitcoinObservationSource,
    ) -> ObservationResult<Self> {
        Self::new_transact(
            manifold,
            payload,
            source,
        )
    }
}
