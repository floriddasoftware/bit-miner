use bitcoin::{
    Amount,
    PublicKey,
    ScriptBuf,
    TxIn,
    Txid,
};
use qtm_graph::graph::frame::Direction;
use quantom_value::QuantPerm;

use crate::{
    bitcoinoutputqpinput::BitcoinOutput,
    bitcoinpayload::BitcoinPayload,
    observestream::{
        BitcoinObservation,
        BitcoinObservationSource,
    },
    qpoutputsbitcoininput::BitcoinInput,
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
    pub transition_delta: Option<u128>,
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
    pub fn from_observedtransaction(
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
            transition_delta: None,
        }
    }

    #[inline(always)]
    pub fn from_settlementtransaction(
        transaction: bitcoin::Transaction,
        source: BitcoinObservationSource,
        payload: &BitcoinPayload,
        direction: &Direction,
        submission: usize,
    ) -> ObservationResult<Self> {
        let txid = transaction.compute_txid();
        let coinbase = Self::is_coinbase(&transaction);
        let heritage = direction.heritage();
        let transition_delta = heritage.transition.delta;

        let weight = u64::try_from(
            payload.replay_mass(&heritage.state),
        )
        .map_err(|error| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "Payload replay mass exceeds supported range: {error}"
                ),
            )
        })?;

        let fee = u64::try_from(
            payload.fee_mass(
                &heritage.state,
                transition_delta,
            ),
        )
        .map_err(|error| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "Retained fee quantity exceeds supported range: {error}"
                ),
            )
        })?;

        Ok(Self {
            observation: None,
            source,
            txid,
            transaction,
            coinbase,
            fee: Some(fee),
            weight: Some(weight),
            depends: None,
            template_index: Some(submission),
            transition_delta: Some(transition_delta),
        })
    }

    #[inline(always)]
    pub fn new_transaction(
        manifold: &QuantPerm,
        inputs: Vec<BitcoinInput>,
        public_key: PublicKey,
        source: BitcoinObservationSource,
        direction: &Direction,
        submission: usize,
    ) -> ObservationResult<(Self, BitcoinPayload)> {
        Self::new_transact(
            manifold,
            inputs,
            public_key,
            source,
            direction,
            submission,
        )
    }

    #[inline(always)]
    pub fn new_transact(
        manifold: &QuantPerm,
        inputs: Vec<BitcoinInput>,
        public_key: PublicKey,
        source: BitcoinObservationSource,
        direction: &Direction,
        submission: usize,
    ) -> ObservationResult<(Self, BitcoinPayload)> {
        if inputs.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Cannot materialize a Bitcoin transaction without inputs",
            )
            .into());
        }

        let mut outputs = Vec::with_capacity(inputs.len());
        let mut total_fee = 0u64;

        for (position, input) in inputs.iter().enumerate() {
            let vout = u32::try_from(position).map_err(|error| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!(
                        "Output position exceeds supported vout range: {error}"
                    ),
                )
            })?;

            let fee = BitcoinPayload::replay_sat(input.value);

            let output_value = input.value.checked_sub(fee).ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!(
                        "Input {} value ({} sat) is less than replay fee ({} sat)",
                        position,
                        input.value,
                        fee,
                    ),
                )
            })?;

            total_fee = total_fee.checked_add(fee).ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Total replay fee overflow",
                )
            })?;

            let (mut output, _) = BitcoinOutput::new(
                manifold,
                vout,
                output_value,
                ScriptBuf::new(),
            );

            let retained = output.assign_public_key(
                manifold,
                public_key,
            );

            eprintln!(
                "[bitcoin-output-assigned] input={} vout={} input_value_sat={} fee_sat={} output_value_sat={} retained_mass={} script_bytes={}",
                position,
                output.vout,
                input.value,
                fee,
                output.value,
                retained.mass,
                output.script_pubkey.len(),
            );

            outputs.push(output);
        }

        let payload = BitcoinPayload::new(
            inputs,
            outputs,
        );

        let heritage = direction.heritage();
        let transition_delta = heritage.transition.delta;

        let weight = u64::try_from(
            payload.replay_mass(&heritage.state),
        )
        .map_err(|error| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "Payload replay mass exceeds supported range: {error}"
                ),
            )
        })?;

        let transaction_inputs = payload.inputs
            .iter()
            .map(|input| TxIn {
                previous_output: input.previous_output,
                script_sig: input.script_sig.clone(),
                sequence: input.sequence,
                witness: input.witness.clone(),
            })
            .collect::<Vec<_>>();

        let transaction_outputs = payload.outputs
            .iter()
            .map(|output| bitcoin::TxOut {
                value: Amount::from_sat(output.value),
                script_pubkey: output.script_pubkey.clone(),
            })
            .collect::<Vec<_>>();

        let input_value = payload.inputs
            .iter()
            .try_fold(0u64, |total, input| {
                total.checked_add(input.value)
            })
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Materialized input value overflow",
                )
            })?;

        let output_value = transaction_outputs
            .iter()
            .try_fold(0u64, |total, output| {
                total.checked_add(output.value.to_sat())
            })
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Materialized output value overflow",
                )
            })?;

        let actual_fee = input_value.checked_sub(output_value).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Materialized outputs exceed input value",
            )
        })?;

        if actual_fee != total_fee {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "Materialized fee mismatch: expected={} sat actual={} sat",
                    total_fee,
                    actual_fee,
                ),
            )
            .into());
        }

        let transaction = bitcoin::Transaction {
            version: bitcoin::transaction::Version(1),
            lock_time: bitcoin::absolute::LockTime::ZERO,
            input: transaction_inputs,
            output: transaction_outputs,
        };

        eprintln!(
            "[bitcoin-materialized] inputs={} outputs={} input_value_sat={} output_value_sat={} fee_sat={} replay_mass={} submission={}",
            transaction.input.len(),
            transaction.output.len(),
            input_value,
            output_value,
            total_fee,
            weight,
            submission,
        );

        let observed = Self {
            observation: None,
            source,
            txid: transaction.compute_txid(),
            coinbase: Self::is_coinbase(&transaction),
            transaction,
            fee: Some(total_fee),
            weight: Some(weight),
            depends: None,
            template_index: Some(submission),
            transition_delta: Some(transition_delta),
        };

        eprintln!(
            "[bitcoin-materialized-complete] txid={} coinbase={} inputs={} outputs={} fee_sat={} replay_mass={} submission={}",
            observed.txid,
            observed.coinbase,
            observed.transaction.input.len(),
            observed.transaction.output.len(),
            total_fee,
            weight,
            submission,
        );

        Ok((observed, payload))
    }
}
