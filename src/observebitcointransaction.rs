
use bitcoin::{
    Amount,
    TxIn,
    Txid,
};
use qtm_graph::graph::frame::Direction;
use quantom_value::QuantPerm;
use qp_hd::protocolvalue::Qtm;

use crate::{
    bitcoinpayload::BitcoinPayload,
    observestream::{
        BitcoinObservation,
        BitcoinObservationSource,
    },
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
        payload: BitcoinPayload,
        source: BitcoinObservationSource,
        direction: &Direction,
        submission: usize,
    ) -> ObservationResult<(Self, BitcoinPayload)> {
        Self::new_transact(
            manifold,
            payload,
            source,
            direction,
            submission,
        )
    }

    #[inline(always)]
    pub fn new_transact(
        manifold: &QuantPerm,
        mut payload: BitcoinPayload,
        source: BitcoinObservationSource,
        direction: &Direction,
        submission: usize,
    ) -> ObservationResult<(Self, BitcoinPayload)> {
        let coordinate = Qtm::derive_coordinate(manifold);
        let fee_payload = payload.clone();
        let heritage = direction.heritage();

        let fee = u64::try_from(
            fee_payload.fee_mass(
                &heritage.state,
                heritage.transition.delta,
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

        payload.outputs.sort_by_key(|output| output.vout);

        let original_value = payload.outputs.iter().try_fold(
            0u64,
            |total, output| {
                total.checked_add(output.value).ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "Settlement output value overflow",
                    )
                })
            },
        )?;

        if original_value < fee {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "Insufficient total settlement output value: value={original_value} sat, fee={fee} sat",
                ),
            )
            .into());
        }

        let mut remaining_fee = fee;

        for (position, output) in payload.outputs.iter_mut().enumerate() {
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

            let fee_deduction = output.value.min(remaining_fee);
            output.value = output.value.checked_sub(fee_deduction)
                .ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        format!(
                            "Settlement fee deduction failed at vout={}",
                            output.vout,
                        ),
                    )
                })?;

            remaining_fee = remaining_fee.checked_sub(fee_deduction)
                .ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "Remaining settlement fee underflow",
                    )
                })?;

            let previous_script = output.script_pubkey.clone();
            let retained = output.assign(manifold, coordinate)?;

            eprintln!(
                "[bitcoin-retain] vout={} value_sat={} fee_deduction_sat={} remaining_fee_sat={} assigned_retained_mass={} previous_script_bytes={} assigned_script_bytes={}",
                output.vout,
                output.value,
                fee_deduction,
                remaining_fee,
                retained.mass,
                previous_script.len(),
                output.script_pubkey.len(),
            );
        }

        if remaining_fee != 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "Settlement fee was not fully allocated: remaining={remaining_fee} sat",
                ),
            )
            .into());
        }

        let settlement_value = original_value.checked_sub(fee)
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Settlement value underflow",
                )
            })?;

        let materialized_payload = payload.clone();

        let inputs = payload.inputs
            .into_iter()
            .map(|input| TxIn {
                previous_output: input.previous_output,
                script_sig: input.script_sig,
                sequence: input.sequence,
                witness: input.witness,
            })
            .collect::<Vec<_>>();

        let outputs = payload.outputs
            .into_iter()
            .map(|output| bitcoin::TxOut {
                value: Amount::from_sat(output.value),
                script_pubkey: output.script_pubkey,
            })
            .collect::<Vec<_>>();

        let transaction = bitcoin::Transaction {
            version: bitcoin::transaction::Version(1),
            lock_time: bitcoin::absolute::LockTime::ZERO,
            input: inputs,
            output: outputs,
        };

        let materialized_input_value = transaction.input
            .iter()
            .map(|input| input.previous_output)
            .collect::<Vec<_>>();

        let materialized_output_value = transaction.output
            .iter()
            .try_fold(0u64, |total, output| {
                total.checked_add(output.value.to_sat())
            })
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Materialized transaction output value overflow",
                )
            })?;

        eprintln!(
            "[bitcoin-materialized] inputs={} outputs={} output_value_sat={} expected_settlement_value_sat={} input_outpoints={:?}",
            transaction.input.len(),
            transaction.output.len(),
            materialized_output_value,
            settlement_value,
            materialized_input_value,
        );

        let observed = Self::from_settlementtransaction(
            transaction,
            source,
            &fee_payload,
            direction,
            submission,
        )?;

        eprintln!(
            "[bitcoin-materialized-complete] txid={} coinbase={} inputs={} outputs={} fee_sat={} replay_mass={} submission={}",
            observed.txid,
            observed.coinbase,
            observed.transaction.input.len(),
            observed.transaction.output.len(),
            observed.fee.unwrap_or_default(),
            observed.weight.unwrap_or_default(),
            submission,
        );

        Ok((observed, materialized_payload))
    }
}
