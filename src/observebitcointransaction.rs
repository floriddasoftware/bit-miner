
use bitcoin::{Amount, ScriptBuf, TxIn, Txid, Witness};
use quantom_value::QuantPerm;
use qp_hd::protocolvalue::Qtm;

use crate::bitcoinpayload::BitcoinPayload;
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
        _previous_transaction: &bitcoin::Transaction,
        source: BitcoinObservationSource,
    ) -> ObservationResult<Self> {
        // Derive the canonical coordinate from the manifold.
        let coordinate: [u8; 32] =
            Qtm::derive_coordinate(manifold);

        // Calculate retained fee for the independent settlement assignment.
        let fee = u64::try_from(payload.fee_mass(manifold))
            .map_err(|error| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!(
                        "Retained fee quantity exceeds supported range: {error}"
                    ),
                )
            })?;

        // Independently assign the new settlement output vout=0.
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

        output.value = original_value
            .checked_sub(fee)
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!(
                        "Insufficient settlement output value: value={original_value} sat, fee={fee} sat",
                    ),
                )
            })?;

        let assigned_output = output.assign(
            manifold,
            coordinate,
        )?;

        eprintln!(
            "[bitcoin-retain] fee_sat={} settlement_value_sat={}->{} assigned_output_retained_mass={}",
            fee,
            original_value,
            output.value,
            assigned_output.mass,
        );

        // Materialize inputs in their existing index order.
        // Input 0 receives fresh script buffers; its structural realization
        // was already established by BitcoinInput::new().
        let inputs: Vec<TxIn> = payload
            .inputs
            .into_iter()
            .enumerate()
            .map(|(index, input)| {
                TxIn {
                    previous_output: input.previous_output,
                    script_sig: if index == 0 {
                        ScriptBuf::new()
                    } else {
                        input.script_sig
                    },
                    sequence: input.sequence,
                    witness: if index == 0 {
                        Witness::new()
                    } else {
                        input.witness
                    },
                }
            })
            .collect();

        // Materialize outputs in their existing vout order.
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
