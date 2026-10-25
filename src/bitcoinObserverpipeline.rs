use bitcoin::BlockHash;
use quantom_value::Retain;

use crate::{
    bitcoinpayload::BitcoinPayload,
    bitcoinutxocollector::BitcoinUTXOCollector,
    broadcast::FinalizedSettlement,
    miner::Miner,
    observebitcointransaction::ObservedBitcoinTransaction,
    qpeventreceipt::EventReceipt,
    sharedtypes::SettlementState,
    settlement::{SettlementEngine, SettlementError},
};

pub struct BitcoinObservationPipeline {
    utxo: BitcoinUTXOCollector,
    pub miner: Miner,
    chain: ChainStateManager,
    schedule: Option<BitcoinSchedule>,
}
pub struct BitcoinObservationPipeline {
    utxo: BitcoinUTXOCollector,
    pub miner: Miner,
    chain: ChainStateManager,
    schedule: Option<BitcoinSchedule>,
}

impl BitcoinObservationPipeline {
    #[inline(always)]
    pub fn new(
        activation: u64,
        substrate: &str,
    ) -> Result<Self, SettlementError> {
        Ok(Self {
            utxo: BitcoinUTXOCollector::new(
                substrate,
                activation,
            )?,
            miner: Miner::new(
                activation.saturating_sub(1),
            ),
            chain: ChainStateManager::new(),
            schedule: None,
        })
    }

    #[inline(always)]
    pub fn miner_mut(&mut self) -> &mut Miner {
        &mut self.miner
    }

    #[inline(always)]
    pub fn miner(&self) -> &Miner {
        &self.miner
    }

    #[inline(always)]
    pub fn best_tip(&self) -> Option<(BlockHash, u64)> {
        self.chain.best_tip()
    }

    #[inline(always)]
    pub fn schedule(&self) -> Option<BitcoinSchedule> {
        self.schedule.clone()
    }

    #[inline(always)]
    pub fn update_schedule(
        &mut self,
        schedule: BitcoinSchedule,
    ) {
        self.schedule = Some(schedule);
    }

    #[inline(always)]
    pub fn process(
        &mut self,
        substrate: &str,
        manifold: &quantom_value::QuantPerm,
        observed: ObservedBitcoinTransaction,
    ) -> Result<
        Option<(FinalizedSettlement, Vec<Retain>)>,
        SettlementError,
    > {
        let (bitcoin, retained) =
            self.bitcoin_payload(&observed, manifold)?;

        self.advance_materialized(
            substrate,
            bitcoin,
            retained,
            observed,
        )
    }

    #[inline(always)]
    pub fn bitcoin_payload(
        &mut self,
        observed: &ObservedBitcoinTransaction,
        manifold: &quantom_value::QuantPerm,
    ) -> Result<
        (BitcoinPayload, Vec<Retain>),
        SettlementError,
    > {
        self.utxo.collect(observed, manifold)
    }

    #[inline(always)]
    pub fn advance_materialized(
        &mut self,
        substrate: &str,
        bitcoin: BitcoinPayload,
        mut retained: Vec<Retain>,
        observed: ObservedBitcoinTransaction,
    ) -> Result<
        Option<(FinalizedSettlement, Vec<Retain>)>,
        SettlementError,
    > {
        let Some(tip_height) = self.chain.tip_height() else {
            return Ok(None);
        };

        let payload_activation = self.utxo.activation();

        if tip_height <= payload_activation {
            return Ok(None);
        }

        if bitcoin.inputs.len() != observed.transaction.output.len() {
            return Err(SettlementError::Backend(format!(
                "materialized input cardinality changed: inputs={} observed_outputs={}",
                bitcoin.inputs.len(),
                observed.transaction.output.len(),
            )));
        }

        let (finalized, settlement_retained) =
            SettlementEngine::new(substrate).process(
                bitcoin,
                observed,
            )?;

        retained.extend(settlement_retained);

        Ok(Some((finalized, retained)))
    }
}
