use qp_hd::commands::{
    load_event_at_activation,
    qtm_open_manifold_until,
    PersistedEvent,
};
use quantom_value::{QuantPerm, Retain};
use crate::{
    bitcoinpayload::BitcoinPayload,
    bitcoinutxocollector::BitcoinUTXOCollector,
    broadcast::FinalizedSettlement,
    miner::Miner,
    observebitcointransaction::ObservedBitcoinTransaction,
    qpeventreceipt::EventReceipt,
    render::RenderState,
    settlement::{SettlementEngine, SettlementError},
    sharedtypes::SettlementState,
};
pub struct BitcoinObservationPipeline {
    utxo: BitcoinUTXOCollector,
    pub miner: Miner,
    chain: RenderState,
}
impl BitcoinObservationPipeline {
    #[inline(always)]
    pub fn new(
        activation: u64,
        substrate: &str,
    ) -> Result<Self, SettlementError> {
        Ok(Self {
            utxo: BitcoinUTXOCollector::new(BitcoinPayload::new()),
            miner: Miner::new(activation.saturating_sub(1)),
            chain: RenderState::window(substrate.to_owned()),
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
    pub fn render_state(&self) -> &RenderState {
        &self.chain
    }
    #[inline(always)]
    pub fn render_state_mut(&mut self) -> &mut RenderState {
        &mut self.chain
    }
    #[inline(always)]
    pub fn process(
        &mut self,
        substrate: &str,
        observed: ObservedBitcoinTransaction,
    ) -> Result<Option<(FinalizedSettlement, Vec<Retain>)>, SettlementError> {
        let activation = self
            .miner
            .clip(substrate)
            .map(|clip| clip.activation)
            .ok_or_else(|| {
                SettlementError::Backend(format!(
                    "no active miner clip for substrate {substrate}"
                ))
            })?;
        let current = qtm_open_manifold_until(substrate, activation);
        let realized = current.state.retained_mass();
        let event_path = self
            .miner
            .submit(substrate, realized)
            .map_err(|error| {
                SettlementError::Backend(format!(
                    "miner submission failed: {error}"
                ))
            })?;
        let event_activation = event_path
            .file_name()
            .and_then(|name| name.to_str())
            .and_then(|name| name.strip_prefix("event_"))
            .and_then(|name| name.split('_').next())
            .and_then(|value| value.parse::<u64>().ok())
            .ok_or_else(|| {
                SettlementError::Backend(format!(
                    "invalid event path: {}",
                    event_path.display()
                ))
            })?;
        let persisted = load_event_at_activation(substrate, event_activation);
        let event = match persisted {
            PersistedEvent::Transit(event) => event,
            PersistedEvent::Exile(_) => {
                return Err(SettlementError::Backend(
                    "expected a transit event, found an exile event".into(),
                ));
            }
        };
        let locked = qp_hd::protocol_id::LockedStructure {
            dimension: event.heritage.state.dimension(),
            structural_value: event.heritage.state.structural_value(),
        };
        let event_receipt =
            EventReceipt::new(event, SettlementState::Pending, locked)
                .map_err(|error| {
                    SettlementError::Backend(error.to_string())
                })?;
        let (bitcoin, retained) =
            self.bitcoin_payload(&observed, &current.state)?;
        self.advance_materialized(
            substrate,
            bitcoin,
            retained,
            event_receipt,
            observed,
        )
    }
    #[inline(always)]
    pub fn bitcoin_payload(
        &mut self,
        observed: &ObservedBitcoinTransaction,
        manifold: &QuantPerm,
    ) -> Result<(BitcoinPayload, Vec<Retain>), SettlementError> {
        self.utxo.collect(observed, manifold)
    }
    #[inline(always)]
    pub fn advance_materialized(
        &mut self,
        substrate: &str,
        bitcoin: BitcoinPayload,
        retained: Vec<Retain>,
        event: EventReceipt,
        observed: ObservedBitcoinTransaction,
    ) -> Result<Option<(FinalizedSettlement, Vec<Retain>)>, SettlementError> {
        if !self.chain.active {
            return Ok(None);
        }
        if bitcoin.inputs.len() != observed.transaction.output.len() {
            return Err(SettlementError::Backend(format!(
                "materialized input cardinality changed: inputs={} observed_outputs={}",
                bitcoin.inputs.len(),
                observed.transaction.output.len(),
            )));
        }
        let finalized =
            SettlementEngine::new(substrate).process(event, bitcoin, observed)?;
        Ok(Some((finalized, retained)))
    }
}