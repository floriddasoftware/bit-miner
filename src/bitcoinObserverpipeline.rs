
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
    miner::{ActiveScript, Miner},
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
            utxo: BitcoinUTXOCollector::new(),
            miner: Miner::new(ActiveScript {
                activation,
            }),
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
        if !self.chain.active {
            return Ok(None);
        }

        // Resolve the active substrate clip.
        let activation = self
            .miner
            .clip(substrate)
            .map(|clip| clip.activation)
            .ok_or_else(|| {
                SettlementError::Backend(format!(
                    "no active miner clip for substrate {substrate}"
                ))
            })?;

        // Recover the current manifold.
        let current = qtm_open_manifold_until(
            substrate,
            activation,
        );

        // Materialize the Bitcoin payload first.
        let (bitcoin, retained) = self.bitcoin_payload(
            &observed,
            &current.state,
        )?;

        // Submit and recover the persisted payload depth.
        let depth = self
            .miner
            .submit(substrate, activation)
            .map_err(|error| {
                SettlementError::Backend(format!(
                    "miner submission failed: {error}"
                ))
            })?;

        // Recover the persisted event.
        let persisted = load_event_at_activation(
            substrate,
            activation,
        );

        let mut event = match persisted {
            PersistedEvent::Transit(event) => event,

            PersistedEvent::Exile(_) => {
                return Err(SettlementError::Backend(
                    "expected a transit event, found an exile event".into(),
                ));
            }
        };

        // Use the depth recovered by the miner submission.
        event.depth = depth;

        // Derive the canonical locked structure.
        let locked = qp_hd::protocol_id::LockedStructure {
            dimension: event.heritage.state.dimension(),
            structural_value: event.heritage.state.structural_value(),
        };

        // Construct the settlement receipt.
        let event_receipt = EventReceipt::new(
            event,
            SettlementState::Pending,
            locked,
        )
        .map_err(|error| {
            SettlementError::Backend(error.to_string())
        })?;

        // Advance the materialized payload into settlement.
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

        let finalized = SettlementEngine::new(substrate)
            .process(event, bitcoin, observed)?;

        Ok(Some((finalized, retained)))
    }
}
