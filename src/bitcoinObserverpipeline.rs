use qp_hd::commands::{
    load_budget,
    load_event_at_activation,
    qtm_open_manifold_until,
    PersistedEvent,
};
use qtm_graph::graph::frame::Depth;
use quantom_value::QuantPerm;

use crate::{
    bitcoinpayload::BitcoinPayload,
    bitcoinutxocollector::BitcoinUTXOCollector,
    broadcast::{
        ElectrumBroadcastService,
        FinalizedSettlement,
    },
    miner::{ActiveClip, Miner},
    observebitcointransaction::ObservedBitcoinTransaction,
    qpeventreceipt::EventReceipt,
    render::RenderState,
    settlement::{SettlementEngine, SettlementError},
    sharedtypes::SettlementState,
};

pub struct BitcoinObservationPipeline {
    utxo: BitcoinUTXOCollector,
    pub miner: Miner,
    pub electrum: ElectrumBroadcastService,
    chain: RenderState,
}

impl BitcoinObservationPipeline {
    #[inline(always)]
    pub fn new(
        activation: u64,
        substrate: &str,
    ) -> Result<Self, SettlementError> {
        let clip = ActiveClip {
            substrate: substrate.to_owned(),
            activation,
        };

        Ok(Self {
            utxo: BitcoinUTXOCollector::new(),
            miner: Miner::new(clip),
            electrum: ElectrumBroadcastService::init(1024),
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
    ) -> Result<
        Option<(
            FinalizedSettlement,
            u128,
            [u8; 32],
            Depth,
        )>,
        SettlementError,
    > {
        if !self.chain.active {
            return Ok(None);
        }

        let activation = self
            .miner
            .clip(substrate)
            .map(|clip| clip.activation)
            .ok_or_else(|| {
                SettlementError::Backend(format!(
                    "no active miner clip for substrate {substrate}"
                ))
            })?;

        let current = qtm_open_manifold_until(
            substrate,
            activation,
        );

        let (bitcoin, retained_mass) = self.bitcoin_payload(
            &observed,
            &current.state,
        )?;

        let budget = load_budget(substrate)
            .map_err(|error| {
                SettlementError::Backend(format!(
                    "failed to load submission budget: {error}"
                ))
            })?;

        let submissions = self
            .miner
            .submissions
            .get(substrate)
            .copied()
            .unwrap_or(0);

        if submissions >= budget.max_events {
            return Err(SettlementError::Backend(format!(
                "submission budget exhausted: substrate={} activation={} submissions={} max_events={}",
                substrate,
                activation,
                submissions,
                budget.max_events,
            )));
        }

        let depth = self
            .miner
            .submit(substrate, activation)
            .map_err(|error| {
                SettlementError::Backend(format!(
                    "miner submission failed: {error}"
                ))
            })?;

        let persisted = load_event_at_activation(
            substrate,
            activation,
        );

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

        let event_receipt = EventReceipt::new(
            event,
            SettlementState::Pending,
            locked,
        )
        .map_err(|error| {
            SettlementError::Backend(error.to_string())
        })?;

        let materialized = self.advance_materialized(
            substrate,
            bitcoin,
            retained_mass,
            event_receipt,
            observed,
            &current.state,
        )?;

        Ok(materialized.map(
            |(finalized, retained_mass, commitment)| {
                (
                    finalized,
                    retained_mass,
                    commitment,
                    depth,
                )
            },
        ))
    }

    #[inline(always)]
    pub fn bitcoin_payload(
        &mut self,
        observed: &ObservedBitcoinTransaction,
        manifold: &QuantPerm,
    ) -> Result<(BitcoinPayload, u128), SettlementError> {
        self.utxo.collect(observed, manifold)
    }

    #[inline(always)]
    pub fn advance_materialized(
        &mut self,
        substrate: &str,
        mut bitcoin: BitcoinPayload,
        retained_mass: u128,
        event_receipt: EventReceipt,
        observed: ObservedBitcoinTransaction,
        manifold: &QuantPerm,
    ) -> Result<
        Option<(
            FinalizedSettlement,
            u128,
            [u8; 32],
        )>,
        SettlementError,
    > {
        if !self.chain.active {
            return Ok(None);
        }

        let private_key: [u8; 32] =
            event_receipt.event.qtm.coordinate;

        let commitment = event_receipt.event.qtm.commitment;

        let mut finalized = SettlementEngine::new(substrate)
            .process(
                event_receipt,
                bitcoin.clone(),
                observed.clone(),
            )?;

        for (index, input) in bitcoin.inputs.iter_mut().enumerate() {
            let previous_output = observed
                .transaction
                .output
                .get(input.previous_output.vout as usize)
                .cloned()
                .ok_or_else(|| {
                    SettlementError::Backend(format!(
                        "previous output not found for input {}: vout={}",
                        index,
                        input.previous_output.vout,
                    ))
                })?;

            input
                .sign(
                    manifold,
                    private_key,
                    &finalized.transaction,
                    index,
                    previous_output.value.to_sat(),
                    &previous_output.script_pubkey,
                )
                .map_err(|error| {
                    SettlementError::Backend(error.to_string())
                })?;

            finalized.transaction.input[index].witness =
                input.witness.clone();
        }

        Ok(Some((
            finalized,
            retained_mass,
            commitment,
        )))
    }
}