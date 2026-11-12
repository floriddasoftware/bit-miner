use qp_hd::commands::{
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
    activation: u64,
    network: String,
}
impl BitcoinObservationPipeline {
    #[inline(always)]
    pub fn new(
        activation: u64,
        substrate: &str,
        network: &str,
    ) -> Result<Self, SettlementError> {
        if network.trim().is_empty() {
            return Err(SettlementError::Backend(
                "Bitcoin observation pipeline requires a network ID".into(),
            ));
        }
        let clip = ActiveClip {
            substrate: substrate.to_owned(),
            activation,
        };
        Ok(Self {
            utxo: BitcoinUTXOCollector::new(),
            miner: Miner::new(clip),
            electrum: ElectrumBroadcastService::init(1024),
            chain: RenderState::window(substrate.to_owned()),
            activation,
            network: network.to_owned(),
        })
    }
    #[inline(always)]
    pub fn activation(&self) -> u64 {
        self.activation
    }
    #[inline(always)]
    pub fn network(&self) -> &str {
        &self.network
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
            BitcoinPayload,
            u128,
            [u8; 32],
            Depth,
        )>,
        SettlementError,
    > {
        if !self.chain.active {
            return Ok(None);
        }
        let clip_activation = self
            .miner
            .clip(substrate)
            .map(|clip| clip.activation)
            .ok_or_else(|| {
                SettlementError::Backend(format!(
                    "no active miner clip for substrate={}",
                    substrate,
                ))
            })?;
        let current = qtm_open_manifold_until(
            substrate,
            self.activation,
        );
        let (bitcoin, retained_mass) =
            self.bitcoin_payload(&observed, &current.state)?;
        let depth = self
            .miner
            .submit(substrate, clip_activation)
            .map_err(|error| {
                SettlementError::Backend(format!(
                    "miner submission failed: substrate={} activation={} network={}: {}",
                    substrate,
                    clip_activation,
                    self.network,
                    error,
                ))
            })?;
        let persisted = load_event_at_activation(
            substrate,
            self.activation,
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
        )?;
        Ok(materialized.map(
            |(finalized, payload, retained_mass, commitment)| {
                (
                    finalized,
                    payload,
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
        bitcoin: BitcoinPayload,
        retained_mass: u128,
        event_receipt: EventReceipt,
        observed: ObservedBitcoinTransaction,
    ) -> Result<
        Option<(
            FinalizedSettlement,
            BitcoinPayload,
            u128,
            [u8; 32],
        )>,
        SettlementError,
    > {
        if !self.chain.active {
            return Ok(None);
        }
        let commitment = event_receipt.event.qtm.commitment;
        let (finalized, payload) = SettlementEngine::new(substrate)
            .process(event_receipt, bitcoin, observed)?;
        Ok(Some((
            finalized,
            payload,
            retained_mass,
            commitment,
        )))
    }
}