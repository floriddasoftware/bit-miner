use qp_hd::{
    commands::qtm_open_manifold_until,
    protocol_id::LockedStructure,
    protocolvalue::Qtm,
    qp44::QP44Event,
};
use qtm_graph::graph::{
    frame::{Depth, Direction},
    scale::Scale,
};
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
    fn duplicate_depth(depth: &Depth) -> Depth {
        let mut duplicate = Depth::new(depth.path());

        for (position, identifier, retain) in depth.objects() {
            duplicate.insert(
                identifier,
                position,
                retain.clone(),
            );
        }

        duplicate
    }

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

        if clip_activation != self.activation {
            return Err(SettlementError::Backend(format!(
                "miner clip activation mismatch: substrate={} requested={} active={} network={}",
                substrate,
                self.activation,
                clip_activation,
                self.network,
            )));
        }

        let pre_settlement =
            qtm_open_manifold_until(substrate, self.activation);

        let (bitcoin, retained_mass, depth) =
            self.bitcoin_payload(&observed, &pre_settlement.state)?;

        let (heritage, submission_count) = self
            .miner
            .submit(
                substrate,
                clip_activation,
                pre_settlement.state,
                &depth,
                None,
            )
            .map_err(|error| {
                SettlementError::Backend(format!(
                    "miner submission failed: substrate={} activation={} network={}: {}",
                    substrate,
                    clip_activation,
                    self.network,
                    error,
                ))
            })?;

        let submission = usize::try_from(submission_count)
            .map_err(|error| {
                SettlementError::Backend(format!(
                    "submission index exceeds supported range: {error}",
                ))
            })?;

        let direction = Miner::direction(
            heritage,
            Scale::Global,
        );

        let qtm = Qtm::commit(
            &direction.event.heritage.state,
            direction.event.net_work(),
        );

        let commitment = qtm.commitment;

        let locked = LockedStructure {
            dimension: direction.event.dimension(),
            structural_value: direction.event.structural_value(),
        };

        let event = QP44Event {
            heritage: direction.into_heritage(),
            qtm,
            depth: Self::duplicate_depth(&depth),
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
            submission,
        )?;

        Ok(materialized.map(
    |(finalized, payload, retained_mass)| {
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
    ) -> Result<(BitcoinPayload, u128, Depth), SettlementError> {
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
        submission: usize,
    ) -> Result<
        Option<(
            FinalizedSettlement,
            BitcoinPayload,
            u128,
        )>,
        SettlementError,
    > {
        if !self.chain.active {
            return Ok(None);
        }

        let (finalized, payload) =
            SettlementEngine::new(substrate).process(
                event_receipt,
                bitcoin,
                observed,
                submission,
            )?;

        Ok(Some((
            finalized,
            payload,
            retained_mass,
        )))
    }
}