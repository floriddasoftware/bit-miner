use qp_hd::{
    commands::{
        load_depth,
        qtm_open_manifold_until,
        substrate_root,
    },
    protocolvalue::Qtm,
    qp44::QP44Event,
};
use qtm_graph::graph::frame::Direction;

use crate::{
    bitcoinpayload::{BitcoinPayload, PayloadType},
    broadcast::Broadcaster,
    substrate::Substrate,
};

pub struct ActiveClip {
    pub substrate: String,
    pub activation: u64,
}

pub struct Miner {
    pub clips: Vec<ActiveClip>,
    pub broadcaster: Broadcaster,
    pub submissions: usize,
}

impl Miner {
    #[inline(always)]
    pub fn new(activation: u64) -> Self {
        let broadcaster = Broadcaster::init();
        let substrates = Substrate::discover(activation);

        assert!(
            !substrates.is_empty(),
            "No substrates discovered."
        );

        println!("⚡ Miner online substrates={}", substrates.len());

        let clips = substrates
            .into_iter()
            .map(|substrate| ActiveClip {
                substrate: substrate.lab,
                activation: substrate.activation,
            })
            .collect::<Vec<_>>();

        Self {
            clips,
            broadcaster,
            submissions: 0,
        }
    }

    #[inline(always)]
    pub fn clips(&self) -> &[ActiveClip] {
        &self.clips
    }

    #[inline(always)]
    pub fn clip(&self, substrate: &str) -> Option<&ActiveClip> {
        self.clips
            .iter()
            .find(|clip| clip.substrate == substrate)
    }

    #[inline]
    fn payload_identifier(payload_type: PayloadType) -> &'static str {
        match payload_type {
            PayloadType::Payload => "bitcoin/payload",
            PayloadType::Transaction => "bitcoin/transaction",
            PayloadType::Address => "bitcoin/address",
            PayloadType::Wallet => "bitcoin/wallet",
            PayloadType::Mempool => "bitcoin/mempool",
        }
    }

    #[inline]
    pub fn submit(
        &mut self,
        substrate: &str,
        payload: BitcoinPayload,
    ) -> QP44Event {
        let activation = self
            .clip(substrate)
            .unwrap_or_else(|| {
                panic!("active clip not found: {}", substrate)
            })
            .activation;

        let root = substrate_root().join(substrate);

        let opened = qtm_open_manifold_until(substrate, activation);
        let manifold = opened.state;
        let mut depth = load_depth(&root);

        let mass = payload
            .output_value_sat()
            .unwrap_or_else(|| {
                panic!(
                    "BitcoinPayload output value overflow: {}",
                    substrate
                )
            }) as u128;

        let identifier = Self::payload_identifier(payload.payload_type);
        let position = depth.next_position();

        depth.insert(
            identifier,
            position,
            manifold.retain(mass, manifold.dimension()),
        );

        let heritage = Direction::exile(manifold);

        let qtm = Qtm::commit(
            &heritage.state,
            heritage.transition.net_work,
        );

        self.submissions = self.submissions.saturating_add(1);

        QP44Event {
            heritage,
            qtm,
            depth,
        }
    }
}