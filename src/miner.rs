use std::path::PathBuf;

use qp_hd::{
    commands::substrate_root,

};
use crate::{
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
            .collect();

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

    #[inline(always)]
    pub fn submit(
        &mut self,
        substrate: &str,
        realized: u128,
    ) -> Result<PathBuf, std::io::Error> {
        let clip = self.clip(substrate).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("active clip not found: {substrate}"),
            )
        })?;

        let root = substrate_root().join(substrate);
        let events = root.join("events");

        if !events.is_dir() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("event store not found: {}", events.display()),
            ));
        }

        let event_path = events.join(format!(
            "event_{}_{}",
            clip.activation,
            realized,
        ));

        if !event_path.exists() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!(
                    "stored event not found: {}",
                    event_path.display()
                ),
            ));
        }

        self.submissions = self.submissions.saturating_add(1);

        Ok(event_path)
    }
}