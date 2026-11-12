
use std::{
    collections::HashMap,
    io,
};

use qtm_graph::graph::{
    frame::{
        Depth,
        Direction,
    },
    scale::Scale,
};
use quantom_value::{
    Heritage,
    QuantPerm,
    Retain,
};

use crate::broadcast::Broadcaster;

pub struct ActiveClip {
    pub substrate: String,
    pub activation: u64,
}

pub struct Miner {
    pub clips: Vec<ActiveClip>,
    pub broadcaster: Broadcaster,
    pub submissions: HashMap<String, u64>,
}

fn not_found(message: String) -> io::Error {
    io::Error::new(
        io::ErrorKind::NotFound,
        message,
    )
}

impl Miner {
    #[inline(always)]
    pub fn new(clip: ActiveClip) -> Self {
        println!(
            "⚡ Miner online substrate={} activation={}",
            clip.substrate,
            clip.activation,
        );

        Self {
            clips: vec![clip],
            broadcaster: Broadcaster::init(),
            submissions: HashMap::new(),
        }
    }

    #[inline(always)]
    pub fn clips(&self) -> &[ActiveClip] {
        &self.clips
    }

    #[inline(always)]
    pub fn clip(
        &self,
        substrate: &str,
    ) -> Option<&ActiveClip> {
        self.clips
            .iter()
            .find(|clip| clip.substrate == substrate)
    }

    #[inline(always)]
    pub fn submission_count(
        &self,
        substrate: &str,
    ) -> u64 {
        self.submissions
            .get(substrate)
            .copied()
            .unwrap_or(0)
    }

    // -------------------------------------------------------------------------
    // DIRECTION DELEGATION
    // -------------------------------------------------------------------------

    #[inline(always)]
    pub fn realize_depth(
        &self,
        manifold: &QuantPerm,
        depth: &Depth,
    ) -> Retain {
        Direction::realize_depth(
            manifold,
            depth,
        )
    }

    #[inline(always)]
    pub fn deep(
        &self,
        manifold: &QuantPerm,
        depth: &Depth,
    ) -> Retain {
        self.realize_depth(
            manifold,
            depth,
        )
    }

    #[inline(always)]
    pub fn transit(
        &self,
        manifold: QuantPerm,
        depth: &Depth,
        provided_seed: Option<&[u8]>,
    ) -> Heritage {
        Direction::transit(
            manifold,
            depth,
            provided_seed,
        )
    }

    #[inline(always)]
    pub fn exile(
        &self,
        manifold: QuantPerm,
    ) -> Heritage {
        Direction::exile(manifold)
    }

    #[inline(always)]
    pub fn direction(
        &self,
        heritage: Heritage,
        scale: Scale,
    ) -> Direction {
        Direction::new(
            heritage,
            scale,
        )
    }

    // -------------------------------------------------------------------------
    // SUBMISSION
    // -------------------------------------------------------------------------

    pub fn submit(
        &mut self,
        substrate: &str,
        activation: u64,
        manifold: QuantPerm,
        depth: &Depth,
        provided_seed: Option<&[u8]>,
    ) -> Result<(Heritage, u64), io::Error> {
        let clip = self
            .clip(substrate)
            .ok_or_else(|| {
                not_found(format!(
                    "active clip not found: {substrate}"
                ))
            })?;

        if clip.activation != activation {
            return Err(not_found(format!(
                "activation mismatch: substrate={} requested={} active={}",
                substrate,
                activation,
                clip.activation,
            )));
        }

        let heritage = self.transit(
            manifold,
            depth,
            provided_seed,
        );

        let submission = self
            .submission_count(substrate)
            .saturating_add(1);

        self.submissions.insert(
            substrate.to_owned(),
            submission,
        );

        Ok((heritage, submission))
    }
}
