
use std::{
    collections::HashMap,
    io,
};

use qtm_graph::graph::frame::Depth;

use qp_hd::commands::{
    event_path_at_activation,
    load_budget,
    load_depth,
    substrate_root,
};

use crate::broadcast::Broadcaster;

pub struct ActiveClip {
    pub substrate: String,
    pub activation: u64,
}

pub struct ActiveScript {
    pub activation: u64,
}

pub struct Miner {
    pub clips: Vec<ActiveClip>,
    pub broadcaster: Broadcaster,
    pub submissions: HashMap<String, u64>,
}

fn not_found(message: String) -> io::Error {
    io::Error::new(io::ErrorKind::NotFound, message)
}

impl Miner {
    #[inline(always)]
    pub fn new(script: ActiveScript) -> Self {
        let root = substrate_root();

        let mut clips = root
            .read_dir()
            .expect("Failed to read substrate root")
            .filter_map(Result::ok)
            .filter_map(|entry| {
                let path = entry.path();

                if !path.is_dir() {
                    return None;
                }

                let active = std::fs::read_to_string(
                    path.join("activation"),
                )
                .ok()?
                .trim()
                .parse::<u64>()
                .ok()?;

                (active == script.activation).then(|| ActiveClip {
                    substrate: entry.file_name().to_string_lossy().into_owned(),
                    activation: active,
                })
            })
            .collect::<Vec<_>>();

        clips.sort_by(|a, b| a.substrate.cmp(&b.substrate));

        assert!(
            !clips.is_empty(),
            "No active clips found for activation {}.",
            script.activation
        );

        println!(
            "⚡ Miner online active_clips={} activation={}",
            clips.len(),
            script.activation
        );

        Self {
            clips,
            broadcaster: Broadcaster::init(),
            submissions: HashMap::new(),
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
        activation: u64,
    ) -> Result<Depth, io::Error> {
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

        let clip_substrate = clip.substrate.clone();

        let budget = load_budget(&clip_substrate)?;

        let submissions = self
            .submissions
            .get(&clip_substrate)
            .copied()
            .unwrap_or(0);

        if submissions >= budget.max_events {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                format!(
                    "submission budget exhausted: substrate={} activation={} submissions={} max_events={}",
                    clip_substrate,
                    activation,
                    submissions,
                    budget.max_events,
                ),
            ));
        }

        let stored = event_path_at_activation(
            &clip_substrate,
            activation,
        )
        .ok_or_else(|| {
            not_found(format!(
                "stored event not found: {}/events/event_{activation}_*",
                substrate_root()
                    .join(&clip_substrate)
                    .display()
            ))
        })?;

        if !stored.is_dir() {
            return Err(not_found(format!(
                "stored event directory not found: {}",
                stored.display()
            )));
        }

        let depth = load_depth(&stored);

        self.submissions.insert(
            clip_substrate,
            submissions.saturating_add(1),
        );

        Ok(depth)
    }
}
