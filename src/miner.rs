use std::path::PathBuf;
use qp_hd::commands::substrate_root;
use crate::broadcast::Broadcaster;
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
        let substrate = substrate_root()
            .read_dir()
            .expect("Failed to read substrate root")
            .filter_map(Result::ok)
            .filter(|entry| entry.path().is_dir())
            .find_map(|entry| {
                let path = entry.path();
                let activation_path = path.join("activation");
                let active_activation = std::fs::read_to_string(activation_path)
                    .ok()?
                    .trim()
                    .parse::<u64>()
                    .ok()?;
                if active_activation == activation {
                    Some(ActiveClip {
                        substrate: entry.file_name().to_string_lossy().into_owned(),
                        activation: active_activation,
                    })
                } else {
                    None
                }
            });
        let clips = substrate.into_iter().collect::<Vec<_>>();
        assert!(
            !clips.is_empty(),
            "No active clip found for activation {activation}."
        );
        println!("⚡ Miner online active_clips={}", clips.len());
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
        activation: u64,
    ) -> Result<PathBuf, std::io::Error> {
        let clip = self.clip(substrate).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("active clip not found: {substrate}"),
            )
        })?;
        if clip.activation != activation {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!(
                    "activation mismatch: substrate={} requested={} active={}",
                    substrate,
                    activation,
                    clip.activation,
                ),
            ));
        }
        let clip_substrate = clip.substrate.clone();
        let budget = qp_hd::commands::load_budget(&clip_substrate)?;
        if self.submissions as u64 >= budget.max_events {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                format!(
                    "submission budget exhausted: substrate={} activation={} submissions={} max_events={}",
                    clip_substrate,
                    activation,
                    self.submissions,
                    budget.max_events,
                ),
            ));
        }
        let events = substrate_root()
            .join(&clip_substrate)
            .join("events");
        if !events.is_dir() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("event store not found: {}", events.display()),
            ));
        }
        let event_path = events.join(format!("event_{activation}"));
        if !event_path.is_file() {
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