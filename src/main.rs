use std::env;
use std::path::PathBuf;

use bit_minner::{
    BitcoinObservationPipeline,
    RenderState,
};

use qp_hd::commands::{
    load_event_at_activation,
    qtm_open_manifold_until,
    PersistedEvent,
};

fn main() {
    let args = env::args().collect::<Vec<_>>();

    let activation = args
        .get(1)
        .expect("missing activation")
        .parse::<u64>()
        .expect("invalid activation");

    let substrate = args
        .get(2)
        .expect("missing substrate")
        .clone();

    let mut pipeline = BitcoinObservationPipeline::new(
        activation,
        &substrate,
    )
    .expect("failed to initialize Bitcoin observation pipeline");

    let mut windows = Vec::new();

    for clip in pipeline.miner().clips() {
        windows.push(RenderState::window(
            clip.substrate.clone(),
        ));
    }

    loop {
        for window in &mut windows {
            let substrate = window.substrate.clone();

            let _ = refresh(
                &mut pipeline,
                &substrate,
            );
        }
    }
}

#[inline(always)]
fn refresh(
    pipeline: &mut BitcoinObservationPipeline,
    substrate: &str,
) -> PersistedEvent {
    let activation = pipeline
        .miner()
        .clip(substrate)
        .map(|clip| clip.activation)
        .expect("no active miner clip for substrate");

    let current = qtm_open_manifold_until(
        substrate,
        activation,
    );

    let realized = current.state.retained_mass();

    let event_path: PathBuf = pipeline
        .miner_mut()
        .submit(substrate, realized)
        .expect("miner submission failed");

    let event_activation = event_path
        .file_name()
        .and_then(|name| name.to_str())
        .and_then(|name| name.strip_prefix("event_"))
        .and_then(|name| name.split('_').next())
        .and_then(|value| value.parse::<u64>().ok())
        .expect("invalid event path");

    load_event_at_activation(
        substrate,
        event_activation,
    )
}