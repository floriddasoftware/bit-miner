use std::env;
use bit_miner::{
    observestream::refresh,
    BitcoinObservationPipeline,
    RenderState,
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
