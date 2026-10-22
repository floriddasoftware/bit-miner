use std::env;

use bit_minner::{
    BitcoinObservationPipeline,
    Miner,
    RenderState,
};

use qp_hd::{
    PersistedEvent,
    QP44Event,
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
        .as_str();

    let mut pipeline = BitcoinObservationPipeline::new(
        activation,
        substrate,
    )
    .expect("failed to initialize Bitcoin observation pipeline");

    let mut windows = Vec::new();

    for clip in pipeline.miner().clips() {
        windows.push(
            RenderState::window(
                clip.substrate.clone(),
            ),
        );
    }

    loop {
        for window in &mut windows {
            let substrate = window.substrate.as_str();

            let persisted = refresh(
                &mut pipeline,
                substrate,
            );

            let _ = persisted;
        }
    }
}

#[inline(always)]
fn refresh(
    pipeline: &mut BitcoinObservationPipeline,
    substrate: &str,
) -> PersistedEvent {
    let event: QP44Event = pipeline
        .miner_mut()
        .submit(substrate);

    PersistedEvent::Transit(event)
}