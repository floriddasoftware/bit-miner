
use std::error::Error;

use bit_miner::{
    observestream::{
        MempoolSpaceStream,
        MempoolStreamMode,
    },
    BitcoinObservationPipeline,
    RenderState,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let _ = rustls::crypto::ring::default_provider()
        .install_default();

    let args = std::env::args().collect::<Vec<_>>();

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
    )?;

    let mut windows = pipeline
        .miner()
        .clips()
        .iter()
        .map(|clip| {
            RenderState::window(clip.substrate.clone())
        })
        .collect::<Vec<_>>();

    let mut stream = MempoolSpaceStream::connect(
        activation,
        MempoolStreamMode::TransactionIds,
    )
    .await?;

    loop {
        for window in &mut windows {
            let substrate = window.substrate.clone();

            match stream.stream(
                &substrate,
                &mut pipeline,
            ).await? {
                Ok(Some(_)) => {}

                Ok(None) => {}

                Err(error) => {
                    eprintln!(
                        "Pipeline settlement failed for {}: {}",
                        substrate,
                        error
                    );
                }
            }
        }
    }
}
