use std::{
    env,
};

use bit_minner::{
    CollectorRegistry,
    Miner,
    PayloadType,
    RenderState,
};

use qp_hd::{
    PersistedEvent,
    QP44Event,
};


fn main() {

    let args =
        env::args()
            .collect::<Vec<_>>();

    let activation =
        args.get(1)
            .expect("missing activation")
            .parse::<u64>()
            .expect("invalid activation");


    let mut miner =
        Miner::new(
            activation,
        );


    let mut windows =
        Vec::new();


    for clip in &miner.clips {

        windows.push(
            RenderState::window(
                clip.substrate.clone(),
            )
        );
    }


    loop {

        for window in &mut windows {

            let substrate =
                window.substrate.as_str();


            let payload_type =
                PayloadType::BitcoinUTXO;


            let persisted =
                refresh(
                    &mut miner,
                    substrate,
                );


            if let Ok(Some(finalized)) =
                CollectorRegistry::dispatch(
                    substrate,
                    payload_type,
                    persisted,
                )
            {

                let packet =
                    finalized.txid();


                let (
                    heritage,
                    scale,
                ) =
                    finalized.frame();


                let activation =
                    heritage
                        .state
                        .activations();


                let retained =
                    heritage
                        .state
                        .retained_mass();


                window.frames(
                    heritage,
                    scale,
                );


                println!(
                    "| packet: {:?} | activation: {} | qp kg unit: {} |",
                    packet,
                    activation,
                    retained,
                );
            }
        }
    }
}


#[inline(always)]
fn refresh(
    miner: &mut Miner,
    substrate: &str,
) -> PersistedEvent {

    let event: QP44Event =
        miner.submit(
            substrate,
        );


    PersistedEvent::Transit(
        event,
    )
}