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

        println!(
            "⚡ Miner online substrates={}",
            substrates.len(),
        );

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
}
pub fn submit(
    &mut self,
    substrate: &str,
    realized: u128,
) -> (QP44Event, Vec<WirePacket>)


let (
    event,
    packets,
) =
    miner.submit(
        substrate.as_str(),
        payload.realized,
    );



let receipt =
    EventReceipt {

        event,

        state:
            SettlementState::Pending,
    };


let locked =
    LockedStructure {

        dimension:
            event
                .heritage
                .state
                .dimension(),

        structural_value:
            event
                .heritage
                .state
                .structural_value(),
    };



for packet in packets {

    let authorization =
        Authorization::new(
            packet.commitment,

            event
                .heritage
                .state
                .activations(),

            locked,
        );


    let request =
        SettlementRequest {

            packet,

            authorization,
        };


    settlement_queue
        .lock()
        .unwrap()
        .push(
            request
        );
}


receipt

