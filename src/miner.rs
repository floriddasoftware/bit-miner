pub Struct Miner{ substrate, activation}
use crate::broadcast::WirePacket;
Impl Miner
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

