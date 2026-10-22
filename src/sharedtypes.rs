use crate::broadcast::WirePacket;
use qp_hd::protocol_id::LockedStructure;


#[repr(u8)]
#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    Hash,
)]
pub enum AuthorizationState {

    Unauthorized = 0,

    Authorized = 1,

    Spent = 2,

    Invalidated = 3,
}



#[repr(u8)]
#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    Hash,
)]
pub enum SettlementState {

    Pending = 0,

    Success = 1,

    Failed = 2,
}



#[repr(C)]
#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
)]
pub struct Authorization {

    pub commitment: [u8;32],

    pub activation: u64,

    pub locked: LockedStructure,
}



impl Authorization {

    #[inline(always)]
    pub fn new(
        commitment: [u8;32],
        activation: u64,
        locked: LockedStructure,
    ) -> Self {

        Self {

            commitment,

            activation,

            locked,
        }
    }
}



//
// Packet + authorization travel together.
// Packet is never reconstructed.
//
#[repr(C)]
#[derive(
    Clone,
    Debug,
)]
pub struct SettlementRequest {

    pub packet: WirePacket,

    pub authorization: Authorization,
}