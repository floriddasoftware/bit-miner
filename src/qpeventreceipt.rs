use bitcoin::ScriptBuf;
use qp_hd::{
    protocol_id::LockedStructure,
    qp44::QP44Event,
};

use crate::{
    bit_block::{derive_public_key, p2wpkh_script},
    sharedtypes::SettlementState,
};

#[repr(C)]
pub struct EventReceipt {
    pub event: QP44Event,
    pub state: SettlementState,
    pub locked: LockedStructure,
    pub script_pubkey: ScriptBuf,
}

impl EventReceipt {
    #[inline(always)]
    pub fn new(
        event: QP44Event,
        state: SettlementState,
        locked: LockedStructure,
    ) -> Self {
        let (_, hash160) = derive_public_key(&event.qtm.coordinate);

        Self {
            event,
            state,
            locked,
            script_pubkey: p2wpkh_script(&hash160),
        }
    }

    #[inline(always)]
    pub fn retained_mass(&self) -> u128 {
        self.event.heritage.state.retained_mass()
    }
}