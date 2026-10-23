
use bitcoin::ScriptBuf;
use qp_hd::{
    protocol_id::LockedStructure,
    qp44::PQ44Event,
};

use crate::sharedtypes::SettlementState;

pub struct SettlementReceipt {
    pub event: PQ44Event,
    pub state: SettlementState,
    pub locked: LockedStructure,
    pub script_pubkey: ScriptBuf,
}

impl SettlementReceipt {
    #[inline(always)]
    pub fn new(
        event: PQ44Event,
        state: SettlementState,
        locked: LockedStructure,
        script_pubkey: ScriptBuf,
    ) -> Self {
        Self {
            event,
            state,
            locked,
            script_pubkey,
        }
    }

    #[inline(always)]
    pub fn retained_mass(&self) -> u128 {
        self.event
            .heritage
            .state
            .retained_mass()
    }

    #[inline(always)]
    pub fn delta(&self) -> u128 {
        self.event.heritage.transition.delta
    }
}
