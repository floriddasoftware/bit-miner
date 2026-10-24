
use std::error::Error;

use bitcoin::{
    hashes::Hash,
    secp256k1::PublicKey as SecpPublicKey,
    PublicKey, ScriptBuf, WPubkeyHash,
};
use quantom_value::{QuantPerm, Retain};
use qtm_graph::graph::frame::{Depth, Direction};

use crate::{
    bit_block::p2wpkh_script,
    bitcoinpayload::PayloadComponent,
};

type BitcoinOutputResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

#[repr(C)]
#[derive(Clone, Debug)]
pub struct BitcoinOutput {
    pub vout: u32,
    pub value: u64,
    pub script_pubkey: ScriptBuf,
}

#[inline(always)]
fn bytes_mass(bytes: &[u8]) -> u128 {
    bytes.iter().fold(0u128, |mass, byte| {
        mass.saturating_add(*byte as u128)
    })
}

impl BitcoinOutput {
    #[inline(always)]
    pub fn new(
        manifold: &QuantPerm,
        vout: u32,
        value: u64,
        script_pubkey: ScriptBuf,
    ) -> (Self, Retain) {
        let output = Self {
            vout,
            value,
            script_pubkey,
        };

        let retained = output.deep(manifold, "qp/payload", vout as usize);
        (output, retained)
    }

    #[inline(always)]
    pub fn assign(
        &mut self,
        manifold: &QuantPerm,
        private_key: [u8; 32],
    ) -> BitcoinOutputResult<Retain> {
        self.script_pubkey = p2wpkh_script(private_key)?;

        println!(
            "trace=assigned vout={} script={}",
            self.vout,
            hex::encode(self.script_pubkey.as_bytes()),
        );

        Ok(self.deep(manifold, "qp/payload", self.vout as usize))
    }

    #[inline(always)]
    pub fn assign_public_key(
        &mut self,
        manifold: &QuantPerm,
        public_key: PublicKey,
    ) -> Retain {
        let hash160 = WPubkeyHash::hash(&public_key.to_bytes());
        self.script_pubkey = ScriptBuf::new_p2wpkh(&hash160);

        println!(
            "trace=assigned_public_key vout={} script={}",
            self.vout,
            hex::encode(self.script_pubkey.as_bytes()),
        );

        self.deep(manifold, "qp/payload", self.vout as usize)
    }

    #[inline(always)]
    pub fn assign_public_key_bytes(
        &mut self,
        manifold: &QuantPerm,
        public_key: &[u8],
    ) -> BitcoinOutputResult<Retain> {
        let secp_public_key = SecpPublicKey::from_slice(public_key)?;
        Ok(self.assign_public_key(
            manifold,
            PublicKey::new(secp_public_key),
        ))
    }
}

impl PayloadComponent for BitcoinOutput {
    #[inline(always)]
    fn deep(
        &self,
        manifold: &QuantPerm,
        _: impl Into<String>,
        position: usize,
    ) -> Retain {
        let mut depth = Depth::new("qp/payload");

        depth.insert(
            "vout",
            position,
            manifold.retain(
                self.vout as u128,
                manifold.dimension(),
            ),
        );

        depth.insert(
            "value",
            position + 1,
            manifold.retain(
                self.value as u128,
                manifold.dimension(),
            ),
        );

        depth.insert(
            "script_pubkey",
            position + 2,
            manifold.retain(
                bytes_mass(self.script_pubkey.as_bytes()),
                manifold.dimension(),
            ),
        );

        Direction::realize_depth(manifold, &depth)
    }

    #[inline(always)]
    fn retained(&self, manifold: &QuantPerm) -> Retain {
        manifold.retain(
            (self.vout as u128)
                .saturating_add(self.value as u128)
                .saturating_add(bytes_mass(self.script_pubkey.as_bytes())),
            manifold.dimension(),
        )
    }
}
