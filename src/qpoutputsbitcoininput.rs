
use bitcoin::{
    hashes::Hash,
    secp256k1::{Message, PublicKey as SecpPublicKey, Secp256k1, SecretKey},
    Amount, EcdsaSighashType, OutPoint, PublicKey, ScriptBuf, Sequence,
    Witness,
};
use bitcoin::sighash::SighashCache;
use quantom_value::{QuantPerm, Retain};
use qtm_graph::graph::frame::{Depth, Direction};

use crate::bitcoinpayload::PayloadComponent;

type BitcoinInputResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[repr(C)]
#[derive(Clone, Debug)]
pub struct BitcoinInput {
    pub previous_output: OutPoint,
    pub script_sig: ScriptBuf,
    pub witness: Witness,
    pub sequence: Sequence,
}

#[inline(always)]
fn bytes_mass(bytes: &[u8]) -> u128 {
    bytes.iter().fold(0u128, |mass, byte| {
        mass.saturating_add(*byte as u128)
    })
}

#[inline(always)]
fn hash_mass(bytes: &[u8]) -> u128 {
    bytes_mass(bytes)
}

impl BitcoinInput {
    #[inline(always)]
    pub fn new(
        manifold: &QuantPerm,
        previous_output: OutPoint,
        script_sig: ScriptBuf,
        witness: Witness,
        sequence: Sequence,
    ) -> (Self, Retain) {
        let input = Self {
            previous_output,
            script_sig,
            witness,
            sequence,
        };

        let retained = input.deep(manifold, "qp/payload", 0);
        (input, retained)
    }

    #[inline(always)]
    pub fn assign_public_key(
        &mut self,
        manifold: &QuantPerm,
        public_key: PublicKey,
        position: usize,
    ) -> Retain {
        self.witness = Witness::from_slice(&[
            &[],
            &public_key.to_bytes(),
        ]);

        self.deep(manifold, "qp/payload", position)
    }

    #[inline(always)]
    pub fn assign_public_key_bytes(
        &mut self,
        manifold: &QuantPerm,
        public_key: &[u8],
        position: usize,
    ) -> BitcoinInputResult<Retain> {
        let public_key = SecpPublicKey::from_slice(public_key)?;
        Ok(self.assign_public_key(
            manifold,
            PublicKey::new(public_key),
            position,
        ))
    }

    #[inline]
    pub fn sign(
        &mut self,
        manifold: &QuantPerm,
        private_key: [u8; 32],
        transaction: &bitcoin::Transaction,
        input_index: usize,
        previous_output_value: u64,
    ) -> BitcoinInputResult<Retain> {
        if input_index >= transaction.input.len() {
            return Err("input index is outside the transaction".into());
        }

        if transaction.input[input_index].previous_output != self.previous_output {
            return Err("input outpoint does not match transaction input".into());
        }

        let secret_key = SecretKey::from_slice(&private_key)?;
        let secp = Secp256k1::new();
        let secp_public_key =
            SecpPublicKey::from_secret_key(&secp, &secret_key);
        let public_key = PublicKey::new(secp_public_key);

        let hash160 =
            bitcoin::WPubkeyHash::hash(&public_key.to_bytes());
        let script_code = ScriptBuf::new_p2wpkh(&hash160);

        let sighash = SighashCache::new(transaction)
            .p2wpkh_signature_hash(
                input_index,
                &script_code,
                Amount::from_sat(previous_output_value),
                EcdsaSighashType::All,
            )?;

        let message = Message::from_slice(&sighash.to_byte_array())?;
        let signature = secp.sign_ecdsa(&message, &secret_key);

        let mut signature_bytes = signature.serialize_der().to_vec();
        signature_bytes.push(EcdsaSighashType::All as u8);

        self.witness = Witness::from_slice(&[
            signature_bytes.as_slice(),
            public_key.to_bytes().as_slice(),
        ]);

        Ok(self.deep(manifold, "qp/payload", input_index))
    }
}

impl PayloadComponent for BitcoinInput {
    #[inline(always)]
    fn deep(
        &self,
        manifold: &QuantPerm,
        _: impl Into<String>,
        position: usize,
    ) -> Retain {
        let mut depth = Depth::new("qp/payload");

        depth.insert(
            "previous_output",
            position,
            manifold.retain(
                hash_mass(
                    self.previous_output
                        .txid
                        .as_raw_hash()
                        .as_byte_array(),
                )
                .saturating_add(self.previous_output.vout as u128),
                manifold.dimension(),
            ),
        );

        depth.insert(
            "script_sig",
            position + 1,
            manifold.retain(
                bytes_mass(self.script_sig.as_bytes()),
                manifold.dimension(),
            ),
        );

        depth.insert(
            "witness",
            position + 2,
            manifold.retain(
                self.witness.iter().fold(0u128, |mass, item| {
                    mass.saturating_add(bytes_mass(item))
                }),
                manifold.dimension(),
            ),
        );

        Direction::realize_depth(manifold, &depth)
    }

    #[inline(always)]
    fn retained(&self, manifold: &QuantPerm) -> Retain {
        manifold.retain(
            hash_mass(
                self.previous_output
                    .txid
                    .as_raw_hash()
                    .as_byte_array(),
            )
            .saturating_add(self.previous_output.vout as u128)
            .saturating_add(bytes_mass(self.script_sig.as_bytes()))
            .saturating_add(
                self.witness.iter().fold(0u128, |mass, item| {
                    mass.saturating_add(bytes_mass(item))
                }),
            ),
            manifold.dimension(),
        )
    }
}
