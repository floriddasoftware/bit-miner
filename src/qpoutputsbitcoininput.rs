
use std::{
    error::Error,
    fmt,
};

use bitcoin::{
    hashes::Hash,
    secp256k1::{
        Message,
        PublicKey as SecpPublicKey,
        Secp256k1,
        SecretKey,
    },
    Amount,
    EcdsaSighashType,
    OutPoint,
    PublicKey,
    ScriptBuf,
    Sequence,
    Witness,
};
use bitcoin::sighash::SighashCache;
use quantom_value::{QuantPerm, Retain};
use qtm_graph::graph::frame::{Depth, Direction};

use crate::bitcoinpayload::PayloadComponent;

#[derive(Debug)]
pub enum SigningError {
    InputIndexOutOfRange {
        index: usize,
        input_count: usize,
    },
    PreviousOutputMismatch {
        index: usize,
    },
    InvalidPrivateKey(String),
    PreviousOutputScriptMismatch {
        index: usize,
    },
    Sighash(String),
    InvalidMessage(String),
}

impl fmt::Display for SigningError {
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        match self {
            Self::InputIndexOutOfRange {
                index,
                input_count,
            } => write!(
                f,
                "input index {} exceeds transaction input count {}",
                index,
                input_count,
            ),
            Self::PreviousOutputMismatch { index } => write!(
                f,
                "transaction outpoint does not match BitcoinInput at index {}",
                index,
            ),
            Self::InvalidPrivateKey(error) => {
                write!(f, "invalid Bitcoin private key: {}", error)
            }
            Self::PreviousOutputScriptMismatch { index } => write!(
                f,
                "wallet public key does not control previous output at input {}",
                index,
            ),
            Self::Sighash(error) => {
                write!(f, "Bitcoin signature hash failed: {}", error)
            }
            Self::InvalidMessage(error) => {
                write!(f, "invalid Bitcoin signing message: {}", error)
            }
        }
    }
}

impl Error for SigningError {}

#[repr(C)]
#[derive(Clone, Debug)]
pub struct BitcoinInput {
    pub previous_output: OutPoint,
    pub script_sig: ScriptBuf,
    pub witness: Witness,
    pub sequence: Sequence,
}

impl BitcoinInput {
    #[inline(always)]
    fn byte_mass(&self, bytes: &[u8]) -> u128 {
        bytes.iter().fold(0u128, |mass, byte| {
            mass.saturating_add(u128::from(*byte))
        })
    }

    #[inline(always)]
    fn hash_mass(&self, bytes: &[u8]) -> u128 {
        self.byte_mass(bytes)
    }

    #[inline(always)]
    fn previous_output_mass(&self) -> u128 {
        self.hash_mass(
            self.previous_output
                .txid
                .as_raw_hash()
                .as_byte_array(),
        )
        .saturating_add(u128::from(self.previous_output.vout))
    }

    #[inline(always)]
    fn witness_mass(&self) -> u128 {
        self.witness.iter().fold(0u128, |mass, item| {
            mass.saturating_add(self.byte_mass(item))
        })
    }

    #[inline(always)]
    fn payload_mass(&self) -> u128 {
        self.previous_output_mass()
            .saturating_add(
                self.byte_mass(self.script_sig.as_bytes()),
            )
            .saturating_add(self.witness_mass())
    }

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

    #[inline]
    pub fn sign(
        &mut self,
        manifold: &QuantPerm,
        private_key: [u8; 32],
        transaction: &bitcoin::Transaction,
        input_index: usize,
        previous_output_value: u64,
        previous_output_script: &ScriptBuf,
    ) -> Result<Retain, SigningError> {
        let transaction_input =
            transaction.input.get(input_index).ok_or(
                SigningError::InputIndexOutOfRange {
                    index: input_index,
                    input_count: transaction.input.len(),
                },
            )?;

        if transaction_input.previous_output != self.previous_output {
            return Err(
                SigningError::PreviousOutputMismatch {
                    index: input_index,
                },
            );
        }

        let secret_key =
            SecretKey::from_slice(&private_key).map_err(|error| {
                SigningError::InvalidPrivateKey(error.to_string())
            })?;

        let secp = Secp256k1::new();

        let secp_public_key =
            SecpPublicKey::from_secret_key(&secp, &secret_key);

        let public_key = PublicKey::new(secp_public_key);
        let public_key_bytes = public_key.to_bytes();

        let pubkey_hash =
            bitcoin::WPubkeyHash::hash(&public_key_bytes);

        let script_code =
            ScriptBuf::new_p2wpkh(&pubkey_hash);

        let expected_script =
            ScriptBuf::new_p2wpkh(&pubkey_hash);

        if previous_output_script != &expected_script {
            return Err(
                SigningError::PreviousOutputScriptMismatch {
                    index: input_index,
                },
            );
        }

        let sighash = SighashCache::new(transaction)
            .p2wpkh_signature_hash(
                input_index,
                &script_code,
                Amount::from_sat(previous_output_value),
                EcdsaSighashType::All,
            )
            .map_err(|error| {
                SigningError::Sighash(error.to_string())
            })?;

        let message =
            Message::from_slice(&sighash.to_byte_array())
                .map_err(|error| {
                    SigningError::InvalidMessage(
                        error.to_string(),
                    )
                })?;

        let signature =
            secp.sign_ecdsa(&message, &secret_key);

        let mut signature_bytes =
            signature.serialize_der().to_vec();

        signature_bytes.push(
            EcdsaSighashType::All as u8,
        );

        let witness = Witness::from_slice(&[
            signature_bytes.as_slice(),
            public_key_bytes.as_slice(),
        ]);

        self.witness = witness;

        Ok(self.deep(
            manifold,
            "qp/payload",
            input_index,
        ))
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
                self.previous_output_mass(),
                manifold.dimension(),
            ),
        );

        depth.insert(
            "script_sig",
            position.saturating_add(1),
            manifold.retain(
                self.byte_mass(self.script_sig.as_bytes()),
                manifold.dimension(),
            ),
        );

        depth.insert(
            "witness",
            position.saturating_add(2),
            manifold.retain(
                self.witness_mass(),
                manifold.dimension(),
            ),
        );

        Direction::realize_depth(manifold, &depth)
    }

    #[inline(always)]
    fn retained(
        &self,
        manifold: &QuantPerm,
    ) -> Retain {
        manifold.retain(
            self.payload_mass(),
            manifold.dimension(),
        )
    }
}
