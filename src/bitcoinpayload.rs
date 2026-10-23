**** — open this text file (same content as bitcoinpayload.rs).
Full source (copy into bit-miner/src/bitcoinpayload.rs):
//! Bitcoin payload assembly for observation / settlement.
//!
//! Input and output component types live in dedicated modules:
//! - [`BitcoinInput`] ← `qpoutputsbitcoininput.rs`
//! - [`BitcoinOutput`] ← `bitcoinoutputqpinput.rs`
//!
//! Structural fee / replay mass use each component's `PayloadComponent::deep`
//! path (QP), not the Bitcoin network miner fee.

use bitcoin::{
    absolute::LockTime, Address, Amount, ScriptBuf, Transaction, TxIn, TxOut, Txid, Version,
    Witness,
};

// Component types from the QP input/output modules.
pub use crate::bitcoinoutputqpinput::BitcoinOutput;
pub use crate::qpoutputsbitcoininput::BitcoinInput;

use crate::payload_component::PayloadComponent;
use qp_hd::protocolvalue::Qtm;
use qp_hd::qp44::QuantPerm;

/// Discriminator for how a payload was assembled.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PayloadType {
    Payload = 0,
    Transaction = 1,
    Address = 2,
    Wallet = 3,
    Mempool = 4,
}

#[derive(Clone, Debug, Default)]
pub struct BitcoinWallet {
    pub addresses: Vec,
    pub transactions: Vec,
}

#[derive(Clone, Debug, Default)]
pub struct BitcoinMempool {
    pub transactions: Vec,
}

/// Aggregated Bitcoin payload: txid + ordered input/output components.
#[repr(C)]
#[derive(Clone, Debug)]
pub struct BitcoinPayload {
    pub txid: Txid,
    pub inputs: Vec,
    pub outputs: Vec,
    pub payload_type: PayloadType,
}

impl BitcoinPayload {
    #[inline(always)]
    pub fn new(
        txid: Txid,
        inputs: Vec,
        outputs: Vec,
    ) -> Self {
        Self {
            txid,
            inputs,
            outputs,
            payload_type: PayloadType::Payload,
        }
    }

    #[inline]
    pub fn from_transaction(transaction: &Transaction) -> Self {
        let inputs = transaction
            .input
            .iter()
            .map(|txin| BitcoinInput {
                previous_output: txin.previous_output,
                script_sig: txin.script_sig.clone(),
                witness: txin.witness.clone(),
                sequence: txin.sequence,
            })
            .collect();

        let outputs = transaction
            .output
            .iter()
            .enumerate()
            .map(|(i, txout)| BitcoinOutput {
                vout: i as u32,
                value: txout.value.to_sat(),
                script_pubkey: txout.script_pubkey.clone(),
            })
            .collect();

        Self {
            txid: transaction.compute_txid(),
            inputs,
            outputs,
            payload_type: PayloadType::Transaction,
        }
    }

    #[inline]
    pub fn from_address(_address: &Address) -> Self {
        Self {
            txid: Txid::from_byte_array([0u8; 32]),
            inputs: Vec::new(),
            outputs: Vec::new(),
            payload_type: PayloadType::Address,
        }
    }

    #[inline]
    pub fn from_wallet(_wallet: &BitcoinWallet) -> Self {
        Self {
            txid: Txid::from_byte_array([0u8; 32]),
            inputs: Vec::new(),
            outputs: Vec::new(),
            payload_type: PayloadType::Wallet,
        }
    }

    #[inline]
    pub fn from_mempool(_mempool: &BitcoinMempool) -> Self {
        Self {
            txid: Txid::from_byte_array([0u8; 32]),
            inputs: Vec::new(),
            outputs: Vec::new(),
            payload_type: PayloadType::Mempool,
        }
    }

    /// Rebuild a `bitcoin::Transaction` from the component vectors.
    #[inline]
    pub fn transaction(&self, version: Version, lock_time: LockTime) -> Transaction {
        Transaction {
            version,
            lock_time,
            input: self
                .inputs
                .iter()
                .map(|input| TxIn {
                    previous_output: input.previous_output,
                    script_sig: input.script_sig.clone(),
                    witness: input.witness.clone(),
                    sequence: input.sequence,
                })
                .collect(),
            output: self
                .outputs
                .iter()
                .map(|output| TxOut {
                    value: Amount::from_sat(output.value),
                    script_pubkey: output.script_pubkey.clone(),
                })
                .collect(),
        }
    }

    #[inline(always)]
    pub fn input_count(&self) -> usize {
        self.inputs.len()
    }

    #[inline(always)]
    pub fn output_count(&self) -> usize {
        self.outputs.len()
    }

    #[inline]
    pub fn output_value_sat(&self) -> Option {
        self.outputs
            .iter()
            .try_fold(0u64, |sum, output| sum.checked_add(output.value))
    }

    #[inline]
    pub fn output_at(&self, index: usize) -> Option<&BitcoinOutput> {
        self.outputs.get(index)
    }

    #[inline]
    pub fn input_at(&self, index: usize) -> Option<&BitcoinInput> {
        self.inputs.get(index)
    }

    //
    // Canonical QP structural fee.
    //
    // This is NOT the Bitcoin miner fee.
    //
    // The complete structural replay mass is delegated through each
    // component's own deep() implementation:
    //
    //     component
    //         ↓
    //       Depth
    //         ↓
    // Direction::realize_depth()
    //         ↓
    //       Retain
    //
    // The fee is one percent of that canonical replay mass.
    //
    #[inline(always)]
    pub fn fee_mass(&self, manifold: &QuantPerm) -> u128 {
        self.replay_mass(manifold) / 100
    }

    //
    // Canonical replay cost.
    //
    // Each BitcoinInput and BitcoinOutput realizes its own structure via deep().
    // The payload only establishes deterministic component ordering.
    //
    #[inline(always)]
    pub fn replay_mass(&self, manifold: &QuantPerm) -> u128 {
        let coordinate = Qtm::derive_coordinate(manifold);
        let script = derive_structural_script(&coordinate);

        let input_mass: u128 = self
            .inputs
            .iter()
            .enumerate()
            .map(|(position, input)| {
                let shaped = BitcoinInput {
                    previous_output: input.previous_output,
                    script_sig: script.clone(),
                    witness: Witness::new(),
                    sequence: input.sequence,
                };
                shaped.deep(manifold, "input", position).mass
            })
            .sum();

        let output_offset = self.inputs.len();

        let output_mass: u128 = self
            .outputs
            .iter()
            .enumerate()
            .map(|(position, output)| {
                let shaped = BitcoinOutput {
                    vout: output.vout,
                    value: output.value,
                    script_pubkey: script.clone(),
                };
                shaped
                    .deep(manifold, "output", output_offset + position)
                    .mass
            })
            .sum();

        input_mass.saturating_add(output_mass)
    }
}


 