use bitcoin::{
absolute::LockTime,
hashes::Hash,
transaction::Version,
Address,
Amount,
ScriptBuf,
Transaction,
TxIn,
TxOut,
Txid,
Witness,
};
use qtm_graph::graph::frame::{Depth, Direction};
use quantom_value::{QuantPerm, Retain};
use qp_hd::protocolvalue::Qtm;

use crate::bitcoinoutputqpinput::BitcoinOutput;
use crate::qpoutputsbitcoininput::BitcoinInput;

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PayloadType {
Payload = 0,
Transaction = 1,
Address = 2,
Wallet = 3,
Mempool = 4,
}

pub trait PayloadComponent: std::fmt::Debug + Send + Sync {
fn retained(&self, manifold: &QuantPerm) -> Retain;

#[inline(always)]
fn deep(
    &self,
    manifold: &QuantPerm,
    identifier: impl Into<String>,
    position: usize,
) -> Retain {
    let mut depth = Depth::new("qp/payload");
    depth.insert(identifier, position, self.retained(manifold));
    Direction::realize_depth(manifold, &depth)
}
#[inline(always)]
fn mass(
    &self,
    manifold: &QuantPerm,
    identifier: impl Into<String>,
    position: usize,
) -> u128 {
    self.deep(manifold, identifier, position).mass
}

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
        .map(|(index, txout)| BitcoinOutput {
            vout: u32::try_from(index).unwrap_or(u32::MAX),
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
#[inline]
pub fn transaction(
    &self,
    version: Version,
    lock_time: LockTime,
) -> Transaction {
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
pub fn output_value_sat(&self) -> Option<u64> {
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
#[inline(always)]
pub fn fee_mass(&self, manifold: &QuantPerm) -> u128 {
    self.replay_mass(manifold) / 100
}
#[inline(always)]
pub fn replay_mass(&self, manifold: &QuantPerm) -> u128 {
    let coordinate = Qtm::derive_coordinate(manifold);
    let script = crate::bit_block::derive_script_pubkey(coordinate)
        .unwrap_or_else(|_| ScriptBuf::new());
    let input_mass = self
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
        .fold(0u128, u128::saturating_add);
    let output_offset = self.inputs.len();
    let output_mass = self
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
        .fold(0u128, u128::saturating_add);
    input_mass.saturating_add(output_mass)
}

}