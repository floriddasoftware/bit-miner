use qtm_graph::graph::frame::{Depth, Direction};
use quantom_value::{QuantPerm, Retain};
use crate::bitcoinoutputqpinput::BitcoinOutput;
use crate::qpoutputsbitcoininput::BitcoinInput;
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PayloadType {
    Payload = 0,
    Address = 1,
    Wallet = 2,
    Mempool = 3,
}
impl Default for PayloadType {
    #[inline(always)]
    fn default() -> Self {
        Self::Payload
    }
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
    pub addresses: Vec<String>,
}
#[derive(Clone, Debug, Default)]
pub struct BitcoinMempool {
    pub transaction_ids: Vec<bitcoin::Txid>,
}
#[repr(C)]
#[derive(Clone, Debug, Default)]
pub struct BitcoinPayload {
    pub addresses: Vec<String>,
    pub inputs: Vec<BitcoinInput>,
    pub outputs: Vec<BitcoinOutput>,
    pub payload_type: PayloadType,
}
impl BitcoinPayload {
    #[inline(always)]
    pub fn new(
        inputs: Vec<BitcoinInput>,
        outputs: Vec<BitcoinOutput>,
    ) -> Self {
        Self {
            addresses: Vec::new(),
            inputs,
            outputs,
            payload_type: PayloadType::Payload,
        }
    }
    #[inline(always)]
    pub fn from_address(address: impl Into<String>) -> Self {
        Self {
            addresses: vec![address.into()],
            inputs: Vec::new(),
            outputs: Vec::new(),
            payload_type: PayloadType::Address,
        }
    }
    #[inline(always)]
    pub fn from_wallet(wallet: &BitcoinWallet) -> Self {
        Self {
            addresses: wallet.addresses.clone(),
            inputs: Vec::new(),
            outputs: Vec::new(),
            payload_type: PayloadType::Wallet,
        }
    }
    #[inline(always)]
    pub fn from_mempool(_mempool: &BitcoinMempool) -> Self {
        Self {
            addresses: Vec::new(),
            inputs: Vec::new(),
            outputs: Vec::new(),
            payload_type: PayloadType::Mempool,
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
    #[inline(always)]
    pub fn address_count(&self) -> usize {
        self.addresses.len()
    }
    #[inline]
    pub fn output_value_sat(&self) -> Option<u64> {
        self.outputs
            .iter()
            .try_fold(0u64, |sum, output| sum.checked_add(output.value))
    }
    #[inline(always)]
    pub fn output_at(&self, index: usize) -> Option<&BitcoinOutput> {
        self.outputs.get(index)
    }
    #[inline(always)]
    pub fn input_at(&self, index: usize) -> Option<&BitcoinInput> {
        self.inputs.get(index)
    }
    #[inline(always)]
    pub fn retained(&self, manifold: &QuantPerm) -> Retain {
        let input_mass = self
            .inputs
            .iter()
            .enumerate()
            .fold(0u128, |mass, (position, input)| {
                mass.saturating_add(
                    input.deep(manifold, "input", position).mass,
                )
            });
        let output_offset = self.inputs.len();
        let output_mass = self
            .outputs
            .iter()
            .enumerate()
            .fold(0u128, |mass, (position, output)| {
                mass.saturating_add(
                    output
                        .deep(manifold, "output", output_offset + position)
                        .mass,
                )
            });
        let address_offset = self
            .inputs
            .len()
            .saturating_add(self.outputs.len());
        let address_mass = self
            .addresses
            .iter()
            .enumerate()
            .fold(0u128, |mass, (position, address)| {
                let byte_mass = address
                    .as_bytes()
                    .iter()
                    .fold(0u128, |sum, byte| {
                        sum.saturating_add(u128::from(*byte))
                    });
                let retained =
                    manifold.retain(byte_mass, manifold.dimension());
                let mut depth = Depth::new("qp/payload");
                depth.insert("address", address_offset + position, retained);
                mass.saturating_add(
                    Direction::realize_depth(manifold, &depth).mass,
                )
            });
        manifold.retain(
            input_mass
                .saturating_add(output_mass)
                .saturating_add(address_mass),
            manifold.dimension(),
        )
    }
    #[inline(always)]
    pub fn replay_mass(&self, manifold: &QuantPerm) -> u128 {
        self.retained(manifold).mass
    }
    #[inline(always)]
    pub fn fee_mass(&self, manifold: &QuantPerm) -> u128 {
        self.replay_mass(manifold) / 100
    }
}
