
use bitcoin::{
    absolute::LockTime,
    Address,
    Transaction,
    TxIn,
    TxOut,
    Version,
};

pub type BitcoinInput = Vec<TxIn>;
pub type BitcoinOutput = Vec<TxOut>;

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
    pub addresses: Vec<String>,
    pub transactions: Vec<Transaction>,
}

#[derive(Clone, Debug, Default)]
pub struct BitcoinMempool {
    pub transactions: Vec<Transaction>,
}

#[derive(Clone, Debug)]
pub struct BitcoinPayload {
    pub payload_type: PayloadType,
    pub inputs: BitcoinInput,
    pub outputs: BitcoinOutput,
}

pub struct BitcointxPayload {
    pub txid: Txid,
    pub inputs: Vec<BitcoinInput>,
    pub outputs: Vec<BitcoinOutput>,
    pub payload_type: Transaction,  // kept from existing helpers
}

impl BitcoinPayload {
    #[inline]
    pub fn new(inputs: BitcoinInput, outputs: BitcoinOutput) -> Self {
        Self {
            payload_type: PayloadType::Payload,
            inputs,
            outputs,
        }
    }

    #[inline]
    pub fn from_transaction(transaction: &Transaction) -> Self {
        Self {
            payload_type: PayloadType::Transaction,
            inputs: transaction.input.clone(),
            outputs: transaction.output.clone(),
        }
    }

    #[inline]
    pub fn from_address(_address: &Address) -> Self {
        Self {
            payload_type: PayloadType::Address,
            inputs: Vec::new(),
            outputs: Vec::new(),
        }
    }

    #[inline]
    pub fn from_wallet(_wallet: &BitcoinWallet) -> Self {
        Self {
            payload_type: PayloadType::Wallet,
            inputs: Vec::new(),
            outputs: Vec::new(),
        }
    }

    #[inline]
    pub fn from_mempool(_mempool: &BitcoinMempool) -> Self {
        Self {
            payload_type: PayloadType::Mempool,
            inputs: Vec::new(),
            outputs: Vec::new(),
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
            input: self.inputs.clone(),
            output: self.outputs.clone(),
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
        self.outputs.iter().try_fold(0u64, |sum, output| {
            sum.checked_add(output.value.to_sat())
        })
    }

    #[inline]
    pub fn output_at(&self, index: usize) -> Option<&TxOut> {
        self.outputs.get(index)
    }

    #[inline]
    pub fn input_at(&self, index: usize) -> Option<&TxIn> {
        self.inputs.get(index)
    }
}
