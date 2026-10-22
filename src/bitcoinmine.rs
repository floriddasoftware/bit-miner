
use std::collections::VecDeque;

use crate::broadcast::FinalizedSettlement;

pub struct BitcoinMiner {
    substrate: String,
    activation: u64,
    finalized: VecDeque<FinalizedSettlement>,
}

impl BitcoinMiner {
    #[inline]
    pub fn new(substrate: impl Into<String>, activation: u64) -> Self {
        Self {
            substrate: substrate.into(),
            activation,
            finalized: VecDeque::new(),
        }
    }

    #[inline(always)]
    pub fn substrate(&self) -> &str {
        &self.substrate
    }

    #[inline(always)]
    pub fn activation(&self) -> u64 {
        self.activation
    }

    #[inline(always)]
    pub fn set_activation(&mut self, activation: u64) {
        self.activation = activation;
    }

    #[inline]
    pub fn finalize(&mut self, settlement: FinalizedSettlement) {
        self.finalized.push_back(settlement);
    }

    #[inline(always)]
    pub fn next_finalized(&mut self) -> Option<FinalizedSettlement> {
        self.finalized.pop_front()
    }

    #[inline]
    pub fn drain_finalized(
        &mut self,
    ) -> impl Iterator<Item = FinalizedSettlement> + '_ {
        self.finalized.drain(..)
    }

    #[inline(always)]
    pub fn pending_len(&self) -> usize {
        self.finalized.len()
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.finalized.is_empty()
    }
}
