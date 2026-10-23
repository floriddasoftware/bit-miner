#[repr(C)]
#[derive(Clone, Debug)]
pub struct BitcoinInput {
    pub previous_output: bitcoin::OutPoint,
    pub script_sig: bitcoin::ScriptBuf,
    pub witness: bitcoin::Witness,
    pub sequence: bitcoin::Sequence,
}

impl BitcoinInput {
    #[inline(always)]
    pub fn new(
        manifold: &QuantPerm,
        previous_output: bitcoin::OutPoint,
        script_sig: bitcoin::ScriptBuf,
        witness: bitcoin::Witness,
        sequence: bitcoin::Sequence,
    ) -> (Self, Retain) {
        let input = Self {
            previous_output,
            script_sig,
            witness,
            sequence,
        };

        let retained =
            input.deep(
                manifold,
                "qp/payload",
                0,
            );

        (input, retained)
    }

    #[inline(always)]
    pub fn sign(
        &mut self,
        manifold: &QuantPerm,
        transaction: &bitcoin::Transaction,
        input_index: usize,
        value: u64,
    ) -> Retain {
        let coordinate =
            Qtm::derive_coordinate(
                manifold,
            );

        self.witness =
            crate::bit_block::derive_p2wpkh_witness(
                &coordinate,
                transaction,
                input_index,
                value,
            );

        self.deep(
            manifold,
            "qp/payload",
            input_index,
        )
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
        let mut depth =
            Depth::new("qp/payload");

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
                .saturating_add(
                    self.previous_output.vout as u128,
                ),
                manifold.dimension(),
            ),
        );

        depth.insert(
            "script_sig",
            position + 1,
            manifold.retain(
                bytes_mass(
                    self.script_sig.as_bytes(),
                ),
                manifold.dimension(),
            ),
        );

        depth.insert(
            "witness",
            position + 2,
            manifold.retain(
                self.witness
                    .iter()
                    .map(|item| bytes_mass(item))
                    .sum::<u128>(),
                manifold.dimension(),
            ),
        );

        Direction::realize_depth(
            manifold,
            &depth,
        )
    }

    #[inline(always)]
    fn retained(
        &self,
        manifold: &QuantPerm,
    ) -> Retain {
        manifold.retain(
            hash_mass(
                self.previous_output
                    .txid
                    .as_raw_hash()
                    .as_byte_array(),
            )
            .saturating_add(
                self.previous_output.vout as u128,
            )
            .saturating_add(
                bytes_mass(
                    self.script_sig.as_bytes(),
                ),
            )
            .saturating_add(
                self.witness
                    .iter()
                    .map(|item| bytes_mass(item))
                    .sum::<u128>(),
            ),
            manifold.dimension(),
        )
    }
}