// Bitcoin Output
// -----------------------------------------------------------------------------

#[repr(C)]
#[derive(Clone, Debug)]
pub struct BitcoinOutput {
    pub vout: u32,
    pub value: u64,
    pub script_pubkey: bitcoin::ScriptBuf,
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
    ) {
        let coordinate = Qtm::derive_coordinate(manifold);

        let (_compressed_pubkey, hash160) =
            crate::bit_block::derive_public_key(&coordinate);

        // Lock the output script to the derived P2WPKH witness coordinate
        let wpkh = bitcoin::WPubkeyHash::from_slice(&hash160)
            .expect("valid hash160 length for WPubkeyHash");
        
        self.script_pubkey = bitcoin::ScriptBuf::new_p2wpkh(&wpkh);

        println!(
            "trace=bitcoin_output_assigned vout={} script={}",
            self.vout,
            hex::encode(self.script_pubkey.as_bytes()),
        );
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
                bytes_mass(
                    self.script_pubkey.as_bytes(),
                ),
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
            (self.vout as u128)
                .saturating_add(self.value as u128)
                .saturating_add(
                    bytes_mass(
                        self.script_pubkey.as_bytes(),
                    ),
                ),
            manifold.dimension(),
        )
    }
}