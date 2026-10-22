pub struct CollectorRegistry;

impl CollectorRegistry {

    #[inline(always)]
    pub fn dispatch(
        substrate:&str,
        payload_type:PayloadType,
        event:&QP44Event,
        transaction:bitcoin::Transaction,
        pipeline:&mut BitcoinObservationPipeline,
    ) -> Result<Option<(FinalizedSettlement, Vec<Retain>)>, SettlementError> {

        let activation=
            event
                .heritage
                .state
                .activations();

        let source=
            BitcoinObservationSource::new(
                BitcoinObservationSourceKind::Substrate,
                activation,
            );

        /*
         * Transaction-local topology check.
         *
         * Inputs identify the Bitcoin outputs being spent:
         *
         *     input.previous_output
         *          = producer_txid:vout
         *
         * Outputs identify what this transaction creates.
         *
         * This check deliberately does not consult ChainStateManager.
         * It only determines whether this returning transaction contains
         * an output that can continue the assignment path.
         */
        let has_inputs=
            !transaction
                .input
                .is_empty();

        let has_assignable_outputs=
            transaction
                .output
                .iter()
                .any(|output| {
                    let script=
                        output
                            .script_pubkey
                            .as_bytes();

                    let empty_op_return=
                        output.value.is_zero()
                            && script.first() == Some(&0x6a);

                    !empty_op_return
                });

        if !has_inputs {
            return Ok(None);
        }

        if !has_assignable_outputs {
            return Ok(None);
        }

        /*
         * Notice terminal output assignments without treating them as
         * Bitcoin inputs. An OP_RETURN output is an output assignment
         * boundary; it does not become a QP44 input.
         */
        for (vout, output) in
            transaction.output.iter().enumerate()
        {
            let script=
                output
                    .script_pubkey
                    .as_bytes();

            let empty_op_return=
                output.value.is_zero()
                    && script.first() == Some(&0x6a);

            if empty_op_return {
                println!(
                    "collector=assignment_terminal \
                     substrate={} \
                     txid={} \
                     vout={} \
                     value=0 \
                     op_return=true",
                    substrate,
                    transaction.compute_txid(),
                    vout,
                );
            }
        }

        /*
         * The inputs are the actual Bitcoin spending topology that returns
         * through dispatch. Do not replace these with the transaction's
         * outputs or with the historical inputs of its producer.
         */
        for (index, input) in
            transaction.input.iter().enumerate()
        {
            println!(
                "collector=spend_match \
                 substrate={} \
                 txid={} \
                 input={} \
                 previous_txid={} \
                 previous_vout={} \
                 script_sig_bytes={} \
                 witness_items={}",
                substrate,
                transaction.compute_txid(),
                index,
                input.previous_output.txid,
                input.previous_output.vout,
                input.script_sig.len(),
                input.witness.len(),
            );
        }

        let observed=
            ObservedBitcoinTransaction::from_transaction(
                transaction,
                source,
            );

        BitcoinCollectorRegistry{
            payload_type,
        }
        .dispatch(
            substrate,
            &observed,
            pipeline,
        )
    }
}