use std::{
    collections::{HashMap, HashSet, VecDeque},
    error::Error,
    str::FromStr,
};

use bitcoin::{
    Amount,
    OutPoint,
    TxOut,
    Txid,
};

use futures_util::{SinkExt, StreamExt};
use qp_hd::commands::{load_budget, load_wirepackets, persist_wirepackets};
use qp_hd::pathsregistry::CoinType;
use serde_json::Value;
use tokio::net::TcpStream;
use tokio_tungstenite::{
    connect_async,
    tungstenite::Message,
    MaybeTlsStream,
    WebSocketStream,
};

use crate::{
    bitcoinpsbt::{BitcoinPsbt, ResolvedPrevout},
    broadcast::{FinalizedSettlement, WirePacket},
    observebitcointransaction::ObservedBitcoinTransaction,
    settlement::SettlementError,
    BitcoinObservationPipeline,
};

pub type Blockchain = CoinType;
pub type ObservationResult<T> = Result<T, Box<dyn Error + Send + Sync>>;
type ParentIndex = HashMap<OutPoint, Vec<(u64, WirePacket)>>;

pub type PipelineOutcome = Result<
    Option<(FinalizedSettlement, BitcoinPsbt, u128, [u8; 32])>,
    SettlementError,
>;

const MEMPOOL_SPACE_WS: &str = "wss://mempool.space/api/v1/ws";
const MEMPOOL_SPACE_API: &str = "https://mempool.space/api";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum BlockchainStream {
    Transactions,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MempoolStreamMode {
    TransactionIds,
    FullTransactions,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BitcoinObservationSource {
    pub activation: u64,
}

impl BitcoinObservationSource {
    #[inline(always)]
    pub const fn mempool_space(activation: u64) -> Self {
        Self { activation }
    }
}

#[derive(Clone, Debug)]
pub enum BitcoinObservation {
    Transaction(bitcoin::Transaction),
}

impl BitcoinObservation {
    #[inline(always)]
    pub const fn stream(&self) -> BlockchainStream {
        BlockchainStream::Transactions
    }

    #[inline(always)]
    pub const fn blockchain(&self) -> Blockchain {
        Blockchain::Bitcoin
    }
}

pub struct MempoolSpaceStream {
    socket: WebSocketStream<MaybeTlsStream<TcpStream>>,
    client: reqwest::Client,
    source: BitcoinObservationSource,
    mode: MempoolStreamMode,
    pending: VecDeque<BitcoinObservation>,
    skipped: u64,
}

impl MempoolSpaceStream {
    pub async fn connect(
        activation: u64,
        mode: MempoolStreamMode,
    ) -> ObservationResult<Self> {
        let (mut socket, _) = connect_async(MEMPOOL_SPACE_WS).await?;

        let subscription = match mode {
            MempoolStreamMode::TransactionIds => {
                r#"{"track-mempool-txids":true}"#
            }
            MempoolStreamMode::FullTransactions => {
                r#"{"track-mempool":true}"#
            }
        };

        socket
            .send(Message::Text(subscription.into()))
            .await?;

        Ok(Self {
            socket,
            client: reqwest::Client::new(),
            source: BitcoinObservationSource::mempool_space(activation),
            mode,
            pending: VecDeque::new(),
            skipped: 0,
        })
    }

    #[inline(always)]
    pub const fn source(&self) -> BitcoinObservationSource {
        self.source
    }

    #[inline(always)]
    pub const fn mode(&self) -> MempoolStreamMode {
        self.mode
    }

    #[inline(always)]
    pub const fn skipped(&self) -> u64 {
        self.skipped
    }

    pub async fn next_observation(
        &mut self,
    ) -> ObservationResult<BitcoinObservation> {
        loop {
            if let Some(observation) = self.pending.pop_front() {
                return Ok(observation);
            }

            self.fill().await?;
        }
    }

    pub async fn stream(
        &mut self,
        substrate: &str,
        pipeline: &mut BitcoinObservationPipeline,
    ) -> ObservationResult<PipelineOutcome> {
        let BitcoinObservation::Transaction(transaction) =
            self.next_observation().await?;

        let observed =
            ObservedBitcoinTransaction::from_observedtransaction(
                transaction,
                self.source,
            );

        Ok(refresh(pipeline, substrate, observed).await)
    }

    async fn fill(&mut self) -> ObservationResult<()> {
        let message = self
            .socket
            .next()
            .await
            .ok_or("Mempool.space WebSocket closed")??;

        let value: Value = match message {
            Message::Text(text) => serde_json::from_str(text.as_ref())?,
            Message::Binary(bytes) => serde_json::from_slice(&bytes)?,
            Message::Close(_) => {
                return Err("Mempool.space WebSocket closed".into());
            }
            Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => {
                return Ok(());
            }
        };

        self.decode(&value).await
    }

    async fn decode(&mut self, value: &Value) -> ObservationResult<()> {
        let mut ids = Vec::<String>::new();
        let mut seen = HashSet::<String>::new();

        let mut want = |id: &str| {
            if seen.insert(id.to_owned()) {
                ids.push(id.to_owned());
            }
        };

        for entry in added(value, "mempool-transactions")
            .into_iter()
            .flatten()
        {
            match entry
                .get("hex")
                .and_then(Value::as_str)
                .map(decode_hex_transaction)
            {
                Some(Ok(tx)) => {
                    self.pending
                        .push_back(BitcoinObservation::Transaction(tx));
                    continue;
                }
                Some(Err(_)) => {
                    self.skipped = self.skipped.saturating_add(1);
                }
                None => {}
            }

            if let Some(id) = entry.get("txid").and_then(Value::as_str) {
                want(id);
            }
        }

        for id in added(value, "mempool-txids")
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
        {
            want(id);
        }

        if let Some(id) = value.get("txid").and_then(Value::as_str) {
            want(id);
        }

        for id in ids {
            let Ok(txid) = Txid::from_str(&id) else {
                self.skipped = self.skipped.saturating_add(1);
                continue;
            };

            match fetch_decode(
                &self.client,
                format!("{MEMPOOL_SPACE_API}/tx/{txid}/raw"),
            )
            .await
            {
                Ok(tx) => {
                    self.pending
                        .push_back(BitcoinObservation::Transaction(tx));
                }
                Err(_) => {
                    self.skipped = self.skipped.saturating_add(1);
                }
            }
        }

        Ok(())
    }
}

#[inline(always)]
fn added<'a>(value: &'a Value, key: &str) -> Option<&'a Vec<Value>> {
    value.get(key)?.get("added")?.as_array()
}

#[inline(always)]
fn decode_hex_transaction(
    hex_tx: &str,
) -> ObservationResult<bitcoin::Transaction> {
    Ok(bitcoin::consensus::deserialize(&hex::decode(hex_tx)?)?)
}

async fn fetch_decode(
    client: &reqwest::Client,
    url: String,
) -> ObservationResult<bitcoin::Transaction> {
    let raw = client
        .get(url)
        .send()
        .await?
        .error_for_status()?
        .bytes()
        .await?;

    Ok(bitcoin::consensus::deserialize(&raw)?)
}

#[inline(always)]
fn fail(message: impl Into<String>) -> SettlementError {
    SettlementError::Backend(message.into())
}

#[inline(always)]
fn packet_outpoint(packet: &WirePacket) -> OutPoint {
    OutPoint::new(packet.txid(), packet.vout())
}


/// Validates the clip and budget, processes the observation, resolves
/// miner-generated and external Bitcoin prevouts, persists and verifies
/// wire packets, signs the PSBT, and broadcasts the signed Bitcoin
/// transaction and QTM packets.
pub async fn refresh(
    pipeline: &mut BitcoinObservationPipeline,
    substrate: &str,
    observed: ObservedBitcoinTransaction,
) -> PipelineOutcome {
    let activation = pipeline
        .miner
        .clip(substrate)
        .map(|clip| clip.activation)
        .ok_or_else(|| {
            fail(format!(
                "no active miner clip: substrate={substrate}"
            ))
        })?;

    let network = pipeline.network().to_owned();

    if observed.source.activation != activation {
        return Err(fail(format!(
            "observation activation mismatch: substrate={substrate} observation={} active={activation}",
            observed.source.activation,
        )));
    }

    let budget = load_budget(substrate, activation, &network).map_err(|e| {
        fail(format!(
            "budget load failed: substrate={substrate} activation={activation} network={network}: {e}"
        ))
    })?;

    let persisted = pipeline
        .miner
        .submissions
        .get(substrate)
        .copied()
        .unwrap_or(0);

    if persisted > budget.max_events {
        return Err(fail(format!(
            "submission count exceeds budget: substrate={substrate} activation={activation} network={network} persisted={persisted} max={}",
            budget.max_events,
        )));
    }

    let submission = persisted
        .checked_add(1)
        .filter(|next| *next <= budget.max_events)
        .ok_or_else(|| {
            fail(format!(
                "submission budget exhausted: substrate={substrate} activation={activation} network={network} max={}",
                budget.max_events,
            ))
        })?;

    // Index persisted packets from every previous submission.
    let mut parents = ParentIndex::new();

    for parent_submission in 1..=persisted {
        let bytes = load_wirepackets(
            substrate,
            activation,
            &network,
            parent_submission,
        )
        .map_err(|e| {
            fail(format!(
                "wirepacket load failed: substrate={substrate} activation={activation} network={network} submission={parent_submission}: {e}"
            ))
        })?;

        let packets = decode_wirepackets(&bytes)?;

        let commitment = packets
            .first()
            .ok_or_else(|| {
                fail(format!(
                    "persisted wire packets are empty: substrate={substrate} activation={activation} network={network} submission={parent_submission}"
                ))
            })?
            .commitment;

        if packets
            .iter()
            .any(|packet| packet.commitment != commitment)
        {
            return Err(fail(format!(
                "mixed commitments in persisted wire packets: substrate={substrate} activation={activation} network={network} submission={parent_submission}"
            )));
        }

        for packet in packets {
            parents
                .entry(packet_outpoint(&packet))
                .or_default()
                .push((parent_submission, packet));
        }
    }

    let Some((finalized, payload, retained_mass, commitment)) =
        pipeline.process(substrate, observed)?
    else {
        return Ok(None);
    };

    let tx = &finalized.transaction;

    // Validate materialized inputs.
    if payload.inputs.len() != tx.input.len()
        || payload
            .inputs
            .iter()
            .zip(&tx.input)
            .any(|(payload_input, tx_input)| {
                payload_input.previous_output != tx_input.previous_output
            })
    {
        return Err(fail(
            "materialized inputs do not match finalized transaction",
        ));
    }

    // Validate materialized outputs.
    if payload.outputs.len() != tx.output.len()
        || payload
            .outputs
            .iter()
            .zip(&tx.output)
            .enumerate()
            .any(|(index, (payload_output, tx_output))| {
                payload_output.vout as usize != index
                    || payload_output.value != tx_output.value.to_sat()
                    || payload_output.script_pubkey != tx_output.script_pubkey
            })
    {
        return Err(fail(
            "materialized outputs do not match finalized transaction",
        ));
    }

    let generated = finalized.wire_packets(commitment);

    if generated.is_empty() {
        return Err(fail(
            "finalized settlement produced no wire packets",
        ));
    }

    let txid = tx.compute_txid();

    let matches = |packet: &WirePacket| {
        packet.txid() == txid && packet.commitment == commitment
    };

    if !generated.iter().all(matches) {
        return Err(fail(
            "generated packet does not match finalized settlement",
        ));
    }

    // Resolve every input before persisting or committing this submission.
    let client = reqwest::Client::new();
    let mut prevouts = Vec::with_capacity(tx.input.len());

    for (i, input) in tx.input.iter().enumerate() {
        let outpoint = &input.previous_output;

        let resolved = match parents.get(outpoint).map(Vec::as_slice) {
            // Parent was created by a previous QTM submission.
            Some([(parent_submission, packet)]) => {
                let resolved = ResolvedPrevout {
                    outpoint: packet.outpoint,
                    output: TxOut {
                        value: Amount::from_sat(packet.value),
                        script_pubkey: packet.script_pubkey.clone(),
                    },
                    submission: *parent_submission,
                    commitment: packet.commitment,
                };

                if resolved.outpoint != *outpoint {
                    return Err(fail(format!(
                        "resolved QTM prevout mismatch: input={i} expected={outpoint} resolved={}",
                        resolved.outpoint,
                    )));
                }

                if resolved.output.value.to_sat() != packet.value
                    || resolved.output.script_pubkey
                        != packet.script_pubkey
                {
                    return Err(fail(format!(
                        "resolved QTM prevout output mismatch: input={i} outpoint={outpoint}"
                    )));
                }

                resolved
            }

            Some(_) => {
                return Err(fail(format!(
                    "ambiguous parent packets: input={i} outpoint={outpoint}"
                )));
            }

            // Parent is not in QTM persistence. Resolve it from Bitcoin.
            None => {
                let parent_txid = outpoint.txid;

                let parent_transaction = fetch_decode(
                    &client,
                    format!(
                        "{MEMPOOL_SPACE_API}/tx/{parent_txid}/raw"
                    ),
                )
                .await
                .map_err(|e| {
                    fail(format!(
                        "Bitcoin parent transaction lookup failed: input={i} outpoint={outpoint}: {e}"
                    ))
                })?;

                let actual_parent_txid =
                    parent_transaction.compute_txid();

                if actual_parent_txid != parent_txid {
                    return Err(fail(format!(
                        "Bitcoin parent transaction ID mismatch: input={i} expected={parent_txid} actual={actual_parent_txid}"
                    )));
                }

                let parent_output = parent_transaction
                    .output
                    .get(outpoint.vout as usize)
                    .ok_or_else(|| {
                        fail(format!(
                            "Bitcoin parent output not found: input={i} outpoint={outpoint} outputs={}",
                            parent_transaction.output.len(),
                        ))
                    })?;

                ResolvedPrevout {
                    outpoint: *outpoint,
                    output: parent_output.clone(),

                    // Zero identifies an external Bitcoin prevout,
                    // not a QTM submission.
                    submission: 0,
                    commitment: [0u8; 32],
                }
            }
        };

        if resolved.outpoint != *outpoint {
            return Err(fail(format!(
                "resolved prevout mismatch: input={i} expected={outpoint} resolved={}",
                resolved.outpoint,
            )));
        }

        prevouts.push(resolved);
    }

    // Build the PSBT only after all Bitcoin prevouts have been resolved.
    let mut psbt = BitcoinPsbt::from_settlement(&finalized, prevouts)
        .map_err(|e| {
            fail(format!(
                "Bitcoin PSBT preparation failed: {e}"
            ))
        })?;

    // Serialize the current submission's generated QTM packets.
    let mut packet_bytes = Vec::new();

    for packet in &generated {
        let body = packet.serialize();

        if body.is_empty() {
            return Err(fail(
                "wire-packet serialization failed",
            ));
        }

        let len = u32::try_from(body.len())
            .map_err(|e| fail(e.to_string()))?;

        packet_bytes.extend_from_slice(&len.to_be_bytes());
        packet_bytes.extend_from_slice(&body);
    }

    // Persist the current submission.
    persist_wirepackets(
        substrate,
        activation,
        &network,
        submission,
        &packet_bytes,
    )
    .map_err(|e| {
        fail(format!(
            "wire-packet persistence failed: substrate={substrate} activation={activation} network={network} submission={submission}: {e}"
        ))
    })?;

    // Reload and verify the exact persisted submission.
    let persisted_bytes = load_wirepackets(
        substrate,
        activation,
        &network,
        submission,
    )
    .map_err(|e| {
        fail(format!(
            "wire-packet reload failed: substrate={substrate} activation={activation} network={network} submission={submission}: {e}"
        ))
    })?;

    let wire_packets = decode_wirepackets(&persisted_bytes)?;

    if wire_packets.len() != generated.len()
        || !wire_packets.iter().all(matches)
    {
        return Err(fail(
            "persisted packets differ from finalized submission",
        ));
    }

    println!(
        "qtm_wirepackets=persisted substrate={substrate} activation={activation} network={network} submission={submission} commitment={} packets={} bytes={}",
        hex::encode(commitment),
        wire_packets.len(),
        persisted_bytes.len(),
    );

    println!(
        "bitcoin_psbt=prepared submission={submission} unsigned_txid={}",
        psbt.unsigned_txid(),
    );

    // Sign the PSBT using the finalized settlement receipt.
    let settlementreceipt = finalized
        .receipt
        .as_ref()
        .ok_or_else(|| {
            fail("finalized settlement has no receipt")
        })?;

    let coordinate = settlementreceipt.event.qtm.coordinate;
    let manifold = &settlementreceipt.event.heritage.state;

    let unsigned_transaction = &psbt.psbt.unsigned_tx;
    let mut signed_transaction = unsigned_transaction.clone();

    // Sign each materialized input against its resolved previous output.
    for (index, payload_input) in payload.inputs.iter_mut().enumerate() {
        let transaction_input = unsigned_transaction
            .input
            .get(index)
            .ok_or_else(|| {
                fail(format!(
                    "PSBT transaction input missing: index={index}"
                ))
            })?;

        let outpoint = transaction_input.previous_output;

        let psbt_input = psbt
            .psbt
            .inputs
            .get(index)
            .ok_or_else(|| {
                fail(format!(
                    "PSBT input metadata missing: index={index}"
                ))
            })?;

        let previous_output = psbt_input
            .witness_utxo
            .as_ref()
            .ok_or_else(|| {
                fail(format!(
                    "PSBT witness UTXO missing: input={index} outpoint={outpoint}"
                ))
            })?;

        let value = previous_output.value.to_sat();

        if payload_input.previous_output != outpoint {
            return Err(fail(format!(
                "Bitcoin input outpoint mismatch: input={index} PSBT={outpoint} payload={}",
                payload_input.previous_output,
            )));
        }

        if payload_input.value != value {
            return Err(fail(format!(
                "Bitcoin input value mismatch: input={index} PSBT={value} payload={}",
                payload_input.value,
            )));
        }

        payload_input
            .sign(
                manifold,
                coordinate,
                unsigned_transaction,
                index,
                value,
                &previous_output.script_pubkey,
            )
            .map_err(|e| {
                fail(format!(
                    "Bitcoin input signing failed: input={index} outpoint={outpoint}: {e}"
                ))
            })?;

        // Apply the signature witness to the transaction.
        signed_transaction.input[index].script_sig =
            payload_input.script_sig.clone();

        signed_transaction.input[index].witness =
            payload_input.witness.clone();

        // Finalize the PSBT input internally.
        psbt.psbt.inputs[index].final_script_sig =
            Some(payload_input.script_sig.clone());

        psbt.psbt.inputs[index].final_script_witness =
            Some(payload_input.witness.clone());

        println!(
            "bitcoin_input=signed index={index} outpoint={outpoint} value_sat={value} witness_items={}",
            payload_input.witness.len(),
        );
    }

    // Submit the fully signed Bitcoin transaction to Electrum.
    let broadcast_txid = pipeline
        .electrum
        .submit(signed_transaction.clone())
        .map_err(|e| {
            fail(format!(
                "Electrum broadcast submission failed: {e}"
            ))
        })?;

    println!(
        "bitcoin_broadcast=submitted submission={submission} txid={broadcast_txid}",
    );

    // Commit the submission after signing and Electrum enqueue succeed.
    pipeline
        .miner
        .submissions
        .insert(substrate.to_owned(), submission);

    println!(
        "bitcoin_psbt=signed submission={submission} txid={broadcast_txid}",
    );

    // Broadcast QTM packets after Bitcoin submission.
    for packet in wire_packets {
        pipeline.miner().broadcaster.publish(packet);
    }

    Ok(Some((
    finalized,
    psbt,
    retained_mass,
    commitment,
)))
}


pub fn decode_wirepackets(
    bytes: &[u8],
) -> Result<Vec<WirePacket>, SettlementError> {
    let mut rest = bytes;
    let mut packets = Vec::new();

    while !rest.is_empty() {
        let (head, tail) = rest
            .split_at_checked(4)
            .ok_or_else(|| {
                fail("truncated wire-packet frame header")
            })?;

        let len = u32::from_be_bytes(
            head.try_into()
                .map_err(|_| {
                    fail("invalid wire-packet frame header")
                })?,
        ) as usize;

        if !(WirePacket::MIN_SIZE..=WirePacket::MAX_SIZE)
            .contains(&len)
        {
            return Err(fail(format!(
                "invalid wire-packet frame length={len}"
            )));
        }

        let (frame, tail) = tail
            .split_at_checked(len)
            .ok_or_else(|| {
                fail("truncated wire-packet frame payload")
            })?;

        packets.push(
            WirePacket::deserialize(frame)
                .map_err(|e| {
                    fail(format!(
                        "wire-packet deserialization failed: {e}"
                    ))
                })?,
        );

        rest = tail;
    }

    if packets.is_empty() {
        return Err(fail(
            "persisted wire-packet file is empty",
        ));
    }

    Ok(packets)
}