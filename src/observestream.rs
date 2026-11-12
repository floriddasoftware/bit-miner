use std::{
    collections::{HashSet, VecDeque},
    error::Error,
    io::ErrorKind,
    str::FromStr,
};

use bitcoin::Txid;
use futures_util::{SinkExt, StreamExt};
use qp_hd::commands::{load_budget, load_wirepackets, persist_wirepackets};
use qp_hd::pathsregistry::CoinType;
use serde_json::Value;
use tokio::net::TcpStream;
use tokio_tungstenite::{
    connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream,
};

use crate::{
    bitcoinpsbt::{BitcoinPsbt, PrevoutResolver},
    broadcast::{FinalizedSettlement, WirePacket},
    observebitcointransaction::ObservedBitcoinTransaction,
    settlement::SettlementError,
    BitcoinObservationPipeline,
};

pub type Blockchain = CoinType;

pub type ObservationResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

/// Unsigned coordination result: canonical finalized settlement, unsigned PSBT
/// for the wallet to sign, retained mass, QTM commitment, depth.
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
            MempoolStreamMode::TransactionIds => r#"{"track-mempool-txids":true}"#,
            MempoolStreamMode::FullTransactions => r#"{"track-mempool":true}"#,
        };

        socket.send(Message::Text(subscription.into())).await?;

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

    pub async fn next_observation(&mut self) -> ObservationResult<BitcoinObservation> {
        loop {
            if let Some(observation) = self.pending.pop_front() {
                return Ok(observation);
            }
            self.fill().await?;
        }
    }

    pub async fn stream<R: PrevoutResolver>(
        &mut self,
        substrate: &str,
        pipeline: &mut BitcoinObservationPipeline,
        resolver: &R,
    ) -> ObservationResult<PipelineOutcome> {
        let BitcoinObservation::Transaction(transaction) =
            self.next_observation().await?;

        let observed = ObservedBitcoinTransaction::from_observedtransaction(
            transaction,
            self.source,
        );

        Ok(refresh(pipeline, substrate, observed, resolver))
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
            Message::Close(_) => return Err("Mempool.space WebSocket closed".into()),
            Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => return Ok(()),
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

        for entry in added(value, "mempool-transactions").into_iter().flatten() {
            match entry
                .get("hex")
                .and_then(Value::as_str)
                .map(decode_hex_transaction)
            {
                Some(Ok(tx)) => {
                    self.pending.push_back(BitcoinObservation::Transaction(tx));
                    continue;
                }
                Some(Err(_)) => self.skipped = self.skipped.saturating_add(1),
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

            match fetch_decode(&self.client, format!("{MEMPOOL_SPACE_API}/tx/{txid}/raw")).await {
                Ok(tx) => self.pending.push_back(BitcoinObservation::Transaction(tx)),
                Err(_) => self.skipped = self.skipped.saturating_add(1),
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
fn decode_hex_transaction(hex_tx: &str) -> ObservationResult<bitcoin::Transaction> {
    Ok(bitcoin::consensus::deserialize(&hex::decode(hex_tx)?)?)
}

async fn fetch_decode(
    client: &reqwest::Client,
    url: String,
) -> ObservationResult<bitcoin::Transaction> {
    let raw = client.get(url).send().await?.error_for_status()?.bytes().await?;
    Ok(bitcoin::consensus::deserialize(&raw)?)
}

#[inline(always)]
fn fail(message: impl Into<String>) -> SettlementError {
    SettlementError::Backend(message.into())
}

/// Server-side flow: validate clip/budget, process, validate transaction against
/// payload, persist wire packets, resolve parent outpoints, build unsigned PSBT.
/// No private keys, no signing, no Electrum submission: the wallet signs.
pub fn refresh<R: PrevoutResolver>(
    pipeline: &mut BitcoinObservationPipeline,
    substrate: &str,
    observed: ObservedBitcoinTransaction,
    resolver: &R,
) -> PipelineOutcome {
    let activation = pipeline
        .miner
        .clip(substrate)
        .map(|clip| clip.activation)
        .ok_or_else(|| fail(format!("no active miner clip: substrate={substrate}")))?;

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

    let mut persisted = 0u64;
    let mut ended = false;

    for number in 1..=budget.max_events {
        match load_wirepackets(substrate, activation, number) {
            Ok(bytes) => {
                if ended {
                    return Err(fail(format!(
                        "submission sequence gap: substrate={substrate} activation={activation} missing_before={number}"
                    )));
                }
                let packets = decode_wirepackets(&bytes)?;
                let commitment = packets[0].commitment;
                if packets.iter().any(|p| p.commitment != commitment) {
                    return Err(fail(format!("mixed commitments in submission: {number}")));
                }
                persisted = number;
            }
            Err(e) if e.kind() == ErrorKind::NotFound => ended = true,
            Err(e) => {
                return Err(fail(format!(
                    "wirepacket load failed: substrate={substrate} activation={activation} submission={number}: {e}"
                )));
            }
        }
    }

    let miner_submission = pipeline
        .miner
        .submissions
        .get(substrate)
        .copied()
        .unwrap_or(0);

    if miner_submission > persisted {
        return Err(fail(format!(
            "miner counter exceeds persisted sequence: miner={miner_submission} persisted={persisted}"
        )));
    }

    pipeline
        .miner
        .submissions
        .insert(substrate.to_owned(), persisted);

    let submission = persisted
        .checked_add(1)
        .filter(|next| *next <= budget.max_events)
        .ok_or_else(|| {
            fail(format!(
                "submission budget exhausted: substrate={substrate} activation={activation} network={network} max={}",
                budget.max_events,
            ))
        })?;

    let Some((finalized, payload, retained_mass, commitment)) =
    pipeline.process(substrate, observed)?
else {
    return Ok(None);
};

    let actual = pipeline
        .miner
        .submissions
        .get(substrate)
        .copied()
        .unwrap_or(0);

    if actual != submission {
        return Err(fail(format!(
            "submission mismatch: expected={submission} actual={actual}"
        )));
    }

    let tx = &finalized.transaction;

    if payload.inputs.len() != tx.input.len()
        || payload
            .inputs
            .iter()
            .zip(&tx.input)
            .any(|(p, t)| p.previous_output != t.previous_output)
    {
        return Err(fail("materialized inputs do not match finalized transaction"));
    }

    if payload.outputs.len() != tx.output.len()
        || payload.outputs.iter().zip(&tx.output).enumerate().any(|(i, (p, t))| {
            p.vout as usize != i
                || p.value != t.value.to_sat()
                || p.script_pubkey != t.script_pubkey
        })
    {
        return Err(fail("materialized outputs do not match finalized transaction"));
    }

    let generated = finalized.wire_packets(commitment);

    if generated.is_empty() {
        return Err(fail("finalized settlement produced no wire packets"));
    }

    let txid = tx.compute_txid();
    let matches = |p: &WirePacket| p.txid() == txid && p.commitment == commitment;

    if !generated.iter().all(matches) {
        return Err(fail("generated packet does not match finalized settlement"));
    }

    let mut packet_bytes = Vec::new();

    for packet in &generated {
        let body = packet.serialize();
        if body.is_empty() {
            return Err(fail("wire-packet serialization failed"));
        }
        let len = u32::try_from(body.len()).map_err(|e| fail(e.to_string()))?;
        packet_bytes.extend_from_slice(&len.to_be_bytes());
        packet_bytes.extend_from_slice(&body);
    }

    persist_wirepackets(substrate, activation, submission, &packet_bytes).map_err(|e| {
        fail(format!(
            "wire-packet persistence failed: substrate={substrate} activation={activation} submission={submission}: {e}"
        ))
    })?;

    let persisted_bytes = load_wirepackets(substrate, activation, submission)
        .map_err(|e| fail(format!("wire-packet reload failed: submission={submission}: {e}")))?;

    let wire_packets = decode_wirepackets(&persisted_bytes)?;

    if wire_packets.len() != generated.len() || !wire_packets.iter().all(matches) {
        return Err(fail("persisted packets differ from finalized submission"));
    }

    println!(
        "qtm_wirepackets=persisted substrate={substrate} activation={activation} network={network} submission={submission} commitment={} packets={} bytes={}",
        hex::encode(commitment),
        wire_packets.len(),
        persisted_bytes.len(),
    );

    let receipt = finalized
        .receipt
        .as_ref()
        .ok_or_else(|| fail("finalized settlement has no receipt"))?;

    let previous_submission = receipt.event.heritage.state.activations();
    let previous_commitment = receipt.event.qtm.commitment;

    let mut prevouts = Vec::with_capacity(tx.input.len());

    for (i, input) in tx.input.iter().enumerate() {
        let outpoint = &input.previous_output;

        let resolved = resolver
            .resolve(outpoint, previous_submission, previous_commitment)
            .map_err(|e| {
                fail(format!(
                    "outpoint resolution failed: input={i} outpoint={outpoint} previous_submission={previous_submission} commitment={}: {e}",
                    hex::encode(previous_commitment),
                ))
            })?;

        if resolved.outpoint != *outpoint
            || resolved.submission != previous_submission
            || resolved.commitment != previous_commitment
        {
            return Err(fail(format!(
                "resolved prevout mismatch: input={i} outpoint={outpoint}"
            )));
        }

        prevouts.push(resolved);
    }

    // BitcoinPsbt::from_settlement must borrow &FinalizedSettlement and hold only
    // the unsigned PSBT and unsigned txid.
    let psbt = BitcoinPsbt::from_settlement(&finalized, prevouts)
        .map_err(|e| fail(format!("Bitcoin PSBT preparation failed: {e}")))?;

    println!(
        "bitcoin_psbt=prepared submission={submission} unsigned_txid={}",
        psbt.unsigned_txid(),
    );

    for packet in wire_packets {
        pipeline.miner().broadcaster.publish(packet);
    }

    Ok(Some((finalized, psbt, retained_mass, commitment, depth)))
}

fn decode_wirepackets(bytes: &[u8]) -> Result<Vec<WirePacket>, SettlementError> {
    let mut rest = bytes;
    let mut packets = Vec::new();

    while !rest.is_empty() {
        let (head, tail) = rest
            .split_at_checked(4)
            .ok_or_else(|| fail("truncated wire-packet frame header"))?;

        let len = u32::from_be_bytes(
            head.try_into()
                .map_err(|_| fail("invalid wire-packet frame header"))?,
        ) as usize;

        if !(WirePacket::MIN_SIZE..=WirePacket::MAX_SIZE).contains(&len) {
            return Err(fail(format!("invalid wire-packet frame length={len}")));
        }

        let (frame, tail) = tail
            .split_at_checked(len)
            .ok_or_else(|| fail("truncated wire-packet frame payload"))?;

        packets.push(
            WirePacket::deserialize(frame)
                .map_err(|e| fail(format!("wire-packet deserialization failed: {e}")))?,
        );

        rest = tail;
    }

    if packets.is_empty() {
        return Err(fail("persisted wire-packet file is empty"));
    }

    Ok(packets)
}
