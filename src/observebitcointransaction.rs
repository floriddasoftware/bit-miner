use crate::qpoutputsbitcoininput::BitcoinInput;
use std::{
    collections::{HashSet, VecDeque},
    error::Error,
    io::ErrorKind,
    str::FromStr,
};

use bitcoin::{
    consensus::Decodable,
    Txid,
};
use futures_util::{SinkExt, StreamExt};
use qp_hd::commands::{
    load_budget,
    load_wirepackets,
    persist_wirepackets,
    qtm_open_manifold_until,
    load_qtm_at_activation,
};
use qp_hd::pathsregistry::CoinType;
use qtm_graph::graph::{
    frame::{Depth, Direction},
    Scale,
};
use serde_json::Value;
use tokio::net::TcpStream;
use tokio_tungstenite::{
    connect_async,
    tungstenite::Message,
    MaybeTlsStream,
    WebSocketStream,
};

use crate::{
    bitcoinpsbt::{
        BitcoinPsbt,
        PrevoutResolver,
    },
    broadcast::WirePacket,
    observebitcointransaction::ObservedBitcoinTransaction,
    settlement::SettlementError,
    BitcoinObservationPipeline,
};

pub type Blockchain = CoinType;

pub type ObservationResult<T> =
    Result<T, Box<dyn Error + Send + Sync>>;

pub type PipelineOutcome =
    Result<
        Option<(
            BitcoinPsbt,
            u128,
            [u8; 32],
            Depth,
        )>,
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

/// Owns one WebSocket connection and decodes Bitcoin transactions.
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
        let (mut socket, _) =
            connect_async(MEMPOOL_SPACE_WS).await?;

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
            source: BitcoinObservationSource::mempool_space(
                activation,
            ),
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

    /// Returns the next decoded transaction from the WebSocket feed.
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

    /// Passes the next transaction through settlement construction
    /// and PSBT preparation.
    pub async fn stream<R: PrevoutResolver>(
        &mut self,
        substrate: &str,
        pipeline: &mut BitcoinObservationPipeline,
        resolver: &R,
    ) -> ObservationResult<PipelineOutcome> {
        let observation = self.next_observation().await?;

        let BitcoinObservation::Transaction(transaction) =
            observation;

        let observed = ObservedBitcoinTransaction::from_transaction(
            transaction,
            self.source,
        );

        Ok(refresh(
            pipeline,
            substrate,
            observed,
            resolver,
        ))
    }

    async fn fill(&mut self) -> ObservationResult<()> {
        let message = self
            .socket
            .next()
            .await
            .ok_or("Mempool.space WebSocket closed")??;

        let value: Value = match message {
            Message::Text(text) => {
                serde_json::from_str(text.as_ref())?
            }
            Message::Binary(bytes) => {
                serde_json::from_slice(&bytes)?
            }
            Message::Close(_) => {
                return Err(
                    "Mempool.space WebSocket closed".into()
                );
            }
            Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => {
                return Ok(());
            }
        };

        self.decode(&value).await
    }

    async fn decode(
        &mut self,
        value: &Value,
    ) -> ObservationResult<()> {
        let mut ids = Vec::<String>::new();
        let mut seen = HashSet::<String>::new();

        if let Some(entries) =
            added(value, "mempool-transactions")
        {
            for entry in entries {
                let txid = entry
                    .get("txid")
                    .and_then(Value::as_str)
                    .map(str::to_owned);

                if let Some(hex_tx) =
                    entry.get("hex").and_then(Value::as_str)
                {
                    match decode_hex_transaction(hex_tx) {
                        Ok(transaction) => {
                            self.pending.push_back(
                                BitcoinObservation::Transaction(
                                    transaction,
                                ),
                            );
                            continue;
                        }
                        Err(_) => {
                            self.skipped =
                                self.skipped.saturating_add(1);
                        }
                    }
                }

                if let Some(txid) = txid {
                    if seen.insert(txid.clone()) {
                        ids.push(txid);
                    }
                }
            }
        }

        if let Some(entries) =
            added(value, "mempool-txids")
        {
            for txid in entries.iter().filter_map(Value::as_str) {
                if seen.insert(txid.to_owned()) {
                    ids.push(txid.to_owned());
                }
            }
        }

        if let Some(txid) =
            value.get("txid").and_then(Value::as_str)
        {
            if seen.insert(txid.to_owned()) {
                ids.push(txid.to_owned());
            }
        }

        for txid in ids {
            let txid = match Txid::from_str(&txid) {
                Ok(txid) => txid,
                Err(_) => {
                    self.skipped =
                        self.skipped.saturating_add(1);
                    continue;
                }
            };

            match fetch_decode::<bitcoin::Transaction>(
                &self.client,
                format!("{MEMPOOL_SPACE_API}/tx/{txid}/raw"),
            )
            .await
            {
                Ok(transaction) => {
                    self.pending.push_back(
                        BitcoinObservation::Transaction(
                            transaction,
                        ),
                    );
                }
                Err(_) => {
                    self.skipped =
                        self.skipped.saturating_add(1);
                }
            }
        }

        Ok(())
    }
}

#[inline(always)]
fn added<'a>(
    value: &'a Value,
    key: &str,
) -> Option<&'a Vec<Value>> {
    value.get(key)?.get("added")?.as_array()
}

#[inline(always)]
fn decode_hex_transaction(
    hex_tx: &str,
) -> ObservationResult<bitcoin::Transaction> {
    let raw = hex::decode(hex_tx)?;

    Ok(bitcoin::consensus::deserialize::<bitcoin::Transaction>(
        &raw,
    )?)
}

async fn fetch_decode<T: Decodable>(
    client: &reqwest::Client,
    url: String,
) -> ObservationResult<T> {
    let raw = client
        .get(url)
        .send()
        .await?
        .error_for_status()?
        .bytes()
        .await?;

    Ok(bitcoin::consensus::deserialize::<T>(&raw)?)
}

#[inline(always)]
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
        .ok_or_else(|| {
            SettlementError::Backend(format!(
                "no active miner clip: substrate={}",
                substrate,
            ))
        })?;

    let network = pipeline.network().to_owned();

    if observed.source.activation != activation {
        return Err(SettlementError::Backend(format!(
            "observation activation mismatch: substrate={} observation={} active={}",
            substrate,
            observed.source.activation,
            activation,
        )));
    }

    let budget = load_budget(substrate, activation, &network)
        .map_err(|error| {
            SettlementError::Backend(format!(
                "budget load failed: substrate={} activation={} network={}: {}",
                substrate, activation, network, error,
            ))
        })?;

    let mut persisted_submission = 0u64;
    let mut sequence_ended = false;

    for number in 1..=budget.max_events {
        match load_wirepackets(substrate, activation, number) {
            Ok(bytes) => {
                if sequence_ended {
                    return Err(SettlementError::Backend(format!(
                        "submission sequence gap: substrate={} activation={} missing_before={}",
                        substrate, activation, number,
                    )));
                }

                let packets = decode_wirepackets(&bytes)
                    .map_err(|error| {
                        SettlementError::Backend(format!(
                            "persisted wirepacket decode failed: submission={}: {}",
                            number, error,
                        ))
                    })?;

                let commitment = packets
                    .first()
                    .map(|packet| packet.commitment)
                    .ok_or_else(|| {
                        SettlementError::Backend(format!(
                            "empty persisted submission: {}",
                            number,
                        ))
                    })?;

                if packets.iter().any(|packet| packet.commitment != commitment) {
                    return Err(SettlementError::Backend(format!(
                        "mixed commitments in submission: {}",
                        number,
                    )));
                }

                persisted_submission = number;
            }
            Err(error) if error.kind() == ErrorKind::NotFound => {
                sequence_ended = true;
            }
            Err(error) => {
                return Err(SettlementError::Backend(format!(
                    "wirepacket load failed: substrate={} activation={} submission={}: {}",
                    substrate, activation, number, error,
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

    if miner_submission > persisted_submission {
        return Err(SettlementError::Backend(format!(
            "miner counter exceeds persisted sequence: miner={} persisted={}",
            miner_submission, persisted_submission,
        )));
    }

    pipeline
        .miner
        .submissions
        .insert(substrate.to_owned(), persisted_submission);

    let submission = persisted_submission
        .checked_add(1)
        .ok_or_else(|| {
            SettlementError::Backend("submission sequence overflow".into())
        })?;

    if submission > budget.max_events {
        return Err(SettlementError::Backend(format!(
            "submission budget exhausted: substrate={} activation={} network={} next={} max={}",
            substrate, activation, network, submission, budget.max_events,
        )));
    }

    let Some((
        finalized,
        materialized_payload,
        retained_mass,
        commitment,
        depth,
    )) = pipeline.process(substrate, observed)?
    else {
        return Ok(None);
    };

    let actual_submission = pipeline
        .miner
        .submissions
        .get(substrate)
        .copied()
        .unwrap_or(0);

    if actual_submission != submission {
        return Err(SettlementError::Backend(format!(
            "submission mismatch: expected={} actual={}",
            submission, actual_submission,
        )));
    }

    let generated_packets = finalized.wire_packets(commitment);

    if generated_packets.is_empty() {
        return Err(SettlementError::Backend(
            "finalized settlement produced no wire packets".into(),
        ));
    }

    let settlement_txid = finalized.transaction.compute_txid();
    let mut packet_bytes = Vec::new();

    for packet in &generated_packets {
        if packet.txid() != settlement_txid || packet.commitment != commitment {
            return Err(SettlementError::Backend(
                "generated packet does not match finalized settlement".into(),
            ));
        }

        let payload = packet.serialize();
        if payload.is_empty() {
            return Err(SettlementError::Backend(
                "wire-packet serialization failed".into(),
            ));
        }

        let frame_len = u32::try_from(payload.len())
            .map_err(|error| SettlementError::Backend(error.to_string()))?;

        packet_bytes.extend_from_slice(&frame_len.to_be_bytes());
        packet_bytes.extend_from_slice(&payload);
    }

    persist_wirepackets(substrate, activation, submission, &packet_bytes)
        .map_err(|error| {
            SettlementError::Backend(format!(
                "wire-packet persistence failed: substrate={} activation={} submission={}: {}",
                substrate, activation, submission, error,
            ))
        })?;

    let persisted_bytes = load_wirepackets(substrate, activation, submission)
        .map_err(|error| {
            SettlementError::Backend(format!(
                "wire-packet reload failed: submission={}: {}",
                submission, error,
            ))
        })?;

    let wire_packets = decode_wirepackets(&persisted_bytes)?;

    if wire_packets.len() != generated_packets.len()
        || wire_packets.iter().any(|packet| {
            packet.txid() != settlement_txid || packet.commitment != commitment
        })
    {
        return Err(SettlementError::Backend(
            "persisted packets differ from finalized submission".into(),
        ));
    }

    println!(
        "qtm_wirepackets=persisted substrate={} activation={} network={} submission={} commitment={} packets={} bytes={}",
        substrate,
        activation,
        network,
        submission,
        hex::encode(commitment),
        wire_packets.len(),
        persisted_bytes.len(),
    );

    let qtm = load_qtm_at_activation(substrate, activation);
    let private_key = qtm.coordinate;
    let heritage = qtm_open_manifold_until(substrate, activation);

    // Parent provenance is the heritage activity state activation.
    // outpoint.vout is only the output index inside the parent transaction.
    // All inputs in this settlement share the same parent activity state.
    let parent_submission = heritage.state.activation;

    let mut resolved_prevouts =
        Vec::with_capacity(finalized.transaction.input.len());

    for (input_index, payload_input) in materialized_payload.inputs.iter().enumerate() {
        let outpoint = &payload_input.previous_output;

        let resolved = resolver
            .resolve(outpoint, parent_submission, qtm.commitment)
            .map_err(|error| {
                SettlementError::Backend(format!(
                    "outpoint resolution failed: input={} outpoint={} parent_submission={} commitment={}: {}",
                    input_index,
                    outpoint,
                    parent_submission,
                    hex::encode(qtm.commitment),
                    error,
                ))
            })?;

        if resolved.outpoint != *outpoint {
            return Err(SettlementError::Backend(format!(
                "resolved prevout outpoint mismatch: input={} expected={} resolved={}",
                input_index, outpoint, resolved.outpoint,
            )));
        }

        resolved_prevouts.push(resolved);
    }

    let mut psbt = BitcoinPsbt::from_settlement(finalized, resolved_prevouts)
        .map_err(|error| {
            SettlementError::Backend(format!(
                "Bitcoin PSBT preparation failed: {}",
                error,
            ))
        })?;

    println!(
        "bitcoin_psbt=prepared submission={} unsigned_txid={}",
        submission,
        psbt.unsigned_txid(),
    );

    pipeline
        .render_state_mut()
        .frames(heritage, Scale::Global);

    {
        let direction = pipeline
            .render_state()
            .direction
            .as_ref()
            .ok_or_else(|| {
                SettlementError::Backend(
                    "QP render direction is unavailable for Bitcoin signing".into(),
                )
            })?;

        let manifold = &direction.heritage().state;

        if materialized_payload.inputs.len() != psbt.psbt.inputs.len() {
            return Err(SettlementError::Backend(format!(
                "materialized payload input count mismatch: payload={} psbt={}",
                materialized_payload.inputs.len(),
                psbt.psbt.inputs.len(),
            )));
        }

        for (input_index, mut bitcoin_input) in
            materialized_payload.inputs.iter().cloned().enumerate()
        {
            psbt.sign_input(
                &mut bitcoin_input,
                manifold,
                private_key,
                input_index,
            )
            .map_err(|error| {
                SettlementError::Backend(format!(
                    "Bitcoin input signing failed: input={} outpoint={}: {}",
                    input_index,
                    bitcoin_input.previous_output,
                    error,
                ))
            })?;
        }
    }

    println!(
        "bitcoin_psbt=signed submission={} inputs={}",
        submission,
        psbt.psbt.inputs.len(),
    );

    for packet in wire_packets {
        pipeline.miner().broadcaster.publish(packet);
    }

    Ok(Some((psbt, retained_mass, commitment, depth)))
}

/// Decode the persisted QTMGSP02 length-prefixed packet sequence.
#[inline(always)]
fn decode_wirepackets(
    bytes: &[u8],
) -> Result<Vec<WirePacket>, SettlementError> {
    if bytes.is_empty() {
        return Err(SettlementError::Backend(
            "persisted wire-packet file is empty".into(),
        ));
    }

    let mut offset = 0usize;
    let mut packets = Vec::new();

    while offset < bytes.len() {
        let header_end = offset.checked_add(4).ok_or_else(|| {
            SettlementError::Backend(
                "wire-packet frame offset overflow".into(),
            )
        })?;

        if header_end > bytes.len() {
            return Err(SettlementError::Backend(
                "truncated wire-packet frame header".into(),
            ));
        }

        let frame_len = u32::from_be_bytes(
            bytes[offset..header_end]
                .try_into()
                .map_err(|_| {
                    SettlementError::Backend(
                        "invalid wire-packet frame header".into(),
                    )
                })?,
        ) as usize;

        offset = header_end;

        if !(WirePacket::MIN_SIZE..=WirePacket::MAX_SIZE)
            .contains(&frame_len)
        {
            return Err(SettlementError::Backend(format!(
                "invalid wire-packet frame length={}",
                frame_len,
            )));
        }

        let frame_end = offset.checked_add(frame_len).ok_or_else(|| {
            SettlementError::Backend(
                "wire-packet frame length overflow".into(),
            )
        })?;

        if frame_end > bytes.len() {
            return Err(SettlementError::Backend(
                "truncated wire-packet frame payload".into(),
            ));
        }

        let packet = WirePacket::deserialize(
            &bytes[offset..frame_end],
        )
        .map_err(|error| {
            SettlementError::Backend(format!(
                "wire-packet deserialization failed: {error}"
            ))
        })?;

        packets.push(packet);
        offset = frame_end;
    }

    if packets.is_empty() {
        return Err(SettlementError::Backend(
            "persisted wire-packet file contains no packets".into(),
        ));
    }

    Ok(packets)
}