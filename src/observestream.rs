
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
};
use qp_hd::pathsregistry::CoinType;
use qtm_graph::graph::frame::Depth;
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

/// Converts the constructed settlement into a PSBT.
///
/// The active clip supplies the activation. The network-scoped
/// budget and submission sequence are enforced at this boundary.
///
/// Ordering:
/// 1. Load the budget.
/// 2. Load persisted wire-packet files for submissions 1..=max_events.
/// 3. Reconcile the persisted sequence with the miner counter.
/// 4. Gate and construct the next settlement.
/// 5. Persist the new wire-packet sequence.
/// 6. Reload the persisted packets.
/// 7. Resolve settlement prevouts.
/// 8. Prepare the PSBT.
/// 9. Publish the reloaded packets.
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
                "no active miner clip for substrate={}",
                substrate,
            ))
        })?;

    let network = pipeline.network().to_owned();
    let source = observed.source;

    if source.activation != activation {
        return Err(SettlementError::Backend(format!(
            "observation activation mismatch: substrate={} observation_activation={} active_activation={}",
            substrate,
            source.activation,
            activation,
        )));
    }

    let budget = load_budget(
        substrate,
        activation,
        &network,
    )
    .map_err(|error| {
        SettlementError::Backend(format!(
            "failed to load event budget: substrate={} activation={} network={}: {}",
            substrate,
            activation,
            network,
            error,
        ))
    })?;

    // Scan the complete permitted submission range.
    // Existing files must form a contiguous sequence beginning at 1.
    let mut persisted_submission = 0u64;
    let mut first_missing = None;

    for submission in 1..=budget.max_events {
        match load_wirepackets(
            substrate,
            activation,
            submission,
        ) {
            Ok(bytes) => {
                if let Some(missing) = first_missing {
                    return Err(SettlementError::Backend(format!(
                        "wire-packet submission sequence has a gap: missing={} but submission={} exists",
                        missing,
                        submission,
                    )));
                }

                let packets = decode_wirepackets(&bytes)?;

                if packets.is_empty() {
                    return Err(SettlementError::Backend(format!(
                        "wire-packet submission is empty: substrate={} activation={} submission={}",
                        substrate,
                        activation,
                        submission,
                    )));
                }

                persisted_submission = submission;
            }

            Err(error) if error.kind() == ErrorKind::NotFound => {
                if first_missing.is_none() {
                    first_missing = Some(submission);
                }
            }

            Err(error) => {
                return Err(SettlementError::Backend(format!(
                    "failed to load wire packets: substrate={} activation={} submission={}: {}",
                    substrate,
                    activation,
                    submission,
                    error,
                )));
            }
        }
    }

    // The persisted sequence is authoritative across process restarts.
    // Synchronize a lagging in-memory counter to that sequence.
    let miner_submission = pipeline
        .miner
        .submissions
        .get(substrate)
        .copied()
        .unwrap_or(0);

    if miner_submission > persisted_submission {
        return Err(SettlementError::Backend(format!(
            "miner submission counter is ahead of persisted wire packets: substrate={} activation={} network={} miner_submission={} persisted_submission={}",
            substrate,
            activation,
            network,
            miner_submission,
            persisted_submission,
        )));
    }

    if miner_submission < persisted_submission {
        pipeline.miner.submissions.insert(
            substrate.to_owned(),
            persisted_submission,
        );
    }

    let submission = persisted_submission
        .checked_add(1)
        .ok_or_else(|| {
            SettlementError::Backend(
                "submission sequence overflow".into(),
            )
        })?;

    if submission > budget.max_events {
        return Err(SettlementError::Backend(format!(
            "submission budget exhausted: substrate={} activation={} network={} next_submission={} max_events={}",
            substrate,
            activation,
            network,
            submission,
            budget.max_events,
        )));
    }

    // The miner counter is now aligned with the next permitted
    // settlement. process() invokes Miner::submit(), advancing it once.
    let outcome = pipeline.process(
        substrate,
        observed,
    )?;

    let Some((
        finalized,
        retained_mass,
        commitment,
        depth,
    )) = outcome else {
        return Ok(None);
    };

    let actual_submission = pipeline
        .miner
        .submissions
        .get(substrate)
        .copied()
        .ok_or_else(|| {
            SettlementError::Backend(format!(
                "miner submission counter missing after settlement: substrate={} activation={}",
                substrate,
                activation,
            ))
        })?;

    if actual_submission != submission {
        return Err(SettlementError::Backend(format!(
            "miner submission sequence mismatch: substrate={} activation={} expected={} actual={}",
            substrate,
            activation,
            submission,
            actual_submission,
        )));
    }

    let generated_packets = finalized.wire_packets(commitment);

    if generated_packets.is_empty() {
        return Err(SettlementError::Backend(
            "finalized settlement produced no wire packets".into(),
        ));
    }

    let settlement_txid = finalized.transaction.compute_txid();

    // Serialize and frame every packet before persistence.
    let mut packet_bytes = Vec::new();

    for packet in &generated_packets {
        if packet.txid() != settlement_txid {
            return Err(SettlementError::Backend(format!(
                "wire packet transaction mismatch: packet_txid={} settlement_txid={}",
                packet.txid(),
                settlement_txid,
            )));
        }

        let payload = packet.serialize();

        if payload.is_empty() {
            return Err(SettlementError::Backend(
                "QTM wire packet serialization failed".into(),
            ));
        }

        let frame_len = u32::try_from(payload.len())
            .map_err(|error| {
                SettlementError::Backend(format!(
                    "QTM wire packet frame length failed: {error}"
                ))
            })?;

        packet_bytes.extend_from_slice(
            &frame_len.to_be_bytes(),
        );
        packet_bytes.extend_from_slice(&payload);
    }

    // Persist before any settlement prevout lookup.
    persist_wirepackets(
        substrate,
        activation,
        submission,
        &packet_bytes,
    )
    .map_err(|error| {
        SettlementError::Backend(format!(
            "QTM wire packet persistence failed: substrate={} activation={} network={} submission={}: {}",
            substrate,
            activation,
            network,
            submission,
            error,
        ))
    })?;

    println!(
        "qtm_wirepackets=persisted substrate={} activation={} network={} submission={} commitment={} packets={} bytes={}",
        substrate,
        activation,
        network,
        submission,
        hex::encode(commitment),
        generated_packets.len(),
        packet_bytes.len(),
    );

    // Reload the exact persisted packet file, rather than publishing
    // regenerated in-memory packets.
    let persisted_bytes = load_wirepackets(
        substrate,
        activation,
        submission,
    )
    .map_err(|error| {
        SettlementError::Backend(format!(
            "failed to reload persisted wire packets: substrate={} activation={} submission={}: {}",
            substrate,
            activation,
            submission,
            error,
        ))
    })?;

    let wire_packets = decode_wirepackets(
        &persisted_bytes,
    )?;

    if wire_packets.len() != generated_packets.len() {
        return Err(SettlementError::Backend(format!(
            "persisted wire packet count mismatch: generated={} persisted={}",
            generated_packets.len(),
            wire_packets.len(),
        )));
    }

    if wire_packets.iter().any(|packet| {
        packet.txid() != settlement_txid
            || packet.commitment != commitment
    }) {
        return Err(SettlementError::Backend(
            "persisted wire packets do not match the finalized settlement".into(),
        ));
    }

    // Resolve inputs only after the packet file has been persisted
    // and loaded back successfully.
    let settlement_observed =
        ObservedBitcoinTransaction::from_transaction(
            finalized.transaction.clone(),
            source,
        );

    let resolved_prevouts = settlement_observed
        .resolve_wire_packet_inputs(
            &wire_packets[0],
            resolver,
        )
        .map_err(|error| {
            SettlementError::Backend(format!(
                "settlement input resolution failed: {error}"
            ))
        })?;

    let psbt = BitcoinPsbt::from_settlement(
        finalized,
        resolved_prevouts,
    )
    .map_err(|error| {
        SettlementError::Backend(format!(
            "Bitcoin PSBT preparation failed: {error}"
        ))
    })?;

    println!(
        "bitcoin_psbt=prepared submission={} unsigned_txid={}",
        submission,
        psbt.unsigned_txid(),
    );

    // Publish the exact packet sequence reloaded from storage.
    for wire_packet in wire_packets {
        pipeline
            .miner()
            .broadcaster
            .publish(wire_packet);
    }

    Ok(Some((
        psbt,
        retained_mass,
        commitment,
        depth,
    )))
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
