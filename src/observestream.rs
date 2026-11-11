
use std::{
    collections::{HashSet, VecDeque},
    error::Error,
    str::FromStr,
};
use qp_hd::commands::persist_wirepackets;
use bitcoin::{consensus::Decodable, Txid};
use futures_util::{SinkExt, StreamExt};
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

        // Resolve transaction IDs sequentially.
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




/// Converts the constructed settlement into a PSBT, persists its
/// submission-scoped QTM wire packets, and publishes those packets.
///
/// The finalized settlement transaction is the source of truth for
/// PSBT inputs. Its receipt is preserved by BitcoinPsbt.
///
/// Ordering:
/// 1. Finalize settlement.
/// 2. Construct and serialize wire packets.
/// 3. Persist wire packets.
/// 4. Resolve settlement prevouts.
/// 5. Prepare PSBT.
/// 6. Publish persisted packets.
///
/// Bitcoin broadcasting happens only after the PSBT has been
/// signed and finalized.
#[inline(always)]
pub fn refresh<R: PrevoutResolver>(
    pipeline: &mut BitcoinObservationPipeline,
    substrate: &str,
    observed: ObservedBitcoinTransaction,
    resolver: &R,
) -> PipelineOutcome {
    let activation = observed.source.activation;
    let source = observed.source;

    let outcome = pipeline.process(substrate, observed)?;

    let Some((
        finalized,
        retained_mass,
        commitment,
        depth,
    )) = outcome else {
        return Ok(None);
    };

    // Miner::submit() has already incremented this counter
    // inside BitcoinObservationPipeline::process().
    let submission = pipeline
        .miner
        .submissions
        .get(substrate)
        .copied()
        .ok_or_else(|| {
            SettlementError::Backend(format!(
                "submission sequence missing: substrate={} activation={}",
                substrate,
                activation,
            ))
        })?;

    // Derive packets from the exact finalized transaction.
    let wire_packets = finalized.wire_packets(commitment);

    if wire_packets.is_empty() {
        return Err(SettlementError::Backend(
            "finalized settlement produced no wire packets".into(),
        ));
    }

    // The finalized transaction is the authoritative transaction
    // identified by every wire packet.
    let settlement_txid = finalized.transaction.compute_txid();

    // Serialize and frame the complete packet sequence before
    // persistence. No prevout lookup occurs before this step.
    let mut packet_bytes = Vec::new();

    for packet in &wire_packets {
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

    // PERSISTENCE FIRST.
    //
    // Each miner submission has its own WirePackets file.
    // Persist the complete sequence before performing any
    // settlement input lookups.
    persist_wirepackets(
        substrate,
        activation,
        submission,
        &packet_bytes,
    )
    .map_err(|error| {
        SettlementError::Backend(format!(
            "QTM wire packet persistence failed: {error}"
        ))
    })?;

    println!(
        "qtm_wirepackets=persisted substrate={} activation={} submission={} commitment={} packets={} bytes={}",
        substrate,
        activation,
        submission,
        hex::encode(commitment),
        wire_packets.len(),
        packet_bytes.len(),
    );

    // LOOKUPS NEXT.
    //
    // The finalized settlement transaction, not the original
    // mempool observation, owns the inputs required by the PSBT.
    let settlement_observed =
        ObservedBitcoinTransaction::from_transaction(
            finalized.transaction.clone(),
            source,
        );

    // Resolve the actual settlement inputs once, preserving
    // transaction input order.
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

    // Prepare the PSBT from the exact finalized settlement.
    // The receipt is transferred unchanged from FinalizedSettlement.
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

    // Publish the same packets that were persisted.
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
