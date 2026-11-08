
use std::{
    collections::{HashSet, VecDeque},
    error::Error,
    str::FromStr,
};

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
    broadcast::FinalizedSettlement,
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
            FinalizedSettlement,
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

    /// Passes the next transaction through finalization and broadcasting.
    pub async fn stream(
        &mut self,
        substrate: &str,
        pipeline: &mut BitcoinObservationPipeline,
    ) -> ObservationResult<PipelineOutcome> {
        let observation = self.next_observation().await?;

        let BitcoinObservation::Transaction(transaction) =
            observation;

        let observed = ObservedBitcoinTransaction::from_transaction(
            transaction,
            self.source,
        );

        Ok(refresh(pipeline, substrate, observed))
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



/// Finalizes the observation and publishes its settlement.
#[inline(always)]
pub fn refresh(
    pipeline: &mut BitcoinObservationPipeline,
    substrate: &str,
    observed: ObservedBitcoinTransaction,
) -> PipelineOutcome {
    let outcome = pipeline.process(
        substrate,
        observed.clone(),
    )?;

    let Some((
        finalized,
        retained_mass,
        commitment,
        depth,
    )) = outcome else {
        return Ok(None);
    };

    let receipt = finalized.receipt.as_ref().ok_or(
        SettlementError::Unsupported(
            "cannot proceed without a settlement receipt",
        ),
    )?;

    let manifold = &receipt.event.heritage.state;

    let proceed_observed =
        ObservedBitcoinTransaction::from_transaction(
            finalized.transaction.clone(),
            observed.source,
        );

    let (bitcoin, _) = pipeline.bitcoin_payload(
        &proceed_observed,
        manifold,
    )?;

    let finalized = SettlementEngine::new(substrate)
        .proceed(finalized, bitcoin)?;

    // The returned txid confirms local queue acceptance only.
    let electrum_txid = pipeline
        .electrum
        .submit(finalized.transaction.clone())
        .map_err(|error| {
            SettlementError::Backend(format!(
                "Electrum transaction submission failed: {error}"
            ))
        })?;

    println!(
        "electrum_submission=queued txid={}",
        electrum_txid,
    );

    // Publish packets from the transaction returned by proceed().
    let wire_packets = finalized.wire_packets(commitment);

    for wire_packet in wire_packets {
        pipeline
            .miner()
            .broadcaster
            .publish(wire_packet);
    }

    Ok(Some((
        finalized,
        retained_mass,
        commitment,
        depth,
    )))
}