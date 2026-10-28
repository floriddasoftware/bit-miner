use std::{
collections::{HashSet, VecDeque},
error::Error,
path::PathBuf,
str::FromStr,
};

use bitcoin::{consensus::Decodable, Txid};
use futures_util::{stream, SinkExt, StreamExt};
use qp_hd::{
commands::{
load_event_at_activation,
qtm_open_manifold_until,
PersistedEvent,
},
pathsregistry::CoinType,
};
use quantom_value::Retain;
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

pub type ObservationResult =
Result<T, Box<dyn Error + Send + Sync>>;

pub type PipelineOutcome =
Result<Option<(FinalizedSettlement, Vec)>, SettlementError>;

const MEMPOOL_SPACE_WS: &str = “wss://mempool.space/api/v1/ws”;
const MEMPOOL_SPACE_API: &str = “https://mempool.space/api”;

const FETCH_CONCURRENCY: usize = 8;
const CHANNEL_CAPACITY: usize = 4096;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum BlockchainStream {
Transactions,
Block,
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
Block(bitcoin::Block),
}

impl BitcoinObservation {
#[inline(always)]
pub const fn stream(&self) -> BlockchainStream {
match self {
Self::Transaction() => BlockchainStream::Transactions,
Self::Block() => BlockchainStream::Block,
}
}

#[inline(always)]
pub const fn blockchain(&self) -> Blockchain {
    Blockchain::Bitcoin
}

}

pub struct MempoolSpaceStream {
socket: WebSocketStream<MaybeTlsStream>,
client: reqwest::Client,
source: BitcoinObservationSource,
mode: MempoolStreamMode,
pending: VecDeque,
track_blocks: bool,
skipped: u64,
}

impl MempoolSpaceStream {
pub async fn connect(
activation: u64,
mode: MempoolStreamMode,
) -> ObservationResult {
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
    socket
        .send(Message::Text(
            r#"{"action":"want","data":["blocks"]}"#.into(),
        ))
        .await?;
    Ok(Self {
        socket,
        client: reqwest::Client::new(),
        source: BitcoinObservationSource::mempool_space(activation),
        mode,
        pending: VecDeque::new(),
        track_blocks: true,
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
/// Reads the network stream independently and sends wrapped transactions
/// to the existing pipeline through a bounded queue.
pub async fn stream<F>(
    mut self,
    substrate: &str,
    pipeline: &mut BitcoinObservationPipeline,
    mut sink: F,
) -> ObservationResult<()>
where
    F: FnMut(Txid, PipelineOutcome),
{
    self.track_blocks = false;
    let source = self.source;
    let (sender, mut receiver) =
        tokio::sync::mpsc::channel::<ObservedBitcoinTransaction>(
            CHANNEL_CAPACITY,
        );
    let reader = tokio::spawn(async move {
        loop {
            match self.next_observation().await? {
                BitcoinObservation::Transaction(transaction) => {
                    let observed =
                        ObservedBitcoinTransaction::from_transaction(
                            transaction,
                            source,
                        );
                    if sender.send(observed).await.is_err() {
                        return Ok(());
                    }
                }
                BitcoinObservation::Block(_) => {}
            }
        }
        #[allow(unreachable_code)]
        Ok::<(), Box<dyn Error + Send + Sync>>(())
    });
    while let Some(observed) = receiver.recv().await {
        let txid = observed.txid;
        let outcome = pipeline.process(substrate, observed);
        sink(txid, outcome);
    }
    match reader.await {
        Ok(result) => result,
        Err(error) => Err(Box::new(error)),
    }
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
    if let Some(entries) = added(value, "mempool-transactions") {
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
                            BitcoinObservation::Transaction(transaction),
                        );
                        continue;
                    }
                    Err(_) => {
                        self.skipped = self.skipped.saturating_add(1);
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
    if let Some(entries) = added(value, "mempool-txids") {
        for txid in entries.iter().filter_map(Value::as_str) {
            if seen.insert(txid.to_owned()) {
                ids.push(txid.to_owned());
            }
        }
    }
    if let Some(txid) = value.get("txid").and_then(Value::as_str) {
        if seen.insert(txid.to_owned()) {
            ids.push(txid.to_owned());
        }
    }
    if !ids.is_empty() {
        let client = self.client.clone();
        let mut fetched = stream::iter(ids)
            .map(move |txid| {
                let client = client.clone();
                async move {
                    let txid = Txid::from_str(&txid)?;
                    fetch_decode::<bitcoin::Transaction>(
                        &client,
                        format!("{MEMPOOL_SPACE_API}/tx/{txid}/raw"),
                    )
                    .await
                }
            })
            .buffered(FETCH_CONCURRENCY);
        while let Some(result) = fetched.next().await {
            match result {
                Ok(transaction) => {
                    self.pending.push_back(
                        BitcoinObservation::Transaction(transaction),
                    );
                }
                Err(_) => {
                    self.skipped = self.skipped.saturating_add(1);
                }
            }
        }
    }
    if self.track_blocks {
        if let Some(hash) = block_hash(value) {
            match fetch_decode::<bitcoin::Block>(
                &self.client,
                format!("{MEMPOOL_SPACE_API}/block/{hash}/raw"),
            )
            .await
            {
                Ok(block) => {
                    self.pending.push_back(
                        BitcoinObservation::Block(block),
                    );
                }
                Err(_) => {
                    self.skipped = self.skipped.saturating_add(1);
                }
            }
        }
    }
    Ok(())
}

}

#[inline(always)]
fn added<’a>(value: &’a Value, key: &str) -> Option<&’a Vec> {
value.get(key)?.get(“added”)?.as_array()
}

#[inline(always)]
fn decode_hex_transaction(
hex_tx: &str,
) -> ObservationResultbitcoin::Transaction {
let raw = hex::decode(hex_tx)?;
Ok(bitcoin::consensus::deserialize::bitcoin::Transaction(&raw)?)
}

async fn fetch_decode<T: Decodable>(
client: &reqwest::Client,
url: String,
) -> ObservationResult {
let raw = client
.get(url)
.send()
.await?
.error_for_status()?
.bytes()
.await?;

Ok(bitcoin::consensus::deserialize::<T>(&raw)?)

}

fn block_hash(value: &Value) -> Option<&str> {
let block = value.get(“block”);

block
    .and_then(|block| block.get("id"))
    .and_then(Value::as_str)
    .or_else(|| block.and_then(Value::as_str))
    .or_else(|| {
        value
            .get("blocks")
            .and_then(Value::as_array)
            .and_then(|blocks| blocks.first())
            .and_then(|block| block.get("id"))
            .and_then(Value::as_str)
    })

}

/// Refreshes the miner’s substrate state and returns the persisted event.
///
/// This helper is shared by the library and the binary; keep its imports
/// at the top of this module rather than duplicating them below.
#[inline(always)]
pub fn refresh(
pipeline: &mut BitcoinObservationPipeline,
substrate: &str,
) -> PersistedEvent {
let activation = pipeline
.miner()
.clip(substrate)
.map(|clip| clip.activation)
.expect(“no active miner clip for substrate”);

let current = qtm_open_manifold_until(
    substrate,
    activation,
);
let realized = current.state.retained_mass();
let event_path: PathBuf = pipeline
    .miner_mut()
    .submit(substrate, realized)
    .expect("miner submission failed");
let event_activation = event_path
    .file_name()
    .and_then(|name| name.to_str())
    .and_then(|name| name.strip_prefix("event_"))
    .and_then(|name| name.split('_').next())
    .and_then(|value| value.parse::<u64>().ok())
    .expect("invalid event path");
load_event_at_activation(
    substrate,
    event_activation,
)

}