
use std::error::Error;

use futures_util::{SinkExt, StreamExt};
use qp_hd::pathsregistry::CoinType;
use serde_json::Value;
use tokio::net::TcpStream;
use tokio_tungstenite::{
    connect_async,
    tungstenite::Message,
    MaybeTlsStream,
    WebSocketStream,
};

pub type Blockchain = CoinType;

pub type ObservationResult<T> =
    Result<T, Box<dyn Error + Send + Sync>>;

const MEMPOOL_SPACE_WS: &str = "wss://mempool.space/api/v1/ws";
const MEMPOOL_SPACE_API: &str = "https://mempool.space/api";

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
            Self::Transaction(_) => BlockchainStream::Transactions,
            Self::Block(_) => BlockchainStream::Block,
        }
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
}

impl MempoolSpaceStream {
    pub async fn connect(
        activation: u64,
        mode: MempoolStreamMode,
    ) -> ObservationResult<Self> {
        let (mut socket, _) = connect_async(MEMPOOL_SPACE_WS).await?;

        let mempool_subscription = match mode {
            MempoolStreamMode::TransactionIds => {
                serde_json::json!({
                    "track-mempool-txids": true
                })
            }
            MempoolStreamMode::FullTransactions => {
                serde_json::json!({
                    "track-mempool": true
                })
            }
        };

        socket
            .send(Message::Text(
                mempool_subscription.to_string().into(),
            ))
            .await?;

        socket
            .send(Message::Text(
                serde_json::json!({
                    "action": "want",
                    "data": ["blocks"]
                })
                .to_string()
                .into(),
            ))
            .await?;

        Ok(Self {
            socket,
            client: reqwest::Client::new(),
            source: BitcoinObservationSource::mempool_space(activation),
            mode,
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

    pub async fn next_observation(
        &mut self,
    ) -> ObservationResult<BitcoinObservation> {
        loop {
            let message = self
                .socket
                .next()
                .await
                .ok_or("Mempool.space WebSocket closed")??;

            let text = match message {
                Message::Text(text) => text.to_string(),
                Message::Binary(bytes) => {
                    String::from_utf8(bytes.to_vec())?
                }
                Message::Close(_) => {
                    return Err(
                        "Mempool.space WebSocket closed".into()
                    );
                }
                Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => {
                    continue;
                }
            };

            let value: Value = serde_json::from_str(&text)?;

            if let Some(transaction) = full_transaction(&value)? {
                return Ok(BitcoinObservation::Transaction(transaction));
            }

            if let Some(txid) = transaction_id(&value) {
                let url = format!("{MEMPOOL_SPACE_API}/tx/{txid}/raw");
                let raw = self
                    .client
                    .get(url)
                    .send()
                    .await?
                    .error_for_status()?
                    .bytes()
                    .await?;

                let transaction =
                    bitcoin::consensus::deserialize::<bitcoin::Transaction>(
                        &raw,
                    )?;

                return Ok(BitcoinObservation::Transaction(transaction));
            }

            if let Some(hash) = block_hash(&value) {
                let url = format!("{MEMPOOL_SPACE_API}/block/{hash}/raw");
                let raw = self
                    .client
                    .get(url)
                    .send()
                    .await?
                    .error_for_status()?
                    .bytes()
                    .await?;

                let block =
                    bitcoin::consensus::deserialize::<bitcoin::Block>(
                        &raw,
                    )?;

                return Ok(BitcoinObservation::Block(block));
            }
        }
    }
}

fn full_transaction(
    value: &Value,
) -> ObservationResult<Option<bitcoin::Transaction>> {
    let Some(added) = value
        .get("mempool-transactions")
        .and_then(|mempool| mempool.get("added"))
        .and_then(Value::as_array)
    else {
        return Ok(None);
    };

    let Some(tx) = added.first() else {
        return Ok(None);
    };

    let Some(hex_tx) = tx
        .get("hex")
        .and_then(Value::as_str)
    else {
        return Ok(None);
    };

    let raw = hex::decode(hex_tx)?;
    let transaction =
        bitcoin::consensus::deserialize::<bitcoin::Transaction>(&raw)?;

    Ok(Some(transaction))
}

fn transaction_id(value: &Value) -> Option<&str> {
    let mempool = value.get("mempool-txids")?;

    mempool
        .get("added")
        .and_then(Value::as_array)
        .and_then(|items| items.iter().find_map(Value::as_str))
        .or_else(|| {
            value
                .get("txid")
                .and_then(Value::as_str)
        })
}

fn block_hash(value: &Value) -> Option<&str> {
    value
        .get("block")
        .and_then(|block| block.get("id"))
        .and_then(Value::as_str)
        .or_else(|| {
            value
                .get("block")
                .and_then(Value::as_str)
        })
        .or_else(|| {
            value
                .get("blocks")
                .and_then(Value::as_array)
                .and_then(|blocks| blocks.first())
                .and_then(|block| block.get("id"))
                .and_then(Value::as_str)
        })
}
