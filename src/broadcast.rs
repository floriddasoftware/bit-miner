use std::{
    env,
    fmt,
    io::{Error, ErrorKind, Read, Write},
    net::{SocketAddr, TcpStream, UdpSocket},
    sync::{
        mpsc::{sync_channel, Receiver, SyncSender},
        Arc,
    },
    thread,
    time::Duration,
};

use bitcoin::{
    hashes::Hash,
    OutPoint,
    ScriptBuf,
    Txid,
};

use mio::{
    net::TcpListener,
    Events,
    Interest,
    Poll,
    Token,
};

use crate::bitcoinreceipt::SettlementReceipt;

// -----------------------------------------------------------------------------
// Canonical finalized settlement
// -----------------------------------------------------------------------------

pub struct FinalizedSettlement {
    pub receipt: Option<SettlementReceipt>,
    pub transaction: bitcoin::Transaction,
}

impl FinalizedSettlement {
    #[inline(always)]
    pub fn new(
        receipt: Option<SettlementReceipt>,
        transaction: bitcoin::Transaction,
    ) -> Self {
        Self {
            receipt,
            transaction,
        }
    }

    #[inline(always)]
    pub fn txid(&self) -> Txid {
        self.transaction.compute_txid()
    }

    #[inline(always)]
    pub fn script_pubkey(&self) -> Option<&ScriptBuf> {
        self.receipt
            .as_ref()
            .map(|receipt| &receipt.script_pubkey)
    }

    #[inline]
    pub fn wire_packets(
        &self,
        commitment: [u8; 32],
    ) -> Vec<WirePacket> {
        self.transaction
            .output
            .iter()
            .enumerate()
            .filter_map(|(index, _)| {
                let vout = u32::try_from(index).ok()?;
                self.wire_packet(vout, commitment)
            })
            .collect()
    }

    /// Constructs a packet for an existing transaction output.
    ///
    /// The output index determines the value and OutPoint. When a settlement
    /// receipt exists, its script is the canonical settlement script.
    #[inline]
    pub fn wire_packet(
        &self,
        vout: u32,
        commitment: [u8; 32],
    ) -> Option<WirePacket> {
        let output = self.transaction.output.get(vout as usize)?;

        let script_pubkey = self
            .script_pubkey()
            .cloned()
            .unwrap_or_else(|| output.script_pubkey.clone());

        Some(WirePacket::new(
            OutPoint {
                txid: self.txid(),
                vout,
            },
            output.value.to_sat(),
            script_pubkey,
            commitment,
        ))
    }
}

// -----------------------------------------------------------------------------
// Broadcast driver
// -----------------------------------------------------------------------------

pub trait BroadcastDriver: Send + Sync + 'static {
    fn publish(&self, packet: &WirePacket) -> WirePacket;
}

// -----------------------------------------------------------------------------
// Wire protocol — QTMGSP02
//
// TCP framing:
//     [4 bytes payload length, big-endian]
//     [WirePacket payload]
//
// Payload:
//     [8 bytes magic]
//     [32 bytes txid]
//     [4 bytes vout, little-endian]
//     [8 bytes value, little-endian]
//     [4 bytes script length, little-endian]
//     [N bytes script_pubkey]
//     [32 bytes commitment]
//
// Do not change this payload without introducing a negotiated version.
// -----------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct WirePacket {
    pub outpoint: OutPoint,
    pub value: u64,
    pub script_pubkey: ScriptBuf,
    pub commitment: [u8; 32],
}

impl WirePacket {
    pub const MAGIC: &'static [u8; 8] = b"QTMGSP02";

    /// Fixed payload size excluding the script bytes.
    pub const MIN_SIZE: usize = 8 + 32 + 4 + 8 + 4 + 32;

    pub const MAX_SIZE: usize = 1_048_576;

    #[inline(always)]
    pub fn new(
        outpoint: OutPoint,
        value: u64,
        script_pubkey: ScriptBuf,
        commitment: [u8; 32],
    ) -> Self {
        Self {
            outpoint,
            value,
            script_pubkey,
            commitment,
        }
    }

    #[inline(always)]
    pub fn txid(&self) -> Txid {
        self.outpoint.txid
    }

    #[inline(always)]
    pub fn vout(&self) -> u32 {
        self.outpoint.vout
    }

    /// Stable identifier for logs and downstream indexing.
    /// This identifies the QTM commitment without changing the wire format.
    #[inline(always)]
    pub fn commitment_id(&self) -> String {
        hex::encode(self.commitment)
    }

    /// Serializes the packet payload, excluding the TCP length prefix.
    ///
    /// Returns an empty vector if the script cannot fit in the wire format.
    #[inline(always)]
    pub fn serialize(&self) -> Vec<u8> {
        let script = self.script_pubkey.as_bytes();

        let Some(total_size) = Self::MIN_SIZE.checked_add(script.len()) else {
            return Vec::new();
        };

        if total_size > Self::MAX_SIZE
            || u32::try_from(script.len()).is_err()
        {
            return Vec::new();
        }

        let mut out = Vec::with_capacity(total_size);

        out.extend_from_slice(Self::MAGIC);
        out.extend_from_slice(self.outpoint.txid.as_byte_array());
        out.extend_from_slice(&self.outpoint.vout.to_le_bytes());
        out.extend_from_slice(&self.value.to_le_bytes());
        out.extend_from_slice(
            &(script.len() as u32).to_le_bytes(),
        );
        out.extend_from_slice(script);
        out.extend_from_slice(&self.commitment);

        out
    }

    pub fn deserialize(bytes: &[u8]) -> Result<Self, Error> {
        if bytes.len() < Self::MIN_SIZE {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                "wire packet too short",
            ));
        }

        if bytes.len() > Self::MAX_SIZE {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "wire packet exceeds maximum size",
            ));
        }

        let mut offset = 0usize;

        let magic = take(bytes, &mut offset, 8)?;

        if magic != Self::MAGIC {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "invalid wire packet magic",
            ));
        }

        let txid = Txid::from_slice(
            take(bytes, &mut offset, 32)?,
        )
        .map_err(|_| {
            Error::new(ErrorKind::InvalidData, "invalid txid")
        })?;

        let vout = u32::from_le_bytes(
            take_array::<4>(bytes, &mut offset)?,
        );

        let value = u64::from_le_bytes(
            take_array::<8>(bytes, &mut offset)?,
        );

        let script_len = u32::from_le_bytes(
            take_array::<4>(bytes, &mut offset)?,
        ) as usize;

        let remaining = bytes.len().saturating_sub(offset);

        if remaining < 32 || script_len > remaining - 32 {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "script length exceeds packet boundary",
            ));
        }

        let script_pubkey = ScriptBuf::from_bytes(
            take(bytes, &mut offset, script_len)?.to_vec(),
        );

        let commitment = take_array::<32>(bytes, &mut offset)?;

        if offset != bytes.len() {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "trailing bytes in wire packet",
            ));
        }

        Ok(Self {
            outpoint: OutPoint { txid, vout },
            value,
            script_pubkey,
            commitment,
        })
    }
}

// -----------------------------------------------------------------------------
// Listening portal — mio TCP ingress
//
// BIT_MINNER_TCP_BIND=0.0.0.0:PORT
// -----------------------------------------------------------------------------

pub struct TcpReceiver;

impl TcpReceiver {
    pub fn from_env() -> Option<SocketAddr> {
        env::var("BIT_MINNER_TCP_BIND")
            .ok()?
            .parse()
            .ok()
    }

    pub fn spawn(bind: SocketAddr) {
        thread::spawn(move || {
            if let Err(error) = Self::run(bind) {
                eprintln!(
                    "tcp_receiver stopped addr={} error={}",
                    bind,
                    error,
                );
            }
        });
    }

    fn run(bind: SocketAddr) -> Result<(), Error> {
        const LISTENER: Token = Token(0);

        let mut poll = Poll::new()?;
        let mut events = Events::with_capacity(128);
        let mut listener = TcpListener::bind(bind)?;

        poll.registry().register(
            &mut listener,
            LISTENER,
            Interest::READABLE,
        )?;

        println!("tcp_portal=online addr={}", bind);

        loop {
            poll.poll(&mut events, None)?;

            for event in events.iter() {
                if event.token() != LISTENER || !event.is_readable() {
                    continue;
                }

                loop {
                    match listener.accept() {
                        Ok((mut stream, peer)) => {
                            println!(
                                "tcp_portal=accepted peer={}",
                                peer,
                            );

                            if let Err(error) =
                                poll.registry().deregister(&mut stream)
                            {
                                eprintln!(
                                    "tcp_portal=deregister_failed \
                                     peer={} error={}",
                                    peer,
                                    error,
                                );
                                continue;
                            }

                            let stream: TcpStream = stream.into();

                            thread::spawn(move || {
                                if let Err(error) = Self::receive(stream) {
                                    eprintln!(
                                        "tcp_receive peer={} error={}",
                                        peer,
                                        error,
                                    );
                                }
                            });
                        }

                        Err(error)
                            if error.kind() == ErrorKind::WouldBlock =>
                        {
                            break;
                        }

                        Err(error)
                            if error.kind() == ErrorKind::Interrupted =>
                        {
                            continue;
                        }

                        Err(error) => return Err(error),
                    }
                }
            }
        }
    }

    fn receive(mut stream: TcpStream) -> Result<(), Error> {
        let peer = stream.peer_addr().ok();

        stream.set_nodelay(true)?;

        loop {
            let mut length = [0u8; 4];

            match stream.read_exact(&mut length) {
                Ok(()) => {}
                Err(error)
                    if matches!(
                        error.kind(),
                        ErrorKind::UnexpectedEof
                            | ErrorKind::ConnectionReset
                            | ErrorKind::ConnectionAborted
                    ) =>
                {
                    return Ok(());
                }
                Err(error) => return Err(error),
            }

            let frame_len = u32::from_be_bytes(length) as usize;

            if !(WirePacket::MIN_SIZE..=WirePacket::MAX_SIZE)
                .contains(&frame_len)
            {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    format!("invalid frame length={}", frame_len),
                ));
            }

            let mut bytes = vec![0u8; frame_len];
            stream.read_exact(&mut bytes)?;

            let packet = WirePacket::deserialize(&bytes)?;

            println!(
                "qtm=wire_packet_received \
                 peer={} \
                 txid={} \
                 vout={} \
                 value={} \
                 script={} \
                 commitment_id={}",
                peer.map(|addr| addr.to_string())
                    .unwrap_or_else(|| "<unknown>".into()),
                packet.txid(),
                packet.vout(),
                packet.value,
                short_hex(packet.script_pubkey.as_bytes()),
                packet.commitment_id(),
            );
        }
    }
}

// -----------------------------------------------------------------------------
// Console driver
// -----------------------------------------------------------------------------

pub struct ConsoleBroadcastDriver;

impl BroadcastDriver for ConsoleBroadcastDriver {
    #[inline(always)]
    fn publish(&self, packet: &WirePacket) -> WirePacket {
        println!(
            "qtm=broadcast \
             txid={} \
             vout={} \
             value={} \
             script={} \
             commitment_id={}",
            packet.txid(),
            packet.vout(),
            packet.value,
            short_hex(packet.script_pubkey.as_bytes()),
            packet.commitment_id(),
        );

        packet.clone()
    }
}

// -----------------------------------------------------------------------------
// UDP gossip
//
// BIT_MINNER_UDP_PEERS=host:port,host:port
// -----------------------------------------------------------------------------

pub struct UdpGossipDriver {
    peers: Vec<SocketAddr>,
}

impl UdpGossipDriver {
    pub fn from_env() -> Option<Self> {
        parse_peers("BIT_MINNER_UDP_PEERS")
            .map(|peers| Self { peers })
    }
}

impl BroadcastDriver for UdpGossipDriver {
    #[inline(always)]
    fn publish(&self, packet: &WirePacket) -> WirePacket {
        let bytes = packet.serialize();

        if bytes.is_empty() {
            eprintln!(
                "udp_gossip=invalid_packet commitment_id={}",
                packet.commitment_id(),
            );
            return packet.clone();
        }

        match UdpSocket::bind("[::]:0")
            .or_else(|_| UdpSocket::bind("0.0.0.0:0"))
        {
            Ok(socket) => {
                for peer in &self.peers {
                    if let Err(error) = socket.send_to(&bytes, peer) {
                        eprintln!(
                            "udp_gossip=failed \
                             peer={} \
                             commitment_id={} \
                             error={}",
                            peer,
                            packet.commitment_id(),
                            error,
                        );
                    }
                }
            }
            Err(error) => {
                eprintln!(
                    "udp_gossip=socket_failed error={}",
                    error,
                );
            }
        }

        packet.clone()
    }
}

// -----------------------------------------------------------------------------
// TCP gossip
//
// BIT_MINNER_TCP_PEERS=host:port,host:port
// -----------------------------------------------------------------------------

pub struct TcpGossipDriver {
    peers: Vec<SocketAddr>,
}

impl TcpGossipDriver {
    pub fn from_env() -> Option<Self> {
        parse_peers("BIT_MINNER_TCP_PEERS")
            .map(|peers| Self { peers })
    }
}

impl BroadcastDriver for TcpGossipDriver {
    #[inline(always)]
    fn publish(&self, packet: &WirePacket) -> WirePacket {
        let bytes = packet.serialize();

        if bytes.is_empty() {
            eprintln!(
                "tcp_gossip=invalid_packet commitment_id={}",
                packet.commitment_id(),
            );
            return packet.clone();
        }

        let Ok(length) = u32::try_from(bytes.len()) else {
            eprintln!(
                "tcp_gossip=frame_too_large commitment_id={}",
                packet.commitment_id(),
            );
            return packet.clone();
        };

        let length = length.to_be_bytes();

        for peer in &self.peers {
            match TcpStream::connect_timeout(
                peer,
                Duration::from_millis(250),
            ) {
                Ok(mut stream) => {
                    if let Err(error) = stream.set_nodelay(true) {
                        eprintln!(
                            "tcp_gossip=failed peer={} error={}",
                            peer,
                            error,
                        );
                        continue;
                    }

                    if let Err(error) = stream
                        .write_all(&length)
                        .and_then(|()| stream.write_all(&bytes))
                    {
                        eprintln!(
                            "tcp_gossip=failed \
                             peer={} \
                             commitment_id={} \
                             error={}",
                            peer,
                            packet.commitment_id(),
                            error,
                        );
                    } else {
                        println!(
                            "tcp_gossip=sent peer={} commitment_id={}",
                            peer,
                            packet.commitment_id(),
                        );
                    }
                }
                Err(error) => {
                    eprintln!(
                        "tcp_gossip=unreachable peer={} error={}",
                        peer,
                        error,
                    );
                }
            }
        }

        packet.clone()
    }
}

// -----------------------------------------------------------------------------
// QTM packet broadcaster
// -----------------------------------------------------------------------------

pub struct Broadcaster {
    queue: SyncSender<WirePacket>,
}

impl Broadcaster {
    pub fn init() -> Self {
        if let Some(bind) = TcpReceiver::from_env() {
            TcpReceiver::spawn(bind);
        }

        let mut drivers: Vec<Arc<dyn BroadcastDriver>> = vec![
            Arc::new(ConsoleBroadcastDriver),
        ];

        if let Some(driver) = UdpGossipDriver::from_env() {
            drivers.push(Arc::new(driver));
        }

        if let Some(driver) = TcpGossipDriver::from_env() {
            drivers.push(Arc::new(driver));
        }

        Self::new(drivers, 4096)
    }

    pub fn new(
        drivers: Vec<Arc<dyn BroadcastDriver>>,
        queue_depth: usize,
    ) -> Self {
        let (tx, rx): (SyncSender<WirePacket>, Receiver<WirePacket>) =
            sync_channel(queue_depth.max(1));

        thread::spawn(move || {
            println!(
                "qtm_broadcaster=online drivers={}",
                drivers.len(),
            );

            while let Ok(packet) = rx.recv() {
                for driver in &drivers {
                    let _ = driver.publish(&packet);
                }
            }

            println!("qtm_broadcaster=stopped");
        });

        Self { queue: tx }
    }

    /// Blocking send applies backpressure instead of silently dropping a
    /// packet when the queue is full.
    #[inline(always)]
    pub fn publish(&self, packet: WirePacket) -> WirePacket {
        self.queue
            .send(packet.clone())
            .expect("QTM broadcaster worker disconnected");

        packet
    }
}

// -----------------------------------------------------------------------------
// Utilities
// -----------------------------------------------------------------------------

#[inline(always)]
fn short_hex(bytes: &[u8]) -> String {
    let full = hex::encode(bytes);

    if full.len() <= 16 {
        full
    } else {
        format!(
            "{}...{}",
            &full[..8],
            &full[full.len() - 8..],
        )
    }
}

#[inline(always)]
fn take<'a>(
    bytes: &'a [u8],
    offset: &mut usize,
    len: usize,
) -> Result<&'a [u8], Error> {
    let end = offset.checked_add(len).ok_or_else(|| {
        Error::new(
            ErrorKind::InvalidData,
            "packet offset overflow",
        )
    })?;

    if end > bytes.len() {
        return Err(Error::new(
            ErrorKind::UnexpectedEof,
            "truncated wire packet",
        ));
    }

    let slice = &bytes[*offset..end];
    *offset = end;

    Ok(slice)
}

#[inline(always)]
fn take_array<const N: usize>(
    bytes: &[u8],
    offset: &mut usize,
) -> Result<[u8; N], Error> {
    take(bytes, offset, N)?
        .try_into()
        .map_err(|_| {
            Error::new(
                ErrorKind::InvalidData,
                "invalid fixed-size field",
            )
        })
}

fn parse_peers(key: &str) -> Option<Vec<SocketAddr>> {
    let raw = env::var(key).ok()?;

    let peers: Vec<SocketAddr> = raw
        .split(',')
        .filter_map(|peer| peer.trim().parse::<SocketAddr>().ok())
        .collect();

    (!peers.is_empty()).then_some(peers)
}

// -----------------------------------------------------------------------------
// Electrum transaction broadcaster — BlueWallet ElectrumX pool
//
// This service is independent of QTM WirePacket gossip. Call it from within
// BitcoinObservationPipeline after the canonical settlement transaction has
// been finalized.
//
// submit() only queues the transaction and returns its local txid. DNS,
// connection, TLS and Electrum JSON-RPC work happens on the worker thread.
// -----------------------------------------------------------------------------

use rustls::{ClientConfig, RootCertStore, pki_types::ServerName};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::TcpStream as TokioTcpStream,
    sync::mpsc::{self, error::TrySendError as TokioTrySendError},
    task::JoinSet,
    time::timeout,
};
use tokio_rustls::TlsConnector;

const ELECTRUM_ENDPOINT_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_CONCURRENT_TRANSACTIONS: usize = 32;

#[derive(Clone, Copy, Debug)]
struct ElectrumEndpoint {
    host: &'static str,
    port: u16,
    tls: bool,
}

const BLUEWALLET_ELECTRUM_ENDPOINTS: [ElectrumEndpoint; 6] = [
    ElectrumEndpoint {
        host: "electrum1.bluewallet.io",
        port: 443,
        tls: true,
    },
    ElectrumEndpoint {
        host: "electrum2.bluewallet.io",
        port: 443,
        tls: true,
    },
    ElectrumEndpoint {
        host: "electrum3.bluewallet.io",
        port: 443,
        tls: true,
    },
    ElectrumEndpoint {
        host: "electrum1.bluewallet.io",
        port: 50001,
        tls: false,
    },
    ElectrumEndpoint {
        host: "electrum2.bluewallet.io",
        port: 50001,
        tls: false,
    },
    ElectrumEndpoint {
        host: "electrum3.bluewallet.io",
        port: 50001,
        tls: false,
    },
];

#[derive(Debug)]
pub enum ElectrumQueueError {
    Full,
    Disconnected,
}

impl fmt::Display for ElectrumQueueError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Full => f.write_str("Electrum broadcast queue is full"),
            Self::Disconnected => {
                f.write_str("Electrum broadcast worker disconnected")
            }
        }
    }
}

impl std::error::Error for ElectrumQueueError {}

#[derive(Clone)]
pub struct ElectrumBroadcastService {
    queue: mpsc::Sender<bitcoin::Transaction>,
}

impl ElectrumBroadcastService {
    /// Starts a bounded, non-blocking transaction submission service.
    pub fn init(queue_depth: usize) -> Self {
        let (tx, mut rx) =
            mpsc::channel::<bitcoin::Transaction>(queue_depth.max(1));

        thread::spawn(move || {
            let runtime = match tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
            {
                Ok(runtime) => runtime,
                Err(error) => {
                    eprintln!(
                        "electrum_worker=runtime_failed error={}",
                        error
                    );
                    return;
                }
            };

            let tls = Arc::new(electrum_tls_config());

            runtime.block_on(async move {
                let mut tasks = JoinSet::new();
                let mut channel_closed = false;

                while !channel_closed || !tasks.is_empty() {
                    tokio::select! {
                        transaction = rx.recv(),
                            if !channel_closed
                                && tasks.len() < MAX_CONCURRENT_TRANSACTIONS =>
                        {
                            match transaction {
                                Some(transaction) => {
                                    let tls = Arc::clone(&tls);
                                    tasks.spawn(async move {
                                        let txid = transaction.compute_txid();
                                        let result =
                                            electrum_broadcast_transaction(
                                                transaction,
                                                tls,
                                            ).await;
                                        (txid, result)
                                    });
                                }
                                None => channel_closed = true,
                            }
                        }

                        joined = tasks.join_next(), if !tasks.is_empty() => {
                            match joined {
                                Some(Ok((txid, Ok(endpoint)))) => {
                                    println!(
                                        "electrum_broadcast=accepted \
                                         txid={} endpoint={}",
                                        txid,
                                        endpoint
                                    );
                                }
                                Some(Ok((txid, Err(error)))) => {
                                    eprintln!(
                                        "electrum_broadcast=failed \
                                         txid={} error={}",
                                        txid,
                                        error
                                    );
                                }
                                Some(Err(error)) => {
                                    eprintln!(
                                        "electrum_worker=task_failed error={}",
                                        error
                                    );
                                }
                                None => {}
                            }
                        }
                    }
                }
            });
        });

        Self { queue: tx }
    }

    /// Returns the locally computed txid once the transaction is enqueued.
    /// This is not a relay confirmation or an on-chain confirmation.
    #[inline]
    pub fn submit(
        &self,
        transaction: bitcoin::Transaction,
    ) -> Result<Txid, ElectrumQueueError> {
        let txid = transaction.compute_txid();

        self.queue
            .try_send(transaction)
            .map_err(|error| match error {
                TokioTrySendError::Full(_) => ElectrumQueueError::Full,
                TokioTrySendError::Closed(_) => {
                    ElectrumQueueError::Disconnected
                }
            })?;

        Ok(txid)
    }
}

fn electrum_tls_config() -> ClientConfig {
    let roots = RootCertStore::from_iter(
        webpki_roots::TLS_SERVER_ROOTS.iter().cloned(),
    );

    ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth()
}

async fn electrum_broadcast_transaction(
    transaction: bitcoin::Transaction,
    tls: Arc<ClientConfig>,
) -> Result<String, String> {
    let expected_txid = transaction.compute_txid();
    let raw_transaction = Arc::<str>::from(hex::encode(
        bitcoin::consensus::serialize(&transaction),
    ));

    let mut endpoints = JoinSet::new();

    for endpoint in BLUEWALLET_ELECTRUM_ENDPOINTS {
        let raw_transaction = Arc::clone(&raw_transaction);
        let tls = Arc::clone(&tls);

        endpoints.spawn(async move {
            let result = timeout(
                ELECTRUM_ENDPOINT_TIMEOUT,
                electrum_broadcast_endpoint(
                    endpoint,
                    raw_transaction,
                    tls,
                ),
            )
            .await
            .map_err(|_| "endpoint timed out".to_owned())??;

            if result != expected_txid.to_string() {
                return Err(format!(
                    "server returned txid {}, expected {}",
                    result,
                    expected_txid
                ));
            }

            Ok::<_, String>((endpoint, result))
        });
    }

    let mut failures = Vec::new();

    while let Some(joined) = endpoints.join_next().await {
        match joined {
            Ok(Ok((endpoint, txid))) => {
                endpoints.abort_all();

                return Ok(format!(
                    "{}:{} txid={}",
                    endpoint.host,
                    endpoint.port,
                    txid
                ));
            }
            Ok(Err(error)) => failures.push(error),
            Err(error) if error.is_cancelled() => {}
            Err(error) => failures.push(error.to_string()),
        }
    }

    Err(if failures.is_empty() {
        "all Electrum endpoint attempts were cancelled".to_owned()
    } else {
        failures.join("; ")
    })
}

async fn electrum_broadcast_endpoint(
    endpoint: ElectrumEndpoint,
    raw_transaction: Arc<str>,
    tls_config: Arc<ClientConfig>,
) -> Result<String, String> {
    let stream = TokioTcpStream::connect((endpoint.host, endpoint.port))
        .await
        .map_err(|error| {
            format!(
                "connect {}:{}: {}",
                endpoint.host,
                endpoint.port,
                error
            )
        })?;

    stream
        .set_nodelay(true)
        .map_err(|error| error.to_string())?;

    if endpoint.tls {
        let connector = TlsConnector::from(tls_config);
        let server_name = ServerName::try_from(endpoint.host.to_owned())
            .map_err(|error| {
                format!("invalid TLS server name: {}", error)
            })?;

        let stream = connector
            .connect(server_name, stream)
            .await
            .map_err(|error| {
                format!("TLS {}: {}", endpoint.host, error)
            })?;

        electrum_exchange(stream, raw_transaction).await
    } else {
        electrum_exchange(stream, raw_transaction).await
    }
}

async fn electrum_exchange<S>(
    stream: S,
    raw_transaction: Arc<str>,
) -> Result<String, String>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let mut stream = BufReader::new(stream);

    let version_request = serde_json::json!({
        "id": 0,
        "method": "server.version",
        "params": ["bit-miner", "1.4"]
    });

    let broadcast_request = serde_json::json!({
        "id": 1,
        "method": "blockchain.transaction.broadcast",
        "params": [raw_transaction.as_ref()]
    });

    let mut requests = serde_json::to_vec(&version_request)
        .map_err(|error| error.to_string())?;
    requests.push(b'\n');
    requests.extend_from_slice(
        &serde_json::to_vec(&broadcast_request)
            .map_err(|error| error.to_string())?,
    );
    requests.push(b'\n');

    stream
        .get_mut()
        .write_all(&requests)
        .await
        .map_err(|error| {
            format!("write Electrum requests: {}", error)
        })?;

    stream
        .get_mut()
        .flush()
        .await
        .map_err(|error| {
            format!("flush Electrum requests: {}", error)
        })?;

    let mut version_received = false;
    let mut broadcast_result: Option<String> = None;
    let mut line = String::new();

    while !version_received || broadcast_result.is_none() {
        line.clear();

        let count = stream
            .read_line(&mut line)
            .await
            .map_err(|error| {
                format!("read Electrum response: {}", error)
            })?;

        if count == 0 {
            return Err(
                "Electrum server closed before both responses arrived"
                    .to_owned(),
            );
        }

        let response: serde_json::Value =
            serde_json::from_str(line.trim()).map_err(|error| {
                format!("invalid Electrum JSON response: {}", error)
            })?;

        match response
            .get("id")
            .and_then(serde_json::Value::as_u64)
        {
            Some(0) => {
                if let Some(error) = response
                    .get("error")
                    .filter(|value| !value.is_null())
                {
                    return Err(format!(
                        "server.version rejected: {}",
                        error
                    ));
                }

                if response.get("result").is_none() {
                    return Err(
                        "server.version response has no result".to_owned()
                    );
                }

                version_received = true;
            }
            Some(1) => {
                if let Some(error) = response
                    .get("error")
                    .filter(|value| !value.is_null())
                {
                    return Err(format!(
                        "transaction rejected: {}",
                        error
                    ));
                }

                let result = response
                    .get("result")
                    .and_then(serde_json::Value::as_str)
                    .ok_or_else(|| {
                        "broadcast response has no txid result".to_owned()
                    })?;

                broadcast_result = Some(result.to_owned());
            }
            _ => {}
        }
    }

    broadcast_result.ok_or_else(|| {
        "Electrum returned no transaction id".to_owned()
    })
}
