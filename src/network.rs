//! Permissionless replication of one explicitly trusted sequencer.
//!
//! Peers are transports, never voting authorities. Every accepted block has a
//! pinned Ed25519 signature covering its complete contents, and local history is
//! append-only. This protocol deliberately does not implement validator consensus.

use crate::{Block, BlockchainState, Chain, ChainError, Result, Store};
use ed25519_dalek::{Signer, SigningKey, VerifyingKey};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{watch, Mutex};

pub(crate) const PROTOCOL_VERSION: u32 = 1;
const MAX_PEERS: usize = 16;
const BLOCKS_PER_PEER: u64 = 32;
const MAX_WIRE_BYTES: usize = 64 * 1024 * 1024;
const MAX_INFO_BYTES: usize = 16 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum Role {
    Authority,
    Follower,
}

#[derive(Clone, Debug)]
pub(crate) struct NetworkConfig {
    pub role: Role,
    pub peers: Vec<String>,
    pub poll_interval: Duration,
    pub authority_key_file: PathBuf,
    pub authority_public_key: Option<String>,
    pub expected_chain_id: Option<String>,
}

impl NetworkConfig {
    #[cfg(test)]
    pub fn test_authority(data_dir: &str) -> Self {
        Self {
            role: Role::Authority,
            peers: Vec::new(),
            poll_interval: Duration::from_millis(250),
            authority_key_file: PathBuf::from(data_dir).join("authority.key"),
            authority_public_key: None,
            expected_chain_id: None,
        }
    }

    pub fn from_env(data_dir: &str) -> Result<Self> {
        let role = match std::env::var("TOMEI_NODE_ROLE").as_deref() {
            Ok("authority") | Err(std::env::VarError::NotPresent) => Role::Authority,
            Ok("follower") => Role::Follower,
            _ => return Err(invalid("TOMEI_NODE_ROLE must be authority or follower")),
        };
        let peers = parse_peers(&std::env::var("TOMEI_PEERS").unwrap_or_default())?;
        let authority_public_key = optional_env("TOMEI_AUTHORITY_PUBLIC_KEY");
        if role == Role::Follower && (authority_public_key.is_none() || peers.is_empty()) {
            return Err(invalid(
                "follower requires TOMEI_AUTHORITY_PUBLIC_KEY and TOMEI_PEERS",
            ));
        }
        if let Some(key) = &authority_public_key {
            parse_public_key(key)?;
        }
        let millis = match optional_env("TOMEI_SYNC_INTERVAL_MS") {
            Some(value) => value
                .parse::<u64>()
                .map_err(|_| invalid("invalid TOMEI_SYNC_INTERVAL_MS"))?,
            None => 2_000,
        };
        if !(250..=60_000).contains(&millis) {
            return Err(invalid("TOMEI_SYNC_INTERVAL_MS must be 250..60000"));
        }
        Ok(Self {
            role,
            peers,
            poll_interval: Duration::from_millis(millis),
            authority_key_file: optional_env("TOMEI_AUTHORITY_KEY_FILE")
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from(data_dir).join("authority.key")),
            authority_public_key,
            expected_chain_id: optional_env("TOMEI_CHAIN_ID"),
        })
    }
}

fn optional_env(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
}

fn parse_peers(value: &str) -> Result<Vec<String>> {
    let mut peers = Vec::new();
    for peer in value
        .split(',')
        .map(str::trim)
        .filter(|peer| !peer.is_empty())
    {
        let url = reqwest::Url::parse(peer).map_err(|_| invalid("invalid peer URL"))?;
        if !matches!(url.scheme(), "http" | "https")
            || url.host_str().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(invalid(
                "peers must be HTTP(S) URLs without credentials, queries or fragments",
            ));
        }
        let peer = url.as_str().trim_end_matches('/').to_owned();
        if !peers.contains(&peer) {
            peers.push(peer);
        }
        if peers.len() > MAX_PEERS {
            return Err(invalid("at most 16 peers may be configured"));
        }
    }
    Ok(peers)
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SignedCheckpoint {
    pub protocol_version: u32,
    pub chain_id: String,
    pub genesis_hash: String,
    pub authority_public_key: String,
    pub height: u64,
    pub block_hash: String,
    /// Hash of bincode(Block), including transaction signatures and all metadata.
    pub block_digest: String,
    pub signature: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SignedBlock {
    pub block: Block,
    pub checkpoint: SignedCheckpoint,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct NetworkInfo {
    pub protocol_version: u32,
    pub role: Role,
    pub chain_id: String,
    pub genesis_hash: String,
    pub authority_public_key: String,
    pub height: u64,
    pub tip_hash: String,
    pub total_txs: u64,
    pub checkpoint: Option<SignedCheckpoint>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub(crate) struct SyncStatus {
    pub last_success_ms: Option<u64>,
    pub last_error: Option<String>,
    pub remote_height: Option<u64>,
    pub active_peer: Option<String>,
    pub halted: bool,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
struct Identity {
    protocol_version: u32,
    chain_id: String,
    genesis_hash: String,
    authority_public_key: String,
}

pub(crate) struct Network {
    pub config: NetworkConfig,
    pub chain_id: String,
    pub authority_public_key: String,
    identity: Identity,
    signer: Option<SigningKey>,
    verifier: VerifyingKey,
    signatures: sled::Tree,
    pub sync_status: Mutex<SyncStatus>,
}

impl Network {
    /// Call only after verifying the existing local chain. Old authority history
    /// is attested once; a follower must already have evidence for every block.
    pub fn open(store: &Store, config: &NetworkConfig) -> Result<Self> {
        let signer = if config.role == Role::Authority {
            Some(load_or_create_key(&config.authority_key_file)?)
        } else {
            None
        };
        let verifier = match &signer {
            Some(key) => key.verifying_key(),
            None => parse_public_key(
                config
                    .authority_public_key
                    .as_deref()
                    .ok_or_else(|| invalid("follower authority key missing"))?,
            )?,
        };
        if let Some(expected) = &config.authority_public_key {
            if parse_public_key(expected)? != verifier {
                return Err(invalid(
                    "authority secret key does not match pinned public key",
                ));
            }
        }
        let genesis_hash = Block::genesis()?.hash;
        let authority_public_key = encode_hex(verifier.as_bytes());
        let chain_id = blake3::hash(&bincode::serialize(&(
            "tomei-chain/network-identity/v1",
            &genesis_hash,
            &authority_public_key,
        ))?)
        .to_hex()
        .to_string();
        if config
            .expected_chain_id
            .as_ref()
            .is_some_and(|expected| expected != &chain_id)
        {
            return Err(invalid(
                "TOMEI_CHAIN_ID does not match authority and genesis",
            ));
        }
        let identity = Identity {
            protocol_version: PROTOCOL_VERSION,
            chain_id,
            genesis_hash,
            authority_public_key,
        };
        let metadata = store.db.open_tree("network_identity")?;
        let encoded_identity = bincode::serialize(&identity)?;
        if let Some(bytes) = metadata.get(b"identity")? {
            let stored: Identity = bincode::deserialize(&bytes)?;
            if stored != identity {
                return Err(invalid("database belongs to another network/authority; use its original key or a new data directory"));
            }
        }
        let network = Self {
            config: config.clone(),
            chain_id: identity.chain_id.clone(),
            authority_public_key: identity.authority_public_key.clone(),
            identity,
            signer,
            verifier,
            signatures: store.signatures.clone(),
            sync_status: Mutex::new(SyncStatus::default()),
        };
        // Never overwrite existing evidence: verify it before serving history.
        for entry in store.blocks.iter() {
            let (_, bytes) = entry?;
            let block: Block = bincode::deserialize(&bytes)?;
            if let Some(checkpoint) = network.stored_checkpoint(block.header.index)? {
                network.verify_block_checkpoint(&block, &checkpoint)?;
            } else if network.signer.is_some() {
                let checkpoint = network.create_checkpoint(&block)?;
                network.signatures.insert(
                    block.header.index.to_be_bytes(),
                    checkpoint_json(&checkpoint)?.as_bytes(),
                )?;
            } else if block.header.index != 0 {
                return Err(invalid(
                    "follower database contains an unsigned block; resync into a fresh directory",
                ));
            }
        }
        metadata.insert(b"identity", encoded_identity)?;
        store.db.flush()?;
        Ok(network)
    }

    pub fn signature_for_commit(&self, block: &Block) -> Result<Option<String>> {
        if self.signer.is_none() {
            return Err(invalid("followers cannot sign blocks"));
        }
        Ok(Some(checkpoint_json(&self.create_checkpoint(block)?)?))
    }

    pub fn signed_block(&self, store: &Store, height: u64) -> Result<Option<SignedBlock>> {
        let Some(block) = store.get_block(height)? else {
            return Ok(None);
        };
        let Some(checkpoint) = self.stored_checkpoint(height)? else {
            return Ok(None);
        };
        self.verify_block_checkpoint(&block, &checkpoint)?;
        Ok(Some(SignedBlock { block, checkpoint }))
    }

    pub fn info(&self, state: &BlockchainState) -> Result<NetworkInfo> {
        Ok(NetworkInfo {
            protocol_version: PROTOCOL_VERSION,
            role: self.config.role,
            chain_id: self.identity.chain_id.clone(),
            genesis_hash: self.identity.genesis_hash.clone(),
            authority_public_key: self.identity.authority_public_key.clone(),
            height: state.height,
            tip_hash: state.tip_hash.clone(),
            total_txs: state.total_txs,
            checkpoint: self.stored_checkpoint(state.height)?,
        })
    }

    pub fn verify(&self, signed: &SignedBlock) -> Result<()> {
        self.verify_block_checkpoint(&signed.block, &signed.checkpoint)
    }

    pub fn verify_checkpoint(&self, checkpoint: &SignedCheckpoint) -> Result<()> {
        if checkpoint.protocol_version != PROTOCOL_VERSION
            || checkpoint.chain_id != self.identity.chain_id
            || checkpoint.genesis_hash != self.identity.genesis_hash
            || checkpoint.authority_public_key != self.identity.authority_public_key
        {
            return Err(invalid(
                "peer checkpoint belongs to another network/authority or protocol",
            ));
        }
        decode_hex::<32>(&checkpoint.block_hash)?;
        decode_hex::<32>(&checkpoint.block_digest)?;
        let signature =
            ed25519_dalek::Signature::from_bytes(&decode_hex::<64>(&checkpoint.signature)?);
        self.verifier
            .verify_strict(&checkpoint_message(checkpoint)?, &signature)
            .map_err(|_| invalid("invalid authority checkpoint signature"))
    }

    fn verify_block_checkpoint(&self, block: &Block, checkpoint: &SignedCheckpoint) -> Result<()> {
        self.verify_checkpoint(checkpoint)?;
        if checkpoint.height != block.header.index
            || checkpoint.block_hash != block.hash
            || checkpoint.block_digest != block_digest(block)?
        {
            return Err(invalid("block contents do not match signed checkpoint"));
        }
        block.verify_integrity()?;
        if block.header.index == 0 && block != &Block::genesis()? {
            return Err(invalid("peer supplied a noncanonical genesis block"));
        }
        Ok(())
    }

    fn create_checkpoint(&self, block: &Block) -> Result<SignedCheckpoint> {
        let signer = self
            .signer
            .as_ref()
            .ok_or_else(|| invalid("follower has no signing key"))?;
        block.verify_integrity()?;
        let mut checkpoint = SignedCheckpoint {
            protocol_version: PROTOCOL_VERSION,
            chain_id: self.identity.chain_id.clone(),
            genesis_hash: self.identity.genesis_hash.clone(),
            authority_public_key: self.identity.authority_public_key.clone(),
            height: block.header.index,
            block_hash: block.hash.clone(),
            block_digest: block_digest(block)?,
            signature: String::new(),
        };
        checkpoint.signature =
            encode_hex(&signer.sign(&checkpoint_message(&checkpoint)?).to_bytes());
        Ok(checkpoint)
    }

    fn stored_checkpoint(&self, height: u64) -> Result<Option<SignedCheckpoint>> {
        self.signatures
            .get(height.to_be_bytes())?
            .map(|bytes| {
                serde_json::from_slice(&bytes).map_err(|_| invalid("invalid stored checkpoint"))
            })
            .transpose()
    }
}

pub(crate) fn checkpoint_json(checkpoint: &SignedCheckpoint) -> Result<String> {
    serde_json::to_string(checkpoint).map_err(|_| invalid("cannot encode checkpoint"))
}

fn checkpoint_message(checkpoint: &SignedCheckpoint) -> Result<Vec<u8>> {
    Ok(bincode::serialize(&(
        "tomei-chain/signed-block/v1",
        checkpoint.protocol_version,
        &checkpoint.chain_id,
        &checkpoint.genesis_hash,
        &checkpoint.authority_public_key,
        checkpoint.height,
        &checkpoint.block_hash,
        &checkpoint.block_digest,
    ))?)
}

fn block_digest(block: &Block) -> Result<String> {
    Ok(blake3::hash(&bincode::serialize(block)?)
        .to_hex()
        .to_string())
}

fn invalid(message: impl Into<String>) -> ChainError {
    ChainError::Integrity(message.into())
}

fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut result = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        result.push(HEX[(byte >> 4) as usize] as char);
        result.push(HEX[(byte & 15) as usize] as char);
    }
    result
}

fn decode_hex<const N: usize>(text: &str) -> Result<[u8; N]> {
    if text.len() != N * 2 {
        return Err(invalid(format!(
            "expected {} hexadecimal characters",
            N * 2
        )));
    }
    let mut result = [0; N];
    for (index, pair) in text.as_bytes().chunks_exact(2).enumerate() {
        let digit = |c: u8| match c {
            b'0'..=b'9' => Ok(c - b'0'),
            b'a'..=b'f' => Ok(c - b'a' + 10),
            b'A'..=b'F' => Ok(c - b'A' + 10),
            _ => Err(invalid("invalid hexadecimal character")),
        };
        result[index] = digit(pair[0])? * 16 + digit(pair[1])?;
    }
    Ok(result)
}

fn parse_public_key(text: &str) -> Result<VerifyingKey> {
    let key = VerifyingKey::from_bytes(&decode_hex::<32>(text)?)
        .map_err(|_| invalid("invalid Ed25519 authority public key"))?;
    if key.is_weak() {
        return Err(invalid("weak Ed25519 authority public key"));
    }
    Ok(key)
}

fn load_or_create_key(path: &PathBuf) -> Result<SigningKey> {
    if !path.exists() {
        if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
            fs::create_dir_all(parent)?;
        }
        let mut seed = [0u8; 32];
        getrandom::getrandom(&mut seed).map_err(|_| invalid("OS random source unavailable"))?;
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(path)?;
        file.write_all(format!("{}\n", encode_hex(&seed)).as_bytes())?;
        file.sync_all()?;
    }
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.file_type().is_file() || metadata.len() > 128 {
        return Err(invalid(
            "authority key must be a regular file containing one 32-byte hex seed",
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err(invalid(
                "authority key permissions must be owner-only (chmod 600)",
            ));
        }
    }
    let seed = decode_hex::<32>(fs::read_to_string(path)?.trim())?;
    Ok(SigningKey::from_bytes(&seed))
}

async fn fetch_json<T: DeserializeOwned>(
    client: &reqwest::Client,
    url: &str,
    limit: usize,
) -> Result<T> {
    let mut response = client
        .get(url)
        .send()
        .await
        .map_err(|error| invalid(format!("peer request failed: {error}")))?;
    if !response.status().is_success() {
        return Err(invalid(format!("peer returned HTTP {}", response.status())));
    }
    if response
        .content_length()
        .is_some_and(|size| size > limit as u64)
    {
        return Err(invalid("peer response exceeds protocol size limit"));
    }
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|error| invalid(format!("peer response failed: {error}")))?
    {
        if chunk.len() > limit.saturating_sub(body.len()) {
            return Err(invalid("peer response exceeds protocol size limit"));
        }
        body.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&body).map_err(|_| invalid("peer returned invalid protocol JSON"))
}

struct SyncFailure {
    error: ChainError,
    conflict: bool,
}

impl From<ChainError> for SyncFailure {
    fn from(error: ChainError) -> Self {
        Self {
            error,
            conflict: false,
        }
    }
}

fn conflict(message: &str) -> SyncFailure {
    SyncFailure {
        error: invalid(message),
        conflict: true,
    }
}

async fn sync_peer(
    chain: &Chain,
    client: &reqwest::Client,
    peer: &str,
) -> std::result::Result<(), SyncFailure> {
    let info: NetworkInfo =
        fetch_json(client, &format!("{peer}/network/info"), MAX_INFO_BYTES).await?;
    let checkpoint = info
        .checkpoint
        .ok_or_else(|| invalid("peer has no authenticated checkpoint yet"))?;
    chain.network.verify_checkpoint(&checkpoint)?;
    if info.protocol_version != checkpoint.protocol_version
        || info.chain_id != checkpoint.chain_id
        || info.genesis_hash != checkpoint.genesis_hash
        || info.authority_public_key != checkpoint.authority_public_key
        || info.height != checkpoint.height
        || info.tip_hash != checkpoint.block_hash
    {
        return Err(invalid("peer metadata does not match its signed checkpoint").into());
    }
    {
        let mut status = chain.network.sync_status.lock().await;
        status.remote_height = Some(status.remote_height.unwrap_or(0).max(checkpoint.height));
    }
    let (local, _) = chain.snapshot().await;
    if checkpoint.height <= local.height {
        let known = chain
            .store
            .get_block(checkpoint.height)?
            .ok_or_else(|| invalid("local block missing"))?;
        if known.hash != checkpoint.block_hash || block_digest(&known)? != checkpoint.block_digest {
            return Err(conflict(
                "authority signed conflicting history; replication halted for operator review",
            ));
        }
        // Also bootstraps signed genesis on a fresh follower of an empty chain.
        chain
            .append_replica(SignedBlock {
                block: known,
                checkpoint,
            })
            .await?;
        return Ok(());
    }
    // Verify that a peer extending us actually contains our complete current tip.
    let anchor: SignedBlock = fetch_json(
        client,
        &format!("{peer}/network/block/{}", local.height),
        MAX_WIRE_BYTES,
    )
    .await?;
    chain.network.verify(&anchor)?;
    if anchor.block.header.index != local.height {
        return Err(invalid("peer returned the wrong anchor height").into());
    }
    let known = chain
        .store
        .get_block(local.height)?
        .ok_or_else(|| invalid("local tip missing"))?;
    if anchor.block != known {
        return Err(conflict(
            "authority signed a fork at the local tip; replication halted for operator review",
        ));
    }
    chain.append_replica(anchor).await?;
    let end = checkpoint
        .height
        .min(local.height.saturating_add(BLOCKS_PER_PEER));
    let mut previous_hash = local.tip_hash;
    for height in (local.height + 1)..=end {
        let signed: SignedBlock = fetch_json(
            client,
            &format!("{peer}/network/block/{height}"),
            MAX_WIRE_BYTES,
        )
        .await?;
        chain.network.verify(&signed)?;
        if signed.block.header.index != height {
            return Err(invalid("peer returned the wrong block height").into());
        }
        if signed.block.header.prev_block_hash != previous_hash {
            return Err(conflict(
                "authority signed a nonextending block; replication halted for operator review",
            ));
        }
        if height == checkpoint.height
            && (signed.checkpoint.block_hash != checkpoint.block_hash
                || signed.checkpoint.block_digest != checkpoint.block_digest)
        {
            return Err(conflict("authority signed inconsistent tip checkpoints; replication halted for operator review"));
        }
        previous_hash = signed.block.hash.clone();
        chain.append_replica(signed).await?;
    }
    Ok(())
}

pub(crate) async fn run_follower(chain: Arc<Chain>, mut shutdown: watch::Receiver<bool>) {
    let client = match reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(20))
        .user_agent(concat!("tomei-chain/", env!("CARGO_PKG_VERSION")))
        .build()
    {
        Ok(client) => client,
        Err(error) => {
            let mut status = chain.network.sync_status.lock().await;
            status.last_error = Some(format!("cannot initialize peer client: {error}"));
            status.halted = true;
            crate::log_line(status.last_error.as_deref().unwrap_or("peer client error"));
            return;
        }
    };
    loop {
        if *shutdown.borrow() {
            break;
        }
        for peer in &chain.network.config.peers {
            chain.network.sync_status.lock().await.active_peer = Some(peer.clone());
            let outcome = tokio::select! {
                result = sync_peer(&chain, &client, peer) => result,
                _ = shutdown.changed() => return,
            };
            let mut status = chain.network.sync_status.lock().await;
            match outcome {
                Ok(()) => {
                    status.last_success_ms = Some(crate::now_millis());
                    status.last_error = None;
                }
                Err(failure) => {
                    let message = failure.error.to_string();
                    if status.last_error.as_ref() != Some(&message) {
                        crate::log_line(&format!("peer {peer}: {message}"));
                    }
                    status.last_error = Some(message);
                    if failure.conflict {
                        status.halted = true;
                        // Keep the API available for investigation; no replacement
                        // authority, rollback, or automatic branch selection.
                        return;
                    }
                }
            }
        }
        tokio::select! {
            _ = tokio::time::sleep(chain.network.config.poll_interval) => {},
            _ = shutdown.changed() => break,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::{temp_dir, test_chain, test_config};
    use std::collections::HashMap;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn follower_config(directory: &str, authority: &Network) -> crate::Config {
        let mut config = test_config(directory.to_owned());
        config.network.role = Role::Follower;
        config.network.authority_public_key = Some(authority.authority_public_key.clone());
        config.network.expected_chain_id = Some(authority.chain_id.clone());
        config.network.peers = vec!["http://127.0.0.1:1".into()];
        config
    }

    async fn fixture_server(
        fixtures: HashMap<String, String>,
    ) -> (String, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            loop {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request = [0u8; 4096];
                let size = stream.read(&mut request).await.unwrap();
                let text = String::from_utf8_lossy(&request[..size]);
                let path = text.split_whitespace().nth(1).unwrap_or("");
                let (status, body) = match fixtures.get(path) {
                    Some(body) => ("200 OK", body.as_str()),
                    None => ("404 Not Found", "{}"),
                };
                let response = format!("HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
                // A client rejecting a length early may close before this write.
                let _ = stream.write_all(response.as_bytes()).await;
            }
        });
        (url, task)
    }

    async fn chain_fixtures(chain: &Chain) -> HashMap<String, String> {
        let (state, _) = chain.snapshot().await;
        let mut fixtures = HashMap::new();
        fixtures.insert(
            "/network/info".into(),
            serde_json::to_string(&chain.network.info(&state).unwrap()).unwrap(),
        );
        for height in 0..=state.height {
            let signed = chain
                .network
                .signed_block(&chain.store, height)
                .unwrap()
                .unwrap();
            fixtures.insert(
                format!("/network/block/{height}"),
                serde_json::to_string(&signed).unwrap(),
            );
        }
        fixtures
    }

    async fn seal(chain: &Chain, payload: &str) -> Block {
        chain
            .submit_tx(
                "network-test".into(),
                payload.into(),
                Some("original".into()),
                Vec::new(),
            )
            .await
            .unwrap();
        chain.seal_pending_block().await.unwrap().unwrap()
    }

    #[tokio::test]
    async fn signature_covers_transaction_signature_even_when_legacy_hash_does_not() {
        let (authority, path) = test_chain();
        let block = seal(&authority, "evidence").await;
        let signed = authority
            .network
            .signed_block(&authority.store, block.header.index)
            .unwrap()
            .unwrap();
        authority.network.verify(&signed).unwrap();
        let mut tampered = signed.clone();
        tampered.block.transactions[0].signature = Some("substituted".into());
        assert_eq!(tampered.block.hash, signed.block.hash);
        tampered.block.verify_integrity().unwrap();
        assert!(authority.network.verify(&tampered).is_err());
        let mut wrong_network = signed.clone();
        wrong_network.checkpoint.chain_id = "0".repeat(64);
        assert!(authority.network.verify(&wrong_network).is_err());
        let mut wrong_signature = signed;
        wrong_signature.checkpoint.signature = "0".repeat(128);
        assert!(authority.network.verify(&wrong_signature).is_err());
        drop(authority);
        fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn followers_relay_authenticated_history_and_resume_without_private_keys() {
        let (authority, authority_path) = test_chain();
        let first = seal(&authority, "first").await;
        let last = seal(&authority, "second").await;
        let follower_path = temp_dir("follower");
        let config = follower_config(&follower_path, &authority.network);
        let (follower, _) = Chain::bootstrap(&config).unwrap();
        let (url, server) = fixture_server(chain_fixtures(&authority).await).await;
        let client = reqwest::Client::new();
        sync_peer(&follower, &client, &url)
            .await
            .map_err(|e| e.error)
            .unwrap();
        assert_eq!(follower.snapshot().await.0.tip_hash, last.hash);
        assert_eq!(
            follower.store.tx_height(&first.transactions[0].id).unwrap(),
            Some(1)
        );
        assert!(follower.network.signature_for_commit(&last).is_err());
        assert!(!config.network.authority_key_file.exists());
        server.abort();
        drop(follower);
        let (resumed, restored) = Chain::bootstrap(&config).unwrap();
        assert!(restored);
        let relay_path = temp_dir("relay");
        let (relay, _) =
            Chain::bootstrap(&follower_config(&relay_path, &authority.network)).unwrap();
        let (url, server) = fixture_server(chain_fixtures(&resumed).await).await;
        sync_peer(&relay, &client, &url)
            .await
            .map_err(|e| e.error)
            .unwrap();
        assert_eq!(relay.snapshot().await.0, authority.snapshot().await.0);
        server.abort();
        drop((authority, resumed, relay));
        for path in [authority_path, follower_path, relay_path] {
            fs::remove_dir_all(path).unwrap();
        }
    }

    #[tokio::test]
    async fn authenticated_same_hash_content_fork_halts_without_overwriting_history() {
        let (authority, authority_path) = test_chain();
        let original = seal(&authority, "history").await;
        let follower_path = temp_dir("fork-follower");
        let (follower, _) =
            Chain::bootstrap(&follower_config(&follower_path, &authority.network)).unwrap();
        follower
            .append_replica(
                authority
                    .network
                    .signed_block(&authority.store, 0)
                    .unwrap()
                    .unwrap(),
            )
            .await
            .unwrap();
        follower
            .append_replica(
                authority
                    .network
                    .signed_block(&authority.store, 1)
                    .unwrap()
                    .unwrap(),
            )
            .await
            .unwrap();
        let mut fork = original.clone();
        fork.transactions[0].signature = Some("equivocated".into());
        let checkpoint = authority.network.create_checkpoint(&fork).unwrap();
        let mut info = authority
            .network
            .info(&authority.snapshot().await.0)
            .unwrap();
        info.checkpoint = Some(checkpoint);
        let fixtures = HashMap::from([(
            "/network/info".into(),
            serde_json::to_string(&info).unwrap(),
        )]);
        let (url, server) = fixture_server(fixtures).await;
        let failure = match sync_peer(&follower, &reqwest::Client::new(), &url).await {
            Ok(()) => panic!("authenticated content fork must be rejected"),
            Err(error) => error,
        };
        assert!(failure.conflict);
        assert_eq!(follower.store.get_block(1).unwrap(), Some(original));
        server.abort();
        drop((authority, follower));
        fs::remove_dir_all(authority_path).unwrap();
        fs::remove_dir_all(follower_path).unwrap();
    }

    #[test]
    fn database_identity_rejects_switching_pinned_authorities() {
        let (authority, authority_path) = test_chain();
        let (other, other_path) = test_chain();
        let follower_path = temp_dir("pinned-follower");
        let mut config = follower_config(&follower_path, &authority.network);
        let (follower, _) = Chain::bootstrap(&config).unwrap();
        drop(follower);
        config.network.authority_public_key = Some(other.network.authority_public_key.clone());
        config.network.expected_chain_id = None;
        assert!(Chain::bootstrap(&config).is_err());
        drop((authority, other));
        for path in [authority_path, other_path, follower_path] {
            fs::remove_dir_all(path).unwrap();
        }
    }

    #[tokio::test]
    async fn follower_rejects_unsigned_existing_history() {
        let (authority, authority_path) = test_chain();
        let block = seal(&authority, "unsigned").await;
        let follower_path = temp_dir("unsigned-follower");
        let config = follower_config(&follower_path, &authority.network);
        let store = Store::open(&follower_path).unwrap();
        store.init_or_resume().unwrap();
        store
            .commit_block(
                &block,
                &BlockchainState {
                    height: 1,
                    tip_hash: block.hash.clone(),
                    total_txs: 1,
                },
            )
            .unwrap();
        assert!(Network::open(&store, &config.network).is_err());
        drop((authority, store));
        fs::remove_dir_all(authority_path).unwrap();
        fs::remove_dir_all(follower_path).unwrap();
    }

    #[test]
    fn peer_configuration_rejects_unsafe_protocols_and_hidden_destinations() {
        for peer in [
            "file:///etc/passwd",
            "ftp://example.com",
            "https://user:pass@example.com",
            "https://example.com?a=1",
            "https://example.com#fragment",
        ] {
            assert!(parse_peers(peer).is_err(), "accepted {peer}");
        }
        assert_eq!(
            parse_peers("http://127.0.0.1:8642/, https://example.com/,http://127.0.0.1:8642")
                .unwrap(),
            vec!["http://127.0.0.1:8642", "https://example.com"]
        );
    }

    #[tokio::test]
    async fn peer_http_body_is_bounded_before_json_decoding() {
        let fixtures = HashMap::from([("/large".into(), " ".repeat(4096))]);
        let (url, server) = fixture_server(fixtures).await;
        let result =
            fetch_json::<serde_json::Value>(&reqwest::Client::new(), &format!("{url}/large"), 32)
                .await;
        assert!(result.unwrap_err().to_string().contains("size limit"));
        server.abort();
    }
}
