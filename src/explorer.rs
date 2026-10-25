//! Read-only explorer UI and bounded, rebuildable indexes over committed blocks.
//!
//! `explorer_v1` contains only derived summaries; the core chain, signatures and
//! application state are never changed by explorer requests. Each block and its
//! cursor are indexed atomically, so an interrupted first scan resumes safely.

use crate::{Block, Chain, ChainError, Result, Store};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeSet, HashMap};
use std::ops::Bound::{Excluded, Included};
use std::sync::{Arc, Mutex};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

const INDEX_BATCH_BLOCKS: usize = 128;
const INDEX_BATCH_BYTES: usize = 32 * 1024 * 1024;
const LAST_HEIGHT: &[u8] = b"z:height";
const TX_COUNT: &[u8] = b"z:transactions";
const NS_COUNT: &[u8] = b"z:namespaces";

#[derive(Clone)]
pub(crate) struct Index {
    tree: sled::Tree,
    gate: Arc<Mutex<()>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct BlockSummary {
    height: u64,
    hash: String,
    prev_block_hash: String,
    merkle_root: String,
    timestamp: u64,
    tx_count: usize,
    payload_bytes: usize,
    namespaces: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct TransactionSummary {
    id: String,
    namespace: String,
    timestamp: u64,
    height: u64,
    block_hash: String,
    payload_bytes: usize,
    tags_count: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct NamespaceSummary {
    namespace: String,
    count: u64,
    last_height: u64,
    last_tx_id: String,
}

fn error(status: u16, message: impl ToString) -> (u16, String) {
    (status, json!({"error": message.to_string()}).to_string())
}

fn invalid(message: &str) -> ChainError {
    ChainError::InvalidTx(message.to_owned())
}

fn key(prefix: u8, suffix: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(1 + suffix.len());
    key.push(prefix);
    key.extend_from_slice(suffix);
    key
}

fn tx_position(height: u64, position: u32) -> Vec<u8> {
    [
        height.to_be_bytes().as_slice(),
        position.to_be_bytes().as_slice(),
    ]
    .concat()
}

fn ns_tx_prefix(namespace: &str) -> Vec<u8> {
    let mut prefix = vec![b'n'];
    prefix.extend_from_slice(&(namespace.len() as u16).to_be_bytes());
    prefix.extend_from_slice(namespace.as_bytes());
    prefix
}

fn append(mut prefix: Vec<u8>, suffix: &[u8]) -> Vec<u8> {
    prefix.extend_from_slice(suffix);
    prefix
}

fn read_u64(bytes: &[u8]) -> Result<u64> {
    bytes
        .try_into()
        .map(u64::from_be_bytes)
        .map_err(|_| ChainError::Integrity("invalid explorer index cursor".into()))
}

impl Index {
    pub(crate) fn open(db: &sled::Db) -> Result<Self> {
        Ok(Self {
            tree: db.open_tree("explorer_v1")?,
            gate: Arc::new(Mutex::new(())),
        })
    }

    fn number(&self, name: &[u8]) -> Result<u64> {
        self.tree
            .get(name)?
            .map(|b| read_u64(&b))
            .transpose()
            .map(|n| n.unwrap_or(0))
    }

    fn height(&self) -> Result<Option<u64>> {
        self.tree
            .get(LAST_HEIGHT)?
            .map(|b| read_u64(&b))
            .transpose()
    }

    fn index_block(&self, block: &Block) -> Result<()> {
        let mut batch = sled::Batch::default();
        let mut namespaces: HashMap<String, NamespaceSummary> = HashMap::new();
        let mut new_namespaces = 0u64;
        let mut payload_bytes = 0;
        for (position, tx) in block.transactions.iter().enumerate() {
            let summary = TransactionSummary {
                id: tx.id.clone(),
                namespace: tx.namespace.clone(),
                timestamp: tx.timestamp,
                height: block.header.index,
                block_hash: block.hash.clone(),
                payload_bytes: tx.payload.len(),
                tags_count: tx.tags.len(),
            };
            payload_bytes += tx.payload.len();
            let position = tx_position(block.header.index, position as u32);
            let bytes = bincode::serialize(&summary)?;
            batch.insert(key(b't', &position), bytes.clone());
            batch.insert(append(ns_tx_prefix(&tx.namespace), &position), bytes);
            if !namespaces.contains_key(&tx.namespace) {
                let prior = self
                    .tree
                    .get(key(b's', tx.namespace.as_bytes()))?
                    .map(|bytes| bincode::deserialize::<NamespaceSummary>(&bytes))
                    .transpose()?;
                if prior.is_none() {
                    new_namespaces += 1;
                }
                namespaces.insert(
                    tx.namespace.clone(),
                    prior.unwrap_or(NamespaceSummary {
                        namespace: tx.namespace.clone(),
                        count: 0,
                        last_height: 0,
                        last_tx_id: String::new(),
                    }),
                );
            }
            let namespace = namespaces
                .get_mut(&tx.namespace)
                .expect("namespace inserted");
            namespace.count += 1;
            namespace.last_height = block.header.index;
            namespace.last_tx_id = tx.id.clone();
        }
        let summary = BlockSummary {
            height: block.header.index,
            hash: block.hash.clone(),
            prev_block_hash: block.header.prev_block_hash.clone(),
            merkle_root: block.header.merkle_root.clone(),
            timestamp: block.header.timestamp,
            tx_count: block.transactions.len(),
            payload_bytes,
            namespaces: namespaces
                .keys()
                .cloned()
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect(),
        };
        batch.insert(
            key(b'b', &block.header.index.to_be_bytes()),
            bincode::serialize(&summary)?,
        );
        batch.insert(
            key(b'h', block.hash.as_bytes()),
            block.header.index.to_be_bytes().to_vec(),
        );
        for (name, summary) in namespaces {
            batch.insert(key(b's', name.as_bytes()), bincode::serialize(&summary)?);
        }
        batch.insert(LAST_HEIGHT, block.header.index.to_be_bytes().to_vec());
        batch.insert(
            TX_COUNT,
            (self.number(TX_COUNT)? + block.transactions.len() as u64)
                .to_be_bytes()
                .to_vec(),
        );
        batch.insert(
            NS_COUNT,
            (self.number(NS_COUNT)? + new_namespaces)
                .to_be_bytes()
                .to_vec(),
        );
        self.tree.apply_batch(batch)?;
        Ok(())
    }

    /// At most 128 blocks / 32 MiB per request. Old blocks are never rescanned.
    fn catch_up(&self, store: &Store, tip: u64) -> Result<bool> {
        let last = self.height()?;
        if last.is_some_and(|height| height >= tip) {
            return Ok(true);
        }
        let start = last.map_or(0, |height| height + 1);
        let mut bytes = 0;
        for (count, height) in (start..=tip).enumerate() {
            if count >= INDEX_BATCH_BLOCKS || bytes >= INDEX_BATCH_BYTES {
                return Ok(false);
            }
            let raw = store
                .blocks
                .get(height.to_be_bytes())?
                .ok_or_else(|| ChainError::Integrity(format!("explorer block {height} missing")))?;
            bytes += raw.len();
            let block: Block = bincode::deserialize(&raw)?;
            self.index_block(&block)?;
        }
        Ok(true)
    }

    fn blocks(&self, params: &HashMap<String, String>, limit: usize) -> Result<Value> {
        let height = self.height()?.unwrap_or(0);
        let before = params
            .get("before")
            .map(|s| parse_u64(s, "invalid before cursor"))
            .transpose()?;
        let last = match before {
            Some(0) => None,
            Some(n) => Some((n - 1).min(height)),
            None => Some(height),
        };
        let mut items = Vec::new();
        if let Some(last) = last {
            for item in self
                .tree
                .range(key(b'b', &0u64.to_be_bytes())..=key(b'b', &last.to_be_bytes()))
                .rev()
                .take(limit + 1)
            {
                let (_, bytes) = item?;
                items.push(bincode::deserialize::<BlockSummary>(&bytes)?);
            }
        }
        let next_cursor = if items.len() > limit {
            items.truncate(limit);
            items.last().map(|b| b.height.to_string())
        } else {
            None
        };
        Ok(json!({"items": items, "total": height.saturating_add(1), "next_cursor": next_cursor}))
    }

    fn transactions(&self, params: &HashMap<String, String>, limit: usize) -> Result<Value> {
        let namespace = params.get("namespace").filter(|n| !n.is_empty());
        if namespace.is_some_and(|n| n.len() > crate::MAX_NAMESPACE_BYTES) {
            return Err(invalid("namespace too long"));
        }
        let prefix = namespace.map_or_else(|| vec![b't'], |n| ns_tx_prefix(n));
        let (height, position) = match params.get("cursor") {
            Some(cursor) => parse_cursor(cursor)?,
            None => (u64::MAX, u32::MAX),
        };
        let upper = append(prefix.clone(), &tx_position(height, position));
        let mut items = Vec::new();
        let mut positions = Vec::new();
        for item in self
            .tree
            .range((Included(prefix.clone()), Excluded(upper)))
            .rev()
            .take(limit + 1)
        {
            let (key, bytes) = item?;
            if !key.starts_with(&prefix) {
                break;
            }
            let suffix = &key[prefix.len()..];
            let height = read_u64(&suffix[..8])?;
            let position = u32::from_be_bytes(
                suffix[8..]
                    .try_into()
                    .map_err(|_| invalid("invalid indexed transaction position"))?,
            );
            positions.push(format!("{height}:{position}"));
            items.push(bincode::deserialize::<TransactionSummary>(&bytes)?);
        }
        let next_cursor = if items.len() > limit {
            items.truncate(limit);
            positions.get(limit - 1).cloned()
        } else {
            None
        };
        let total = match namespace {
            Some(namespace) => self
                .tree
                .get(key(b's', namespace.as_bytes()))?
                .map(|b| bincode::deserialize::<NamespaceSummary>(&b))
                .transpose()?
                .map_or(0, |s| s.count),
            None => self.number(TX_COUNT)?,
        };
        Ok(json!({"items":items, "total": total, "next_cursor":next_cursor}))
    }

    fn namespaces(&self, params: &HashMap<String, String>, limit: usize) -> Result<Value> {
        let lower = match params.get("cursor") {
            Some(name) if name.len() <= crate::MAX_NAMESPACE_BYTES => {
                Excluded(key(b's', name.as_bytes()))
            }
            Some(_) => return Err(invalid("namespace cursor too long")),
            None => Included(vec![b's']),
        };
        let mut items = Vec::new();
        for item in self
            .tree
            .range((lower, Excluded(vec![b't'])))
            .take(limit + 1)
        {
            let (_, bytes) = item?;
            items.push(bincode::deserialize::<NamespaceSummary>(&bytes)?);
        }
        let next_cursor = if items.len() > limit {
            items.truncate(limit);
            items.last().map(|n| n.namespace.clone())
        } else {
            None
        };
        Ok(json!({"items":items,"total":self.number(NS_COUNT)?,"next_cursor":next_cursor}))
    }

    fn search(&self, store: &Store, params: &HashMap<String, String>) -> Result<Option<Value>> {
        let query = params.get("q").map(String::as_str).unwrap_or("").trim();
        if query.is_empty() || query.len() > crate::MAX_NAMESPACE_BYTES {
            return Err(invalid("search requires 1..128 bytes"));
        }
        let numeric = query.strip_prefix('#').unwrap_or(query);
        if numeric.bytes().all(|b| b.is_ascii_digit()) && !numeric.is_empty() {
            if let Ok(height) = numeric.parse::<u64>() {
                if let Some(bytes) = self.tree.get(key(b'b', &height.to_be_bytes()))? {
                    let block: BlockSummary = bincode::deserialize(&bytes)?;
                    return Ok(Some(
                        json!({"type":"block","height":height,"hash":block.hash}),
                    ));
                }
            }
        }
        if query.len() == 64 && query.bytes().all(|b| b.is_ascii_hexdigit()) {
            let id = query.to_ascii_lowercase();
            if let Some(height) = store.tx_height(&id)? {
                return Ok(Some(
                    json!({"type":"transaction","id":id,"height":height,"status":"confirmed"}),
                ));
            }
            if let Some(bytes) = self.tree.get(key(b'h', id.as_bytes()))? {
                return Ok(Some(
                    json!({"type":"block","height":read_u64(&bytes)?,"hash":id}),
                ));
            }
        }
        if let Some(bytes) = self.tree.get(key(b's', query.as_bytes()))? {
            let ns: NamespaceSummary = bincode::deserialize(&bytes)?;
            return Ok(Some(
                json!({"type":"namespace","namespace":ns.namespace,"count":ns.count,"last_height":ns.last_height,"last_tx_id":ns.last_tx_id}),
            ));
        }
        Ok(None)
    }
}

fn parse_u64(value: &str, message: &str) -> Result<u64> {
    if value.is_empty() || !value.bytes().all(|b| b.is_ascii_digit()) {
        return Err(invalid(message));
    }
    value.parse().map_err(|_| invalid(message))
}

fn parse_cursor(value: &str) -> Result<(u64, u32)> {
    let (height, position) = value
        .split_once(':')
        .ok_or_else(|| invalid("invalid transaction cursor"))?;
    Ok((
        parse_u64(height, "invalid transaction cursor")?,
        u32::try_from(parse_u64(position, "invalid transaction cursor")?)
            .map_err(|_| invalid("invalid transaction cursor"))?,
    ))
}

fn parse_query(query: Option<&str>) -> Result<HashMap<String, String>> {
    let mut params = HashMap::new();
    for pair in query
        .unwrap_or("")
        .split('&')
        .filter(|part| !part.is_empty())
    {
        let (name, value) = pair.split_once('=').unwrap_or((pair, ""));
        let name = crate::percent_decode(&name.replace('+', " "));
        let value = crate::percent_decode(&value.replace('+', " "));
        if params.insert(name, value).is_some() {
            return Err(invalid("duplicate query parameter"));
        }
    }
    Ok(params)
}

pub(crate) async fn route(
    chain: &Chain,
    method: &str,
    path: &str,
    query: Option<&str>,
) -> (u16, String) {
    if method == "OPTIONS" {
        return (204, String::new());
    }
    if method != "GET" {
        return error(405, "explorer is read-only");
    }
    let operation = match path.trim_end_matches('/').rsplit('/').next().unwrap_or("") {
        name @ ("blocks" | "transactions" | "namespaces" | "search")
            if path.trim_end_matches('/') == format!("/explorer/api/{name}") =>
        {
            name.to_owned()
        }
        _ => return error(404, "explorer endpoint not found"),
    };
    let params = match parse_query(query) {
        Ok(params) => params,
        Err(e) => return error(400, e),
    };
    let limit = match params.get("limit") {
        Some(value) => match parse_u64(value, "limit must be 1..100") {
            Ok(n @ 1..=100) => n as usize,
            _ => return error(400, "limit must be 1..100"),
        },
        None => 20,
    };
    if operation == "search" {
        if let Some(q) = params.get("q") {
            let q = q.trim().to_ascii_lowercase();
            if q.len() == 64 && chain.mempool.lock().await.ids.contains(&q) {
                return (
                    200,
                    json!({"type":"transaction","id":q,"height":null,"status":"pending"})
                        .to_string(),
                );
            }
        }
    }
    let (state, _) = chain.snapshot().await;
    let store = chain.store.clone();
    match tokio::task::spawn_blocking(move || -> Result<(u16, String)> {
        let index = &store.explorer;
        let _gate = index.gate.lock().map_err(|_| ChainError::Integrity("explorer index lock poisoned".into()))?;
        if !index.catch_up(&store, state.height)? {
            return Ok((503, json!({"error":"explorer index is catching up; retry shortly","indexing":true,"indexed_height":index.height()?,"chain_height":state.height}).to_string()));
        }
        let value = match operation.as_str() {
            "blocks" => index.blocks(&params, limit)?,
            "transactions" => index.transactions(&params, limit)?,
            "namespaces" => index.namespaces(&params, limit)?,
            "search" => match index.search(&store, &params)? { Some(value) => value, None => return Ok(error(404, "검색 결과가 없습니다")) },
            _ => unreachable!(),
        };
        Ok((200, value.to_string()))
    }).await {
        Ok(Ok(response)) => response,
        Ok(Err(ChainError::InvalidTx(message))) => error(400, message),
        Ok(Err(error_value)) => error(500, error_value),
        Err(error_value) => error(500, error_value),
    }
}

/// Assets are exact allowlisted compile-time files, never filesystem paths.
pub(crate) async fn serve_asset(stream: &mut TcpStream, method: &str, path: &str) -> Result<bool> {
    if !matches!(method, "GET" | "HEAD") {
        return Ok(false);
    }
    if path == "/explorer" {
        stream.write_all(b"HTTP/1.1 308 Permanent Redirect\r\nLocation: explorer/\r\nContent-Length: 0\r\nCache-Control: no-cache\r\nConnection: close\r\n\r\n").await?;
        stream.shutdown().await?;
        return Ok(true);
    }
    let (content_type, body): (&str, &[u8]) = match path {
        "/explorer/" | "/explorer/index.html" => (
            "text/html; charset=utf-8",
            include_bytes!("../explorer/index.html"),
        ),
        "/explorer/app.js" => (
            "text/javascript; charset=utf-8",
            include_bytes!("../explorer/app.js"),
        ),
        "/explorer/styles.css" => (
            "text/css; charset=utf-8",
            include_bytes!("../explorer/styles.css"),
        ),
        _ => return Ok(false),
    };
    let header = format!("HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nCache-Control: no-cache\r\nX-Content-Type-Options: nosniff\r\nReferrer-Policy: same-origin\r\nContent-Security-Policy: default-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self' data:; base-uri 'none'; frame-ancestors 'self'\r\nConnection: close\r\n\r\n",body.len());
    stream.write_all(header.as_bytes()).await?;
    if method != "HEAD" {
        stream.write_all(body).await?;
    }
    stream.shutdown().await?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn get(chain: &Chain, path: &str, query: &str) -> (u16, Value) {
        let (status, body) = route(chain, "GET", path, Some(query)).await;
        (status, serde_json::from_str(&body).unwrap())
    }

    #[test]
    fn catch_up_is_bounded_and_resumes_without_duplicate_counts() {
        let dir = crate::tests::temp_dir("explorer-index-batch");
        let store = Store::open(&dir).unwrap();
        let (mut state, _) = store.init_or_resume().unwrap();
        for height in 1..=INDEX_BATCH_BLOCKS as u64 {
            let tx =
                crate::Transaction::new("sample".into(), height.to_string(), None, vec![]).unwrap();
            let block =
                Block::with_timestamp(height, state.tip_hash.clone(), vec![tx], height).unwrap();
            state = crate::BlockchainState {
                height,
                tip_hash: block.hash.clone(),
                total_txs: height,
            };
            store.commit_block(&block, &state).unwrap();
        }
        assert!(!store.explorer.catch_up(&store, state.height).unwrap());
        assert_eq!(
            store.explorer.height().unwrap(),
            Some(INDEX_BATCH_BLOCKS as u64 - 1)
        );
        assert_eq!(
            store.explorer.number(TX_COUNT).unwrap(),
            INDEX_BATCH_BLOCKS as u64 - 1
        );
        assert!(store.explorer.catch_up(&store, state.height).unwrap());
        assert_eq!(
            store.explorer.number(TX_COUNT).unwrap(),
            INDEX_BATCH_BLOCKS as u64
        );
        assert!(store.explorer.catch_up(&store, state.height).unwrap());
        assert_eq!(store.explorer.number(NS_COUNT).unwrap(), 1);
        assert_eq!(
            store.explorer.number(TX_COUNT).unwrap(),
            INDEX_BATCH_BLOCKS as u64
        );
        drop(store);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn pages_search_namespace_and_restart_use_complete_stable_indexes() {
        let (chain, dir) = crate::tests::test_chain();
        let mut ids = Vec::new();
        for (position, namespace) in ["first", "a/b 한글", "first"].into_iter().enumerate() {
            ids.push(
                chain
                    .submit_tx(
                        namespace.into(),
                        format!("<script>payload {position}</script>"),
                        None,
                        vec![],
                    )
                    .await
                    .unwrap()
                    .0,
            );
        }
        chain.seal_pending_block().await.unwrap().unwrap();
        let (status, blocks) = get(&chain, "/explorer/api/blocks", "limit=1").await;
        assert_eq!(status, 200);
        assert_eq!(blocks["total"], 2);
        assert_eq!(blocks["items"][0]["height"], 1);
        assert_eq!(blocks["items"][0]["tx_count"], 3);
        assert_eq!(blocks["next_cursor"], "1");
        assert_eq!(
            get(&chain, "/explorer/api/blocks", "before=1").await.1["items"][0]["height"],
            0
        );
        let first = get(&chain, "/explorer/api/transactions", "limit=2").await.1;
        assert_eq!(first["total"], 3);
        assert_eq!(first["items"][0]["id"], ids[2]);
        assert!(first["items"][0].get("payload").is_none());
        let cursor = first["next_cursor"].as_str().unwrap();
        let second = get(
            &chain,
            "/explorer/api/transactions",
            &format!("cursor={cursor}&limit=2"),
        )
        .await
        .1;
        assert_eq!(second["items"].as_array().unwrap().len(), 1);
        assert_eq!(second["items"][0]["id"], ids[0]);
        assert!(second["next_cursor"].is_null());
        let namespace = get(
            &chain,
            "/explorer/api/transactions",
            "namespace=a%2Fb%20%ED%95%9C%EA%B8%80",
        )
        .await
        .1;
        assert_eq!(namespace["total"], 1);
        assert_eq!(namespace["items"][0]["namespace"], "a/b 한글");
        let names = get(&chain, "/explorer/api/namespaces", "limit=1").await.1;
        assert_eq!(names["total"], 2);
        assert_eq!(names["items"][0]["namespace"], "a/b 한글");
        let next = get(
            &chain,
            "/explorer/api/namespaces",
            "cursor=a%2Fb%20%ED%95%9C%EA%B8%80",
        )
        .await
        .1;
        assert_eq!(next["items"][0]["count"], 2);
        for query in [
            "q=1".to_string(),
            format!("q={}", blocks["items"][0]["hash"].as_str().unwrap()),
        ] {
            assert_eq!(
                get(&chain, "/explorer/api/search", &query).await.1["type"],
                "block"
            );
        }
        assert_eq!(
            get(&chain, "/explorer/api/search", &format!("q={}", ids[0]))
                .await
                .1["type"],
            "transaction"
        );
        assert_eq!(
            get(&chain, "/explorer/api/search", "q=first").await.1["type"],
            "namespace"
        );
        for query in ["limit=0", "limit=101", "limit=1&limit=2", "before=-1"] {
            assert_eq!(get(&chain, "/explorer/api/blocks", query).await.0, 400);
        }
        assert_eq!(
            get(&chain, "/explorer/api/transactions", "cursor=oops")
                .await
                .0,
            400
        );
        assert_eq!(get(&chain, "/explorer/api/search", "q=absent").await.0, 404);
        for path in ["blocks", "transactions", "namespaces", "search"] {
            assert_eq!(
                route(
                    &chain,
                    "POST",
                    &format!("/explorer/api/{path}"),
                    Some("q=1")
                )
                .await
                .0,
                405
            );
        }
        let before = chain.snapshot().await.0;
        drop(chain);
        let (chain, _) = Chain::bootstrap(&crate::tests::test_config(dir.clone())).unwrap();
        assert_eq!(
            get(&chain, "/explorer/api/transactions", "").await.1["total"],
            3
        );
        assert_eq!(chain.snapshot().await.0, before);
        drop(chain);
        std::fs::remove_dir_all(dir).unwrap();
    }
}
