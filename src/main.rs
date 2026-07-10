//! ============================================================================
//! tomei-chain — 저사양 홈서버용 초경량 Pure Rust 블록체인 데몬
//! ============================================================================
//!
//! 설계 목표 (claude.md 사양):
//!   - 외부 DB 없이 임베디드 `sled` KV 스토리지로 블록/상태 영속화
//!   - PoW/PoS/P2P 없이 '단일 시퀀서(PoA)' 구조로 리소스 최소화
//!   - 임의 데이터(파일 해시, 메타데이터 등)를 담는 범용 트랜잭션 페이로드
//!   - 재시작 시 sled 디렉토리에서 마지막 블록을 읽어 체인을 이어감
//!
//! 클라이언트 연동 (어댑터 모듈):
//!   - `heartcritique.rs` : HeartCritiqueEAS 의 uploader 사이드카 계약
//!     (`POST /upload`, `GET /wallet`, `GET /bundle/{txId}`) 드롭인 구현
//!   - `mantis.rs`        : mantis-server 의 동기화 계약
//!     (`GET /api/info`, `GET|POST /api/blocks`) 드롭인 구현 — 포트 7878 겸용
//!
//! 실행 방법:
//!   cargo run --release            # 데몬 모드 (Ctrl-C 로 종료)
//!   cargo run --release -- --demo  # 가상 트랜잭션 삽입 → 블록 생성 → 종료 데모
//!
//! 파일 구성 (코어는 이 단일 파일, 섹션별 구분):
//!   [1] 설정        [2] 에러 타입     [3] 데이터 모델   [4] 해싱/머클
//!   [5] 스토리지    [6] 멤풀          [7] 체인 코어     [8] 블록 생산 루프
//!   [9] HTTP API    [10] 데모         [11] main         [12] 테스트

mod heartcritique;
mod mantis;

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{watch, Mutex, Notify, Semaphore};

// ============================================================================
// [1] 설정 — 환경변수로 재정의 가능
// ============================================================================

/// 런타임 설정. 모든 값은 환경변수로 덮어쓸 수 있다.
#[derive(Clone, Debug)]
struct Config {
    /// sled 데이터 디렉토리 (TOMEI_DATA_DIR, 기본 ./tomei-data)
    data_dir: String,
    /// 메인 HTTP API 바인드 주소 (TOMEI_ADDR, 기본 127.0.0.1:8642)
    api_addr: SocketAddr,
    /// mantis-cad 앱의 기본 서버 포트용 보조 리스너 (TOMEI_MANTIS_ADDR,
    /// 기본 127.0.0.1:7878, "off" 로 비활성화). 같은 라우터를 그대로 서빙한다.
    mantis_addr: Option<SocketAddr>,
    /// 외부에서 접근 가능한 베이스 URL (TOMEI_PUBLIC_URL) — /upload 응답의
    /// 번들 조회 URL(arweaveUrl 자리) 생성에 사용
    public_url: String,
    /// 블록 생산 주기 밀리초 (TOMEI_BLOCK_INTERVAL_MS, 기본 2000)
    block_interval: Duration,
    /// 멤풀에 이 개수 이상 쌓이면 주기를 기다리지 않고 즉시 블록 생산 (TOMEI_BATCH)
    batch_threshold: usize,
}

/// 블록 하나에 담는 최대 트랜잭션 수 — 블록 크기(메모리) 상한
const MAX_BLOCK_TXS: usize = 500;
/// 블록 하나의 페이로드 총량 상한 (8 MiB) — 단, 최소 1개는 항상 담는다
const MAX_BLOCK_BYTES: usize = 8 * 1024 * 1024;
/// 멤풀 최대 크기 — 초과 제출은 거부하여 메모리 사용량을 유계로 유지
const MAX_MEMPOOL: usize = 10_000;
/// 트랜잭션 페이로드 최대 크기 (8 MiB — HeartCritique 박제 번들 수용)
const MAX_PAYLOAD_BYTES: usize = 8 * 1024 * 1024;
/// 네임스페이스 최대 길이
const MAX_NAMESPACE_BYTES: usize = 128;
/// HTTP 요청 전체(헤더+바디) 최대 크기 — mantis-server 의 32 MiB 상한과 동일
const MAX_HTTP_REQUEST_BYTES: usize = 32 * 1024 * 1024;
/// 동시 처리 커넥션 상한 — 멈춘 커넥션이 태스크/FD 를 무한 점유하지 못하게 유계화
const MAX_CONNECTIONS: usize = 256;
/// 커넥션당 처리(요청 읽기+응답 쓰기) 제한 시간 — 슬로로리스형 점유 방지
const CONNECTION_TIMEOUT: Duration = Duration::from_secs(60);
/// sled 페이지 캐시 상한 (32 MiB) — 저사양 서버에서 기본값(높음)을 줄인다
const SLED_CACHE_BYTES: u64 = 32 * 1024 * 1024;

impl Config {
    fn from_env() -> Self {
        let data_dir =
            std::env::var("TOMEI_DATA_DIR").unwrap_or_else(|_| "./tomei-data".to_string());
        let api_addr: SocketAddr = std::env::var("TOMEI_ADDR")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or_else(|| "127.0.0.1:8642".parse().expect("기본 주소는 항상 유효"));
        let mantis_addr = match std::env::var("TOMEI_MANTIS_ADDR") {
            Ok(s) if s.eq_ignore_ascii_case("off") => None,
            Ok(s) => s.parse().ok(),
            Err(_) => Some("127.0.0.1:7878".parse().expect("기본 주소는 항상 유효")),
        };
        let public_url = std::env::var("TOMEI_PUBLIC_URL")
            .unwrap_or_else(|_| format!("http://{api_addr}"));
        let block_interval = Duration::from_millis(
            std::env::var("TOMEI_BLOCK_INTERVAL_MS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(2_000),
        );
        let batch_threshold = std::env::var("TOMEI_BATCH")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(100);
        Config { data_dir, api_addr, mantis_addr, public_url, block_interval, batch_threshold }
    }
}

// ============================================================================
// [2] 에러 타입 — thiserror 기반의 단일 에러 열거형
// ============================================================================

#[derive(Error, Debug)]
pub(crate) enum ChainError {
    #[error("스토리지 오류: {0}")]
    Storage(#[from] sled::Error),
    #[error("직렬화 오류: {0}")]
    Codec(#[from] bincode::Error),
    #[error("무결성 오류: {0}")]
    Integrity(String),
    #[error("잘못된 트랜잭션: {0}")]
    InvalidTx(String),
    #[error("I/O 오류: {0}")]
    Io(#[from] std::io::Error),
    /// HTTP 413 로 매핑 (mantis-server 의 "body too large" 응답과 동일)
    #[error("요청이 너무 큽니다: {0}")]
    TooLarge(String),
    /// HTTP 411 로 매핑 — Content-Length 없는 전송 방식(chunked 등) 거부
    #[error("지원하지 않는 전송 방식: {0}")]
    Unsupported(String),
}

pub(crate) type Result<T> = std::result::Result<T, ChainError>;

// ============================================================================
// [3] 데이터 모델 — Transaction / Block / BlockchainState
// ============================================================================

/// 범용 트랜잭션. 토큰 송금이 아니라 "임의 데이터의 공증(anchoring)"이 목적이다.
///
/// - `namespace`: 제출한 애플리케이션 식별자 (예: "heartcritique-eas", "mantis-cad")
/// - `payload`  : 임의 데이터 — 파일 해시, JSON 메타데이터 문자열 등
/// - `tags`     : 월드 스테이트에 인덱싱되는 (키,값) 쌍. 같은 태그의 최초
///                트랜잭션이 승자(first-wins)가 되어 멱등 제출을 지원한다.
/// - `signature`: PoA 확장을 위한 자리. 현재는 구조만 두고 검증하지 않는다.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub(crate) struct Transaction {
    /// 트랜잭션 해시 ID = blake3(namespace ‖ payload ‖ timestamp ‖ tags)
    pub id: String,
    pub namespace: String,
    pub payload: String,
    /// 제출 시각 (unix 밀리초)
    pub timestamp: u64,
    #[serde(default)]
    pub tags: Vec<(String, String)>,
    pub signature: Option<String>,
}

impl Transaction {
    /// 새 트랜잭션을 만들고 내용 기반 해시 ID를 계산한다.
    /// 같은 내용을 같은 밀리초에 두 번 제출하면 같은 ID가 되어
    /// 자연스럽게 멱등(idempotent) 처리된다.
    fn new(
        namespace: String,
        payload: String,
        signature: Option<String>,
        tags: Vec<(String, String)>,
    ) -> Result<Self> {
        if namespace.is_empty() || namespace.len() > MAX_NAMESPACE_BYTES {
            return Err(ChainError::InvalidTx(format!(
                "namespace 는 1~{MAX_NAMESPACE_BYTES} 바이트여야 합니다"
            )));
        }
        if payload.len() > MAX_PAYLOAD_BYTES {
            return Err(ChainError::InvalidTx(format!(
                "payload 가 최대 크기({MAX_PAYLOAD_BYTES} 바이트)를 초과했습니다"
            )));
        }
        let timestamp = now_millis();
        let id = compute_tx_id(&namespace, &payload, timestamp, &tags);
        Ok(Transaction { id, namespace, payload, timestamp, tags, signature })
    }

    /// 저장된 트랜잭션의 ID가 실제 내용과 일치하는지 재계산으로 검증한다.
    fn verify_id(&self) -> bool {
        self.id == compute_tx_id(&self.namespace, &self.payload, self.timestamp, &self.tags)
    }
}

/// 블록 헤더 — 블록 해시는 이 헤더의 bincode 직렬화 바이트를 해싱해 얻는다.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub(crate) struct BlockHeader {
    /// 블록 높이 (제네시스 = 0)
    pub index: u64,
    /// 직전 블록의 해시 — 체인 연결 고리
    pub prev_block_hash: String,
    /// 블록 내 트랜잭션 ID들의 머클 루트
    pub merkle_root: String,
    /// 블록 생성 시각 (unix 밀리초)
    pub timestamp: u64,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub(crate) struct Block {
    pub header: BlockHeader,
    pub transactions: Vec<Transaction>,
    /// blake3(bincode(header)) — 머클 루트를 통해 트랜잭션 내용까지 커버한다
    pub hash: String,
}

impl Block {
    /// 새 블록을 조립한다. (해시/머클루트 자동 계산)
    fn new(index: u64, prev_block_hash: String, transactions: Vec<Transaction>) -> Result<Self> {
        Self::with_timestamp(index, prev_block_hash, transactions, now_millis())
    }

    /// 타임스탬프를 지정해 블록을 만든다. (제네시스 재현성/테스트용)
    fn with_timestamp(
        index: u64,
        prev_block_hash: String,
        transactions: Vec<Transaction>,
        timestamp: u64,
    ) -> Result<Self> {
        let tx_ids: Vec<&str> = transactions.iter().map(|t| t.id.as_str()).collect();
        let header = BlockHeader {
            index,
            prev_block_hash,
            merkle_root: merkle_root(&tx_ids),
            timestamp,
        };
        let hash = compute_block_hash(&header)?;
        Ok(Block { header, transactions, hash })
    }

    /// 모든 노드가 동일하게 재현할 수 있는 결정적(deterministic) 제네시스 블록.
    fn genesis() -> Result<Self> {
        Self::with_timestamp(0, "0".repeat(64), Vec::new(), 0)
    }

    /// 블록 자체 무결성 검증:
    ///   1) 각 트랜잭션 ID가 내용과 일치하는가
    ///   2) 머클 루트가 트랜잭션 ID들과 일치하는가
    ///   3) 블록 해시가 헤더와 일치하는가
    fn verify_integrity(&self) -> Result<()> {
        for tx in &self.transactions {
            if !tx.verify_id() {
                return Err(ChainError::Integrity(format!(
                    "블록 #{}: 트랜잭션 {} 의 ID가 내용과 불일치",
                    self.header.index, tx.id
                )));
            }
        }
        let tx_ids: Vec<&str> = self.transactions.iter().map(|t| t.id.as_str()).collect();
        if self.header.merkle_root != merkle_root(&tx_ids) {
            return Err(ChainError::Integrity(format!(
                "블록 #{}: 머클 루트 불일치",
                self.header.index
            )));
        }
        if self.hash != compute_block_hash(&self.header)? {
            return Err(ChainError::Integrity(format!(
                "블록 #{}: 블록 해시 불일치",
                self.header.index
            )));
        }
        Ok(())
    }
}

/// 체인 전역 상태 — sled `state` 트리에 영속화되어 재시작 시 복원된다.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub(crate) struct BlockchainState {
    /// 최신 블록 높이
    pub height: u64,
    /// 최신 블록 해시
    pub tip_hash: String,
    /// 지금까지 커밋된 총 트랜잭션 수
    pub total_txs: u64,
}

/// 네임스페이스(앱)별 월드 스테이트 — "이 앱이 마지막으로 공증한 것"을 즉시 조회.
#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub(crate) struct NamespaceInfo {
    /// 이 네임스페이스로 커밋된 트랜잭션 수
    pub count: u64,
    /// 가장 최근 트랜잭션 ID
    pub last_tx_id: String,
    /// 가장 최근 트랜잭션이 포함된 블록 높이
    pub last_height: u64,
}

// ============================================================================
// [4] 해싱 / 머클 트리 — blake3 (저전력 CPU에서도 매우 빠름)
// ============================================================================

pub(crate) fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// 트랜잭션 ID. 0x00 구분자는 필드 경계 조작(예: namespace 끝을 payload
/// 앞에 붙이기)을 방지한다.
fn compute_tx_id(
    namespace: &str,
    payload: &str,
    timestamp: u64,
    tags: &[(String, String)],
) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(namespace.as_bytes());
    hasher.update(&[0u8]);
    hasher.update(payload.as_bytes());
    hasher.update(&[0u8]);
    hasher.update(&timestamp.to_be_bytes());
    for (k, v) in tags {
        hasher.update(&[0u8]);
        hasher.update(k.as_bytes());
        hasher.update(&[0u8]);
        hasher.update(v.as_bytes());
    }
    hasher.finalize().to_hex().to_string()
}

/// 태그 인덱스의 sled 키. 키 길이를 접두사로 넣어 `=` 를 포함한 태그가
/// 다른 태그와 같은 키로 뭉개지는 모호성(("a","b=c") vs ("a=b","c"))을 없앤다.
fn tag_key(key: &str, value: &str) -> String {
    format!("tag:{}:{key}={value}", key.len())
}

/// 블록 해시 = blake3(bincode(header))
fn compute_block_hash(header: &BlockHeader) -> Result<String> {
    let bytes = bincode::serialize(header)?;
    Ok(blake3::hash(&bytes).to_hex().to_string())
}

/// 트랜잭션 ID 목록의 머클 루트.
/// 빈 블록은 고정 도메인 문자열의 해시를 루트로 쓴다.
/// 홀수 레벨은 마지막 노드를 복제해 짝을 맞춘다 (비트코인 방식).
fn merkle_root(tx_ids: &[&str]) -> String {
    if tx_ids.is_empty() {
        return blake3::hash(b"tomei-chain/empty-block").to_hex().to_string();
    }
    let mut level: Vec<blake3::Hash> =
        tx_ids.iter().map(|id| blake3::hash(id.as_bytes())).collect();
    while level.len() > 1 {
        if level.len() % 2 == 1 {
            level.push(*level.last().expect("비어있지 않음"));
        }
        level = level
            .chunks(2)
            .map(|pair| {
                let mut hasher = blake3::Hasher::new();
                hasher.update(pair[0].as_bytes());
                hasher.update(pair[1].as_bytes());
                hasher.finalize()
            })
            .collect();
    }
    level[0].to_hex().to_string()
}

// ============================================================================
// [5] 스토리지 레이어 — sled 연동
// ============================================================================
//
// 트리 구성:
//   blocks        : 블록 데이터.  key = 높이(u64 big-endian) → bincode(Block)
//                   (big-endian 키라서 사전순 == 숫자순)
//   state         : 월드 스테이트.
//     "chain_state"       → bincode(BlockchainState)
//     "tx:<트랜잭션ID>"   → 높이(u64 big-endian)    (커밋 여부/위치 인덱스)
//     "ns:<네임스페이스>" → bincode(NamespaceInfo)  (앱별 최신 공증 상태)
//     "tag:<키>=<값>"     → 트랜잭션 ID (utf-8)     (first-wins 멱등 인덱스)
//     "mantis_head"       → bincode((len, head_hash)) (mantis 어댑터 헤드)
//   mantis_blocks : mantis-cad 블록 원본 JSON.  key = index(u64 BE) → raw bytes
//                   (수신 바이트 그대로 보존 — 해시/서명 재검증 가능)

pub(crate) struct Store {
    db: sled::Db,
    blocks: sled::Tree,
    state: sled::Tree,
    pub(crate) mantis_blocks: sled::Tree,
}

/// 체인 검증 결과 리포트
#[derive(Serialize, Debug)]
pub(crate) struct VerifyReport {
    pub ok: bool,
    pub checked_blocks: u64,
    pub from: u64,
    pub to: u64,
    pub error: Option<String>,
}

impl Store {
    /// sled DB를 열고 트리 핸들을 준비한다. 저사양 서버에 맞게 캐시를 제한한다.
    fn open(path: &str) -> Result<Self> {
        let db = sled::Config::new()
            .path(path)
            .cache_capacity(SLED_CACHE_BYTES)
            .mode(sled::Mode::LowSpace)
            .flush_every_ms(Some(1_000))
            .open()?;
        let blocks = db.open_tree("blocks")?;
        let state = db.open_tree("state")?;
        let mantis_blocks = db.open_tree("mantis_blocks")?;
        Ok(Store { db, blocks, state, mantis_blocks })
    }

    /// 초기화/복원 로직:
    ///   - 기존 상태가 있으면 최신 블록과 대조 검증 후 이어서 구동
    ///   - 없으면 제네시스 블록을 만들어 커밋
    fn init_or_resume(&self) -> Result<(BlockchainState, bool)> {
        match self.state.get(b"chain_state")? {
            Some(bytes) => {
                let st: BlockchainState = bincode::deserialize(&bytes)?;
                // 상태가 가리키는 최신 블록이 실제로 존재하고 해시가 일치하는지 확인
                let tip = self.get_block(st.height)?.ok_or_else(|| {
                    ChainError::Integrity(format!(
                        "상태는 높이 {} 를 가리키지만 해당 블록이 없습니다",
                        st.height
                    ))
                })?;
                if tip.hash != st.tip_hash {
                    return Err(ChainError::Integrity(format!(
                        "상태의 팁 해시와 블록 #{} 해시가 불일치합니다",
                        st.height
                    )));
                }
                tip.verify_integrity()?;
                Ok((st, true))
            }
            None => {
                let genesis = Block::genesis()?;
                let st = BlockchainState {
                    height: 0,
                    tip_hash: genesis.hash.clone(),
                    total_txs: 0,
                };
                self.commit_block(&genesis, &st)?;
                Ok((st, false))
            }
        }
    }

    /// 블록과 갱신된 상태를 **하나의 원자적 sled 트랜잭션**으로 커밋한다.
    /// (blocks 트리와 state 트리에 동시에 반영 — 중간 크래시에도 일관성 유지)
    fn commit_block(&self, block: &Block, new_state: &BlockchainState) -> Result<()> {
        use sled::transaction::TransactionError;
        use sled::Transactional;

        let height_be = block.header.index.to_be_bytes();
        let block_bytes = bincode::serialize(block)?;
        let state_bytes = bincode::serialize(new_state)?;

        // 인덱스/월드 스테이트 갱신분을 미리 계산한다.
        // (단일 시퀀서 구조라 커밋 중 동시 쓰기가 없어 사전 read 가 안전하다)
        let mut tx_index: Vec<(Vec<u8>, [u8; 8])> = Vec::with_capacity(block.transactions.len());
        let mut ns_updates: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let mut tag_inserts: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        {
            let mut ns_infos: HashMap<String, NamespaceInfo> = HashMap::new();
            for tx in &block.transactions {
                tx_index.push((format!("tx:{}", tx.id).into_bytes(), height_be));
                // 네임스페이스별 최신 상태 누적
                let entry = match ns_infos.get_mut(&tx.namespace) {
                    Some(e) => e,
                    None => {
                        let existing = self
                            .state
                            .get(format!("ns:{}", tx.namespace).as_bytes())?
                            .map(|b| bincode::deserialize(&b))
                            .transpose()?
                            .unwrap_or_default();
                        ns_infos.entry(tx.namespace.clone()).or_insert(existing)
                    }
                };
                entry.count += 1;
                entry.last_tx_id = tx.id.clone();
                entry.last_height = block.header.index;
                // 태그 인덱스: 최초 기록만 승자 (first-wins) — 멱등 제출 지원
                for (k, v) in &tx.tags {
                    let key = tag_key(k, v).into_bytes();
                    let taken_in_block = tag_inserts.iter().any(|(kk, _)| kk == &key);
                    if !taken_in_block && self.state.get(&key)?.is_none() {
                        tag_inserts.push((key, tx.id.clone().into_bytes()));
                    }
                }
            }
            for (ns, info) in &ns_infos {
                ns_updates.push((format!("ns:{ns}").into_bytes(), bincode::serialize(info)?));
            }
        }

        (&self.blocks, &self.state)
            .transaction(|(blocks_tx, state_tx)| {
                blocks_tx.insert(&height_be, block_bytes.as_slice())?;
                state_tx.insert(b"chain_state", state_bytes.as_slice())?;
                for (key, height) in &tx_index {
                    state_tx.insert(key.as_slice(), &height[..])?;
                }
                for (key, val) in &ns_updates {
                    state_tx.insert(key.as_slice(), val.as_slice())?;
                }
                for (key, val) in &tag_inserts {
                    state_tx.insert(key.as_slice(), val.as_slice())?;
                }
                Ok::<(), sled::transaction::ConflictableTransactionError<()>>(())
            })
            .map_err(|e| match e {
                TransactionError::Abort(_) => {
                    ChainError::Integrity("커밋 트랜잭션이 중단되었습니다".into())
                }
                TransactionError::Storage(e) => ChainError::Storage(e),
            })?;
        Ok(())
    }

    /// 디스크 동기화 — 블록 커밋 후 호출해 내구성(durability)을 보장한다.
    pub(crate) async fn flush(&self) -> Result<()> {
        self.db.flush_async().await?;
        Ok(())
    }

    pub(crate) fn get_block(&self, height: u64) -> Result<Option<Block>> {
        Ok(self
            .blocks
            .get(height.to_be_bytes())?
            .map(|b| bincode::deserialize(&b))
            .transpose()?)
    }

    /// 커밋된 트랜잭션 ID → 포함된 블록 높이
    pub(crate) fn tx_height(&self, tx_id: &str) -> Result<Option<u64>> {
        Ok(self.state.get(format!("tx:{tx_id}").as_bytes())?.map(|b| {
            let mut be = [0u8; 8];
            be.copy_from_slice(&b);
            u64::from_be_bytes(be)
        }))
    }

    pub(crate) fn namespace_info(&self, ns: &str) -> Result<Option<NamespaceInfo>> {
        Ok(self
            .state
            .get(format!("ns:{ns}").as_bytes())?
            .map(|b| bincode::deserialize(&b))
            .transpose()?)
    }

    /// 태그 (키,값) → 그 태그를 최초로 기록한 트랜잭션 ID
    pub(crate) fn tag_lookup(&self, key: &str, value: &str) -> Result<Option<String>> {
        Ok(self
            .state
            .get(tag_key(key, value).as_bytes())?
            .map(|b| String::from_utf8_lossy(&b).to_string()))
    }

    /// mantis 어댑터 헤드 메타 (블록 수, 헤드 해시)
    pub(crate) fn mantis_head_meta(&self) -> Result<Option<(u64, String)>> {
        Ok(self
            .state
            .get(b"mantis_head")?
            .map(|b| bincode::deserialize(&b))
            .transpose()?)
    }

    /// mantis 블록(원본 JSON 바이트) 조회
    pub(crate) fn mantis_get_raw(&self, index: u64) -> Result<Option<sled::IVec>> {
        Ok(self.mantis_blocks.get(index.to_be_bytes())?)
    }

    /// mantis 블록들과 헤드 메타를 하나의 원자적 트랜잭션으로 커밋
    pub(crate) fn mantis_commit(
        &self,
        new_blocks: &[(u64, String)],
        len: u64,
        head_hash: &str,
    ) -> Result<()> {
        use sled::transaction::TransactionError;
        use sled::Transactional;
        let meta = bincode::serialize(&(len, head_hash.to_string()))?;
        (&self.mantis_blocks, &self.state)
            .transaction(|(mb, st)| {
                for (index, raw) in new_blocks {
                    mb.insert(&index.to_be_bytes(), raw.as_bytes())?;
                }
                st.insert(b"mantis_head", meta.as_slice())?;
                Ok::<(), sled::transaction::ConflictableTransactionError<()>>(())
            })
            .map_err(|e| match e {
                TransactionError::Abort(_) => {
                    ChainError::Integrity("mantis 커밋 트랜잭션이 중단되었습니다".into())
                }
                TransactionError::Storage(e) => ChainError::Storage(e),
            })?;
        Ok(())
    }

    /// [from, to] 구간의 블록 연결성과 무결성을 전수 검증한다.
    pub(crate) fn verify_chain(&self, from: u64, to: u64) -> Result<VerifyReport> {
        let mut prev_hash: Option<String> = if from == 0 {
            None
        } else {
            // 구간 시작 직전 블록의 해시로 연결 고리를 이어서 검사
            Some(
                self.get_block(from - 1)?
                    .ok_or_else(|| {
                        ChainError::Integrity(format!("블록 #{} 이 없습니다", from - 1))
                    })?
                    .hash,
            )
        };
        let mut checked = 0u64;
        for h in from..=to {
            let block = match self.get_block(h)? {
                Some(b) => b,
                None => {
                    return Ok(VerifyReport {
                        ok: false,
                        checked_blocks: checked,
                        from,
                        to,
                        error: Some(format!("블록 #{h} 이 없습니다")),
                    })
                }
            };
            if let Err(e) = block.verify_integrity() {
                return Ok(VerifyReport {
                    ok: false,
                    checked_blocks: checked,
                    from,
                    to,
                    error: Some(e.to_string()),
                });
            }
            if let Some(prev) = &prev_hash {
                if &block.header.prev_block_hash != prev {
                    return Ok(VerifyReport {
                        ok: false,
                        checked_blocks: checked,
                        from,
                        to,
                        error: Some(format!("블록 #{h} 의 prev_block_hash 연결 불일치")),
                    });
                }
            }
            prev_hash = Some(block.hash);
            checked += 1;
        }
        Ok(VerifyReport { ok: true, checked_blocks: checked, from, to, error: None })
    }
}

// ============================================================================
// [6] 멤풀 — 간단한 메모리 내 FIFO 큐 + 중복 방지 인덱스
// ============================================================================

#[derive(Default)]
pub(crate) struct Mempool {
    pub queue: VecDeque<Transaction>,
    /// 대기 또는 밀봉 중인 트랜잭션 ID — O(1) 중복 검사용.
    /// (커밋 확정 후에야 제거되어, 밀봉 중에도 중복 제출이 멱등 처리된다)
    pub ids: HashSet<String>,
    /// 대기/밀봉 중인 태그 → 트랜잭션 ID — 커밋 전 멱등 제출 검사용
    pub pending_tags: HashMap<(String, String), String>,
    /// 현재 블록으로 밀봉(커밋) 진행 중인 트랜잭션 — 드레인~커밋 사이에도
    /// GET /tx, GET /bundle 조회가 끊기지 않게 하는 가시성 창
    pub sealing: Vec<Transaction>,
}

impl Mempool {
    /// 대기 중(큐 또는 밀봉 창)인 트랜잭션을 ID로 찾는다.
    pub fn find(&self, tx_id: &str) -> Option<&Transaction> {
        self.queue
            .iter()
            .find(|t| t.id == tx_id)
            .or_else(|| self.sealing.iter().find(|t| t.id == tx_id))
    }

    /// 커밋이 확정된 트랜잭션의 흔적(ID/태그 클레임)을 정리한다.
    fn forget(&mut self, tx: &Transaction) {
        self.ids.remove(&tx.id);
        for tag in &tx.tags {
            // 태그는 이 트랜잭션이 클레임 소유자일 때만 제거 (다른 대기 tx 보호)
            if self.pending_tags.get(tag) == Some(&tx.id) {
                self.pending_tags.remove(tag);
            }
        }
    }
}

/// 트랜잭션 제출 결과. `AlreadyPending`/`AlreadyCommitted` 와 함께 반환되는
/// ID는 **기존(승자) 트랜잭션의 ID**다 — 태그 멱등으로 dedup 된 경우
/// 제출한 내용의 ID와 다를 수 있다.
pub(crate) enum SubmitOutcome {
    /// 새로 큐에 들어감
    Queued,
    /// 이미 멤풀에 대기 중이거나 같은 태그의 트랜잭션이 대기 중 (멱등 처리)
    AlreadyPending,
    /// 이미 커밋됐거나 같은 태그의 트랜잭션이 커밋됨 (멱등 처리)
    AlreadyCommitted { height: u64 },
    /// 멤풀 포화 — 잠시 후 재시도 필요
    PoolFull,
}

// ============================================================================
// [7] 체인 코어 — 상태 + 스토리지 + 멤풀을 묶는 중심 구조체
// ============================================================================

pub(crate) struct Chain {
    pub store: Store,
    /// 인메모리 최신 상태 (sled 의 chain_state 와 항상 동기화됨)
    state: Mutex<BlockchainState>,
    pub mempool: Mutex<Mempool>,
    /// 배치 임계치 도달 시 블록 생산 루프를 깨우는 신호
    pub produce_signal: Notify,
    batch_threshold: usize,
    /// mantis 어댑터의 헤드 (블록 수, 헤드 해시) — sled "mantis_head" 와 동기화
    pub mantis_head: Mutex<(u64, String)>,
    /// /upload 응답의 번들 URL 생성용 외부 접근 베이스 URL
    pub public_url: String,
}

impl Chain {
    /// 스토리지를 열고 상태를 복원(또는 제네시스 생성)해 체인을 초기화한다.
    fn bootstrap(config: &Config) -> Result<(Arc<Self>, bool)> {
        let store = Store::open(&config.data_dir)?;
        let (state, resumed) = store.init_or_resume()?;
        let mantis_head = mantis::init(&store)?;
        let chain = Arc::new(Chain {
            store,
            state: Mutex::new(state),
            mempool: Mutex::new(Mempool::default()),
            produce_signal: Notify::new(),
            batch_threshold: config.batch_threshold,
            mantis_head: Mutex::new(mantis_head),
            public_url: config.public_url.trim_end_matches('/').to_string(),
        });
        Ok((chain, resumed))
    }

    /// 트랜잭션 제출 (검증 → 중복 검사 → 멤풀 삽입). API·데모·어댑터가 공용 사용.
    pub(crate) async fn submit_tx(
        &self,
        namespace: String,
        payload: String,
        signature: Option<String>,
        tags: Vec<(String, String)>,
    ) -> Result<(String, SubmitOutcome)> {
        let tx = Transaction::new(namespace, payload, signature, tags)?;
        let id = tx.id.clone();

        // 이미 커밋된 트랜잭션이면 그대로 알려준다 (멱등 제출 지원)
        if let Some(height) = self.store.tx_height(&id)? {
            return Ok((id, SubmitOutcome::AlreadyCommitted { height }));
        }

        // 멤풀 락 아래에서 ID·태그 멱등 검사와 삽입을 원자적으로 수행한다.
        // (락 밖에서 검사하면 동시 제출이 둘 다 통과하는 TOCTOU 가 생긴다)
        let mut pool = self.mempool.lock().await;
        if pool.ids.contains(&id) {
            return Ok((id, SubmitOutcome::AlreadyPending));
        }
        for tag in &tx.tags {
            // 같은 태그가 대기/밀봉 중이면 그 승자 트랜잭션으로 dedup
            if let Some(existing) = pool.pending_tags.get(tag) {
                return Ok((existing.clone(), SubmitOutcome::AlreadyPending));
            }
            // 같은 태그가 이미 커밋됐으면 커밋된 승자로 dedup (first-wins)
            if let Some(existing) = self.store.tag_lookup(&tag.0, &tag.1)? {
                let height = self.store.tx_height(&existing)?.unwrap_or_default();
                return Ok((existing, SubmitOutcome::AlreadyCommitted { height }));
            }
        }
        if pool.queue.len() >= MAX_MEMPOOL {
            return Ok((id, SubmitOutcome::PoolFull));
        }
        pool.ids.insert(id.clone());
        for tag in &tx.tags {
            pool.pending_tags.insert(tag.clone(), id.clone());
        }
        pool.queue.push_back(tx);
        let pending = pool.queue.len();
        drop(pool);

        // 배치 임계치에 도달했으면 생산 루프를 즉시 깨운다
        if pending >= self.batch_threshold {
            self.produce_signal.notify_one();
        }
        Ok((id, SubmitOutcome::Queued))
    }

    /// 멤풀에서 트랜잭션을 꺼내 새 블록을 만들고 sled 에 원자적으로 커밋한다.
    /// 단일 시퀀서이므로 이 함수만이 체인을 전진시킨다.
    ///
    /// 드레인된 트랜잭션은 커밋이 확정될 때까지 `sealing` 창과 `ids`/`pending_tags`
    /// 에 남아 있어, 밀봉 중에도 조회(GET /tx, /bundle)와 멱등 제출이 끊기지 않는다.
    pub(crate) async fn seal_pending_block(&self) -> Result<Option<Block>> {
        // 1) 멤풀에서 개수/바이트 상한까지 드레인 → 밀봉 창으로 이동
        let mut txs: Vec<Transaction> = {
            let mut pool = self.mempool.lock().await;
            let mut drained = Vec::new();
            let mut bytes = 0usize;
            while let Some(front) = pool.queue.front() {
                if drained.len() >= MAX_BLOCK_TXS {
                    break;
                }
                let size = front.payload.len();
                if !drained.is_empty() && bytes + size > MAX_BLOCK_BYTES {
                    break;
                }
                drained.push(pool.queue.pop_front().expect("front 확인됨"));
                bytes += size;
            }
            pool.sealing = drained.clone();
            drained
        };
        // 제출↔커밋 경합으로 이미 커밋된 트랜잭션이 섞였으면 걸러낸다.
        // (스토리지 읽기 '오류'는 커밋됨으로 오인하지 않고 트랜잭션을 보존한다)
        let mut already_committed = Vec::new();
        txs.retain(|t| match self.store.tx_height(&t.id) {
            Ok(Some(_)) => {
                already_committed.push(t.clone());
                false
            }
            Ok(None) | Err(_) => true,
        });
        if !already_committed.is_empty() {
            let mut pool = self.mempool.lock().await;
            for tx in &already_committed {
                pool.forget(tx);
                pool.sealing.retain(|s| s.id != tx.id);
            }
        }
        if txs.is_empty() {
            self.mempool.lock().await.sealing.clear();
            return Ok(None);
        }

        // 실패 시 복구: 밀봉 창의 트랜잭션을 멤풀 앞쪽에 되돌려 다음 주기에 재시도.
        // (ids/pending_tags 는 드레인 때 제거하지 않았으므로 그대로 유효)
        let restore = |pool: &mut Mempool| {
            let sealed = std::mem::take(&mut pool.sealing);
            for tx in sealed.into_iter().rev() {
                if pool.queue.iter().all(|q| q.id != tx.id) {
                    pool.queue.push_front(tx);
                }
            }
        };

        // 2) 상태 락을 잡은 채 블록 조립 → 커밋 → 상태 갱신 (시퀀서 직렬화 구간)
        let mut state = self.state.lock().await;
        let index = state.height + 1;
        // 블록 조립 + 커밋 직전 마지막 안전장치(자체 무결성 검증)
        let block = match Block::new(index, state.tip_hash.clone(), txs)
            .and_then(|b| b.verify_integrity().map(|()| b))
        {
            Ok(b) => b,
            Err(e) => {
                restore(&mut *self.mempool.lock().await);
                return Err(e);
            }
        };

        let new_state = BlockchainState {
            height: index,
            tip_hash: block.hash.clone(),
            total_txs: state.total_txs + block.transactions.len() as u64,
        };
        if let Err(e) = self.store.commit_block(&block, &new_state) {
            restore(&mut *self.mempool.lock().await);
            return Err(e);
        }
        *state = new_state;
        drop(state);
        // 커밋 확정 — 이제야 멱등 인덱스에서 흔적을 지운다 (스토어 인덱스가 이어받음)
        {
            let mut pool = self.mempool.lock().await;
            for tx in &block.transactions {
                pool.forget(tx);
            }
            pool.sealing.clear();
        }
        // fsync — 전원 차단에도 살아남도록. 실패해도 커밋 자체는 유효하다.
        if let Err(e) = self.store.flush().await {
            log_line(&format!("경고: flush 실패 (데이터는 커밋됨): {e}"));
        }
        Ok(Some(block))
    }

    pub(crate) async fn snapshot(&self) -> (BlockchainState, usize) {
        let state = self.state.lock().await.clone();
        let pending = self.mempool.lock().await.queue.len();
        (state, pending)
    }

    /// 트랜잭션이 블록에 커밋될 때까지 대기 (HeartCritique /upload 의 동기 응답용)
    pub(crate) async fn wait_for_commit(&self, tx_id: &str, timeout: Duration) -> Option<u64> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if let Ok(Some(height)) = self.store.tx_height(tx_id) {
                return Some(height);
            }
            if tokio::time::Instant::now() >= deadline {
                return None;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

// ============================================================================
// [8] 블록 생산 루프 — 주기 타이머 + 배치 신호로 깨어나는 비동기 루틴
// ============================================================================

async fn block_producer(
    chain: Arc<Chain>,
    interval: Duration,
    mut shutdown: watch::Receiver<bool>,
) {
    log_line("블록 생산 루프 시작");
    loop {
        tokio::select! {
            // 주기적 생산 (트랜잭션이 있을 때만 실제로 블록이 만들어짐)
            _ = tokio::time::sleep(interval) => {}
            // 멤풀 배치 임계치 도달 → 즉시 생산
            _ = chain.produce_signal.notified() => {}
            // 종료 신호
            _ = shutdown.changed() => break,
        }
        loop {
            match chain.seal_pending_block().await {
                Ok(Some(block)) => {
                    log_line(&format!(
                        "블록 #{} 커밋 — 트랜잭션 {}개, 해시 {}…",
                        block.header.index,
                        block.transactions.len(),
                        &block.hash[..16]
                    ));
                    // 멤풀에 잔여분이 남았으면 같은 턴에서 연속 생산
                    if chain.mempool.lock().await.queue.is_empty() {
                        break;
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    // 생산 실패는 치명적이지 않음 — 다음 주기에 재시도
                    log_line(&format!("블록 생산 오류(다음 주기에 재시도): {e}"));
                    break;
                }
            }
        }
    }

    // 종료 직전: 멤풀에 남은 트랜잭션을 전부 커밋 (블록 상한을 넘는 잔여분도
    // 여러 블록으로 이어서 밀봉 — 202 로 접수한 트랜잭션을 유실하지 않는다)
    drain_mempool_fully(&chain, "종료 전").await;
    log_line("블록 생산 루프 종료");
}

/// 멤풀이 빌 때까지 블록을 연속 밀봉한다. (종료 시퀀스 전용)
async fn drain_mempool_fully(chain: &Chain, label: &str) {
    loop {
        match chain.seal_pending_block().await {
            Ok(Some(block)) => log_line(&format!(
                "{label} 잔여 트랜잭션 {}개를 블록 #{} 로 커밋",
                block.transactions.len(),
                block.header.index
            )),
            Ok(None) => break,
            Err(e) => {
                log_line(&format!("{label} 커밋 실패: {e}"));
                break;
            }
        }
    }
}

// ============================================================================
// [9] HTTP JSON API — 외부 의존성 없이 tokio 만으로 구현한 미니 서버
// ============================================================================
//
// 코어 엔드포인트:
//   GET  /health            → 생존 확인
//   GET  /status            → 체인 상태 + 멤풀 크기
//   POST /tx                → 트랜잭션 제출 {namespace, payload, signature?, tags?}
//   GET  /tx/{id}           → 트랜잭션 조회 (확정 여부/블록 위치)
//   GET  /block/latest      → 최신 블록
//   GET  /block/{height}    → 특정 높이 블록
//   GET  /ns/{namespace}    → 앱별 월드 스테이트 (마지막 공증 등)
//   GET  /verify            → 전체 체인 무결성 검증
//
// HeartCritiqueEAS 어댑터 (uploader 사이드카 계약 — heartcritique.rs):
//   POST /upload            → 서명 번들 박제  {payload, signature, publicKey, ...}
//   GET  /bundle/{txId}     → 저장된 번들 원본 JSON (검증용 조회 URL)
//   GET  /wallet            → 로컬 체인 비용 정보 (수수료 0)
//
// mantis-cad 어댑터 (mantis-server 계약 — mantis.rs, 포트 7878 겸용):
//   GET  /api/info          → {"len":N,"head":"<hex>"}
//   GET  /api/blocks?from=N → 블록 원본 JSON 배열
//   POST /api/blocks        → fast-forward 병합 (충돌 시 409)
//
// 모든 응답은 JSON + CORS, Connection: close (커넥션 풀 관리 불필요 — 단순함 우선)

/// POST /tx 요청 바디
#[derive(Deserialize)]
struct SubmitTxRequest {
    namespace: String,
    payload: String,
    #[serde(default)]
    signature: Option<String>,
    #[serde(default)]
    tags: Vec<(String, String)>,
}

async fn api_server(chain: Arc<Chain>, listener: TcpListener, mut shutdown: watch::Receiver<bool>) {
    // 동시 커넥션 유계화 — 포화 시 새 커넥션은 즉시 끊어 자원 고갈을 막는다
    let permits = Arc::new(Semaphore::new(MAX_CONNECTIONS));
    loop {
        tokio::select! {
            _ = shutdown.changed() => break,
            accepted = listener.accept() => {
                let (stream, _) = match accepted {
                    Ok(pair) => pair,
                    Err(e) => {
                        log_line(&format!("accept 오류: {e}"));
                        continue;
                    }
                };
                let Ok(permit) = Arc::clone(&permits).try_acquire_owned() else {
                    drop(stream); // 포화 — 커넥션 거절
                    continue;
                };
                let chain = Arc::clone(&chain);
                // 커넥션당 태스크 — 처리 중 오류/시간 초과는 해당 커넥션만 종료
                tokio::spawn(async move {
                    let _ = tokio::time::timeout(
                        CONNECTION_TIMEOUT,
                        handle_connection(stream, chain),
                    )
                    .await;
                    drop(permit);
                });
            }
        }
    }
}

/// HTTP/1.1 요청 하나를 읽어 라우팅하고 응답을 쓴다.
async fn handle_connection(mut stream: TcpStream, chain: Arc<Chain>) -> Result<()> {
    let (status, body) = match read_request(&mut stream).await {
        Ok((method, path, query, body)) => {
            route(&chain, &method, &path, query.as_deref(), &body).await
        }
        Err(ChainError::TooLarge(m)) => (413, format!(r#"{{"error":"{m}"}}"#)),
        Err(ChainError::Unsupported(m)) => (411, format!(r#"{{"error":"{m}"}}"#)),
        Err(e) => (400, format!(r#"{{"error":"잘못된 요청: {e}"}}"#)),
    };
    let reason = match status {
        200 => "OK",
        202 => "Accepted",
        204 => "No Content",
        400 => "Bad Request",
        404 => "Not Found",
        409 => "Conflict",
        411 => "Length Required",
        413 => "Payload Too Large",
        503 => "Service Unavailable",
        _ => "Internal Server Error",
    };
    let body = if status == 204 { String::new() } else { body };
    let response = format!(
        "HTTP/1.1 {status} {reason}\r\n\
         Content-Type: application/json; charset=utf-8\r\n\
         Content-Length: {}\r\n\
         Access-Control-Allow-Origin: *\r\n\
         Access-Control-Allow-Methods: POST, GET, OPTIONS\r\n\
         Access-Control-Allow-Headers: content-type\r\n\
         Connection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(response.as_bytes()).await?;
    stream.shutdown().await?;
    Ok(())
}

/// 요청 파싱: (메서드, 경로, 쿼리, 바디). 크기 상한으로 메모리를 보호한다.
async fn read_request(
    stream: &mut TcpStream,
) -> Result<(String, String, Option<String>, Vec<u8>)> {
    let mut buf: Vec<u8> = Vec::with_capacity(1024);
    let mut chunk = [0u8; 8192];

    // 1) 헤더 끝(\r\n\r\n)이 나올 때까지 읽기
    let header_end = loop {
        if let Some(pos) = find_subslice(&buf, b"\r\n\r\n") {
            break pos;
        }
        if buf.len() > 64 * 1024 {
            return Err(ChainError::InvalidTx("요청 헤더가 너무 큽니다".into()));
        }
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            return Err(ChainError::InvalidTx("요청이 중간에 끊어졌습니다".into()));
        }
        buf.extend_from_slice(&chunk[..n]);
    };

    // 2) 요청 라인과 헤더 파싱
    let header_text = String::from_utf8_lossy(&buf[..header_end]).to_string();
    let mut lines = header_text.split("\r\n");
    let request_line = lines.next().unwrap_or_default();
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or_default().to_uppercase();
    let target = parts.next().unwrap_or_default();
    let (path, query) = match target.split_once('?') {
        Some((p, q)) => (p.to_string(), Some(q.to_string())),
        None => (target.to_string(), None),
    };

    let mut content_length = 0usize;
    let mut expect_continue = false;
    let mut chunked = false;
    for line in lines {
        if let Some((name, value)) = line.split_once(':') {
            let name = name.trim();
            let value = value.trim();
            if name.eq_ignore_ascii_case("content-length") {
                content_length = value.parse().unwrap_or(0);
            } else if name.eq_ignore_ascii_case("expect")
                && value.eq_ignore_ascii_case("100-continue")
            {
                expect_continue = true;
            } else if name.eq_ignore_ascii_case("transfer-encoding")
                && value.to_ascii_lowercase().contains("chunked")
            {
                chunked = true;
            }
        }
    }
    if chunked {
        // 청크 인코딩을 조용히 빈 바디로 취급하면 데이터가 유실된다 — 명시적 거부
        return Err(ChainError::Unsupported(
            "chunked 전송은 지원하지 않습니다. Content-Length 를 사용하세요".into(),
        ));
    }
    if content_length > MAX_HTTP_REQUEST_BYTES {
        return Err(ChainError::TooLarge("body too large".into()));
    }
    // curl 등은 큰 바디에서 Expect: 100-continue 후 1초를 기다린다 — 즉시 승인
    if expect_continue {
        stream.write_all(b"HTTP/1.1 100 Continue\r\n\r\n").await?;
    }

    // 3) 바디 읽기 (헤더와 함께 이미 도착한 부분 + 잔여분)
    let mut body = buf[header_end + 4..].to_vec();
    while body.len() < content_length {
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            break;
        }
        body.extend_from_slice(&chunk[..n]);
    }
    body.truncate(content_length);
    Ok((method, path, query, body))
}

/// 경로 세그먼트의 %XX 퍼센트 인코딩을 해제한다. 잘못된 인코딩이나 비 UTF-8
/// 은 원문 그대로 돌려준다 (조회 실패로 이어질 뿐 안전함).
fn percent_decode(segment: &str) -> String {
    let bytes = segment.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hi = (bytes[i + 1] as char).to_digit(16);
            let lo = (bytes[i + 2] as char).to_digit(16);
            if let (Some(h), Some(l)) = (hi, lo) {
                out.push(((h << 4) | l) as u8);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8(out).unwrap_or_else(|_| segment.to_string())
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

/// serde_json::Value → 응답 문자열 (직렬화 실패 시 안전한 폴백)
pub(crate) fn json_body(value: &serde_json::Value) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| r#"{"error":"internal"}"#.to_string())
}

/// 라우팅 테이블 — (상태코드, JSON 바디 문자열)을 돌려준다.
async fn route(
    chain: &Chain,
    method: &str,
    path: &str,
    query: Option<&str>,
    body: &[u8],
) -> (u16, String) {
    // 세그먼트 단위 퍼센트 디코딩 — 브라우저/httpx 가 인코딩해 보낸 경로도
    // 저장 시 원문 키와 일치하게 조회된다 (%2F 는 세그먼트 분해 뒤라 안전)
    let segments: Vec<String> = path
        .trim_matches('/')
        .split('/')
        .filter(|s| !s.is_empty())
        .map(percent_decode)
        .collect();
    let segments: Vec<&str> = segments.iter().map(|s| s.as_str()).collect();

    match (method, segments.as_slice()) {
        // CORS preflight (웹 프런트엔드 클라이언트용)
        ("OPTIONS", _) => (204, String::new()),

        ("GET", ["health"]) => {
            (200, json_body(&serde_json::json!({ "ok": true, "network": "tomei-local" })))
        }

        ("GET", ["status"]) => {
            let (state, pending) = chain.snapshot().await;
            let mantis = chain.mantis_head.lock().await.clone();
            (
                200,
                json_body(&serde_json::json!({
                    "height": state.height,
                    "tip_hash": state.tip_hash,
                    "total_txs": state.total_txs,
                    "mempool_pending": pending,
                    "mantis_len": mantis.0,
                    "mantis_head": mantis.1,
                })),
            )
        }

        ("POST", ["tx"]) => {
            let req: SubmitTxRequest = match serde_json::from_slice(body) {
                Ok(r) => r,
                Err(e) => {
                    return (
                        400,
                        json_body(&serde_json::json!({ "error": format!("JSON 파싱 실패: {e}") })),
                    )
                }
            };
            match chain.submit_tx(req.namespace, req.payload, req.signature, req.tags).await {
                Ok((id, outcome)) => match outcome {
                    SubmitOutcome::Queued => {
                        (202, json_body(&serde_json::json!({ "id": id, "status": "queued" })))
                    }
                    SubmitOutcome::AlreadyPending => {
                        (202, json_body(&serde_json::json!({ "id": id, "status": "pending" })))
                    }
                    SubmitOutcome::AlreadyCommitted { height } => (
                        200,
                        json_body(&serde_json::json!({
                            "id": id, "status": "confirmed", "height": height
                        })),
                    ),
                    SubmitOutcome::PoolFull => (
                        503,
                        json_body(&serde_json::json!({
                            "id": id, "error": "멤풀 포화 — 잠시 후 재시도하세요"
                        })),
                    ),
                },
                Err(e) => (400, json_body(&serde_json::json!({ "error": e.to_string() }))),
            }
        }

        ("GET", ["tx", id]) => {
            // 1) 커밋된 트랜잭션인지 확인
            match chain.store.tx_height(id) {
                Ok(Some(height)) => match chain.store.get_block(height) {
                    Ok(Some(block)) => {
                        let tx = block.transactions.iter().find(|t| t.id == *id);
                        (
                            200,
                            json_body(&serde_json::json!({
                                "status": "confirmed",
                                "height": height,
                                "block_hash": block.hash,
                                "tx": tx,
                            })),
                        )
                    }
                    _ => (
                        500,
                        json_body(
                            &serde_json::json!({ "error": "인덱스는 있으나 블록 조회 실패" }),
                        ),
                    ),
                },
                Ok(None) => {
                    // 2) 멤풀 대기 중인지 확인
                    let pending = chain.mempool.lock().await.ids.contains(*id);
                    if pending {
                        (200, json_body(&serde_json::json!({ "status": "pending" })))
                    } else {
                        (404, json_body(&serde_json::json!({ "error": "존재하지 않는 트랜잭션" })))
                    }
                }
                Err(e) => (500, json_body(&serde_json::json!({ "error": e.to_string() }))),
            }
        }

        ("GET", ["block", "latest"]) => {
            let (state, _) = chain.snapshot().await;
            match chain.store.get_block(state.height) {
                Ok(Some(block)) => match serde_json::to_string(&block) {
                    Ok(s) => (200, s),
                    Err(e) => (500, json_body(&serde_json::json!({ "error": e.to_string() }))),
                },
                _ => (500, json_body(&serde_json::json!({ "error": "최신 블록 조회 실패" }))),
            }
        }

        ("GET", ["block", height_str]) => match height_str.parse::<u64>() {
            Ok(height) => match chain.store.get_block(height) {
                Ok(Some(block)) => match serde_json::to_string(&block) {
                    Ok(s) => (200, s),
                    Err(e) => (500, json_body(&serde_json::json!({ "error": e.to_string() }))),
                },
                Ok(None) => {
                    (404, json_body(&serde_json::json!({ "error": "존재하지 않는 블록 높이" })))
                }
                Err(e) => (500, json_body(&serde_json::json!({ "error": e.to_string() }))),
            },
            Err(_) => {
                (400, json_body(&serde_json::json!({ "error": "블록 높이는 정수여야 합니다" })))
            }
        },

        ("GET", ["ns", ns]) => match chain.store.namespace_info(ns) {
            Ok(Some(info)) => (200, json_body(&serde_json::to_value(&info).unwrap_or_default())),
            Ok(None) => {
                (404, json_body(&serde_json::json!({ "error": "기록이 없는 네임스페이스" })))
            }
            Err(e) => (500, json_body(&serde_json::json!({ "error": e.to_string() }))),
        },

        ("GET", ["verify"]) => {
            let (state, _) = chain.snapshot().await;
            match chain.store.verify_chain(0, state.height) {
                Ok(report) => {
                    let code = if report.ok { 200 } else { 500 };
                    (code, json_body(&serde_json::to_value(&report).unwrap_or_default()))
                }
                Err(e) => (500, json_body(&serde_json::json!({ "error": e.to_string() }))),
            }
        }

        // ── HeartCritiqueEAS 어댑터 (uploader 사이드카 드롭인) ──────────────
        ("POST", ["upload"]) => heartcritique::upload(chain, body).await,
        ("GET", ["bundle", tx_id]) => heartcritique::bundle(chain, tx_id).await,
        ("GET", ["wallet"]) => heartcritique::wallet(chain),

        // ── mantis-cad 어댑터 (mantis-server 드롭인) ─────────────────────────
        ("GET", ["api", "info"]) => mantis::info(chain).await,
        ("GET", ["api", "blocks"]) => mantis::get_blocks(chain, query).await,
        ("POST", ["api", "blocks"]) => mantis::post_blocks(chain, body).await,

        _ => (404, json_body(&serde_json::json!({ "error": "알 수 없는 엔드포인트" }))),
    }
}

// ============================================================================
// [10] 데모 — claude.md 5번 요구사항: 가상 트랜잭션 → 블록 생성 → 영속화 확인
// ============================================================================

async fn run_demo(chain: &Arc<Chain>) -> Result<()> {
    log_line("=== 데모 모드: 가상 트랜잭션을 멤풀에 삽입합니다 ===");

    let samples = [
        // HeartCritiqueEAS 스타일: 리뷰/평가 공증(attestation) 데이터
        (
            "heartcritique-eas",
            serde_json::json!({
                "type": "attestation",
                "schema": "critique.v1",
                "subject": "review:0x3fa9",
                "content_hash": blake3::hash(b"heartfelt critique body").to_hex().to_string(),
            })
            .to_string(),
        ),
        // mantis-cad 스타일: CAD 도면 파일 해시 공증
        (
            "mantis-cad",
            serde_json::json!({
                "type": "design_anchor",
                "file": "bracket_v3.step",
                "file_hash": blake3::hash(b"STEP file bytes ...").to_hex().to_string(),
                "revision": 3,
            })
            .to_string(),
        ),
        (
            "mantis-cad",
            serde_json::json!({
                "type": "design_anchor",
                "file": "housing_v1.step",
                "file_hash": blake3::hash(b"another STEP file").to_hex().to_string(),
                "revision": 1,
            })
            .to_string(),
        ),
    ];

    let mut ids = Vec::new();
    for (ns, payload) in samples {
        let (id, _) = chain.submit_tx(ns.to_string(), payload, None, Vec::new()).await?;
        log_line(&format!("  트랜잭션 제출: {ns} → {}…", &id[..16]));
        ids.push(id);
    }

    // 블록 생산 루프가 커밋할 때까지 대기 (최대 15초)
    log_line("블록 생산 대기 중...");
    for id in &ids {
        chain.wait_for_commit(id, Duration::from_secs(15)).await;
    }

    // 결과 출력: 체인 상태와 각 트랜잭션의 확정 위치
    let (state, pending) = chain.snapshot().await;
    log_line(&format!(
        "체인 상태 — 높이: {}, 총 트랜잭션: {}, 멤풀 대기: {}",
        state.height, state.total_txs, pending
    ));
    for id in &ids {
        match chain.store.tx_height(id)? {
            Some(h) => log_line(&format!("  {}… → 블록 #{h} 에 확정", &id[..16])),
            None => log_line(&format!("  {}… → 아직 미확정", &id[..16])),
        }
    }

    // 무결성 전수 검증으로 마무리
    let report = chain.store.verify_chain(0, state.height)?;
    log_line(&format!(
        "체인 무결성 검증: {} (검사한 블록 {}개)",
        if report.ok { "통과" } else { "실패" },
        report.checked_blocks
    ));
    log_line("=== 데모 종료 — 같은 명령을 다시 실행하면 이 체인에 이어서 기록됩니다 ===");
    Ok(())
}

// ============================================================================
// [11] main — 초기화 → 데몬 구동 → 우아한 종료
// ============================================================================

pub(crate) fn log_line(msg: &str) {
    // 외부 로깅 크레이트 없이 최소한의 타임스탬프(unix 초) 로그
    println!("[tomei-chain {}] {msg}", now_millis() / 1000);
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::from_env();
    let demo_mode = std::env::args().any(|a| a == "--demo");

    log_line(&format!("데이터 디렉토리: {}", config.data_dir));

    // 1) 스토리지 초기화 — 기존 체인이 있으면 복원, 없으면 제네시스 생성
    let (chain, resumed) = Chain::bootstrap(&config)?;
    {
        let (state, _) = chain.snapshot().await;
        if resumed {
            log_line(&format!(
                "기존 체인 복원 — 높이 {}, 팁 {}…",
                state.height,
                &state.tip_hash[..16]
            ));
        } else {
            log_line(&format!("제네시스 블록 생성 — 팁 {}…", &state.tip_hash[..16]));
        }
        let mantis = chain.mantis_head.lock().await.clone();
        log_line(&format!("mantis 체인 — 블록 {}개, 헤드 {}…", mantis.0, &mantis.1[..16]));
    }

    // 2) 종료 신호 채널 (Ctrl-C → 모든 루프에 전파)
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    // 3) 블록 생산 루프 기동
    let producer = tokio::spawn(block_producer(
        Arc::clone(&chain),
        config.block_interval,
        shutdown_rx.clone(),
    ));

    // 4) HTTP API 서버 기동 (메인 포트 + mantis-cad 기본 포트 겸용 리스너)
    let listener = TcpListener::bind(config.api_addr).await?;
    log_line(&format!("HTTP API 대기 중: http://{}", config.api_addr));
    let api = tokio::spawn(api_server(Arc::clone(&chain), listener, shutdown_rx.clone()));

    let mut mantis_api = None;
    if let Some(addr) = config.mantis_addr {
        match TcpListener::bind(addr).await {
            Ok(l) => {
                log_line(&format!("mantis-cad 호환 리스너 대기 중: http://{addr}"));
                mantis_api =
                    Some(tokio::spawn(api_server(Arc::clone(&chain), l, shutdown_rx.clone())));
            }
            Err(e) => log_line(&format!(
                "mantis 리스너({addr}) 바인드 실패 — 건너뜀 (기존 mantis-server 실행 중일 수 있음): {e}"
            )),
        }
    }

    if demo_mode {
        // 데모: 가상 트랜잭션 흐름을 보여준 뒤 스스로 우아하게 종료
        run_demo(&chain).await?;
        let _ = shutdown_tx.send(true);
    } else {
        // 데몬: Ctrl-C(SIGINT)까지 상주
        log_line("데몬 모드로 상주합니다. 종료: Ctrl-C");
        tokio::signal::ctrl_c().await?;
        log_line("종료 신호 수신 — 우아한 종료를 시작합니다");
        let _ = shutdown_tx.send(true);
    }

    // 5) 우아한 종료: 생산 루프(잔여 트랜잭션 커밋 포함) → API 중단 →
    //    API 중단 직전에 접수된 낙오 트랜잭션까지 밀봉 → 최종 flush
    let _ = producer.await;
    api.abort(); // accept 대기는 즉시 중단해도 안전
    if let Some(m) = mantis_api {
        m.abort();
    }
    // 진행 중이던 커넥션이 마무리할 짧은 유예 후, 낙오분을 마지막으로 밀봉
    tokio::time::sleep(Duration::from_millis(200)).await;
    drain_mempool_fully(&chain, "종료 후").await;
    chain.store.flush().await?;
    log_line("모든 데이터가 디스크에 동기화되었습니다. 안녕히!");
    Ok(())
}

// ============================================================================
// [12] 테스트
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    /// 테스트마다 고유한 임시 sled 디렉토리를 만든다.
    pub(crate) fn temp_dir(prefix: &str) -> String {
        static SEQ: AtomicU64 = AtomicU64::new(0);
        std::env::temp_dir()
            .join(format!(
                "tomei-{prefix}-{}-{}",
                std::process::id(),
                SEQ.fetch_add(1, Ordering::SeqCst)
            ))
            .to_string_lossy()
            .to_string()
    }

    fn temp_store() -> (Store, String) {
        let path = temp_dir("store");
        (Store::open(&path).expect("스토어 열기"), path)
    }

    pub(crate) fn test_config(data_dir: String) -> Config {
        Config {
            data_dir,
            api_addr: "127.0.0.1:0".parse().unwrap(),
            mantis_addr: None,
            public_url: "http://127.0.0.1:8642".to_string(),
            block_interval: Duration::from_millis(50),
            batch_threshold: 100,
        }
    }

    pub(crate) fn test_chain() -> (Arc<Chain>, String) {
        let dir = temp_dir("chain");
        let (chain, _) = Chain::bootstrap(&test_config(dir.clone())).unwrap();
        (chain, dir)
    }

    #[test]
    fn tx_id_는_결정적이며_내용에_민감하다() {
        let a = compute_tx_id("ns", "payload", 42, &[]);
        let b = compute_tx_id("ns", "payload", 42, &[]);
        let c = compute_tx_id("ns", "payload!", 42, &[]);
        assert_eq!(a, b);
        assert_ne!(a, c);
        // 필드 경계 조작이 다른 ID를 만드는지 (구분자 검증)
        assert_ne!(compute_tx_id("ab", "c", 1, &[]), compute_tx_id("a", "bc", 1, &[]));
        // 태그도 ID에 반영
        let tagged = compute_tx_id("ns", "payload", 42, &[("k".into(), "v".into())]);
        assert_ne!(a, tagged);
    }

    #[test]
    fn 태그_키는_구분자_조작으로_충돌하지_않는다() {
        // ("a","b=c") 와 ("a=b","c") 는 서로 다른 sled 키여야 한다
        assert_ne!(tag_key("a", "b=c"), tag_key("a=b", "c"));
    }

    #[tokio::test]
    async fn 같은_태그의_동시_제출은_원자적으로_멱등_처리된다() {
        let (chain, dir) = test_chain();
        let tag = vec![("hc.story".to_string(), "s-42".to_string())];
        // 내용이 달라도 같은 태그면 최초 제출이 승자
        let (id1, o1) = chain
            .submit_tx("ns".into(), "내용 A".into(), None, tag.clone())
            .await
            .unwrap();
        assert!(matches!(o1, SubmitOutcome::Queued));
        let (id2, o2) = chain
            .submit_tx("ns".into(), "내용 B".into(), None, tag.clone())
            .await
            .unwrap();
        assert!(matches!(o2, SubmitOutcome::AlreadyPending));
        assert_eq!(id1, id2, "두 번째 제출은 기존 승자 ID 를 돌려받는다");
        // 커밋 후에도 동일 — 커밋된 승자로 dedup
        chain.seal_pending_block().await.unwrap().unwrap();
        let (id3, o3) = chain
            .submit_tx("ns".into(), "내용 C".into(), None, tag)
            .await
            .unwrap();
        assert!(matches!(o3, SubmitOutcome::AlreadyCommitted { height: 1 }));
        assert_eq!(id1, id3);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn 머클_루트는_홀수_개수와_순서를_처리한다() {
        let r1 = merkle_root(&["a", "b", "c"]);
        let r2 = merkle_root(&["a", "b", "c"]);
        let r3 = merkle_root(&["a", "c", "b"]);
        assert_eq!(r1, r2);
        assert_ne!(r1, r3);
        assert_ne!(merkle_root(&[]), merkle_root(&["a"]));
    }

    #[test]
    fn 블록_무결성_검증은_변조를_잡아낸다() {
        let tx = Transaction::new("t".into(), "data".into(), None, Vec::new()).unwrap();
        let mut block = Block::new(1, "0".repeat(64), vec![tx]).unwrap();
        assert!(block.verify_integrity().is_ok());
        // 페이로드 변조 → 트랜잭션 ID 불일치로 검출
        block.transactions[0].payload = "tampered".into();
        assert!(block.verify_integrity().is_err());
    }

    #[test]
    fn 제네시스는_결정적이다() {
        assert_eq!(Block::genesis().unwrap().hash, Block::genesis().unwrap().hash);
    }

    #[test]
    fn 스토어는_커밋과_재시작_복원을_지원한다() {
        let (store, path) = temp_store();
        let (state, resumed) = store.init_or_resume().unwrap();
        assert!(!resumed);
        assert_eq!(state.height, 0);

        // 태그 달린 블록 하나 커밋
        let tx = Transaction::new(
            "ns".into(),
            "hello".into(),
            None,
            vec![("story".into(), "s1".into())],
        )
        .unwrap();
        let block = Block::new(1, state.tip_hash.clone(), vec![tx.clone()]).unwrap();
        let new_state =
            BlockchainState { height: 1, tip_hash: block.hash.clone(), total_txs: 1 };
        store.commit_block(&block, &new_state).unwrap();
        assert_eq!(store.tx_height(&tx.id).unwrap(), Some(1));
        assert_eq!(store.tag_lookup("story", "s1").unwrap(), Some(tx.id.clone()));
        let ns = store.namespace_info("ns").unwrap().unwrap();
        assert_eq!(ns.count, 1);
        assert_eq!(ns.last_height, 1);

        // 같은 태그로 두 번째 커밋 → first-wins (기존 승자 유지)
        let tx2 = Transaction::new(
            "ns".into(),
            "world".into(),
            None,
            vec![("story".into(), "s1".into())],
        )
        .unwrap();
        let block2 = Block::new(2, block.hash.clone(), vec![tx2]).unwrap();
        store
            .commit_block(
                &block2,
                &BlockchainState { height: 2, tip_hash: block2.hash.clone(), total_txs: 2 },
            )
            .unwrap();
        assert_eq!(store.tag_lookup("story", "s1").unwrap(), Some(tx.id.clone()));

        // 재시작 시뮬레이션: 같은 경로로 다시 열어 상태 복원
        drop(store);
        let store2 = Store::open(&path).unwrap();
        let (restored, resumed2) = store2.init_or_resume().unwrap();
        assert!(resumed2);
        assert_eq!(restored.height, 2);
        assert!(store2.verify_chain(0, 2).unwrap().ok);
        drop(store2);
        let _ = std::fs::remove_dir_all(&path);
    }

    #[test]
    fn 체인_검증은_연결_고리_훼손을_잡아낸다() {
        let (store, path) = temp_store();
        let (state, _) = store.init_or_resume().unwrap();
        // 정상 블록 커밋
        let tx = Transaction::new("ns".into(), "a".into(), None, Vec::new()).unwrap();
        let b1 = Block::new(1, state.tip_hash, vec![tx]).unwrap();
        store
            .commit_block(
                &b1,
                &BlockchainState { height: 1, tip_hash: b1.hash.clone(), total_txs: 1 },
            )
            .unwrap();
        // 잘못된 prev_hash 로 연결된 블록 커밋 (검증이 잡아내야 함)
        let tx2 = Transaction::new("ns".into(), "b".into(), None, Vec::new()).unwrap();
        let bad = Block::new(2, "deadbeef".repeat(8), vec![tx2]).unwrap();
        store
            .commit_block(
                &bad,
                &BlockchainState { height: 2, tip_hash: bad.hash.clone(), total_txs: 2 },
            )
            .unwrap();
        let report = store.verify_chain(0, 2).unwrap();
        assert!(!report.ok);
        assert!(report.error.unwrap().contains("연결 불일치"));
        drop(store);
        let _ = std::fs::remove_dir_all(&path);
    }

    #[tokio::test]
    async fn 체인은_제출부터_커밋까지_동작한다() {
        let (chain, dir) = test_chain();

        let (id, outcome) = chain
            .submit_tx("mantis-cad".into(), "file_hash:abc".into(), None, Vec::new())
            .await
            .unwrap();
        assert!(matches!(outcome, SubmitOutcome::Queued));
        // 같은 내용 재제출 → 멱등 처리 (같은 밀리초면 AlreadyPending, 아니면 Queued)
        let (_, outcome2) = chain
            .submit_tx("mantis-cad".into(), "file_hash:abc".into(), None, Vec::new())
            .await
            .unwrap();
        assert!(matches!(outcome2, SubmitOutcome::AlreadyPending | SubmitOutcome::Queued));

        let sealed = chain.seal_pending_block().await.unwrap().unwrap();
        assert_eq!(sealed.header.index, 1);
        assert_eq!(chain.store.tx_height(&id).unwrap(), Some(1));
        assert!(chain.store.verify_chain(0, 1).unwrap().ok);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
