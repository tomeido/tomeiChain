//! ============================================================================
//! mantis-cad 어댑터 — mantis-server 의 HTTP 동기화 계약 드롭인 구현
//! ============================================================================
//!
//! mantis-cad 앱(egui, native/wasm)은 `mantis-server` 와 이 세 엔드포인트로
//! op-log 블록체인을 동기화한다 (crates/mantis-app/src/sync.rs):
//!
//!   GET  /api/info          → {"len":N,"head":"<hex>"}
//!   GET  /api/blocks?from=N → 블록 JSON 배열 (index N..끝)
//!   POST /api/blocks        → fast-forward 병합. 성공 200 {"len":N,"appended":K},
//!                             포크/검증 실패 409 {"len":N,"head":"<hex>"}
//!
//! 와이어 포맷 (mantis-chain 의 고정 포맷, crates/mantis-chain/src/lib.rs):
//!   Block = {index, prev_hash, timestamp_ms, author, author_pk, message,
//!            ops, hash, sig}
//!   hash  = 소문자 hex sha256( serde_json(Signable) )   ← 필드 선언 순서 고정
//!   sig   = hash 의 원시 32바이트에 대한 ed25519 서명 (hex)
//!
//! 핵심 트릭: `ops` 를 `serde_json::value::RawValue` 로 받아 **수신 바이트를
//! 그대로 보존**한다. Signable 재구성 시 ops 가 원문 그대로 직렬화되므로,
//! 클라이언트(serde_json 컴팩트 출력)가 계산한 해시를 바이트 단위로 동일하게
//! 재계산·검증할 수 있다. 블록 원본도 sled 에 바이트 그대로 저장되어
//! GET /api/blocks 응답이 클라이언트 서명과 항상 일치한다.
//!
//! 의도적 차이 (문서화된 트레이드오프): mantis-server 는 병합 시 전체 op-log 를
//! Graph 로 리플레이해 의미 검증(BadOps)까지 수행하지만, tomei-chain 은
//! mantis-graph 에 의존하지 않으므로 구조 검증(인덱스 연속성, prev_hash 연결,
//! 해시 재계산, ed25519 서명)까지만 수행한다. PoA 전제(신뢰된 소수의 제출자)
//! 하에서 클라이언트가 이미 리플레이를 통과시킨 블록만 푸시하므로 충분하다.

use crate::{json_body, Chain, ChainError, Result, Store};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use sha2::{Digest, Sha256};

/// mantis-chain 의 정식 제네시스 해시 (lib.rs 테스트에 고정된 값).
/// 이 값이 재현되지 않으면 mantis-cad 앱이 체인을 Diverged 로 거부한다.
const GENESIS_HASH: &str = "6647ae8b4509faf6518cdfc11e2f778c856e3c0fe82a557e745f675a7cab0bee";
const GENESIS_PREV_HASH: &str =
    "0000000000000000000000000000000000000000000000000000000000000000";

// ---------------------------------------------------------------------------
// 블록 파싱/해싱 — mantis-chain 과 바이트 호환
// ---------------------------------------------------------------------------

/// mantis-chain `Block` 과 동일한 필드/순서. `ops` 는 원문 보존을 위해 RawValue.
#[derive(Serialize, Deserialize)]
struct MantisBlock {
    index: u64,
    prev_hash: String,
    timestamp_ms: u64,
    author: String,
    author_pk: String,
    message: String,
    ops: Vec<Box<RawValue>>,
    hash: String,
    sig: String,
}

/// 해시가 커버하는 정확한 바이트 배치 — mantis-chain `Signable` 과 동일한
/// 필드 선언 순서 (serde_json 은 선언 순서대로 출력한다).
#[derive(Serialize)]
struct Signable<'a> {
    index: u64,
    prev_hash: &'a str,
    timestamp_ms: u64,
    author: &'a str,
    author_pk: &'a str,
    message: &'a str,
    ops: &'a [Box<RawValue>],
}

impl MantisBlock {
    /// 소문자 hex sha256(serde_json(Signable)) — mantis-chain 과 동일
    fn compute_hash(&self) -> String {
        let signable = Signable {
            index: self.index,
            prev_hash: &self.prev_hash,
            timestamp_ms: self.timestamp_ms,
            author: &self.author,
            author_pk: &self.author_pk,
            message: &self.message,
            ops: &self.ops,
        };
        let json = serde_json::to_string(&signable).unwrap_or_default();
        let mut hasher = Sha256::new();
        hasher.update(json.as_bytes());
        hex_encode(&hasher.finalize())
    }

    /// ed25519 서명 검증: author_pk(32B) 로 hash 원시 바이트에 대한 sig(64B) 확인
    fn verify_sig(&self) -> bool {
        use ed25519_dalek::{Signature, Verifier, VerifyingKey};
        let Some(pk_bytes) = hex_decode(&self.author_pk) else { return false };
        let Ok(pk_arr) = <[u8; 32]>::try_from(pk_bytes.as_slice()) else { return false };
        let Ok(vk) = VerifyingKey::from_bytes(&pk_arr) else { return false };
        let Some(sig_bytes) = hex_decode(&self.sig) else { return false };
        let Ok(sig) = Signature::from_slice(&sig_bytes) else { return false };
        let Some(raw_hash) = hex_decode(&self.hash) else { return false };
        vk.verify(&raw_hash, &sig).is_ok()
    }
}

// ---------------------------------------------------------------------------
// GraphOp 미러 검증 — 체인 포이즈닝 방지
// ---------------------------------------------------------------------------
//
// 서버가 구조만 검증하고 ops 내용을 통과시키면, GraphOp 로 파싱되지 않는 op 를
// 담은 블록 하나가 이후 모든 클라이언트의 GET /api/blocks 역직렬화를 영구히
// 깨뜨린다(체인 포이즈닝). mantis-graph 에 의존하지 않고 이를 막기 위해
// GraphOp 의 와이어 포맷을 **미러 타입**으로 재현한다: 각 op 를 미러로 파싱한 뒤
// 다시 직렬화해 원문 바이트와 비교하면 (1) 파싱 가능성과 (2) 캐노니컬 인코딩
// (serde_json 컴팩트 출력)이 동시에 보장된다.

/// mantis-graph `GraphOp` 의 와이어 포맷 미러 (graph.rs:76-104 과 동일 구조)
#[derive(Serialize, Deserialize)]
#[serde(tag = "op")]
enum MirrorOp {
    AddNode { id: String, type_name: String, pos: (f32, f32) },
    RemoveNode { id: String },
    Connect { from: (String, u16), to: (String, u16) },
    Disconnect { from: (String, u16), to: (String, u16) },
    SetParam { id: String, key: String, value: MirrorParam },
    MoveNode { id: String, pos: (f32, f32) },
}

/// mantis-graph `ParamValue` 의 와이어 포맷 미러 (외부 태그드)
#[derive(Serialize, Deserialize)]
enum MirrorParam {
    Number(f64),
    Text(String),
    Bool(bool),
}

/// NodeId 와이어 포맷: 32자 소문자 hex (mantis-graph NodeId 직렬화 규칙)
fn valid_node_id(id: &str) -> bool {
    id.len() == 32 && id.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

impl MirrorOp {
    fn node_ids(&self) -> Vec<&str> {
        match self {
            MirrorOp::AddNode { id, .. }
            | MirrorOp::RemoveNode { id }
            | MirrorOp::SetParam { id, .. }
            | MirrorOp::MoveNode { id, .. } => vec![id],
            MirrorOp::Connect { from, to } | MirrorOp::Disconnect { from, to } => {
                vec![&from.0, &to.0]
            }
        }
    }
}

/// op 원문이 GraphOp 로 파싱 가능하고 캐노니컬 인코딩인지 검증한다.
fn validate_op(raw: &RawValue) -> bool {
    let Ok(op) = serde_json::from_str::<MirrorOp>(raw.get()) else {
        return false;
    };
    if !op.node_ids().into_iter().all(valid_node_id) {
        return false;
    }
    // 재직렬화가 원문과 바이트 동일해야 캐노니컬 (비유한 float 는 여기서 실패)
    match serde_json::to_string(&op) {
        Ok(canon) => canon == raw.get(),
        Err(_) => false,
    }
}

/// mantis-chain 과 동일한 정식 제네시스 블록을 구성한다.
fn genesis_block() -> MantisBlock {
    let mut b = MantisBlock {
        index: 0,
        prev_hash: GENESIS_PREV_HASH.to_string(),
        timestamp_ms: 0,
        author: "genesis".to_string(),
        author_pk: String::new(),
        message: "MantisCAD genesis".to_string(),
        ops: Vec::new(),
        hash: String::new(),
        sig: String::new(),
    };
    b.hash = b.compute_hash();
    b
}

// ---------------------------------------------------------------------------
// hex 헬퍼 (의존성 없는 로컬 구현 — mantis-chain 과 동일 규칙)
// ---------------------------------------------------------------------------

fn hex_encode(bytes: &[u8]) -> String {
    const TABLE: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(TABLE[(b >> 4) as usize] as char);
        out.push(TABLE[(b & 0x0f) as usize] as char);
    }
    out
}

fn hex_decode(s: &str) -> Option<Vec<u8>> {
    fn nibble(c: u8) -> Option<u8> {
        match c {
            b'0'..=b'9' => Some(c - b'0'),
            b'a'..=b'f' => Some(c - b'a' + 10),
            b'A'..=b'F' => Some(c - b'A' + 10),
            _ => None,
        }
    }
    let bytes = s.as_bytes();
    if bytes.len() % 2 != 0 {
        return None;
    }
    let mut out = Vec::with_capacity(bytes.len() / 2);
    for pair in bytes.chunks_exact(2) {
        out.push((nibble(pair[0])? << 4) | nibble(pair[1])?);
    }
    Some(out)
}

// ---------------------------------------------------------------------------
// 초기화 — 제네시스 생성 또는 기존 헤드 복원
// ---------------------------------------------------------------------------

/// 부팅 시 mantis 체인 헤드를 복원한다. 최초 구동이면 정식 제네시스를 만들어
/// 커밋한다. 반환값: (블록 수 len, 헤드 해시)
pub(crate) fn init(store: &Store) -> Result<(u64, String)> {
    if let Some((len, head)) = store.mantis_head_meta()? {
        // 헤드가 가리키는 블록이 실제로 존재하는지 확인
        if len == 0 || store.mantis_get_raw(len - 1)?.is_none() {
            return Err(ChainError::Integrity(format!(
                "mantis 헤드는 {len}개 블록을 가리키지만 마지막 블록이 없습니다"
            )));
        }
        return Ok((len, head));
    }
    let genesis = genesis_block();
    if genesis.hash != GENESIS_HASH {
        // 이 경고가 보이면 Signable 재구성이 mantis-chain 과 어긋난 것이다.
        crate::log_line(&format!(
            "경고: mantis 제네시스 해시 불일치! 계산값 {} ≠ 기대값 {GENESIS_HASH}",
            genesis.hash
        ));
    }
    let raw = serde_json::to_string(&genesis)
        .map_err(|e| ChainError::Integrity(format!("제네시스 직렬화 실패: {e}")))?;
    store.mantis_commit(&[(0, raw)], 1, &genesis.hash)?;
    Ok((1, genesis.hash))
}

// ---------------------------------------------------------------------------
// HTTP 핸들러
// ---------------------------------------------------------------------------

/// GET /api/info → {"len":N,"head":"<hex>"}
pub(crate) async fn info(chain: &Chain) -> (u16, String) {
    let head = chain.mantis_head.lock().await.clone();
    (200, json_body(&serde_json::json!({ "len": head.0, "head": head.1 })))
}

/// `/api/blocks?from=N` 쿼리 파싱 — mantis-server 와 동일하게 불량/누락 → 0
fn parse_from(query: Option<&str>) -> u64 {
    let Some(q) = query else { return 0 };
    for pair in q.split('&') {
        if let Some(v) = pair.strip_prefix("from=") {
            return v.parse::<u64>().unwrap_or(0);
        }
    }
    0
}

/// GET /api/blocks?from=N → 저장된 원본 바이트를 이어붙인 JSON 배열
pub(crate) async fn get_blocks(chain: &Chain, query: Option<&str>) -> (u16, String) {
    let len = chain.mantis_head.lock().await.0;
    let from = parse_from(query).min(len);
    let mut parts: Vec<String> = Vec::with_capacity((len - from) as usize);
    for index in from..len {
        match chain.store.mantis_get_raw(index) {
            Ok(Some(raw)) => parts.push(String::from_utf8_lossy(&raw).to_string()),
            Ok(None) => {
                return (
                    500,
                    json_body(&serde_json::json!({
                        "error": format!("블록 #{index} 이 저장소에 없습니다")
                    })),
                )
            }
            Err(e) => return (500, json_body(&serde_json::json!({ "error": e.to_string() }))),
        }
    }
    (200, format!("[{}]", parts.join(",")))
}

/// POST /api/blocks → fast-forward 병합 (mantis-chain `try_extend` 의 의미론)
///
/// - 이미 아는 (index, hash) 블록은 건너뜀 (재푸시 멱등)
/// - 새 블록은 인덱스 연속성 / prev_hash 연결 / 해시 재계산 / ed25519 서명 검증
/// - 포크(같은 index 다른 hash)나 검증 실패 → 409 + 현재 헤드 정보
/// - 전부-아니면-전무: 배치 전체가 통과해야 커밋
pub(crate) async fn post_blocks(chain: &Chain, body: &[u8]) -> (u16, String) {
    // 원본 바이트 보존을 위해 각 블록을 RawValue 로 먼저 쪼갠다
    let raws: Vec<Box<RawValue>> = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(e) => {
            return (
                400,
                json_body(&serde_json::json!({ "error": format!("bad block JSON: {e}") })),
            )
        }
    };

    // 헤드 락을 잡아 mantis 푸시를 직렬화한다 (단일 시퀀서)
    let mut head = chain.mantis_head.lock().await;
    let (cur_len, cur_head_hash) = head.clone();
    let conflict = |head: &(u64, String)| {
        (409, json_body(&serde_json::json!({ "len": head.0, "head": head.1 })))
    };

    // (index, hash, author, 원본 JSON) — 커밋 대기 목록
    let mut appended: Vec<(u64, String, String, String)> = Vec::new();
    for raw in &raws {
        let block: MantisBlock = match serde_json::from_str(raw.get()) {
            Ok(b) => b,
            Err(e) => {
                return (
                    400,
                    json_body(&serde_json::json!({ "error": format!("bad block JSON: {e}") })),
                )
            }
        };
        let head_index = cur_len - 1 + appended.len() as u64;
        let head_hash =
            appended.last().map(|(_, h, _, _)| h.clone()).unwrap_or_else(|| cur_head_hash.clone());

        if block.index <= head_index {
            // 이미 아는 영역: (index, hash) 가 정확히 일치해야 한다 — 다르면 포크
            let known_hash = if block.index >= cur_len {
                // 이번 배치에서 방금 추가된 블록
                appended
                    .iter()
                    .find(|(i, _, _, _)| *i == block.index)
                    .map(|(_, h, _, _)| h.clone())
            } else {
                match chain.store.mantis_get_raw(block.index) {
                    Ok(Some(stored)) => serde_json::from_slice::<MantisBlock>(&stored)
                        .ok()
                        .map(|b| b.hash),
                    _ => None,
                }
            };
            match known_hash {
                Some(h) if h == block.hash => continue, // 동일 블록 재푸시 → 건너뜀
                _ => return conflict(&head),
            }
        }
        // 새 블록: 정확히 head+1 로 이어지고, 해시·서명·op 캐노니컬 검증 통과
        if block.index != head_index + 1
            || block.prev_hash != head_hash
            || block.hash != block.compute_hash()
            || !block.verify_sig()
            || !block.ops.iter().all(|op| validate_op(op))
        {
            return conflict(&head);
        }
        appended.push((block.index, block.hash.clone(), block.author.clone(), raw.get().to_string()));
    }

    let final_len;
    if !appended.is_empty() {
        let new_len = cur_len + appended.len() as u64;
        let new_head_hash = appended.last().map(|(_, h, _, _)| h.clone()).unwrap_or_default();
        let to_store: Vec<(u64, String)> =
            appended.iter().map(|(i, _, _, raw)| (*i, raw.clone())).collect();
        if let Err(e) = chain.store.mantis_commit(&to_store, new_len, &new_head_hash) {
            return (500, json_body(&serde_json::json!({ "error": e.to_string() })));
        }
        *head = (new_len, new_head_hash.clone());
        final_len = new_len;
        // 커밋 반영 후 즉시 락 해제 — flush/앵커 기록 동안 /api/info 를 막지 않는다
        drop(head);

        if let Err(e) = chain.store.flush().await {
            crate::log_line(&format!("경고: mantis 커밋 flush 실패 (커밋은 유효): {e}"));
        }
        // tomei 코어 체인에 감사(audit) 앵커 기록 — 실패해도 병합은 유효
        for (index, hash, author, _) in &appended {
            let anchor = serde_json::json!({
                "type": "mantis_block_anchor",
                "index": index,
                "hash": hash,
                "author": author,
            })
            .to_string();
            if let Err(e) =
                chain.submit_tx("mantis-cad".to_string(), anchor, None, Vec::new()).await
            {
                crate::log_line(&format!("mantis 앵커 기록 실패(무시): {e}"));
            }
        }
        crate::log_line(&format!(
            "mantis 블록 {}개 병합 — 총 {new_len}개, 헤드 {}…",
            appended.len(),
            &new_head_hash[..16]
        ));
    } else {
        final_len = cur_len;
    }

    (
        200,
        json_body(&serde_json::json!({ "len": final_len, "appended": appended.len() })),
    )
}

// ---------------------------------------------------------------------------
// 테스트
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::{Signer, SigningKey};

    /// 정식 제네시스 해시가 mantis-chain 과 바이트 단위로 일치하는지 —
    /// 이 테스트가 통과해야 mantis-cad 앱이 tomei-chain 을 자기 서버로 받아들인다.
    #[test]
    fn 제네시스_해시가_mantis_chain_과_일치한다() {
        assert_eq!(genesis_block().hash, GENESIS_HASH);
    }

    /// 테스트용 서명 블록 생성기 — mantis-chain 의 append 와 동일한 절차
    fn signed_block(
        key: &SigningKey,
        index: u64,
        prev_hash: &str,
        ops_json: &[&str],
    ) -> String {
        let ops: Vec<Box<RawValue>> = ops_json
            .iter()
            .map(|s| RawValue::from_string(s.to_string()).unwrap())
            .collect();
        let mut block = MantisBlock {
            index,
            prev_hash: prev_hash.to_string(),
            timestamp_ms: 1_751_871_234_567,
            author: "tester".to_string(),
            author_pk: hex_encode(&key.verifying_key().to_bytes()),
            message: "test block".to_string(),
            ops,
            hash: String::new(),
            sig: String::new(),
        };
        block.hash = block.compute_hash();
        let raw_hash = hex_decode(&block.hash).unwrap();
        block.sig = hex_encode(&key.sign(&raw_hash).to_bytes());
        serde_json::to_string(&block).unwrap()
    }

    const OP_ADD: &str =
        r#"{"op":"AddNode","id":"000102030405060708090a0b0c0d0e0f","type_name":"circle","pos":[120.0,80.0]}"#;

    #[test]
    fn op_검증은_비정상_op_를_거부하고_정상_op_를_통과시킨다() {
        let ok = |s: &str| validate_op(&RawValue::from_string(s.to_string()).unwrap());
        // 정상 op 들 (컴팩트 serde_json 출력 형태)
        assert!(ok(OP_ADD));
        assert!(ok(r#"{"op":"RemoveNode","id":"000102030405060708090a0b0c0d0e0f"}"#));
        assert!(ok(
            r#"{"op":"Connect","from":["000102030405060708090a0b0c0d0e0f",0],"to":["0f0102030405060708090a0b0c0d0e0f",1]}"#
        ));
        assert!(ok(
            r#"{"op":"SetParam","id":"000102030405060708090a0b0c0d0e0f","key":"value","value":{"Number":3.5}}"#
        ));
        // 비정상: 알 수 없는 op 태그 → GraphOp 파싱 불가 → 체인 포이즈닝 차단
        assert!(!ok(r#"{"op":"DropTable","id":"000102030405060708090a0b0c0d0e0f"}"#));
        // 비정상: NodeId 가 32-hex 가 아님
        assert!(!ok(r#"{"op":"RemoveNode","id":"zz"}"#));
        // 비정상: 필수 필드 누락
        assert!(!ok(r#"{"op":"AddNode","id":"000102030405060708090a0b0c0d0e0f"}"#));
        // 비정상: 캐노니컬 인코딩 아님 (공백) — 클라이언트 해시 재계산이 깨질 형태
        assert!(!ok(r#"{"op": "RemoveNode", "id": "000102030405060708090a0b0c0d0e0f"}"#));
    }

    #[tokio::test]
    async fn post_blocks_는_비정상_op_블록을_409_로_거부한다() {
        let (chain, dir) = crate::tests::test_chain();
        let key = SigningKey::from_bytes(&[9u8; 32]);
        // 서명·해시는 유효하지만 op 가 GraphOp 로 파싱 불가능한 블록
        let poison = signed_block(&key, 1, GENESIS_HASH, &[
            r#"{"op":"NotARealOp","id":"000102030405060708090a0b0c0d0e0f"}"#,
        ]);
        let (code, _) = post_blocks(&chain, format!("[{poison}]").as_bytes()).await;
        assert_eq!(code, 409);
        // 체인은 오염되지 않았다
        let (_, info_body) = info(&chain).await;
        let v: serde_json::Value = serde_json::from_str(&info_body).unwrap();
        assert_eq!(v["len"], 1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn 해시_재계산과_서명_검증이_동작한다() {
        let key = SigningKey::from_bytes(&[7u8; 32]);
        let raw = signed_block(&key, 1, GENESIS_HASH, &[OP_ADD]);
        let block: MantisBlock = serde_json::from_str(&raw).unwrap();
        assert_eq!(block.hash, block.compute_hash());
        assert!(block.verify_sig());

        // 변조: ops 를 바꾸면 해시가 달라진다
        let tampered = raw.replace("circle", "square");
        let bad: MantisBlock = serde_json::from_str(&tampered).unwrap();
        assert_ne!(bad.hash, bad.compute_hash());
    }

    #[tokio::test]
    async fn post_blocks_는_fast_forward_와_포크_거부를_수행한다() {
        let (chain, dir) = crate::tests::test_chain();
        let key = SigningKey::from_bytes(&[7u8; 32]);

        // 1) 정상 블록 푸시 → appended 1
        let b1 = signed_block(&key, 1, GENESIS_HASH, &[OP_ADD]);
        let body = format!("[{b1}]");
        let (code, resp) = post_blocks(&chain, body.as_bytes()).await;
        assert_eq!(code, 200, "{resp}");
        let v: serde_json::Value = serde_json::from_str(&resp).unwrap();
        assert_eq!(v["len"], 2);
        assert_eq!(v["appended"], 1);

        // 2) 같은 블록 재푸시 → 멱등 (appended 0)
        let (code, resp) = post_blocks(&chain, body.as_bytes()).await;
        assert_eq!(code, 200);
        let v: serde_json::Value = serde_json::from_str(&resp).unwrap();
        assert_eq!(v["appended"], 0);

        // 3) 포크 블록 (같은 index, 다른 내용) → 409 + 헤드 정보
        let fork = signed_block(&key, 1, GENESIS_HASH, &[
            r#"{"op":"RemoveNode","id":"000102030405060708090a0b0c0d0e0f"}"#,
        ]);
        let (code, resp) = post_blocks(&chain, format!("[{fork}]").as_bytes()).await;
        assert_eq!(code, 409);
        let v: serde_json::Value = serde_json::from_str(&resp).unwrap();
        assert_eq!(v["len"], 2);

        // 4) 서명이 깨진 블록 → 409
        let b1_parsed: MantisBlock = serde_json::from_str(&b1).unwrap();
        let mut b2: MantisBlock = serde_json::from_str(&signed_block(
            &key,
            2,
            &b1_parsed.hash,
            &[OP_ADD],
        ))
        .unwrap();
        b2.sig = "00".repeat(64);
        let bad_body = format!("[{}]", serde_json::to_string(&b2).unwrap());
        let (code, _) = post_blocks(&chain, bad_body.as_bytes()).await;
        assert_eq!(code, 409);

        // 5) GET /api/blocks 는 저장된 원본 바이트를 그대로 돌려준다
        let (code, list) = get_blocks(&chain, Some("from=1")).await;
        assert_eq!(code, 200);
        assert_eq!(list, format!("[{b1}]"));

        // 6) /api/info 는 현재 헤드를 보고한다
        let (_, info_body) = info(&chain).await;
        let v: serde_json::Value = serde_json::from_str(&info_body).unwrap();
        assert_eq!(v["len"], 2);
        assert_eq!(v["head"], b1_parsed.hash);

        let _ = std::fs::remove_dir_all(&dir);
    }
}
