//! ============================================================================
//! HeartCritiqueEAS 어댑터 — Irys uploader 사이드카 계약 드롭인 구현
//! ============================================================================
//!
//! HeartCritiqueEAS(Python/FastAPI)는 서명된 박제 번들을 사이드카 서비스에
//! HTTP 로 넘긴다 (services/archive.py → `POST {UPLOADER_URL}/upload`).
//! tomei-chain 이 그 사이드카를 대체한다: `UPLOADER_URL=http://127.0.0.1:8642`
//! 만 바꾸면 Python 코어 수정 없이 로컬 체인에 박제된다.
//!
//!   POST /upload        { payload, signature, publicKey, algorithm }
//!                       → { txId, arweaveUrl, network, permanent, deduped? }
//!   GET  /bundle/{txId} → 저장된 번들 원본 JSON (검증 경로가 재조회하는 URL)
//!   GET  /wallet        → 비용/잔액 정보 (로컬 체인은 수수료 0)
//!   GET  /health        → { ok, network }  (코어 라우터가 처리)
//!
//! 동작 원리:
//!   - 번들 전체(서명 봉투 포함)를 **수신 바이트 그대로** 트랜잭션 페이로드로
//!     저장한다. `GET /bundle/{txId}` 가 그 바이트를 그대로 돌려주므로
//!     서버/브라우저의 ECDSA 서명 재검증이 항상 통과한다.
//!   - Story-Id 멱등: `payload.story.id` 를 태그(`hc.story`)로 인덱싱하고
//!     first-wins 규칙으로 이중 박제를 방지한다 (uploader 의 GraphQL 멱등
//!     조회 + _inFlight 맵과 같은 역할).
//!   - 업로드는 블록 커밋(≤15초 대기)까지 확인한 뒤 응답한다. Python 쪽
//!     타임아웃(300초)보다 훨씬 짧다.

use crate::{json_body, Chain, SubmitOutcome};
use std::time::Duration;

const NAMESPACE: &str = "heartcritique-eas";
const NETWORK: &str = "tomei-local";
/// Story-Id 멱등 인덱스 태그 키
const STORY_TAG: &str = "hc.story";
/// 블록 커밋 확인 대기 상한
const COMMIT_WAIT: Duration = Duration::from_secs(15);

/// 성공 응답 조립 — uploader 와 동일한 필드 (`txId`, `arweaveUrl`, `network`,
/// `permanent`) + 중복 회피 시 `deduped:true`
fn ok_response(chain: &Chain, tx_id: &str, deduped: bool) -> (u16, String) {
    let mut body = serde_json::json!({
        "txId": tx_id,
        "arweaveUrl": format!("{}/bundle/{}", chain.public_url, tx_id),
        "network": NETWORK,
        "permanent": true,
    });
    if deduped {
        body["deduped"] = serde_json::Value::Bool(true);
    }
    (200, json_body(&body))
}

/// POST /upload — 서명 번들을 체인에 박제
pub(crate) async fn upload(chain: &Chain, body: &[u8]) -> (u16, String) {
    // 1) JSON 파싱 + 필수 필드 확인. uploader(JS)의 `!body?.payload` 와 같은
    //    의미론: 누락·null·falsy 원시값은 400 으로 거부한다.
    let parsed: serde_json::Value = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(e) => {
            return (400, json_body(&serde_json::json!({ "error": format!("잘못된 JSON: {e}") })))
        }
    };
    let payload_ok = match parsed.get("payload") {
        None | Some(serde_json::Value::Null) => false,
        Some(serde_json::Value::Bool(b)) => *b,
        Some(serde_json::Value::Number(n)) => n.as_f64().is_none_or(|f| f != 0.0),
        Some(serde_json::Value::String(s)) => !s.is_empty(),
        Some(_) => true,
    };
    if !payload_ok {
        return (400, json_body(&serde_json::json!({ "error": "payload 필드가 필요합니다" })));
    }

    // 2) Story-Id 추출 (문자열/숫자 모두 수용 — JS 의 String(...) 과 동일 의미)
    let story_id = match &parsed["payload"]["story"]["id"] {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Number(n) => n.to_string(),
        _ => String::new(),
    };

    // 3) 수신 바이트 그대로 페이로드로 저장 (서명 재검증 가능성 보존).
    //    Story-Id 멱등은 submit_tx 가 멤풀 락 아래에서 원자적으로 처리한다
    //    (uploader 의 GraphQL 멱등 조회 + _inFlight 맵에 해당).
    let raw = match String::from_utf8(body.to_vec()) {
        Ok(s) => s,
        Err(_) => {
            return (400, json_body(&serde_json::json!({ "error": "UTF-8 이 아닌 바디" })))
        }
    };
    let tags = if story_id.is_empty() {
        Vec::new()
    } else {
        vec![(STORY_TAG.to_string(), story_id)]
    };
    match chain.submit_tx(NAMESPACE.to_string(), raw, None, tags).await {
        Ok((id, SubmitOutcome::PoolFull)) => (
            503,
            json_body(&serde_json::json!({
                "id": id, "error": "멤풀 포화 — 잠시 후 재시도하세요"
            })),
        ),
        // 같은 Story-Id(또는 동일 내용)가 이미 박제됨 — 기존 tx 재사용
        Ok((id, SubmitOutcome::AlreadyCommitted { height })) => {
            crate::log_line(&format!(
                "중복 박제 회피 — 기존 tx 재사용: {}… (블록 #{height})",
                &id[..16.min(id.len())]
            ));
            ok_response(chain, &id, true)
        }
        Ok((id, outcome)) => {
            let deduped = matches!(outcome, SubmitOutcome::AlreadyPending);
            // 블록 생산 루프를 깨워 즉시 커밋시키고, 확정 + fsync 후에만 성공 응답
            chain.produce_signal.notify_one();
            match chain.wait_for_commit(&id, COMMIT_WAIT).await {
                Some(height) => {
                    // permanent:true 로 응답하기 전에 디스크 내구성까지 보장
                    if let Err(e) = chain.store.flush().await {
                        return (
                            500,
                            json_body(&serde_json::json!({
                                "error": format!("디스크 동기화 실패: {e}")
                            })),
                        );
                    }
                    crate::log_line(&format!(
                        "박제 완료 — tx {}… (블록 #{height})",
                        &id[..16]
                    ));
                    ok_response(chain, &id, deduped)
                }
                // 커밋을 확인하지 못했으면 성공을 주장하지 않는다 — Python 쪽
                // reconcile 이 재시도하고, 멤풀의 태그 클레임이 멱등을 보장한다
                None => (
                    500,
                    json_body(&serde_json::json!({
                        "error": "블록 커밋 확인 시간 초과 — 잠시 후 재시도하세요",
                        "txId": id,
                    })),
                ),
            }
        }
        Err(e) => (400, json_body(&serde_json::json!({ "error": e.to_string() }))),
    }
}

/// GET /bundle/{txId} — 저장된 번들 원본 JSON 을 그대로 반환.
/// HeartCritiqueEAS 의 서버측(verify_story_archive)·브라우저측 검증 경로가
/// 이 URL(arweaveUrl 자리)을 재조회해 ECDSA 서명을 재검증한다.
pub(crate) async fn bundle(chain: &Chain, tx_id: &str) -> (u16, String) {
    // 1) 커밋된 트랜잭션
    match chain.store.tx_height(tx_id) {
        Ok(Some(height)) => {
            if let Ok(Some(block)) = chain.store.get_block(height) {
                if let Some(tx) = block.transactions.iter().find(|t| t.id == tx_id) {
                    return (200, tx.payload.clone());
                }
            }
            (500, json_body(&serde_json::json!({ "error": "인덱스는 있으나 블록 조회 실패" })))
        }
        Ok(None) => {
            // 2) 아직 멤풀 대기 중(큐 또는 밀봉 창)인 트랜잭션
            let pool = chain.mempool.lock().await;
            match pool.find(tx_id) {
                Some(tx) => (200, tx.payload.clone()),
                None => {
                    (404, json_body(&serde_json::json!({ "error": "존재하지 않는 트랜잭션" })))
                }
            }
        }
        Err(e) => (500, json_body(&serde_json::json!({ "error": e.to_string() }))),
    }
}

/// GET /wallet — uploader 의 응답 형태를 유지하되, 로컬 체인이므로 비용은 0.
/// (Python services/wallet.py 는 이 JSON 을 그대로 프런트로 프록시한다)
pub(crate) fn wallet(chain: &Chain) -> (u16, String) {
    (
        200,
        json_body(&serde_json::json!({
            "network": NETWORK,
            "permanent": true,
            "token": "none",
            "donation_address": serde_json::Value::Null,
            "wallet_eth": serde_json::Value::Null,
            "irys_balance": serde_json::Value::Null,
            "irys_balance_atomic": serde_json::Value::Null,
            // 로컬 체인은 수수료가 없어 박제 가능 수가 무제한 — null 은 UI 에서 '—'
            "archives_remaining": serde_json::Value::Null,
            "price_per_1kb": { "bytes": 1024, "atomic": "0", "eth": "0" },
            "price_per_10kb": { "bytes": 10240, "atomic": "0", "eth": "0" },
            "explorer": format!("{}/status", chain.public_url),
        })),
    )
}

// ---------------------------------------------------------------------------
// 테스트
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    /// 백그라운드에서 주기적으로 블록을 밀봉하는 테스트용 시퀀서
    fn spawn_sealer(chain: Arc<crate::Chain>) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                let _ = chain.seal_pending_block().await;
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
    }

    const BUNDLE: &str = r#"{"payload":{"story":{"id":"story-42","category":"critique","body":"본문"},"votes":{"count":3},"archived_at":"2026-07-10T00:00:00Z","version":"heart-critique-archive-v2"},"signature":"deadbeef","publicKey":"04abcd","algorithm":"ECDSA-secp256k1-SHA256"}"#;

    #[tokio::test]
    async fn upload_은_박제_멱등_번들조회를_지원한다() {
        let (chain, dir) = crate::tests::test_chain();
        let sealer = spawn_sealer(Arc::clone(&chain));

        // 1) 최초 업로드 → txId + 조회 URL
        let (code, resp) = upload(&chain, BUNDLE.as_bytes()).await;
        assert_eq!(code, 200, "{resp}");
        let v: serde_json::Value = serde_json::from_str(&resp).unwrap();
        let tx_id = v["txId"].as_str().unwrap().to_string();
        assert!(v["arweaveUrl"].as_str().unwrap().ends_with(&format!("/bundle/{tx_id}")));
        assert_eq!(v["permanent"], true);
        assert!(v.get("deduped").is_none());

        // 2) 같은 Story-Id 재업로드 (내용이 조금 달라도) → 기존 tx 재사용
        let changed = BUNDLE.replace("\"count\":3", "\"count\":4");
        let (code, resp) = upload(&chain, changed.as_bytes()).await;
        assert_eq!(code, 200);
        let v: serde_json::Value = serde_json::from_str(&resp).unwrap();
        assert_eq!(v["txId"].as_str().unwrap(), tx_id);
        assert_eq!(v["deduped"], true);

        // 3) /bundle/{txId} 는 수신 바이트를 그대로 돌려준다 (서명 재검증 가능)
        let (code, stored) = bundle(&chain, &tx_id).await;
        assert_eq!(code, 200);
        assert_eq!(stored, BUNDLE);

        // 4) payload 없는 요청은 uploader 와 같은 400
        let (code, resp) = upload(&chain, br#"{"signature":"x"}"#).await;
        assert_eq!(code, 400);
        assert!(resp.contains("payload"));

        sealer.abort();
        let _ = std::fs::remove_dir_all(&dir);
    }
}
