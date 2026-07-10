# tomeiChain

저사양 홈서버에서 데몬으로 상주하는 **Pure Rust 초경량 블록체인**.
외부 DB 없이 임베디드 [`sled`](https://github.com/spacejam/sled) 하나로 블록과 상태를 영속화하고,
PoW/PoS/P2P 없이 **단일 시퀀서(PoA)** 구조로 CPU·메모리 오버헤드를 최소화한다.

[HeartCritiqueEAS](../HeartCritiqueEAS)(박제 아카이브)와 [mantis-cad](../mantis-cad)(파라메트릭 CAD)가
바로 사용할 수 있도록, 두 프로젝트가 이미 기대하는 HTTP 계약을 **드롭인**으로 구현했다.

```
                        ┌──────────────────────────────────────────┐
 HeartCritiqueEAS ────▶ │  POST /upload   GET /bundle/{txId}       │
 (UPLOADER_URL 변경만)  │  GET  /wallet   GET /health              │
                        │            tomei-chain 데몬               │
 mantis-cad 앱 ───────▶ │  GET  /api/info                          │
 (포트 7878 그대로)      │  GET  /api/blocks?from=N                 │
                        │  POST /api/blocks  (fast-forward, 409)   │
                        │                                          │
 범용 클라이언트 ──────▶ │  POST /tx  GET /tx/{id} /block /ns       │
                        │  GET  /status  GET /verify               │
                        └───────────────┬──────────────────────────┘
                                        ▼
                          sled (blocks / state / mantis_blocks)
```

## 구조

| 파일 | 역할 |
|---|---|
| `src/main.rs` | 코어 전체 — 데이터 모델, sled 스토리지, 멤풀, 블록 생산 루프, HTTP 서버, 데모, 테스트 |
| `src/mantis.rs` | mantis-server HTTP 동기화 계약 드롭인 (sha256 시그너블 재구성 + ed25519 검증) |
| `src/heartcritique.rs` | Irys uploader 사이드카 계약 드롭인 (Story-Id 멱등 박제 + 번들 재서빙) |

핵심 설계:

- **트랜잭션** = `{id, namespace, payload, timestamp, tags, signature?}` — 토큰이 아니라 임의
  데이터(파일 해시, 서명 번들, 메타데이터)의 공증(anchoring)이 목적. `id`는 내용 기반
  blake3 해시라 같은 내용의 재제출이 자연스럽게 멱등 처리된다.
- **블록** = `{header{index, prev_block_hash, merkle_root, timestamp}, transactions, hash}` —
  헤더의 bincode 바이트를 blake3 로 해싱. 머클 루트가 트랜잭션 내용까지 커버.
- **월드 스테이트** (sled `state` 트리): 트랜잭션 위치 인덱스(`tx:`), 앱별 최신 공증
  상태(`ns:`), first-wins 멱등 태그 인덱스(`tag:`).
- **블록 생산**: 2초 주기(또는 배치 임계치 도달 시 즉시)로 멤풀을 드레인해
  blocks+state 트리에 **원자적 sled 트랜잭션**으로 커밋 후 fsync. 재시작하면 마지막
  블록에서 그대로 이어간다.

## 빌드 & 실행

```bash
cargo build --release            # 호스트에 C 툴체인(build-essential)이 있을 때
./build.sh                       # 없을 때 — rust:1 도커 이미지로 빌드 (이 서버의 현재 방식)
./build.sh test                  # 테스트

./target/release/tomei-chain           # 데몬 모드 (Ctrl-C 로 우아한 종료)
./target/release/tomei-chain --demo    # 가상 트랜잭션 → 블록 생성 → 영속화 데모 후 종료
```

환경변수:

| 변수 | 기본값 | 의미 |
|---|---|---|
| `TOMEI_DATA_DIR` | `./tomei-data` | sled 데이터 디렉토리 |
| `TOMEI_ADDR` | `127.0.0.1:8642` | 메인 HTTP API 주소 |
| `TOMEI_MANTIS_ADDR` | `127.0.0.1:7878` | mantis-cad 기본 포트 겸용 리스너 (`off`로 비활성화) |
| `TOMEI_PUBLIC_URL` | `http://{TOMEI_ADDR}` | `/upload` 응답의 번들 조회 URL 베이스 |
| `TOMEI_BLOCK_INTERVAL_MS` | `2000` | 블록 생산 주기 |
| `TOMEI_BATCH` | `100` | 이 개수 이상 쌓이면 즉시 블록 생산 |

## HTTP API

### 코어 (범용 공증)

| 엔드포인트 | 설명 |
|---|---|
| `POST /tx` | `{namespace, payload, signature?, tags?}` 제출 → `{id, status}` |
| `GET /tx/{id}` | 확정 여부 + 블록 위치 |
| `GET /block/latest` · `GET /block/{h}` | 블록 조회 |
| `GET /ns/{namespace}` | 앱별 최신 공증 상태 (개수/마지막 tx/높이) |
| `GET /status` | 체인 높이·팁·총 트랜잭션·멤풀·mantis 헤드 |
| `GET /verify` | 전체 체인 무결성 전수 검증 |
| `GET /health` | `{ok, network}` |

`tags`는 `[["키","값"], …]` 형태로, **최초 기록이 승자(first-wins)**가 되는 월드 스테이트
인덱스다. 멱등 제출(예: Story-Id 이중 박제 방지)에 쓰인다.

### HeartCritiqueEAS 연동 (uploader 사이드카 대체)

tomei-chain 이 `uploader/index.js` 의 계약을 그대로 구현하므로 **Python 코어 수정 없이**
환경변수 하나로 전환된다:

```bash
# HeartCritiqueEAS 의 .env
UPLOADER_URL=http://127.0.0.1:8642     # (도커 컴포즈면 host.docker.internal 또는 서비스 주소)
```

- `POST /upload` — `{payload, signature, publicKey, algorithm}` 서명 번들을 **수신 바이트
  그대로** 체인에 박제. `payload.story.id`를 태그로 인덱싱해 이중 박제를 방지(`deduped:true`).
  블록 커밋 확인(≤15초) 후 `{txId, arweaveUrl, network:"tomei-local", permanent:true}` 반환.
- `GET /bundle/{txId}` — 저장된 번들 원본 반환. 서버측 `verify_story_archive` 와 브라우저측
  검증이 이 URL 을 재조회해 ECDSA 서명을 재검증한다 (실제 `verify_bundle` 통과 확인됨).
- `GET /wallet` — 로컬 체인은 수수료 0. uploader 응답 형태 유지.

HeartCritiqueEAS 쪽에서 한 가지만 추가하면 검증 경로까지 완성된다:
`services/transparency.py` 의 `_ALLOWED_GATEWAY_SUFFIXES` 와 `static/index.html` 의 게이트웨이
목록에 tomei-chain 호스트(예: `127.0.0.1:8642`)를 추가할 것 (SSRF 허용 목록).

### mantis-cad 연동 (mantis-server 대체)

mantis-cad 앱의 기본 서버 주소(`http://localhost:7878`)를 tomei-chain 이 겸용 리스너로
받아주므로 **앱 수정도, 설정 변경도 필요 없다**. 기존 `mantis-server` 를 끄고 tomei-chain 을
켜면 된다.

- `GET /api/info` → `{"len":N,"head":"<hex>"}`
- `GET /api/blocks?from=N` → 저장된 블록 **원본 바이트 그대로**의 JSON 배열
- `POST /api/blocks` → fast-forward 병합. 이미 아는 `(index,hash)` 는 건너뛰고(멱등),
  새 블록은 인덱스 연속성·`prev_hash` 연결·**sha256 해시 재계산**·**ed25519 서명**을 검증.
  포크/검증 실패는 `409 + {"len","head"}` (클라이언트가 pull 후 1회 재시도하는 규약 그대로).

바이트 호환의 핵심: 블록의 `ops` 를 `serde_json::RawValue` 로 받아 수신 바이트를 보존한
채 mantis-chain 의 `Signable` 필드 순서로 재구성해 해시를 재계산한다. 제네시스 해시가
mantis-chain 정식 값(`6647ae8b…`)과 일치함을 테스트로 고정했고, 실제 `mantis-cli demo`
체인을 병합→재서빙→`mantis-cli verify` 통과까지 확인했다.

의도적 차이 한 가지: mantis-server 는 병합 시 op-log 전체를 Graph 로 리플레이해 의미
검증까지 하지만, tomei-chain 은 mantis-graph 에 의존하지 않으므로 구조 검증(연결성·해시·
서명)까지만 한다. PoA 전제(신뢰된 소수의 클라이언트) 하에서 클라이언트가 리플레이를
통과시킨 블록만 푸시하므로 실용상 충분하다.

수락된 mantis 블록은 코어 체인에도 `{"type":"mantis_block_anchor", …}` 트랜잭션으로
앵커링되어 감사 추적이 남는다.

## 검증 이력

- 유닛테스트 15개 (머클/해시 결정성, 변조 검출, 재시작 복원, first-wins 태그와
  키 충돌 방지, 태그 멱등의 원자성, mantis 제네시스 바이트 호환, fast-forward/
  포크 거부, GraphOp 미러 검증(체인 포이즈닝 차단), 박제 멱등)
- `--demo` 실행 → 재실행으로 sled 복원 확인, SIGINT 우아한 종료 확인
- 실제 `mantis-cli demo` 서명 체인 병합 → 바이트 동일 재서빙 → `mantis-cli verify` OK,
  포크/갭/비정상 op 블록 409 거부
- HeartCritiqueEAS 컨테이너의 실제 `sign_dataset` 번들 박제 → 재조회 → `verify_bundle` valid,
  Story-Id dedupe, null payload 400
- HTTP 견고성: 커넥션 상한(256)+타임아웃(60s), chunked 411 거부, 32 MiB 초과 413,
  `Expect: 100-continue` 즉시 승인(대용량 curl 1초 지연 제거), 경로 퍼센트 디코딩
- 4차원(동시성/영속성/프로토콜 호환/HTTP 견고성) 멀티에이전트 코드 리뷰에서 나온
  20건을 트리아지해 실제 이슈 전부 수정 (커밋 확인 실패 시 500, ack 전 fsync,
  종료 시 잔여 멤풀 전량 커밋, 밀봉 중 조회 가시성 창 등)
