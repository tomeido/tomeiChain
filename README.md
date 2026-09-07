# tomeiChain

Rust와 내장 DB `sled`로 실행하는 경량 데이터 공증 체인입니다. 누구나 **독립적인 검증·보관 노드(follower)** 를 운영하고, 다른 노드에 검증 가능한 블록을 제공할 수 있습니다. HeartCritiqueEAS의 서명 번들과 mantis-cad의 블록 앵커를 저장합니다.

블록 생산은 현재 **단일 authority(시퀀서)** 가 담당합니다. follower는 고정한 authority 공개키로 Ed25519 서명을 검증하고, 블록 연결·해시·트랜잭션 ID·머클 루트를 검증한 뒤 자기 디스크에 저장합니다. 저장과 조회 경로를 여러 운영자에게 분산하는 구조이며, 다중 검증자 합의·리더 선출·PoS·자동 피어 검색은 구현하지 않았습니다. authority가 멈추면 기존 데이터 조회와 복제는 계속할 수 있지만 새로운 블록은 만들어지지 않습니다.

```text
앱 ── 쓰기 ──▶ authority ── 서명된 블록 ──▶ 운영자 A의 follower
                   │                              │
                   └───────────────▶ 운영자 B      └────▶ 운영자 C
                                        │                    │
                                   독립 저장·검증        독립 저장·검증
```

## 공개 네트워크에 참여하기

Docker Engine + Compose 또는 Docker Desktop, Git이 필요합니다. Windows에서는 Docker Desktop의 Linux 컨테이너를 사용하세요. 이미 실행 중인 노드의 데이터 디렉토리를 공유하지 말고 자신의 저장소를 사용합니다.

```bash
git clone https://github.com/tomeido/tomeiChain.git
cd tomeiChain
cp .env.example .env
chmod 600 .env
docker compose up -d --build
docker compose logs --tail=60 -f node
```

기본 설정은 공개 네트워크를 따르는 읽기 전용 follower입니다. 첫 실행은 Rust 바이너리 빌드와 기존 블록 다운로드에 시간이 걸립니다. HTTP 포트는 기본적으로 자신의 컴퓨터(`127.0.0.1:8642`)에서만 접근할 수 있습니다. 로컬 포트가 사용 중이면 `.env`의 `TOMEI_PORT`와 `TOMEI_PUBLIC_URL`을 바꾸세요.

공개 네트워크의 신뢰 기준:

| 항목 | 값 |
|---|---|
| 최초 피어 | `https://domeido.asuscomm.com/tomei` |
| authority 공개키 | `50b488f64c96f7a2eb212cfdd24369da553eed25b71db162307cc92fa45e102e` |
| 체인 ID | `aed090c182a18553bff094afab7ba0d61ab7b1c689fdd488b52d680677521980` |
| 제네시스 해시 | `9119bc39359e97f82cd26426b8994f21c8dd19bb89daf75242679b34da095050` |

공개키는 **신뢰하는 배포본 또는 운영자에게 별도 경로로 확인한 값**을 사용해야 합니다. 아무 피어의 `/network/info`에서 받은 키를 자동으로 믿으면, 그 피어가 만든 다른 체인에 연결될 수 있습니다. 체인 ID는 authority 공개키와 제네시스로부터 결정되는 값이며 이름처럼 임의로 정하는 문자열이 아닙니다.

이 공개 서비스는 실험용 네트워크입니다. 기존 앱 호환을 위해 현재 공개 authority 쓰기 요청은 인증을 요구하지 않습니다. 민감한 정보는 제출하지 마세요. 운영자가 권한을 분산한 합의 네트워크나 보존을 보증하는 메인넷으로 취급하면 안 됩니다.

### 동기화 확인

```bash
curl -fsS http://127.0.0.1:8642/health
curl -fsS http://127.0.0.1:8642/ready
curl -fsS http://127.0.0.1:8642/network/info
curl -fsS https://domeido.asuscomm.com/tomei/network/info
curl -fsS http://127.0.0.1:8642/verify
```

`/health`는 프로세스가 요청을 받는다는 뜻입니다. `/ready`와 `/status`의 `sync`로 동기화 상태를 확인하고, 로컬과 최신 피어의 `chain_id`, `height`, `tip_hash`를 비교하세요. 새 블록 생산 중에는 잠시 높이가 다를 수 있습니다. `/verify`는 전체 로컬 체인 무결성을 검사하므로 빈번한 상태 점검에는 `/health`와 `/network/info`를 쓰세요.

한 피어에만 의존하지 않으려면 `.env`에 같은 체인을 제공하는 다른 운영자의 주소를 추가합니다.

```dotenv
TOMEI_PEERS=https://domeido.asuscomm.com/tomei,https://another-operator.example.com
```

설정을 바꾼 뒤 `docker compose up -d`로 적용합니다. 피어 주소는 자동 등록·검색되지 않습니다. follower도 검증된 서명을 그대로 재제공하므로 다른 follower의 피어가 될 수 있습니다. HTTP는 로컬 연결에 사용할 수 있고, 인터넷에 공개할 때는 HTTPS를 사용하세요.

### 다른 사람에게 내 노드 제공하기

본인 도메인을 서버에 연결하고 HTTPS reverse proxy로 `8642`를 연결하세요. 같은 호스트에서 실행하는 Caddy 예제가 [deploy/Caddyfile.example](deploy/Caddyfile.example)에 있습니다. Docker 컨테이너에서 실행하는 프록시는 별도의 Docker 네트워크 또는 호스트 게이트웨이 연결을 설정해야 합니다. 기본 `127.0.0.1` 바인딩은 다른 컨테이너의 루프백 주소가 아닙니다.

`.env`의 `TOMEI_PUBLIC_URL`을 본인 HTTPS 주소로 바꾸고, 다음 정보를 함께 공유하면 다른 사람이 피어로 추가할 수 있습니다.

- 공개 HTTPS 기본 URL
- 같은 네트워크의 authority 공개키와 체인 ID
- `/network/info`에서 확인한 현재 높이

follower는 `/tx`, `/upload`, `/api/blocks` 쓰기를 `403`으로 거부합니다. 공증을 새로 제출하는 앱은 authority의 쓰기 경로를 사용해야 합니다. 네트워크 주소에 접두사(`/tomei` 등)가 있어도 피어 URL에 그대로 포함하면 됩니다.

## 새 독립 체인 만들기

새 authority를 만드는 것은 공개 체인에 검증자로 참가하는 동작이 아닙니다. **별도의 키와 체인을 생성**합니다. 기존 follower와 분리된 Compose 프로젝트·볼륨을 사용하세요.

```bash
cp .env.authority.example .env.authority
chmod 600 .env.authority
# 외부 쓰기를 허용하려면 .env.authority에 TOMEI_WRITE_TOKEN을 설정합니다.
# 예: openssl rand -hex 32 로 생성한 값을 사용하세요.
docker compose --env-file .env.authority --project-name my-tomei-chain up -d --build
curl -fsS http://127.0.0.1:8642/network/info
```

같은 컴퓨터에서 공개 follower와 함께 운영한다면 `.env.authority`의 `TOMEI_PORT`와 `TOMEI_PUBLIC_URL`을 다른 포트로 바꿔야 합니다. 최초 실행 때 `authority.key`가 데이터 볼륨에 생성되며 파일 권한은 `0600`입니다. **이 개인키를 follower에게 배포하지 마세요.** 공개키와 체인 ID만 전달하면 됩니다. 개인키를 잃거나 교체하면 같은 네트워크 identity로 블록을 계속 서명할 수 없습니다.

authority에 `TOMEI_WRITE_TOKEN`을 설정하면 모든 POST 요청에 `Authorization: Bearer <token>`이 필요합니다. 토큰을 설정하지 않은 authority는 누구나 쓰기를 요청할 수 있으므로, 공개 읽기 프록시 예제나 신뢰하는 사설 네트워크를 사용하세요. 기존 앱이 인증 헤더를 보낼 수 없다면 앱 전용 프록시/사설 접속 경로를 구성해야 합니다.

## 실행·설정

호스트에 Rust stable과 C 링커가 설치되어 있으면 직접 빌드할 수도 있습니다.

```bash
cargo build --release --locked
cargo test --locked
python3 scripts/smoke-network.py --binary target/release/tomei-chain
```

C 툴체인이 없는 Linux 호스트에서는 `./build.sh`와 `./build.sh test`로 Docker 안에서 빌드·검사할 수 있습니다. 바이너리는 기존 배포 호환을 위해 role 생략 시 authority로 실행됩니다. 직접 follower를 운영할 때는 아래 환경변수를 **명시적으로 설정**하세요. Compose와 Docker 이미지의 기본 role은 follower입니다.

| 변수 | 기본값 / 용도 |
|---|---|
| `TOMEI_NODE_ROLE` | 바이너리 `authority`, Docker/Compose `follower` |
| `TOMEI_PEERS` | follower 필수. 쉼표로 구분한 HTTP(S) 기본 URL |
| `TOMEI_AUTHORITY_PUBLIC_KEY` | follower 필수. 신뢰하는 authority의 64자리 16진수 공개키 |
| `TOMEI_CHAIN_ID` | 선택적 네트워크 고정값. `/network/info`의 체인 ID와 일치해야 함 |
| `TOMEI_AUTHORITY_KEY_FILE` | authority 개인키 경로. 기본 `<data_dir>/authority.key` |
| `TOMEI_SYNC_INTERVAL_MS` | `2000`, follower의 피어 점검 간격. 허용 범위 `250..60000` |
| `TOMEI_DATA_DIR` | 바이너리 `./tomei-data`, 컨테이너 `/var/lib/tomei-chain` |
| `TOMEI_ADDR` | 바이너리 `127.0.0.1:8642`, 컨테이너 `0.0.0.0:8642` |
| `TOMEI_MANTIS_ADDR` | 바이너리 `127.0.0.1:7878`, Compose `off` |
| `TOMEI_PUBLIC_URL` | `/upload` 번들 조회 URL의 기본 주소 |
| `TOMEI_WRITE_TOKEN` | 선택적 authority POST Bearer 토큰 |
| `TOMEI_BLOCK_INTERVAL_MS` | `2000`, authority 블록 생산 주기 |
| `TOMEI_BATCH` | `100`, 즉시 블록 생산을 요청하는 멤풀 개수 |
| `TOMEI_BIND_IP` / `TOMEI_PORT` | Compose 전용. 호스트 기본 `127.0.0.1:8642` |

사용자 systemd 예제는 [deploy/tomei-chain.service](deploy/tomei-chain.service)입니다. 바이너리가 `~/tomeiChain/target/release/tomei-chain`에 있는 것을 전제로 하며, 다른 설치 경로는 서비스 파일을 조정하세요.

```bash
mkdir -p ~/.config/systemd/user ~/.config/tomei-chain ~/.local/share/tomei-chain
cp .env.example ~/.config/tomei-chain/node.env
chmod 600 ~/.config/tomei-chain/node.env
cp deploy/tomei-chain.service ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now tomei-chain
journalctl --user -u tomei-chain -n 60 --no-pager
```

로그아웃 후에도 실행하려면 호스트 관리자가 `loginctl enable-linger <사용자명>`을 설정할 수 있습니다. systemd와 Compose가 같은 포트나 데이터 디렉토리를 사용하도록 동시에 실행하지 마세요.

### 업데이트와 데이터 보존

```bash
git pull --ff-only
docker compose up -d --build
docker compose ps
```

데이터는 Compose의 `node-data` 볼륨에 남습니다. `docker compose down`은 볼륨을 지우지 않지만 `docker compose down --volumes`는 체인과 authority 개인키를 삭제합니다. 데이터는 자동으로 잘리거나 외부 저장소에 백업되지 않으므로 디스크 사용량을 점검하세요.

일관된 백업은 노드를 정지한 다음 데이터 전체를 복사해야 합니다.

```bash
mkdir -p backups
chmod 700 backups
docker compose stop node
docker compose cp node:/var/lib/tomei-chain ./backups/node-data
docker compose start node
```

덮어쓰기를 피하도록 백업마다 새 경로를 사용하세요. authority 백업에는 개인키가 포함되므로 비공개로 보관합니다. 컨테이너에 복구한 데이터는 UID/GID `10001:10001`이 읽고 쓸 수 있어야 합니다. 잘못된 키·체인 ID로 기존 데이터 디렉토리를 재사용하지 마세요. follower는 데이터가 없어도 올바른 피어와 공개키로 다시 동기화할 수 있습니다.

## HTTP 계약

| 요청 | 설명 |
|---|---|
| `GET /health` | 프로세스 생존 확인. 동기화 완료를 의미하지 않음 |
| `GET /ready` | 동기화 준비 상태. 준비되지 않은 follower는 `503` |
| `GET /network/info` | 프로토콜, role, 체인 ID, 제네시스, 공개키, 높이, 팁, 서명된 체크포인트 |
| `GET /network/block/{height}` | 원본 블록과 authority의 Ed25519 체크포인트 서명 |
| `GET /status` | 로컬 체인·멤풀·mantis 상태 |
| `GET /block/latest`, `GET /block/{height}` | 코어 블록 조회 |
| `GET /tx/{id}` | 트랜잭션 확정 여부와 위치 |
| `GET /ns/{namespace}` | 네임스페이스의 최신 공증 상태 |
| `GET /verify` | 로컬 코어 체인 전체 무결성 검사 |
| `POST /tx` | authority에 `{namespace, payload, signature?, tags?}` 제출 |
| `POST /upload` | HeartCritiqueEAS 번들 저장 후 커밋 확인 |
| `GET /bundle/{txId}` | 번들 원문. 동기화된 follower에서도 제공 |
| `GET /wallet` | uploader 호환 응답 |
| `GET /api/info`, `GET /api/blocks?from=N` | authority의 별도 mantis 체인 조회. follower는 미지원 `503` |
| `POST /api/blocks` | authority의 mantis fast-forward 병합 |

`payload`는 문자열이며 `tags`는 `[["키", "값"], …]`입니다. 태그는 first-wins 멱등 인덱스입니다. 트랜잭션 ID에는 제출 시각도 포함되므로 같은 내용을 나중에 다시 보내면 같은 ID가 보장되지 않습니다. 앱 멱등 처리는 태그를 사용하세요. 범용 트랜잭션의 `signature` 필드는 앱 데이터이며 코어가 제출자 서명을 검증하는 기능은 아닙니다. 네트워크 블록의 authority 서명은 별도로 검증합니다.

HeartCritiqueEAS는 `UPLOADER_URL`을 authority 쓰기 주소로 설정합니다. 번들 URL 호스트는 앱의 게이트웨이 허용 목록에도 등록해야 합니다. `permanent:true`는 기존 uploader 응답 호환 필드이며 Arweave 저장이나 영구 보존 보증이 아닙니다. 실제 보존은 노드 운영과 백업에 달려 있습니다.

mantis 어댑터는 원본 JSON 바이트, sha256 해시, Ed25519 서명과 fast-forward 규칙을 검사합니다. 전체 CAD 그래프 재실행 검증은 하지 않습니다. **코어 네트워크가 복제하는 것은 mantis 앵커 트랜잭션이며 별도 `mantis_blocks` 데이터베이스는 복제하지 않습니다.** follower의 `/api/info`와 `/api/blocks`는 지원하지 않음을 알리는 `503`을 반환합니다. follower를 mantis 원본 체인의 완전한 복제본으로 사용하면 안 됩니다.

## 검증과 현재 범위

CI에서 포맷 검사, Rust 테스트, Clippy, 실제 프로세스 네트워크 테스트와 Docker 빌드를 실행합니다. [scripts/smoke-network.py](scripts/smoke-network.py)는 임시 디렉토리와 루프백 포트를 사용하여 운영 데이터에 접근하지 않습니다. authority → follower → follower 복제, 쓰기 인증·읽기 전용 정책, 재시작 후 복구, 변조 피어 거부, authority 정지 후 다른 follower를 통한 신규 복제를 검사합니다.

현재 프로토콜은 하나의 authority 공개키를 신뢰합니다. 서명은 피어의 데이터 위조를 막지만 authority 자체의 검열·서로 다른 포크 서명·키 탈취를 합의로 해결하지는 못합니다. 자동 권한 교체, 다중 authority 합의, 슬래싱, 보상 토큰은 포함하지 않습니다. 실제 다중 검증자 블록 생산은 이 운영 분산 단계 위에 별도의 합의·권한 변경 프로토콜을 설계해야 합니다.

기존 단일 노드 데이터베이스를 authority로 처음 업그레이드하면 저장된 체인을 검증하고 새 authority 키로 기존 블록의 체크포인트를 생성합니다. 이는 업그레이드 시점에 보유한 이력에 대한 서명이며, 과거부터 여러 노드가 이력을 검증했다는 증거는 아닙니다.
