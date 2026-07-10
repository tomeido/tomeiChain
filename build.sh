#!/usr/bin/env bash
# ============================================================================
# tomei-chain 빌드 스크립트 — 호스트에 C 툴체인(cc)이 없어도 빌드 가능
# ============================================================================
# 호스트에 gcc 가 없으므로 mantis-cad 개발에 쓰는 것과 같은 rust:1 도커
# 이미지로 빌드한다. 호스트 ~/.cargo/registry 를 재사용해 재다운로드를 피하고,
# 결과물(target/)은 현재 사용자 소유로 남는다.
#
# 사용법:
#   ./build.sh              # cargo build --release
#   ./build.sh test         # cargo test
#   ./build.sh build        # cargo build (debug)
#   ./build.sh clippy       # cargo clippy
#
# 참고: 호스트에 build-essential 을 설치하면 그냥 `cargo build --release` 로도
# 빌드된다. (sudo apt install build-essential)
set -euo pipefail

cd "$(dirname "$0")"
CMD="${1:-release}"

CARGO_SCRATCH="${TMPDIR:-/tmp}/tomei-cargo-home-$(id -u)"
mkdir -p "$CARGO_SCRATCH"

run() {
  docker run --rm \
    -u "$(id -u):$(id -g)" \
    -e HOME=/tmp \
    -e CARGO_HOME=/cargo \
    -v "$CARGO_SCRATCH":/cargo \
    -v "$HOME/.cargo/registry":/cargo/registry \
    -v "$PWD":/work -w /work \
    rust:1 "$@"
}

case "$CMD" in
  release) run cargo build --release ;;
  build)   run cargo build ;;
  test)    run cargo test "${@:2}" ;;
  clippy)  run cargo clippy --all-targets "${@:2}" ;;
  *)       run "$@" ;;
esac
