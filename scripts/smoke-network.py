#!/usr/bin/env python3
"""Exercise real daemons on loopback with temporary storage; never use live data.

Requires Python 3 and a built tomei-chain binary. No third-party Python packages.
"""

import argparse
import json
import os
from pathlib import Path
import secrets
import signal
import socket
import subprocess
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


def request(base, path, payload=None, token=None):
    headers = {}
    data = None
    if payload is not None:
        headers["Content-Type"] = "application/json"
        data = json.dumps(payload).encode()
    if token:
        headers["Authorization"] = "Bearer " + token
    req = Request(base + path, data=data, headers=headers)
    try:
        with urlopen(req, timeout=4) as response:
            return response.status, json.load(response)
    except HTTPError as error:
        return error.code, json.load(error)


def eventually(check, description, timeout=30):
    deadline = time.monotonic() + timeout
    last_error = None
    while time.monotonic() < deadline:
        try:
            result = check()
            if result:
                return result
        except (URLError, OSError, ValueError) as error:
            last_error = error
        time.sleep(0.1)
    raise AssertionError(f"Timed out: {description}; last error: {last_error}")


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class Node:
    def __init__(self, binary, directory, name, **settings):
        self.binary = binary
        self.name = name
        self.data = directory / name
        self.log = directory / (name + ".log")
        self.port = free_port()
        self.url = f"http://127.0.0.1:{self.port}"
        self.settings = settings
        self.process = None
        self.output = None

    def start(self):
        # Ambient TOMEI_* settings must never point a smoke test at live storage.
        env = {key: value for key, value in os.environ.items() if not key.startswith("TOMEI_")}
        env.update({
            "TOMEI_DATA_DIR": str(self.data),
            "TOMEI_ADDR": f"127.0.0.1:{self.port}",
            "TOMEI_MANTIS_ADDR": "off",
            "TOMEI_PUBLIC_URL": self.url,
            "TOMEI_BLOCK_INTERVAL_MS": "100",
            "TOMEI_SYNC_INTERVAL_MS": "250",
        })
        env.update(self.settings)
        self.output = self.log.open("ab")
        self.process = subprocess.Popen([str(self.binary)], env=env, stdout=self.output,
                                        stderr=subprocess.STDOUT)

        def ready():
            if self.process.poll() is not None:
                raise AssertionError(f"{self.name} exited with {self.process.returncode}")
            return request(self.url, "/health")[0] == 200

        eventually(ready, self.name + " startup")
        return self

    def stop(self):
        if self.process is not None and self.process.poll() is None:
            self.process.send_signal(signal.SIGINT)
            try:
                self.process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
                raise AssertionError(self.name + " did not shut down gracefully")
        if self.output:
            self.output.close()
            self.output = None

    def info(self):
        status, body = request(self.url, "/network/info")
        assert status == 200, (self.name, status, body)
        return body


def run(binary):
    nodes = []
    proxy = None
    proxy_thread = None
    with tempfile.TemporaryDirectory(prefix="tomei-network-smoke-") as temporary:
        directory = Path(temporary)
        try:
            token = secrets.token_hex(24)
            authority = Node(binary, directory, "authority", TOMEI_NODE_ROLE="authority",
                             TOMEI_WRITE_TOKEN=token)
            nodes.append(authority)
            authority.start()
            identity = authority.info()
            assert len(identity["authority_public_key"]) == 64, identity
            assert len(identity["chain_id"]) == 64, identity
            key_file = authority.data / "authority.key"
            assert key_file.exists(), "authority did not persist its identity"
            if os.name == "posix":
                assert key_file.stat().st_mode & 0o777 == 0o600, "private key mode must be 0600"
            submission = {"namespace": "smoke", "payload": "independent-node replication"}
            assert request(authority.url, "/tx", submission)[0] in (401, 403)
            code, receipt = request(authority.url, "/tx", submission, token)
            assert code in (200, 202), (code, receipt)
            tx_id = receipt["id"]

            def confirmed(node, tx):
                code, body = request(node.url, "/tx/" + tx)
                return code == 200 and body.get("status") == "confirmed"

            eventually(lambda: confirmed(authority, tx_id), "authority commit")
            common = {
                "TOMEI_NODE_ROLE": "follower",
                "TOMEI_AUTHORITY_PUBLIC_KEY": identity["authority_public_key"],
                "TOMEI_CHAIN_ID": identity["chain_id"],
            }
            relay = Node(binary, directory, "relay", **common, TOMEI_PEERS=authority.url)
            nodes.append(relay)
            relay.start()
            eventually(lambda: confirmed(relay, tx_id), "authority -> relay")
            follower = Node(binary, directory, "follower", **common, TOMEI_PEERS=relay.url)
            nodes.append(follower)
            follower.start()
            eventually(lambda: confirmed(follower, tx_id), "relay -> follower")
            assert request(follower.url, "/tx", submission)[0] == 403
            assert request(follower.url, "/upload", {})[0] == 403
            assert request(follower.url, "/api/blocks", [])[0] == 403
            assert request(follower.url, "/api/info")[0] == 503
            assert request(follower.url, "/api/blocks?from=0")[0] == 503
            for node in (relay, follower):
                info = node.info()
                assert info["chain_id"] == identity["chain_id"]
                assert info["tip_hash"] == authority.info()["tip_hash"]
                assert request(node.url, "/verify")[1]["ok"] is True
                assert request(node.url, "/block/1")[1] == request(authority.url, "/block/1")[1]
            print("PASS: authenticated writes, pinned identity, authority -> relay -> follower", flush=True)

            follower.stop()
            follower.start()
            assert confirmed(follower, tx_id), "restart lost a committed transaction"
            code, receipt = request(authority.url, "/tx", {
                "namespace": "smoke", "payload": "after follower restart",
            }, token)
            assert code in (200, 202), (code, receipt)
            eventually(lambda: confirmed(follower, receipt["id"]), "continued sync after restart")
            print("PASS: persisted restart and continued synchronization", flush=True)

            tampered = threading.Event()

            class TamperingPeer(BaseHTTPRequestHandler):
                def do_GET(self):
                    # Serve a path-prefix peer to also exercise public reverse-proxy URLs.
                    path = self.path.removeprefix("/chain")
                    status, body = request(authority.url, path)
                    if path.startswith("/network/block/") and status == 200:
                        block = body["block"]
                        if block["transactions"]:
                            block["transactions"][0]["payload"] = "tampered by peer"
                            tampered.set()
                    encoded = json.dumps(body).encode()
                    self.send_response(status)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(encoded)))
                    self.end_headers()
                    self.wfile.write(encoded)

                def log_message(self, *_args):
                    pass

            proxy = ThreadingHTTPServer(("127.0.0.1", 0), TamperingPeer)
            proxy_thread = threading.Thread(target=proxy.serve_forever, daemon=True)
            proxy_thread.start()
            late = Node(binary, directory, "late-follower", **common,
                        TOMEI_PEERS=f"http://127.0.0.1:{proxy.server_port}/chain")
            nodes.append(late)
            late.start()
            assert tampered.wait(timeout=15), "follower never requested a signed block"
            time.sleep(0.5)
            assert late.info()["height"] == 0, "follower accepted tampered peer data"
            assert request(late.url, "/tx/" + tx_id)[0] == 404
            late.stop()
            proxy.shutdown()
            proxy.server_close()
            proxy_thread.join(timeout=5)
            proxy = None
            print("PASS: tampered block rejected without advancing the local chain", flush=True)

            target = authority.info()
            authority.stop()
            late.settings["TOMEI_PEERS"] = authority.url + "," + relay.url
            late.start()
            eventually(lambda: late.info()["tip_hash"] == target["tip_hash"],
                       "peer failover while authority is offline")
            assert confirmed(late, tx_id)
            assert request(late.url, "/verify")[1]["ok"] is True
            print("PASS: new follower bootstraps through a relay while authority is offline", flush=True)

            authority.start()
            restarted = authority.info()
            assert restarted["authority_public_key"] == identity["authority_public_key"]
            assert restarted["chain_id"] == identity["chain_id"]
            assert restarted["tip_hash"] == target["tip_hash"]
            assert request(authority.url, "/ready")[0] == 200
            eventually(lambda: request(late.url, "/ready")[0] == 200, "follower readiness")
            print("PASS: authority restart preserves signing identity and persisted chain", flush=True)
        except Exception:
            for node in nodes:
                if node.log.exists():
                    print(f"\n--- {node.name} log ---\n" + node.log.read_text(errors="replace")[-6000:])
            raise
        finally:
            if proxy:
                proxy.shutdown()
                proxy.server_close()
            for node in reversed(nodes):
                node.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=Path("target/release/tomei-chain"))
    args = parser.parse_args()
    executable = args.binary.resolve()
    if not executable.is_file():
        parser.error(f"Binary missing: {executable}; build it first")
    run(executable)
    print("All network smoke checks passed.")
