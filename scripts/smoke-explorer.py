#!/usr/bin/env python3
"""Check the explorer on an isolated temporary chain, never the production DB."""

import argparse
import importlib.util
import json
from pathlib import Path
import secrets
import tempfile
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen


spec = importlib.util.spec_from_file_location(
    "network_smoke", Path(__file__).with_name("smoke-network.py")
)
network = importlib.util.module_from_spec(spec)
spec.loader.exec_module(network)


def check_json(node, path, expected=200):
    status, body = network.request(node.url, path)
    assert status == expected, (path, status, body)
    return body


def pages(node, path, cursor_key, limit=2):
    result = []
    cursor = None
    seen = set()
    for _ in range(100):
        query = {"limit": limit}
        if cursor is not None:
            assert cursor not in seen, ("repeated cursor", cursor)
            seen.add(cursor)
            query[cursor_key] = cursor
        separator = "&" if "?" in path else "?"
        page = check_json(node, path + separator + urlencode(query))
        assert len(page["items"]) <= limit, page
        result.extend(page["items"])
        cursor = page["next_cursor"]
        if cursor is None:
            return result
    raise AssertionError("pagination did not terminate")


def run(binary):
    with tempfile.TemporaryDirectory(prefix="tomei-explorer-smoke-") as temporary:
        token = secrets.token_hex(24)
        node = network.Node(binary, Path(temporary), "explorer",
                            TOMEI_NODE_ROLE="authority", TOMEI_WRITE_TOKEN=token,
                            TOMEI_BATCH="1")
        try:
            node.start()
            for path, content_type, marker in [
                ("/explorer/", "text/html", b"<!"),
                ("/explorer/app.js", "javascript", None),
                ("/explorer/styles.css", "text/css", None),
            ]:
                with urlopen(node.url + path, timeout=5) as response:
                    data = response.read()
                    assert response.status == 200
                    assert content_type in response.headers["Content-Type"]
                    assert len(data) > 100, (path, len(data))
                    if marker:
                        assert marker in data
                with urlopen(Request(node.url + path, method="HEAD"), timeout=5) as response:
                    assert int(response.headers["Content-Length"]) == len(data)
                    assert response.read() == b""
            with urlopen(node.url + "/explorer", timeout=5) as response:
                assert response.url.endswith("/explorer/"), response.url
            check_json(node, "/explorer/not-a-file", 404)
            assert network.request(node.url, "/explorer/api/blocks", {}, token)[0] == 405
            assert check_json(node, "/explorer/api/transactions")["items"] == []
            assert check_json(node, "/explorer/api/blocks")["total"] == 1
            print("PASS: embedded HTML/JS/CSS, redirect, genesis-only explorer", flush=True)

            namespaces = ["alpha", "beta", "alpha", "공증 / a&b"]
            tx_ids = []
            for index, namespace in enumerate(namespaces):
                status, receipt = network.request(node.url, "/tx", {
                    "namespace": namespace,
                    "payload": json.dumps({"message": "<script>alert('data')</script>",
                                           "index": index}),
                    "tags": [["case", str(index)]],
                }, token)
                assert status in (200, 202), (status, receipt)
                tx_id = receipt["id"]
                tx_ids.append(tx_id)
                network.eventually(
                    lambda: network.request(node.url, "/tx/" + tx_id)[1].get("status")
                    == "confirmed", "fixture transaction commit")

            info = node.info()
            blocks = pages(node, "/explorer/api/blocks", "before")
            assert [row["height"] for row in blocks] == list(range(info["height"], -1, -1))
            assert blocks[-1]["tx_count"] == 0
            assert sum(row["tx_count"] for row in blocks) == len(tx_ids)
            transactions = pages(node, "/explorer/api/transactions", "cursor")
            assert [row["id"] for row in transactions] == list(reversed(tx_ids))
            assert all("payload" not in row for row in transactions)
            all_namespaces = pages(node, "/explorer/api/namespaces", "cursor", limit=1)
            assert [row["namespace"] for row in all_namespaces] == sorted(set(namespaces))
            assert sum(row["count"] for row in all_namespaces) == len(tx_ids)
            filtered = pages(node, "/explorer/api/transactions?namespace=alpha", "cursor", limit=1)
            assert [row["id"] for row in filtered] == [tx_ids[2], tx_ids[0]]
            encoded = quote(namespaces[-1], safe="")
            special = check_json(node, "/explorer/api/transactions?namespace=" + encoded)
            assert [row["id"] for row in special["items"]] == [tx_ids[-1]]
            print("PASS: complete pagination, ordering, summaries, namespace filtering", flush=True)

            for query, expected_type in [
                (str(info["height"]), "block"), (info["tip_hash"], "block"),
                (tx_ids[0], "transaction"), (namespaces[-1], "namespace"),
            ]:
                found = check_json(node, "/explorer/api/search?" + urlencode({"q": query}))
                assert found["type"] == expected_type, found
            check_json(node, "/explorer/api/search?q=does-not-exist", 404)
            for path in [
                "/explorer/api/blocks?limit=0", "/explorer/api/blocks?limit=101",
                "/explorer/api/blocks?before=not-a-height",
                "/explorer/api/transactions?cursor=invalid",
                "/explorer/api/namespaces?limit=-1", "/explorer/api/search?q=",
            ]:
                check_json(node, path, 400)
            print("PASS: exact height/hash/transaction/namespace search and invalid inputs", flush=True)

            node.stop()
            node.start()
            restored = pages(node, "/explorer/api/transactions", "cursor")
            assert restored == transactions, "restart changed indexed history"
            assert node.info()["tip_hash"] == info["tip_hash"]
            assert check_json(node, "/verify")["ok"] is True
            print("PASS: index persistence and unchanged chain integrity after restart", flush=True)
        except Exception:
            if node.log.exists():
                print(node.log.read_text(errors="replace")[-10000:])
            raise
        finally:
            node.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=Path("target/debug/tomei-chain"))
    args = parser.parse_args()
    run(args.binary.resolve(strict=True))
