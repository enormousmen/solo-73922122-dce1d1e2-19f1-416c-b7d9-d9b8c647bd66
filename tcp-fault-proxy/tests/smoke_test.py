#!/usr/bin/env python3
"""End-to-end smoke test for the TCP fault proxy.

Stdlib only. Talks to:
  * proxy control API  (default 127.0.0.1:8080)
  * sender API         (default 127.0.0.1:8081)
  * receiver HTTP API  (default 127.0.0.1:9101)
  * proxy TCP ingress  (default 127.0.0.1:9000)

Scenarios:
  1. baseline: no preset, full upload, hashes match on both ends;
  2. auto pause at N bytes, exact stop, backlog + FIN held, resume ->
     no loss / duplication / reordering (hash + byte count match);
  3. pause exactly at N across multiple read blocks (odd N);
  4. manual pause / resume;
  5. reverse (r2c) pause still lets the upload finish, receipt delivered
     after resume;
  6. force-close interrupts paused direction and blocked write, tasks exit,
     control API and other connections stay usable;
  7. backpressure: while paused the sender cannot drain the whole sample and
     proxy per-direction user-space backlog stays <= 256 KiB.
"""

from __future__ import annotations

import hashlib
import http.client
import json
import os
import socket
import sys
import time

PROXY_API = os.getenv("PROXY_API", "127.0.0.1:8080")
SENDER_API = os.getenv("SENDER_API", "127.0.0.1:8081")
RECEIVER_API = os.getenv("RECEIVER_API", "127.0.0.1:9101")
PROXY_TCP = os.getenv("PROXY_TCP", "127.0.0.1:9000")

PASS, FAIL = "PASS", "FAIL"
results: list[tuple[str, str, str]] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    results.append((PASS if ok else FAIL, name, detail))
    print(f"[{PASS if ok else FAIL}] {name}" + (f" -- {detail}" if detail else ""))


def http_json(host_port: str, method: str, path: str, body: dict | None = None,
              timeout: float = 10.0) -> dict:
    host, port = host_port.split(":")
    conn = http.client.HTTPConnection(host, int(port), timeout=timeout)
    payload = json.dumps(body).encode() if body is not None else None
    conn.request(method, path, body=payload,
                 headers={"Content-Type": "application/json"} if payload else {})
    resp = conn.getresponse()
    raw = resp.read().decode()
    conn.close()
    if resp.status >= 400:
        raise RuntimeError(f"{method} {path} -> {resp.status}: {raw}")
    return json.loads(raw) if raw else {}


def wait_sender(run_id: str, timeout: float = 90.0) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        run = http_json(SENDER_API, "GET", f"/sends/{run_id}")
        if run["phase"] in ("done", "failed"):
            return run
        time.sleep(0.1)
    raise TimeoutError(f"sender run {run_id} did not finish")


def start_send(size: int) -> dict:
    return http_json(SENDER_API, "POST", "/send", {"size_bytes": size})


def newest_conn() -> dict | None:
    data = http_json(PROXY_API, "GET", "/connections")
    active = [c for c in data["connections"] if not c["closed"]]
    return active[0] if active else None


def wait_conn_active(timeout: float = 5.0) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        conn = newest_conn()
        if conn is not None:
            return conn
        time.sleep(0.05)
    raise TimeoutError("connection never appeared at proxy")


def set_preset(c2r=None, r2c=None) -> dict:
    return http_json(PROXY_API, "PUT", "/preset",
                     {"c2r_pause_after": c2r, "r2c_pause_after": r2c})


def clear_preset() -> None:
    set_preset()


def receiver_count() -> int:
    return len(http_json(RECEIVER_API, "GET", "/receipts"))


def wait_receiver_count(target: int, timeout: float = 15.0) -> int:
    """Poll until the receiver has at least `target` receipts."""
    deadline = time.time() + timeout
    count = receiver_count()
    while count < target and time.time() < deadline:
        time.sleep(0.1)
        count = receiver_count()
    return count


def assert_count_stable(count: int, hold: float = 0.8) -> bool:
    """The receipt count must not change while a direction stays paused."""
    time.sleep(hold)
    return receiver_count() == count


def raw_upload(size: int, pause_after: int | None):
    """Open a raw socket through the proxy and upload deterministic bytes."""
    sock = socket.create_connection(PROXY_TCP.split(":"))
    sock.settimeout(5)
    digest = hashlib.sha256()
    # Deterministic pattern different from the sender's.
    block = bytes((i * 31 + 7) & 0xFF for i in range(4096))
    remaining = size
    while remaining > 0:
        chunk = block[: min(len(block), remaining)]
        sock.sendall(chunk)
        digest.update(chunk)
        remaining -= len(chunk)
    sock.shutdown(socket.SHUT_WR)
    return sock, digest.hexdigest()


def recv_all(sock: socket.socket) -> bytes:
    chunks = []
    while True:
        part = sock.recv(65536)
        if not part:
            return b"".join(chunks)
        chunks.append(part)


def main() -> int:
    size = int(os.getenv("TEST_SIZE", str(2 * 1024 * 1024)))  # 2 MiB is enough
    # Backpressure only shows once the payload exceeds the combined kernel
    # socket buffers along the loopback path (each hop can absorb a few MiB),
    # so the backpressure-specific scenarios use a larger payload.
    big_size = int(os.getenv("TEST_BIG_SIZE", str(16 * 1024 * 1024)))
    print(f"== smoke test, payload={size} bytes, backpressure payload={big_size} bytes ==")
    print(f"proxy api={PROXY_API} tcp={PROXY_TCP} sender={SENDER_API} rx={RECEIVER_API}")

    # Health --------------------------------------------------------------- #
    h = http_json(PROXY_API, "GET", "/health")
    check("proxy health", h["status"] == "ok")
    check("user-space cap is 256 KiB", h["stream_limit_bytes"] == 256 * 1024)
    http_json(SENDER_API, "GET", "/health")
    http_json(RECEIVER_API, "GET", "/health")

    # 1. Baseline ---------------------------------------------------------- #
    clear_preset()
    before = receiver_count()
    run = start_send(size)
    run = wait_sender(run["run_id"])
    check("baseline phase done", run["phase"] == "done", run.get("error") or "")
    check("baseline byte match", run["byte_match"] is True,
          f"sent={run['bytes_sent']} receipt={ (run.get('receipt') or {}).get('bytes')}")
    check("baseline sha256 match", run["hash_match"] is True)
    check("baseline receiver got EOF receipt",
          wait_receiver_count(before + 1) >= before + 1)

    # 2. Auto pause, exact N, backlog+FIN held, resume --------------------- #
    n = 300_000
    set_preset(c2r=n)
    before = receiver_count()
    run = start_send(big_size)
    conn = wait_conn_active()
    cid = conn["id"]
    deadline = time.time() + 10
    while time.time() < deadline:
        conn = http_json(PROXY_API, "GET", f"/connections/{cid}")
        st = conn["directions"]["c2r"]
        if st["paused"]:
            break
        time.sleep(0.05)
    st = conn["directions"]["c2r"]
    check("auto pause triggered", st["paused"] is True)
    check("forwarded stopped exactly at N", st["forwarded_bytes"] == n,
          f"forwarded={st['forwarded_bytes']} N={n}")
    check("backlog held while paused", st["pending_bytes"] > 0)
    check("FIN not delivered while paused", st["half_closed_delivered"] is False)
    # With a payload larger than the combined kernel socket buffers on the
    # loopback path, the sender must get stuck in drain() (TCP flow control
    # driven by the paused proxy) instead of buffering the whole sample.
    deadline = time.time() + 10
    blocked = False
    run_mid = {}
    while time.time() < deadline:
        run_mid = http_json(SENDER_API, "GET", f"/sends/{run['run_id']}")
        if run_mid["phase"] == "connected":
            blocked = True
            break
        time.sleep(0.1)
    check("sender blocked by backpressure in drain()", blocked,
          f"phase={run_mid.get('phase')} sent={run_mid.get('bytes_sent')}/{big_size}")
    check("proxy backlog <= 256 KiB", st["pending_bytes"] <= 256 * 1024,
          f"pending={st['pending_bytes']}")
    # Receiver must not have produced a receipt for this run yet, and the
    # count must stay frozen for a while (nothing leaks past the pause).
    check("no receiver receipt while paused",
          assert_count_stable(before),
          f"count={receiver_count()} expected={before}")
    # And the forwarded counter must be frozen at N (nothing leaks past pause).
    conn2 = http_json(PROXY_API, "GET", f"/connections/{cid}")
    check("forwarded bytes frozen at N while paused",
          conn2["directions"]["c2r"]["forwarded_bytes"] == n)
    # Resume: all remaining bytes + held FIN must go through intact.
    http_json(PROXY_API, "POST", f"/connections/{cid}/resume", {"direction": "c2r"})
    run = wait_sender(run["run_id"])
    check("resume -> phase done", run["phase"] == "done", run.get("error") or "")
    check("resume -> byte match", run["byte_match"] is True)
    check("resume -> sha256 match (no loss/dup/reorder)", run["hash_match"] is True)
    check("receipt appears only after resume",
          wait_receiver_count(before + 1) == before + 1)
    clear_preset()

    # 3. Odd N across read blocks (raw client) ----------------------------- #
    n2 = 100_007
    set_preset(c2r=n2)
    sock, expected_hash = raw_upload(size, n2)
    conn = wait_conn_active()
    cid = conn["id"]
    deadline = time.time() + 10
    while time.time() < deadline:
        conn = http_json(PROXY_API, "GET", f"/connections/{cid}")
        if conn["directions"]["c2r"]["paused"]:
            break
        time.sleep(0.05)
    st = conn["directions"]["c2r"]
    check("odd-N exact stop across blocks", st["forwarded_bytes"] == n2,
          f"forwarded={st['forwarded_bytes']}")
    # No EOF should be readable on the receiver-side path while paused:
    # the sender is blocked in sendall once kernel buffers fill; assert the
    # proxy itself has not delivered the FIN.
    check("FIN held at proxy (raw)", st["half_closed_delivered"] is False)
    http_json(PROXY_API, "POST", f"/connections/{cid}/resume", {"direction": "c2r"})
    reply = recv_all(sock)
    sock.close()
    receipt = json.loads(reply.decode().strip().splitlines()[-1])
    check("raw upload byte count", receipt["bytes"] == size)
    check("raw upload sha256", receipt["sha256"] == expected_hash)
    clear_preset()

    # 4. Manual pause / resume --------------------------------------------- #
    run = start_send(size)
    conn = wait_conn_active()
    cid = conn["id"]
    http_json(PROXY_API, "POST", f"/connections/{cid}/pause", {"direction": "c2r"})
    time.sleep(0.5)
    conn = http_json(PROXY_API, "GET", f"/connections/{cid}")
    check("manual pause active", conn["directions"]["c2r"]["paused"] is True)
    http_json(PROXY_API, "POST", f"/connections/{cid}/resume", {"direction": "c2r"})
    run = wait_sender(run["run_id"])
    check("manual pause/resume hash intact", run["hash_match"] is True)

    # 5. Reverse direction (receipt) pause --------------------------------- #
    set_preset(r2c=16)
    run = start_send(size)
    conn = wait_conn_active()
    cid = conn["id"]
    deadline = time.time() + 15
    while time.time() < deadline:
        conn = http_json(PROXY_API, "GET", f"/connections/{cid}")
        if conn["directions"]["r2c"]["paused"]:
            break
        time.sleep(0.05)
    check("r2c auto pause triggered", conn["directions"]["r2c"]["paused"] is True)
    check("upload finished while receipt paused",
          conn["directions"]["c2r"]["half_closed_delivered"] is True)
    run_mid = http_json(SENDER_API, "GET", f"/sends/{run['run_id']}")
    check("sender still waiting for receipt", run_mid["phase"] == "waiting-receipt")
    http_json(PROXY_API, "POST", f"/connections/{cid}/resume", {"direction": "r2c"})
    run = wait_sender(run["run_id"])
    check("receipt delivered after r2c resume", run["phase"] == "done"
          and run["hash_match"] is True)
    clear_preset()

    # 6. Force-close ------------------------------------------------------- #
    set_preset(c2r=123_456)
    run = start_send(size)
    conn = wait_conn_active()
    cid = conn["id"]
    deadline = time.time() + 10
    while time.time() < deadline:
        conn = http_json(PROXY_API, "GET", f"/connections/{cid}")
        if conn["directions"]["c2r"]["paused"]:
            break
        time.sleep(0.05)
    check("paused before force-close", conn["directions"]["c2r"]["paused"] is True)
    t0 = time.time()
    resp = http_json(PROXY_API, "POST", f"/connections/{cid}/force-close", {})
    dt = time.time() - t0
    check("force-close reported ok", resp["ok"] is True)
    check("force-close returned after tasks exited (<2s)", dt < 2.0, f"{dt:.3f}s")
    time.sleep(0.3)
    conns = http_json(PROXY_API, "GET", "/connections")
    check("connection retired from active list",
          all(c["id"] != cid for c in conns["connections"] if not c["closed"]))
    run = wait_sender(run["run_id"])
    check("sender observed failure", run["phase"] == "failed")
    # Control API still works; another connection still works.
    clear_preset()  # the new connection must not inherit the old preset
    http_json(PROXY_API, "GET", "/health")
    run2 = start_send(size)
    run2 = wait_sender(run2["run_id"])
    check("new connection unaffected by force-close",
          run2["phase"] == "done" and run2["hash_match"] is True)
    clear_preset()

    # 7. Backpressure bound ------------------------------------------------ #
    set_preset(c2r=65_537)
    run = start_send(big_size)
    conn = wait_conn_active()
    cid = conn["id"]
    deadline = time.time() + 10
    max_pending = 0
    while time.time() < deadline:
        conn = http_json(PROXY_API, "GET", f"/connections/{cid}")
        st = conn["directions"]["c2r"]
        max_pending = max(max_pending, st["pending_bytes"])
        if st["paused"]:
            break
        time.sleep(0.02)
    # Observe a few more polls while the sender is blocked.
    phases = set()
    for _ in range(10):
        conn = http_json(PROXY_API, "GET", f"/connections/{cid}")
        st = conn["directions"]["c2r"]
        max_pending = max(max_pending, st["pending_bytes"])
        run_mid = http_json(SENDER_API, "GET", f"/sends/{run['run_id']}")
        phases.add(run_mid["phase"])
        time.sleep(0.05)
    check("backlog never exceeded 256 KiB", max_pending <= 256 * 1024,
          f"max_pending={max_pending}")
    check("sender stayed blocked in drain() (no whole-file buffering)",
          phases == {"connected"}, f"phases={phases}")
    http_json(PROXY_API, "POST", f"/connections/{cid}/resume", {"direction": "c2r"})
    run = wait_sender(run["run_id"])
    check("backpressure test finishes clean", run["hash_match"] is True)
    clear_preset()

    # Summary -------------------------------------------------------------- #
    failed = [r for r in results if r[0] == FAIL]
    print("\n== summary ==")
    for status, name, detail in results:
        print(f"  [{status}] {name}" + (f" -- {detail}" if detail else ""))
    print(f"\n{len(results) - len(failed)}/{len(results)} checks passed")
    return 1 if failed else 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print(f"FATAL: {exc!r}")
        sys.exit(2)
