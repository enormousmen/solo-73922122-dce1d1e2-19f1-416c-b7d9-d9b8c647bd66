"""Local sender for the fault-proxy exercise.

FastAPI control plane + raw asyncio TCP uploader in one process:

* POST /send   connects to the proxy (fixed target), uploads a deterministic
               32 MiB sample, half-closes the write side (FIN), then keeps
               reading until the receiver's receipt (JSON line) arrives and
               the reverse direction reaches EOF;
* GET  /sends  lists recent runs with byte counts, hashes and timing;
* GET  /docs   interactive UI to launch a send and inspect the receipt.

The payload is a deterministic, reproducible byte stream (chained SHA-256
blocks), so receiver and sender hashes can be compared byte-for-byte.  Blocks
are generated on demand; the whole 32 MiB sample is never held in memory here.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import time
import uuid
from collections import deque
from typing import Deque, Dict, Literal, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

PROXY_HOST = os.getenv("SENDER_TARGET_HOST", "proxy")
PROXY_PORT = int(os.getenv("SENDER_TARGET_PORT", "9000"))
DEFAULT_SIZE = int(os.getenv("SENDER_DEFAULT_BYTES", str(32 * 1024 * 1024)))
BLOCK_SIZE = 256 * 1024  # generation / write granularity
MAX_RUNS = 50

runs: Dict[str, dict] = {}
run_order: Deque[str] = deque(maxlen=MAX_RUNS)


class SendRequest(BaseModel):
    size_bytes: int = Field(
        default=DEFAULT_SIZE,
        ge=1,
        le=256 * 1024 * 1024,
        description="采样包大小，默认 32 MiB。",
    )
    host: Optional[str] = Field(default=None, description="覆盖默认代理主机（测试用）")
    port: Optional[int] = Field(default=None, description="覆盖默认代理端口（测试用）")


def _gen_block(seed: int, index: int) -> bytes:
    """Deterministic 256 KiB block: chained hashing, no RNG, no disk spool."""
    h = hashlib.sha256(f"sample-{seed}-{index}".encode()).digest()
    parts = []
    produced = 0
    counter = 0
    while produced < BLOCK_SIZE:
        h = hashlib.sha256(h + counter.to_bytes(4, "big")).digest()
        parts.append(h)
        produced += len(h)
        counter += 1
    return b"".join(parts)[:BLOCK_SIZE]


async def _do_send(run_id: str, size: int, host: str, port: int) -> None:
    run = runs[run_id]
    started = time.monotonic()
    reader = writer = None
    try:
        reader, writer = await asyncio.open_connection(host, port)
        run["phase"] = "connected"
        digest = hashlib.sha256()
        seed = int.from_bytes(uuid.uuid4().bytes[:4], "big")
        remaining = size
        index = 0
        while remaining > 0:
            block = _gen_block(seed, index)[:remaining]
            writer.write(block)
            # Backpressure: a paused/throttled proxy stalls us here instead
            # of buffering the whole sample anywhere.
            await writer.drain()
            digest.update(block)
            remaining -= len(block)
            index += 1
            run["bytes_sent"] = size - remaining

        run["phase"] = "uploaded-half-closing"
        # Half-close: FIN goes through the proxy, which must hold it until
        # any paused backlog of this direction has been forwarded.
        if writer.transport.can_write_eof():
            writer.write_eof()

        # Reverse direction: read the receipt until EOF.  A timeout makes a
        # stalled receipt phase observable instead of hanging forever.
        run["phase"] = "waiting-receipt"
        receipt_timeout = float(os.getenv("SENDER_RECEIPT_TIMEOUT", "120"))
        try:
            raw = await asyncio.wait_for(reader.read(), timeout=receipt_timeout)
        except asyncio.TimeoutError:
            run["phase"] = "failed"
            run["error"] = f"no receipt/EOF within {receipt_timeout}s (receipt-phase stall)"
            run["elapsed_seconds"] = round(time.monotonic() - started, 6)
            return
        elapsed = time.monotonic() - started
        run["upload_sha256"] = digest.hexdigest()
        run["bytes_sent"] = size
        run["reverse_bytes"] = len(raw)
        run["phase"] = "done"
        run["elapsed_seconds"] = round(elapsed, 6)
        try:
            receipt = json.loads(raw.decode().strip().splitlines()[-1])
        except (ValueError, UnicodeDecodeError, IndexError):
            receipt = None
        run["receipt"] = receipt
        if receipt is not None:
            run["hash_match"] = receipt.get("sha256") == digest.hexdigest()
            run["byte_match"] = receipt.get("bytes") == size
        else:
            run["hash_match"] = False
            run["byte_match"] = False
    except (ConnectionError, asyncio.CancelledError, RuntimeError, OSError) as exc:
        run["phase"] = "failed"
        run["error"] = repr(exc)
        run["elapsed_seconds"] = round(time.monotonic() - started, 6)
    finally:
        if writer is not None:
            try:
                writer.close()
                await writer.wait_closed()
            except (OSError, RuntimeError):
                pass


app = FastAPI(
    title="本地发送器 (Local Sample Sender)",
    description="通过故障代理上传采样包（默认 32 MiB），半关闭后读取接收端回执。",
    version="1.0.0",
)


@app.get("/health")
async def health() -> dict:
    return {
        "status": "ok",
        "default_target": f"{PROXY_HOST}:{PROXY_PORT}",
        "default_size_bytes": DEFAULT_SIZE,
        "runs": len(runs),
    }


@app.post("/send", status_code=202)
async def start_send(body: SendRequest) -> dict:
    """启动一次上传（后台任务），立即返回 run id，再用 /sends/{id} 查回执。"""
    run_id = uuid.uuid4().hex[:12]
    run = {
        "run_id": run_id,
        "target": f"{body.host or PROXY_HOST}:{body.port or PROXY_PORT}",
        "size_bytes": body.size_bytes,
        "bytes_sent": 0,
        "reverse_bytes": 0,
        "phase": "starting",
        "started_at": time.time(),
        "receipt": None,
        "upload_sha256": None,
        "hash_match": None,
        "byte_match": None,
        "error": None,
        "elapsed_seconds": None,
    }
    runs[run_id] = run
    run_order.append(run_id)
    # Trim old runs beyond the deque window.
    for rid in list(runs):
        if rid not in run_order:
            runs.pop(rid, None)
    asyncio.create_task(
        _do_send(run_id, body.size_bytes, body.host or PROXY_HOST, body.port or PROXY_PORT),
        name=f"send-{run_id}",
    )
    return run


@app.get("/sends")
async def list_sends() -> list[dict]:
    return [runs[rid] for rid in reversed(run_order) if rid in runs]


@app.get("/sends/{run_id}")
async def get_send(run_id: str) -> dict:
    run = runs.get(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="unknown run id")
    return run
