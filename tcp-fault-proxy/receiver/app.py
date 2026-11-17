"""Local receiver for the fault-proxy exercise.

Pure-asyncio TCP server in a single process:

* reads until EOF (the sender half-closes after uploading the sample),
* computes the byte count and SHA-256 incrementally,
* sends a JSON receipt back and then half-closes its own write side, so the
  sender can finish the reverse direction even while the upload side is done,
* exposes the last receipts on a tiny stdlib-free HTTP endpoint used by the
  smoke tests and by the sender's "view receipt" feature.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import time
from collections import deque
from typing import Deque, Dict

LISTEN_HOST = os.getenv("RECEIVER_HOST", "0.0.0.0")
TCP_PORT = int(os.getenv("RECEIVER_TCP_PORT", "9100"))
HTTP_PORT = int(os.getenv("RECEIVER_HTTP_PORT", "9101"))
READ_CHUNK = 64 * 1024
MAX_RECEIPTS = int(os.getenv("RECEIVER_MAX_RECEIPTS", "500"))

receipts: Deque[dict] = deque(maxlen=MAX_RECEIPTS)
counter = 0


async def handle_conn(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    global counter
    peer = writer.get_extra_info("peername")
    started = time.monotonic()
    digest = hashlib.sha256()
    size = 0
    chunks = 0
    try:
        while True:
            data = await reader.read(READ_CHUNK)
            if data == b"":
                break
            digest.update(data)
            size += len(data)
            chunks += 1

        counter += 1
        receipt_id = f"rx-{counter}"
        elapsed = time.monotonic() - started
        receipt: Dict[str, object] = {
            "receipt_id": receipt_id,
            "peer": f"{peer[0]}:{peer[1]}" if peer else "?",
            "bytes": size,
            "sha256": digest.hexdigest(),
            "chunks": chunks,
            "read_seconds": round(elapsed, 6),
            "eof_seen": True,
            "received_at": time.time(),
        }
        receipts.appendleft(receipt)
        payload = (json.dumps(receipt, ensure_ascii=False) + "\n").encode()
        writer.write(payload)
        await writer.drain()
        if writer.transport.can_write_eof():
            writer.write_eof()
        # Give the peer a moment to read the receipt before full close.
        await asyncio.sleep(0.05)
    except (ConnectionError, asyncio.CancelledError, RuntimeError):
        pass
    finally:
        try:
            writer.close()
            await writer.wait_closed()
        except (OSError, RuntimeError):
            pass


async def _close_writer(writer: asyncio.StreamWriter) -> None:
    try:
        writer.close()
        await writer.wait_closed()
    except (OSError, RuntimeError):
        pass


# --------------------------------------------------------------------------- #
# Minimal HTTP/1.1 endpoint for receipt lookup (asyncio, no third party)
# --------------------------------------------------------------------------- #


async def http_handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    try:
        request_line = await reader.readline()
        line = request_line.decode("ascii", "replace")
        # Drain remaining headers.
        while True:
            header = await reader.readline()
            if header in (b"\r\n", b"", b"\n"):
                break
        if line.startswith("GET /health"):
            body = json.dumps({"status": "ok", "receipts": len(receipts)}).encode()
            status = "200 OK"
        elif line.startswith("GET /receipts"):
            body = json.dumps(list(receipts), ensure_ascii=False).encode()
            status = "200 OK"
        else:
            body = b'{"detail":"not found"}'
            status = "404 Not Found"
        writer.write(
            b"HTTP/1.1 " + status.encode() + b"\r\n"
            b"Content-Type: application/json; charset=utf-8\r\n"
            b"Content-Length: " + str(len(body)).encode() + b"\r\n"
            b"Connection: close\r\n\r\n" + body
        )
        await writer.drain()
    except (ConnectionError, asyncio.CancelledError):
        pass
    finally:
        try:
            writer.close()
            await writer.wait_closed()
        except (OSError, RuntimeError):
            pass


async def main() -> None:
    tcp_server = await asyncio.start_server(handle_conn, LISTEN_HOST, TCP_PORT)
    http_server = await asyncio.start_server(http_handle, LISTEN_HOST, HTTP_PORT)
    print(f"receiver: tcp on {LISTEN_HOST}:{TCP_PORT}, http on {LISTEN_HOST}:{HTTP_PORT}")
    try:
        await asyncio.gather(tcp_server.serve_forever(), http_server.serve_forever())
    finally:
        tcp_server.close()
        http_server.close()
        await asyncio.gather(tcp_server.wait_closed(), http_server.wait_closed())


if __name__ == "__main__":
    asyncio.run(main())
