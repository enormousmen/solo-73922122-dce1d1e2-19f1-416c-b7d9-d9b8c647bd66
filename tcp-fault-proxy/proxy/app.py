"""Single-process TCP fault proxy built with FastAPI + asyncio.

The proxy accepts TCP connections on one port and always forwards them to a
fixed local receiver endpoint.  For every connection, each direction
(client->proxy->receiver and receiver->proxy->client) is pumped by its own
asyncio task.

Per direction the following can be configured (via the control API before a
new connection arrives, or manually on a live connection):

* pause after N cumulative bytes have been forwarded in that direction;
* resume a paused direction (or all paused directions of a connection);
* forcibly close the whole connection, interrupting paused directions and
  blocked writes immediately.

Guarantees implemented in this file:

* A read chunk that crosses the N-byte threshold is split, so forwarding
  stops *exactly* at N bytes.  The unsent tail (backlog) and any FIN stay on
  the proxy side while the direction is paused.
* Backpressure: while paused no further bytes are read from the socket; the
  kernel receive buffer fills and TCP flow control throttles the sender.
  Nothing is spooled to disk and no whole packets are buffered.  User-space
  buffering per direction is bounded by the stream read limit (<=256 KiB).
* A FIN (half-close) is held back until the backlog of its own direction has
  been forwarded after resume; the reverse direction keeps flowing.
* A force-close aborts both transports, cancels both pump tasks and waits
  until they have exited before reporting completion.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import time
import uuid
from collections import deque
from contextlib import suppress
from typing import Deque, Dict, Literal, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

# --------------------------------------------------------------------------- #
# Configuration / limits
# --------------------------------------------------------------------------- #

LISTEN_HOST = os.getenv("PROXY_HOST", "0.0.0.0")
LISTEN_PORT = int(os.getenv("PROXY_PORT", "9000"))
TARGET_HOST = os.getenv("TARGET_HOST", "receiver")
TARGET_PORT = int(os.getenv("TARGET_PORT", "9100"))

# asyncio.StreamReader byte limit: a single read() can return at most this
# many bytes, and it is also the hard cap on user-space per-direction state
# (one reader buffer + at most one unsent split tail).
CHUNK = 64 * 1024  # 64 KiB per read
STREAM_LIMIT = 256 * 1024  # 256 KiB, required user-space cap
MAX_CLOSED_RECORDS = 200

Direction = Literal["c2r", "r2c"]
DIRECTIONS: tuple[Direction, Direction] = ("c2r", "r2c")


def _now() -> float:
    return time.monotonic()


# --------------------------------------------------------------------------- #
# Preset policy applied to every new connection
# --------------------------------------------------------------------------- #


class Preset(BaseModel):
    c2r_pause_after: Optional[int] = Field(
        default=None,
        ge=0,
        description="Pause client->receiver after this many bytes were "
        "forwarded. null/omitted disables it for new connections.",
    )
    r2c_pause_after: Optional[int] = Field(
        default=None,
        ge=0,
        description="Pause receiver->client after this many bytes were "
        "forwarded. null/omitted disables it for new connections.",
    )


preset = Preset()


# --------------------------------------------------------------------------- #
# Per-connection / per-direction state
# --------------------------------------------------------------------------- #


class DirState:
    """Bookkeeping for one direction of one connection."""

    def __init__(self, direction: Direction, pause_after: Optional[int]):
        self.direction: Direction = direction
        # Snapshot of the preset at connection creation time.
        self.pause_after: Optional[int] = pause_after
        self.forwarded: int = 0
        self.pending: bytes = b""  # unsent tail held while paused
        self.fin_pending: bool = False  # local peer half-closed, FIN held back
        self.eof_seen: bool = False
        self.finished: bool = False  # half-close delivered to remote peer
        self.closed: bool = False  # remote side fully closed (read returned)
        self.paused: bool = False
        self.auto_triggered: bool = False
        self.resume_event: asyncio.Event = asyncio.Event()
        self.paused_since: Optional[float] = None

    def snapshot(self) -> dict:
        return {
            "direction": self.direction,
            "pause_after": self.pause_after,
            "forwarded_bytes": self.forwarded,
            "pending_bytes": len(self.pending),
            "paused": self.paused,
            "auto_triggered": self.auto_triggered,
            "fin_pending": self.fin_pending,
            "eof_seen": self.eof_seen,
            "half_closed_delivered": self.finished,
            "peer_closed": self.closed,
            "paused_since": self.paused_since,
        }


class Conn:
    def __init__(
        self,
        conn_id: str,
        client_reader: asyncio.StreamReader,
        client_writer: asyncio.StreamWriter,
        remote_reader: asyncio.StreamReader,
        remote_writer: asyncio.StreamWriter,
        peer: tuple[str, int],
    ):
        self.id = conn_id
        self.client_reader = client_reader
        self.client_writer = client_writer
        self.remote_reader = remote_reader
        self.remote_writer = remote_writer
        self.peer = peer
        self.created_at = _now()
        self.closed = False
        self.closed_at: Optional[float] = None
        self.error: Optional[str] = None
        self.tasks: Dict[Direction, Optional[asyncio.Task]] = {
            "c2r": None,
            "r2c": None,
        }
        self.dir: Dict[Direction, DirState] = {
            "c2r": DirState("c2r", preset.c2r_pause_after),
            "r2c": DirState("r2c", preset.r2c_pause_after),
        }

    # -- pause control ----------------------------------------------------- #

    def pause(self, direction: Direction) -> None:
        st = self.dir[direction]
        if st.paused or st.finished or st.closed:
            return
        st.paused = True
        st.paused_since = _now()
        st.resume_event.clear()

    def resume(self, direction: Direction) -> bool:
        st = self.dir[direction]
        if not st.paused:
            return False
        # Once resumed, the one-shot auto threshold must not fire again.
        st.pause_after = None
        st.paused = False
        st.paused_since = None
        st.resume_event.set()
        return True

    def snapshot(self) -> dict:
        return {
            "id": self.id,
            "peer": f"{self.peer[0]}:{self.peer[1]}",
            "created_at": self.created_at,
            "closed": self.closed,
            "closed_at": self.closed_at,
            "error": self.error,
            "directions": {d: self.dir[d].snapshot() for d in DIRECTIONS},
        }


class Registry:
    def __init__(self) -> None:
        self.active: Dict[str, Conn] = {}
        self.closed: Deque[dict] = deque(maxlen=MAX_CLOSED_RECORDS)

    def add(self, conn: Conn) -> None:
        self.active[conn.id] = conn

    def get_active(self, conn_id: str) -> Conn:
        conn = self.active.get(conn_id)
        if conn is None:
            raise HTTPException(status_code=404, detail=f"unknown connection {conn_id}")
        return conn

    def retire(self, conn: Conn) -> None:
        self.active.pop(conn.id, None)
        self.closed.appendleft(conn.snapshot())

    def list(self) -> list[dict]:
        # Newest first: active connections, then closed records.
        return [c.snapshot() for c in self.active.values()] + list(self.closed)


registry = Registry()


# --------------------------------------------------------------------------- #
# Pump: one direction of one connection
# --------------------------------------------------------------------------- #


async def _wait_resume(st: DirState, conn: Conn) -> None:
    """Block while the direction is paused or while the connection dies."""
    while st.paused and not conn.closed:
        try:
            await asyncio.wait_for(st.resume_event.wait(), timeout=0.5)
        except asyncio.TimeoutError:
            continue


async def pump(conn: Conn, direction: Direction) -> None:
    """Forward exactly one direction, honouring the pause/backlog rules."""
    if direction == "c2r":
        reader, writer = conn.client_reader, conn.remote_writer
    else:
        reader, writer = conn.remote_reader, conn.client_writer
    st = conn.dir[direction]

    try:
        while True:
            # While paused we do NOT touch the socket at all: no reads (which
            # would buffer more user-space data) and no FIN delivery.  The
            # kernel receive buffer fills and TCP backpressure reaches the
            # sender; the held backlog is at most one split read tail.
            if st.paused:
                await _wait_resume(st, conn)
                if conn.closed:
                    return

            # Flush the backlog that was held when the threshold tripped,
            # before reading anything new from the socket.
            if st.pending:
                data = st.pending
                st.pending = b""
                writer.write(data)
                # drain() raises ConnectionReset/aborted when force-close
                # aborts the transport, which cancels this task cleanly.
                await writer.drain()
                st.forwarded += len(data)
                if st.fin_pending:
                    _deliver_fin(writer, st)
                    return

            if st.eof_seen:
                # EOF arrived while paused; backlog (if any) is now flushed.
                if st.fin_pending:
                    _deliver_fin(writer, st)
                return

            data = await reader.read(CHUNK)
            if data == b"":
                st.eof_seen = True
                st.closed = True
                if st.paused:
                    # FIN must not cross the pause; wait, then flush backlog.
                    await _wait_resume(st, conn)
                    if conn.closed:
                        return
                    if st.pending:
                        tail = st.pending
                        st.pending = b""
                        writer.write(tail)
                        await writer.drain()
                        st.forwarded += len(tail)
                _deliver_fin(writer, st)
                return

            # Threshold handling: split the chunk so we stop exactly at N,
            # even across read-block boundaries.
            threshold = st.pause_after
            if (
                not st.auto_triggered
                and threshold is not None
                and st.forwarded + len(data) >= threshold
            ):
                cut = threshold - st.forwarded
                head, tail = data[:cut], data[cut:]
                writer.write(head)
                await writer.drain()
                st.forwarded += len(head)
                st.auto_triggered = True
                if tail:
                    # Held backlog: small, never spooled to disk.
                    st.pending = tail
                st.paused = True
                st.paused_since = _now()
                st.resume_event.clear()
                continue

            writer.write(data)
            await writer.drain()
            st.forwarded += len(data)
    except (asyncio.CancelledError, ConnectionError, RuntimeError):
        # Force-close (transport abort / task cancel) or peer reset: the
        # supervisor performs the cleanup.
        raise
    except Exception as exc:  # pragma: no cover - defensive
        conn.error = f"{direction} pump error: {exc!r}"
        raise


def _deliver_fin(writer: asyncio.StreamWriter, st: DirState) -> None:
    """Half-close the remote write side after the backlog is fully sent."""
    transport = writer.transport
    if transport.can_write_eof():
        writer.write_eof()
    st.fin_pending = False
    st.finished = True


# --------------------------------------------------------------------------- #
# Connection lifecycle
# --------------------------------------------------------------------------- #


async def handle_client(
    client_reader: asyncio.StreamReader,
    client_writer: asyncio.StreamWriter,
) -> None:
    conn_id = uuid.uuid4().hex[:12]
    peer = client_writer.get_extra_info("peername") or ("?", 0)
    try:
        remote_reader, remote_writer = await asyncio.open_connection(
            TARGET_HOST, TARGET_PORT, limit=STREAM_LIMIT
        )
    except OSError as exc:
        client_writer.close()
        with suppress(Exception):
            await client_writer.wait_closed()
        registry.closed.appendleft(
            {
                "id": conn_id,
                "peer": f"{peer[0]}:{peer[1]}",
                "closed": True,
                "error": f"connect target {TARGET_HOST}:{TARGET_PORT} failed: {exc!r}",
                "directions": {},
            }
        )
        return

    conn = Conn(conn_id, client_reader, client_writer, remote_reader, remote_writer, peer)
    registry.add(conn)

    conn.tasks["c2r"] = asyncio.create_task(pump(conn, "c2r"), name=f"{conn_id}-c2r")
    conn.tasks["r2c"] = asyncio.create_task(pump(conn, "r2c"), name=f"{conn_id}-r2c")
    tasks = [t for t in conn.tasks.values() if t is not None]

    try:
        # The connection lives until both pump tasks have exited (normal
        # half-close completion, error, or force-close cancellation).
        await asyncio.gather(*tasks, return_exceptions=True)
    finally:
        conn.closed = True
        conn.closed_at = _now()
        for writer in (client_writer, remote_writer):
            try:
                writer.close()
            except Exception:
                pass
        for t in tasks:
            if not t.done():
                t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        for writer in (client_writer, remote_writer):
            with suppress(OSError, RuntimeError):
                await writer.wait_closed()
        registry.retire(conn)


# --------------------------------------------------------------------------- #
# Control API
# --------------------------------------------------------------------------- #

@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.server = await asyncio.start_server(
        handle_client, LISTEN_HOST, LISTEN_PORT, limit=STREAM_LIMIT
    )
    try:
        yield
    finally:
        app.state.server.close()
        await app.state.server.wait_closed()
        # Drain any in-flight connection supervisors so the process exits
        # without leaving half-closed sockets behind.
        for conn in list(registry.active.values()):
            conn.closed = True
            for d in DIRECTIONS:
                conn.dir[d].resume_event.set()
            for writer in (conn.client_writer, conn.remote_writer):
                with suppress(Exception):
                    writer.transport.abort()
        await asyncio.sleep(0)


app = FastAPI(
    title="TCP 故障代理 (TCP Fault Proxy)",
    description=(
        "单进程 TCP 故障代理：固定转发至本地接收端。可预设新连接每个方向在累计转发 "
        "N 字节后暂停，查询连接、按连接恢复或强断。详见各接口说明。"
    ),
    version="1.0.0",
    lifespan=lifespan,
)


class PresetResponse(BaseModel):
    preset: Preset


class ConnListResponse(BaseModel):
    active_count: int
    connections: list[dict]


class PauseBody(BaseModel):
    direction: Direction = Field(
        ..., description="c2r = 客户端→接收端（上传方向），r2c = 接收端→客户端（回执方向）"
    )


class ResumeBody(BaseModel):
    direction: Optional[Direction] = Field(
        default=None,
        description="要恢复的方向；省略或 null 表示恢复该连接两个方向中所有暂停的方向。",
    )


class OpResponse(BaseModel):
    ok: bool
    connection: dict


@app.get("/health", tags=["control"])
async def health() -> dict:
    return {
        "status": "ok",
        "listen": f"{LISTEN_HOST}:{LISTEN_PORT}",
        "target": f"{TARGET_HOST}:{TARGET_PORT}",
        "active_connections": len(registry.active),
        "stream_limit_bytes": STREAM_LIMIT,
        "chunk_bytes": CHUNK,
    }


@app.get("/preset", response_model=PresetResponse, tags=["control"])
async def get_preset() -> PresetResponse:
    """查看当前预设：对之后新建连接生效（不影响已存在的连接）。"""
    return PresetResponse(preset=preset)


@app.put("/preset", response_model=PresetResponse, tags=["control"])
async def set_preset(body: Preset) -> PresetResponse:
    """预设新连接各方向累计转发多少字节后自动暂停；传 null 取消。"""
    preset.c2r_pause_after = body.c2r_pause_after
    preset.r2c_pause_after = body.r2c_pause_after
    return PresetResponse(preset=preset)


@app.get("/connections", response_model=ConnListResponse, tags=["control"])
async def list_connections() -> ConnListResponse:
    """列出全部连接（活动在前、已关闭在后），含每方向字节计数与暂停状态。"""
    return ConnListResponse(active_count=len(registry.active), connections=registry.list())


@app.get("/connections/{conn_id}", tags=["control"])
async def get_connection(conn_id: str) -> dict:
    return registry.get_active(conn_id).snapshot()


@app.post("/connections/{conn_id}/pause", response_model=OpResponse, tags=["control"])
async def pause_connection(conn_id: str, body: PauseBody) -> OpResponse:
    """手动暂停某连接的某个方向（除自动阈值外的测试手段）。"""
    conn = registry.get_active(conn_id)
    conn.pause(body.direction)
    return OpResponse(ok=True, connection=conn.snapshot())


@app.post("/connections/{conn_id}/resume", response_model=OpResponse, tags=["control"])
async def resume_connection(conn_id: str, body: ResumeBody) -> OpResponse:
    """恢复某连接的指定方向；不传 direction 则恢复该连接全部暂停方向。

    恢复后积压字节按原顺序继续转发，不丢、不重、不乱序；若 FIN 被暂扣，
    会在本方向积压发完后再向对端半关闭。
    """
    conn = registry.get_active(conn_id)
    targets = (body.direction,) if body.direction else DIRECTIONS
    for d in targets:
        conn.resume(d)
    return OpResponse(ok=True, connection=conn.snapshot())


@app.post("/connections/{conn_id}/force-close", response_model=OpResponse, tags=["control"])
async def force_close(conn_id: str) -> OpResponse:
    """强断连接：立即打断暂停与阻塞写，两端关闭且转发任务退出后才返回。

    abort() 会让正卡在 drain() 的写和卡在 read() 的读立刻抛异常，
    两个 pump 任务随之结束；本接口等待它们全部退出（supervisor 完成
    两端 socket 关闭）后才报告完成。
    """
    conn = registry.get_active(conn_id)
    conn.closed = True  # wake any _wait_resume loop
    for d in DIRECTIONS:
        conn.dir[d].resume_event.set()
    for writer in (conn.client_writer, conn.remote_writer):
        try:
            writer.transport.abort()
        except Exception:
            with suppress(Exception):
                writer.close()
    tasks = [t for t in conn.tasks.values() if t is not None]
    for t in tasks:
        if not t.done():
            t.cancel()
    # Wait for the pumps and then the supervisor's own cleanup to finish.
    await asyncio.gather(*tasks, return_exceptions=True)
    for _ in range(100):
        if conn_id not in registry.active:
            break
        await asyncio.sleep(0.02)
    if conn_id in registry.active:  # pragma: no cover - defensive
        raise HTTPException(status_code=500, detail="force-close did not finish in time")
    return OpResponse(ok=True, connection=conn.snapshot())
