"""FastAPI 应用：代理配置、连接统计、上传试传、候选端控制、网页。"""
from __future__ import annotations

import asyncio
import os
import time
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
from fastapi import FastAPI, HTTPException, UploadFile
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field, model_validator

from .engine import DelayProxy, ProxyConfig

STATIC_DIR = Path(__file__).resolve().parent / "static"
UPLOAD_CHUNK = 256 * 1024
CANDIDATE_CONTROL_URL = os.getenv("CANDIDATE_CONTROL_URL",
                                  "http://receiver-candidate:9100")


def _env_config() -> ProxyConfig:
    return ProxyConfig(
        listen_host=os.getenv("LISTEN_HOST", "0.0.0.0"),
        listen_port=int(os.getenv("LISTEN_PORT", "7000")),
        target_host=os.getenv("TARGET_HOST", "127.0.0.1"),
        target_port=int(os.getenv("TARGET_PORT", "9000")),
        up_delay_ms=float(os.getenv("UP_DELAY_MS", "300")),
        down_delay_ms=float(os.getenv("DOWN_DELAY_MS", "300")),
        compare_enabled=os.getenv("COMPARE_ENABLED", "0") not in ("", "0", "false", "False"),
        candidate_host=os.getenv("CANDIDATE_HOST", ""),
        candidate_port=int(os.getenv("CANDIDATE_PORT", "0")),
    )


proxy = DelayProxy(_env_config())


@asynccontextmanager
async def lifespan(_app: FastAPI):
    await proxy.apply_config(proxy.cfg)  # 启动监听器
    yield
    await proxy.shutdown()
    if _control_client is not None:
        await _control_client.aclose()


app = FastAPI(title="TCP Delay Proxy", lifespan=lifespan)


class ConfigIn(BaseModel):
    listen_host: str = "0.0.0.0"
    listen_port: int = Field(ge=1, le=65535)
    target_host: str = Field(min_length=1)
    target_port: int = Field(ge=1, le=65535)
    up_delay_ms: float = Field(ge=0, le=3_600_000)
    down_delay_ms: float = Field(ge=0, le=3_600_000)
    compare_enabled: bool = False
    candidate_host: str = ""
    candidate_port: int = Field(default=0, ge=0, le=65535)

    @model_validator(mode="after")
    def _check_candidate(self):
        if self.compare_enabled:
            if not self.candidate_host.strip():
                raise ValueError("启用旁路比对时必须填写候选地址")
            if not (1 <= self.candidate_port <= 65535):
                raise ValueError("启用旁路比对时必须填写候选端口")
        return self


@app.get("/", include_in_schema=False)
async def index():
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/api/config")
async def get_config():
    snap = proxy.snapshot()
    return {k: snap[k] for k in ("config", "listening", "listen_error")}


@app.post("/api/config")
async def set_config(cfg: ConfigIn):
    await proxy.apply_config(ProxyConfig(**cfg.model_dump()))
    snap = proxy.snapshot()
    return {"ok": snap["listening"],
            **{k: snap[k] for k in ("config", "listening", "listen_error")}}


@app.get("/api/connections")
async def connections():
    return proxy.snapshot()


@app.get("/api/connections/{conn_id}")
async def connection_detail(conn_id: int):
    snap = proxy.snapshot()
    for c in snap["active"] + snap["history"]:
        if c["id"] == conn_id:
            return c
    raise HTTPException(404, f"connection {conn_id} not found")


@app.post("/api/history/clear")
async def clear_history():
    proxy.history.clear()
    return {"ok": True}


# ---------- 候选接收端演示控制（模拟改字 / 慢读 / 断连） ----------

_control_client: httpx.AsyncClient | None = None


def _control() -> httpx.AsyncClient:
    global _control_client
    if _control_client is None:
        _control_client = httpx.AsyncClient(base_url=CANDIDATE_CONTROL_URL,
                                            timeout=5.0)
    return _control_client


@app.get("/api/candidate")
async def get_candidate_controls():
    try:
        r = await _control().get("/control")
        r.raise_for_status()
        return {"ok": True, **r.json()}
    except Exception as exc:
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}


class CandidateControlIn(BaseModel):
    mutate: bool = False
    slow_read: bool = False
    slow_read_ms: int = Field(default=150, ge=0, le=60_000)
    disconnect: bool = False


@app.post("/api/candidate")
async def set_candidate_controls(body: CandidateControlIn):
    try:
        r = await _control().post("/control", json=body.model_dump())
        r.raise_for_status()
        return {"ok": True, **r.json()}
    except Exception as exc:
        raise HTTPException(502, f"candidate control failed: {type(exc).__name__}: {exc}")


async def _read_to_eof(reader: asyncio.StreamReader) -> bytes:
    parts = []
    while True:
        data = await reader.read(65536)
        if not data:
            return b"".join(parts)
        parts.append(data)


def _find_conn_id(up_bytes: int, since_id: int) -> int | None:
    """试传连接经代理转发；按上行字节数定位刚建立的连接（返回连接号）。
    上行字节数在客户端看来可能因 TCP 合并而分块，但总量唯一。"""
    candidates = [c.info for c in proxy.active.values()] + list(proxy.history)
    matches = [i.id for i in candidates
               if i.id >= since_id and i.up.bytes == up_bytes]
    return min(matches) if matches else None


@app.post("/api/test-upload")
async def test_upload(file: UploadFile):
    """把上传的文件经本代理转发到目标：先读欢迎行，发完文件后半关闭，
    再读回执直到 EOF。整条路径都经过延迟代理，等同真实设备链路。
    返回连接号；不等待旁路比对结束。"""
    snap = proxy.snapshot()
    if not snap["listening"]:
        raise HTTPException(503, f"proxy listener not running: {snap['listen_error']}")
    cfg = proxy.cfg
    since_id = proxy._next_id
    delay_s = (cfg.up_delay_ms + cfg.down_delay_ms) / 1000.0
    result: dict = {"file_name": file.filename, "bytes_sent": 0, "ok": False,
                    "conn_id": None, "compare": None}
    writer = None
    t0 = time.monotonic()
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection("127.0.0.1", cfg.listen_port), timeout=15)
        # 目标（示例接收端）先发的欢迎行；自定义目标可能不发，探测失败不阻塞上传
        try:
            welcome = await asyncio.wait_for(reader.readline(),
                                             timeout=max(5.0, delay_s + 5))
            result["welcome"] = welcome.decode("utf-8", "replace").strip()
        except Exception:
            result["welcome"] = None
        while True:
            chunk = await file.read(UPLOAD_CHUNK)
            if not chunk:
                break
            writer.write(chunk)
            await writer.drain()
            result["bytes_sent"] += len(chunk)
        # 半关闭：目标读到 EOF 后才返回回执
        if writer.can_write_eof():
            writer.write_eof()
        else:
            writer.close()
        receipt = await asyncio.wait_for(_read_to_eof(reader),
                                         timeout=max(120.0, 2 * delay_s + 60))
        result["receipt"] = receipt.decode("utf-8", "replace").strip()
        result["ok"] = True
    except Exception as exc:
        result["error"] = f"{type(exc).__name__}: {exc}"
    finally:
        result["duration_ms"] = round((time.monotonic() - t0) * 1000, 1)
        if writer is not None:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
        await file.close()
        # 短时轮询连接表（只等主链路的连接记录出现，不等旁路结束）
        for _ in range(50):
            cid = _find_conn_id(result["bytes_sent"], since_id)
            if cid is not None or result["bytes_sent"] == 0:
                break
            await asyncio.sleep(0.02)
        result["conn_id"] = _find_conn_id(result["bytes_sent"], since_id)
        # 旁路状态快照：pending 表示仍在比对，前端继续轮询该连接号
        if result["conn_id"] is not None:
            snap2 = proxy.snapshot()
            for c in snap2["active"] + snap2["history"]:
                if c["id"] == result["conn_id"]:
                    result["compare"] = c["compare"]
                    break
    return result
