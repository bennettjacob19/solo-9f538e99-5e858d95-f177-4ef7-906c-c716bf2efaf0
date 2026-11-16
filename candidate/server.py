"""候选接收端（升级版服务的模拟器）。

默认行为与主接收端一致：发欢迎行、读到 EOF 后回
`RECEIPT bytes=<n> sha256=<hex>`，因此旁路比对应为“一致”。

可通过控制接口（默认 :9100）模拟三种故障，用于演示：
* mutate      = true  回执中的 SHA-256 被改写（字节数不变）-> 比对“不同”
* slow_read   = true  读取上行时每块前休眠 slow_read_ms -> 旁路变慢/不完整，
                      主链路不受影响
* disconnect  = true  收到第一块上行后立即断开 -> 旁路“不完整”，主链路不受影响

故障配置在每条连接 accept 时快照。控制接口是一个极简 HTTP 服务，运行在
独立线程，不依赖第三方库。
"""
import asyncio
import hashlib
import json
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "9000"))
CONTROL_PORT = int(os.getenv("CONTROL_PORT", "9100"))
WELCOME = b"WELCOME tcp-delay-demo receiver; send data then half-close (EOF) to get the receipt\n"

_state_lock = threading.Lock()
_state = {
    "mutate": os.getenv("MUTATE", "0") not in ("", "0", "false", "False"),
    "slow_read": os.getenv("SLOW_READ", "0") not in ("", "0", "false", "False"),
    "slow_read_ms": int(os.getenv("SLOW_READ_MS", "150")),
    "disconnect": os.getenv("DISCONNECT", "0") not in ("", "0", "false", "False"),
}


def snapshot_state() -> dict:
    with _state_lock:
        return dict(_state)


def update_state(patch: dict) -> dict:
    with _state_lock:
        for k in ("mutate", "slow_read", "disconnect"):
            if k in patch:
                _state[k] = bool(patch[k])
        if "slow_read_ms" in patch:
            _state["slow_read_ms"] = max(0, min(60_000, int(patch["slow_read_ms"])))
        return dict(_state)


def _mutated_digest(hexdigest: str) -> str:
    """保持长度和格式不变，只改一个字符：用于演示“字节数相同但内容不同”。"""
    if not hexdigest:
        return "0" * 64
    first = "0" if hexdigest[0] != "0" else "1"
    return first + hexdigest[1:]


async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    peer = writer.get_extra_info("peername")
    faults = snapshot_state()
    print(f"[candidate] connection from {peer} faults={faults}", flush=True)
    try:
        writer.write(WELCOME)
        await writer.drain()
        h = hashlib.sha256()
        total = 0
        while True:
            if faults["slow_read"]:
                await asyncio.sleep(faults["slow_read_ms"] / 1000.0)
            chunk = await reader.read(65536)
            if not chunk:
                break
            h.update(chunk)
            total += len(chunk)
            if faults["disconnect"]:
                # 突然断开（不返回回执）
                print(f"[candidate] {peer} simulated disconnect after {total} B",
                      flush=True)
                transport = writer.transport
                transport.abort()
                return
        digest = h.hexdigest()
        if faults["mutate"]:
            digest = _mutated_digest(digest)
        receipt = f"RECEIPT bytes={total} sha256={digest}\n"
        writer.write(receipt.encode())
        await writer.drain()
        print(f"[candidate] {peer} -> {total} bytes, receipt sent "
              f"(mutate={faults['mutate']})", flush=True)
    except (ConnectionError, OSError) as exc:
        print(f"[candidate] {peer} error: {exc}", flush=True)
    finally:
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass


class ControlHandler(BaseHTTPRequestHandler):
    def _send(self, code: int, payload: dict):
        body = json.dumps(payload).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):  # noqa: N802
        if self.path != "/control":
            self._send(404, {"error": "not found"})
            return
        self._send(200, snapshot_state())

    def do_POST(self):  # noqa: N802
        if self.path != "/control":
            self._send(404, {"error": "not found"})
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
            patch = json.loads(self.rfile.read(length) or b"{}")
            if not isinstance(patch, dict):
                raise ValueError("body must be an object")
            state = update_state(patch)
        except Exception as exc:
            self._send(400, {"error": f"{type(exc).__name__}: {exc}"})
            return
        print(f"[candidate] control -> {state}", flush=True)
        self._send(200, state)

    def log_message(self, *_args):
        pass


def start_control():
    server = ThreadingHTTPServer(("0.0.0.0", CONTROL_PORT), ControlHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True,
                              name="candidate-control")
    thread.start()
    print(f"[candidate] control on 0.0.0.0:{CONTROL_PORT}", flush=True)
    return server


async def main():
    start_control()
    server = await asyncio.start_server(handle, HOST, PORT)
    addrs = ", ".join(str(s.getsockname()) for s in server.sockets)
    print(f"[candidate] listening on {addrs}", flush=True)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
