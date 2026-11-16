#!/usr/bin/env python3
"""旁路比对端到端自测。

前置（见 README）：
  * 主接收端 127.0.0.1:19000，候选接收端 127.0.0.1:19100（控制口 19101）
  * 代理 API 127.0.0.1:18000，代理 TCP 监听 127.0.0.1:17000

运行：python3 tests/selftest_compare.py
覆盖：一致 / 改字(不同) / 慢读超宽限(不完整) / 断连(不完整) / 候选不可达 /
      64 KiB 旁路缓冲溢出 / 主链路延迟与回执不受旁路影响 / 试传返回连接号。
"""
from __future__ import annotations

import asyncio
import hashlib
import os
import sys
import time

import httpx

WEB = os.environ.get("WEB_URL", "http://127.0.0.1:18000")
PROXY_HOST = os.environ.get("PROXY_HOST", "127.0.0.1")
PROXY_PORT = int(os.environ.get("PROXY_PORT", "17000"))
TARGET = os.environ.get("TARGET", "127.0.0.1:19000")
CANDIDATE = os.environ.get("CANDIDATE", "127.0.0.1:19100")
CONTROL = os.environ.get("CANDIDATE_CONTROL", "http://127.0.0.1:19101")
TARGET_HOST, TARGET_PORT = TARGET.rsplit(":", 1)
CAND_HOST, CAND_PORT = CANDIDATE.rsplit(":", 1)

failures: list[str] = []


def check(name: str, cond: bool, detail: str = ""):
    print(f"[{'ok ' if cond else 'FAIL'}] {name}" + (f"  {detail}" if detail else ""))
    if not cond:
        failures.append(name)


async def set_cfg(client: httpx.AsyncClient, *, up: float, down: float,
                  compare: bool, cand_host: str = CAND_HOST,
                  cand_port: int | None = None):
    r = await client.post("/api/config", json={
        "listen_host": "0.0.0.0", "listen_port": PROXY_PORT,
        "target_host": TARGET_HOST, "target_port": int(TARGET_PORT),
        "up_delay_ms": up, "down_delay_ms": down,
        "compare_enabled": compare,
        "candidate_host": cand_host,
        "candidate_port": int(cand_port if cand_port is not None else CAND_PORT)})
    r.raise_for_status()
    d = r.json()
    assert d["listening"], d
    return d


async def set_candidate(**patch):
    async with httpx.AsyncClient(base_url=CONTROL, timeout=5) as c:
        r = await c.post("/control", json=patch)
        r.raise_for_status()
        return r.json()


async def upload(client: httpx.AsyncClient, payload: bytes, name="blob.bin"):
    t0 = time.monotonic()
    r = await client.post("/api/test-upload", files={"file": (name, payload)},
                          timeout=httpx.Timeout(300.0, connect=10.0))
    dt = time.monotonic() - t0
    r.raise_for_status()
    return r.json(), dt


async def wait_compare(client: httpx.AsyncClient, conn_id: int,
                       timeout: float = 20.0):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        r = await client.get(f"/api/connections/{conn_id}")
        if r.status_code == 200:
            last = r.json()
            st = (last.get("compare") or {}).get("status")
            if st not in ("pending", None):
                return last
        await asyncio.sleep(0.2)
    return last


async def main():
    async with httpx.AsyncClient(base_url=WEB, timeout=httpx.Timeout(300.0, connect=10.0)) as client:
        await set_candidate(mutate=False, slow_read=False, disconnect=False,
                            slow_read_ms=0)
        await set_cfg(client, up=100, down=100, compare=True)

        # 1. 一致：两端下行流（欢迎行 + 回执）应完全相同
        payload = os.urandom(1024 * 1024)
        res, dt_main = await upload(client, payload)
        check("match: main upload ok", res.get("ok") is True, str(res)[:200])
        check("match: test-upload returns conn_id", isinstance(res.get("conn_id"), int),
              str(res.get("conn_id")))
        check("match: upload does not wait for slow compare",
              res.get("compare") is None or res["compare"]["status"] in
              ("pending", "match", "disabled"), str(res.get("compare"))[:120])
        conn = await wait_compare(client, res["conn_id"])
        cmp = (conn or {}).get("compare") or {}
        check("match: compare status match", cmp.get("status") == "match",
              f"{cmp.get('status')} {cmp.get('reason')}")
        md, cd = cmp.get("main_down") or {}, cmp.get("candidate_down") or {}
        check("match: both streams complete", md.get("complete") and cd.get("complete"),
              str(cmp)[:300])
        check("match: byte counts equal and >0",
              md.get("bytes") == cd.get("bytes") and (md.get("bytes") or 0) > 0,
              f"{md.get('bytes')} vs {cd.get('bytes')}")
        check("match: sha256 equal", md.get("sha256") == cd.get("sha256"),
              f"{md.get('sha256')} vs {cd.get('sha256')}")
        check("match: candidate upstream bytes == payload",
              (cmp.get("up") or {}).get("bytes") == len(payload),
              str(cmp.get("up")))
        check("match: candidate buffer peak <= 64 KiB",
              0 < (cmp.get("up") or {}).get("peak", 0) <= 65536,
              str((cmp.get("up") or {}).get("peak")))

        # 2. 改字：候选回执 SHA-256 改一个字符，字节数不变 -> 不同
        await set_candidate(mutate=True)
        res, _ = await upload(client, payload)
        conn = await wait_compare(client, res["conn_id"])
        cmp = (conn or {}).get("compare") or {}
        md, cd = cmp.get("main_down") or {}, cmp.get("candidate_down") or {}
        check("mutate: status mismatch", cmp.get("status") == "mismatch",
              f"{cmp.get('status')} {cmp.get('reason')}")
        check("mutate: byte counts equal", md.get("bytes") == cd.get("bytes"),
              f"{md.get('bytes')} vs {cd.get('bytes')}")
        check("mutate: sha256 differ", md.get("sha256") != cd.get("sha256"))
        check("mutate: main receipt still correct",
              hashlib.sha256(payload).hexdigest() in (res.get("receipt") or ""))

        # 3. 慢读：候选每块 2s，1 MiB 远超 5s 宽限 -> 不完整；主链路不受影响
        await set_candidate(mutate=False, slow_read=True, slow_read_ms=2000)
        res, dt_slow = await upload(client, payload)
        check("slow: main upload still ok", res.get("ok") is True, str(res)[:200])
        check("slow: main receipt unaffected",
              hashlib.sha256(payload).hexdigest() in (res.get("receipt") or ""))
        check("slow: main latency unaffected by slow candidate", dt_slow < 8.0,
              f"{dt_slow:.2f}s")
        conn = await wait_compare(client, res["conn_id"], timeout=15)
        cmp = (conn or {}).get("compare") or {}
        check("slow: status incomplete", cmp.get("status") == "incomplete",
              f"{cmp.get('status')} {cmp.get('reason')}")
        check("slow: reason mentions grace/未结束",
              "5s" in (cmp.get("reason") or "") or "未结束" in (cmp.get("reason") or ""),
              str(cmp.get("reason")))
        check("slow: never judged match", cmp.get("status") != "match")

        # 4. 断连：候选收到首块即断开 -> 不完整，主链路正常
        await set_candidate(slow_read=False, slow_read_ms=0, disconnect=True)
        res, _ = await upload(client, payload)
        check("disconnect: main upload ok", res.get("ok") is True,
              str(res.get("error")))
        check("disconnect: main receipt correct",
              hashlib.sha256(payload).hexdigest() in (res.get("receipt") or ""))
        conn = await wait_compare(client, res["conn_id"], timeout=15)
        cmp = (conn or {}).get("compare") or {}
        check("disconnect: status incomplete", cmp.get("status") == "incomplete",
              f"{cmp.get('status')} {cmp.get('reason')}")
        check("disconnect: never judged match", cmp.get("status") != "match")

        # 5. 候选不可达：连接失败 -> 不完整；主链路照常
        await set_candidate(disconnect=False)
        await set_cfg(client, up=100, down=100, compare=True, cand_port=1)
        res, _ = await upload(client, payload)
        check("unreachable: main upload ok", res.get("ok") is True,
              str(res.get("error")))
        conn = await wait_compare(client, res["conn_id"], timeout=15)
        cmp = (conn or {}).get("compare") or {}
        check("unreachable: status incomplete", cmp.get("status") == "incomplete",
              f"{cmp.get('status')} {cmp.get('reason')}")
        check("unreachable: reason mentions connect failure",
              "候选" in (cmp.get("reason") or ""), str(cmp.get("reason")))

        # 6. 旁路缓冲溢出：候选极慢，64 KiB 待发上限触发中止
        await set_cfg(client, up=0, down=0, compare=True)
        await set_candidate(slow_read=True, slow_read_ms=10000)
        big = os.urandom(2 * 1024 * 1024)
        res, dt_overflow = await upload(client, big)
        check("overflow: main upload ok", res.get("ok") is True, str(res.get("error")))
        conn = await wait_compare(client, res["conn_id"], timeout=15)
        cmp = (conn or {}).get("compare") or {}
        check("overflow: status incomplete", cmp.get("status") == "incomplete",
              f"{cmp.get('status')} {cmp.get('reason')}")
        check("overflow: reason mentions 64 KiB buffer",
              "64" in (cmp.get("reason") or ""), str(cmp.get("reason")))

        # 7. 配置快照：连接建立时快照候选地址；中途改配置不影响已建立的连接
        await set_candidate(slow_read=False, slow_read_ms=0)
        await set_cfg(client, up=200, down=200, compare=True)
        reader, writer = await asyncio.open_connection(PROXY_HOST, PROXY_PORT)
        await asyncio.wait_for(reader.readline(), 30)  # 主端欢迎行
        writer.write(b"snapshot-compare")
        await writer.drain()
        # 此时把候选地址改成一个不可达端口；该连接仍应沿用建立时的候选
        await set_cfg(client, up=200, down=200, compare=True, cand_port=1)
        writer.write_eof()
        await asyncio.wait_for(reader.read(), 30)
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass
        # 用 API 找到这条连接（上行 16 字节）
        conn = None
        for _ in range(50):
            d = (await client.get("/api/connections")).json()
            for c in d["active"] + d["history"]:
                if c["up"]["bytes"] == 16 and c["compare"]["enabled"]:
                    conn = c
                    break
            if conn:
                break
            await asyncio.sleep(0.1)
        conn = await wait_compare(client, conn["id"], timeout=15) if conn else None
        cmp = (conn or {}).get("compare") or {}
        check("snapshot: compare used establish-time candidate",
              cmp.get("candidate") == f"{CAND_HOST}:{CAND_PORT}",
              str(cmp.get("candidate")))
        check("snapshot: status match despite later bad candidate config",
              cmp.get("status") == "match", f"{cmp.get('status')} {cmp.get('reason')}")

        # 8. 恢复默认：关闭比对后上传仍正常，且不发起旁路
        await set_candidate(slow_read=False, slow_read_ms=0)
        await set_cfg(client, up=100, down=100, compare=False)
        res, _ = await upload(client, b"small-payload")
        conn = await wait_compare(client, res["conn_id"], timeout=5)
        check("disabled: main upload ok", res.get("ok") is True)
        check("disabled: compare not enabled",
              ((conn or {}).get("compare") or {}).get("status") == "disabled",
              str((conn or {}).get("compare")))

        await set_cfg(client, up=300, down=300, compare=True)

    print()
    if failures:
        print(f"FAILED: {len(failures)} check(s): {failures}")
        sys.exit(1)
    print("ALL COMPARE CHECKS PASSED")


if __name__ == "__main__":
    asyncio.run(main())
