"""TCP 延迟代理引擎（纯 asyncio，无第三方依赖）。

实现的语义：
* 延迟从代理读到字节时起算：每个数据块获得各自的投递时刻
  (read_time + delay)，按 FIFO 顺序写出，连续数据流水通过延迟线，
  不会按读取块数累加等待。
* 每个方向的应用缓冲（已读未写）上限 64 KiB；缓冲满时只暂停该方向的
  读取，反向及本机其他连接照常传输。
* 连接建立时快照配置，整条连接沿用，之后改配置不影响它。
* 收到单向 EOF 后，先把该方向队列中待发字节全部写出，再转发半关闭
  (write_eof)；反向方向继续传输直到结束。
* 断连（正常结束或对端 RST）时释放队列并关闭两侧连接。

旁路比对（compare）：
* 候选地址与开关在连接建立时快照；之后修改配置不影响已建立的连接。
* 主链路把客户端上行数据复制给候选接收端；客户端只收主端响应，主链路
  的延迟与半关闭语义不变。
* 旁路有独立连接、任务与 64 KiB 待发应用缓冲。候选慢读、连接失败或写入
  报错只会中止旁路并标为“不完整”，不会阻塞或中断主链路。
* 旁路未中止时，先把复制的上行字节全部发给候选，再转发 EOF；若待发缓冲
  将超过 64 KiB，立即中止旁路并标记“不完整”。
* 两端下行流在读取时增量计算字节数与 SHA-256（不保存全量响应），TCP
  分块方式不影响结果。
* 主连接结束后最多再等 5 秒；候选届时仍未结束则清理旁路并标记“不完整”。
* 只有两端下行流都完整、双方均无异常，且字节数与 SHA-256 都相同才判
  “一致”；任一端异常都不得判一致。
"""
from __future__ import annotations

import asyncio
import dataclasses
import hashlib
import time
from collections import deque
from dataclasses import dataclass, field

BUFFER_LIMIT = 64 * 1024   # 每方向应用缓冲上限
READ_CHUNK = 16 * 1024     # 单次 read() 最大字节数
TAIL_KEEP = 4096           # 下行数据尾部保留长度（回执预览）
HISTORY_MAX = 100          # 历史连接保留条数
CONNECT_TIMEOUT = 15.0     # 连接主目标的超时
CANDIDATE_CONNECT_TIMEOUT = 3.0   # 连接候选端超时（旁路独立，不阻塞主链路）
CANDIDATE_BUFFER_LIMIT = 64 * 1024  # 旁路待发应用缓冲上限
COMPARE_GRACE = 5.0        # 主连接结束后等待候选的最长时间

# 比对状态
CMP_DISABLED = "disabled"
CMP_PENDING = "pending"
CMP_MATCH = "match"
CMP_MISMATCH = "mismatch"
CMP_INCOMPLETE = "incomplete"


@dataclass
class ProxyConfig:
    listen_host: str = "0.0.0.0"
    listen_port: int = 7000
    target_host: str = "127.0.0.1"
    target_port: int = 9000
    up_delay_ms: float = 300.0    # 客户端 -> 目标
    down_delay_ms: float = 300.0  # 目标 -> 客户端
    compare_enabled: bool = False
    candidate_host: str = ""
    candidate_port: int = 0


@dataclass
class DirectionStats:
    bytes: int = 0    # 已读累计字节（旁路为已写入候选的字节）
    peak: int = 0     # 应用缓冲峰值
    queued: int = 0   # 当前缓冲字节


@dataclass
class StreamSummary:
    """一条下行流的增量摘要（不保存响应本体）。"""
    bytes: int = 0
    sha256: str | None = None
    complete: bool = False
    error: str | None = None


@dataclass
class CompareInfo:
    """单条连接的旁路比对状态。

    status:
      disabled   未启用
      pending    主连接尚未结束 / 候选尚未完成
      match      两端下行流完整且字节数、SHA-256 都相同
      mismatch   两端都完整，但字节数或 SHA-256 不同
      incomplete 任一端异常、旁路中止或候选未在宽限期内结束
    """
    enabled: bool = False
    status: str = CMP_DISABLED
    reason: str | None = None
    candidate: str = ""
    copied: int = 0                 # 已从主链路上行复制的字节
    up: DirectionStats = field(default_factory=DirectionStats)  # 候选上行
    main_down: StreamSummary = field(default_factory=StreamSummary)
    candidate_down: StreamSummary = field(default_factory=StreamSummary)
    started_at: float | None = None
    ended_at: float | None = None


@dataclass
class ConnInfo:
    id: int
    client: str
    cfg: ProxyConfig  # 建立时的配置快照
    started_at: float = field(default_factory=time.time)
    ended_at: float | None = None
    state: str = "active"  # active | closed | error
    error: str | None = None
    up: DirectionStats = field(default_factory=DirectionStats)    # 客户端 -> 目标
    down: DirectionStats = field(default_factory=DirectionStats)  # 目标 -> 客户端
    downlink_tail: bytes = b""  # 下行数据尾部（回执预览）
    compare: CompareInfo = field(default_factory=CompareInfo)


class DirectionPump:
    """单向延迟管道：从 reader 读，延迟后写入 writer。"""

    def __init__(self, name, reader, writer, delay_s, stats,
                 on_data=None, on_read=None, on_eof=None, on_error=None,
                 on_eof_sent=None):
        self.name = name
        self.reader = reader
        self.writer = writer
        self.delay = delay_s
        self.stats = stats
        self.on_data = on_data    # 写出后的回调（用于抓回执）
        self.on_read = on_read    # 读到后的回调（复制给旁路）
        self.on_eof = on_eof      # 读到 EOF 的回调（通知旁路：该向上行已读完）
        self.on_error = on_error  # 读写异常的回调（旁路标记不完整）
        self.on_eof_sent = on_eof_sent  # EOF 已转发后的回调（下行流定稿）
        self._queue: deque[tuple[float, bytes | None]] = deque()  # None = EOF 哨兵
        self._queued = 0
        self._space = asyncio.Event()   # 缓冲未满
        self._space.set()
        self._items = asyncio.Event()   # 队列非空

    async def run(self):
        rx = asyncio.create_task(self._read_loop(), name=f"pump-{self.name}-rx")
        tx = asyncio.create_task(self._write_loop(), name=f"pump-{self.name}-tx")
        try:
            await asyncio.gather(rx, tx)
        finally:
            for t in (rx, tx):
                t.cancel()
            await asyncio.gather(rx, tx, return_exceptions=True)
            # 断连/结束：释放队列
            self._queue.clear()
            self._queued = 0
            self.stats.queued = 0

    @staticmethod
    def _notify(cb, *args):
        if cb is None:
            return
        try:
            cb(*args)
        except Exception:
            # 回调（例如旁路复制）绝不能中断主链路
            pass

    async def _read_loop(self):
        loop = asyncio.get_running_loop()
        try:
            while True:
                await self._space.wait()  # 缓冲满则暂停该向读取（反向不受影响）
                max_read = min(READ_CHUNK, BUFFER_LIMIT - self._queued)
                chunk = await self.reader.read(max_read)
                ready_at = loop.time() + self.delay  # 延迟自读到字节时起算
                if chunk:
                    if self.on_read is not None:
                        self._notify(self.on_read, chunk)
                        # 让旁路写出任务有机会运行：主链路自身从不让读循环
                        # 连续占用事件循环（数据就绪时 read 不再让出），否则
                        # 一次突发就能把旁路 64 KiB 待发缓冲顶溢出。
                        await asyncio.sleep(0)
                    self._queue.append((ready_at, chunk))
                    self._queued += len(chunk)
                    self.stats.bytes += len(chunk)
                    self.stats.queued = self._queued
                    if self._queued > self.stats.peak:
                        self.stats.peak = self._queued
                    if self._queued >= BUFFER_LIMIT:
                        self._space.clear()
                    self._items.set()
                else:
                    self._notify(self.on_eof)
                    # EOF 哨兵入队：半关闭只在该向待发字节全部写出后转发
                    self._queue.append((ready_at, None))
                    self._items.set()
                    return
        except Exception as exc:
            self._notify(self.on_error, exc)
            raise

    async def _write_loop(self):
        loop = asyncio.get_running_loop()
        try:
            while True:
                while not self._queue:
                    self._items.clear()
                    await self._items.wait()
                ready_at, chunk = self._queue[0]
                wait = ready_at - loop.time()
                if wait > 0:
                    # 只等到队首块到期；后续块各自带投递时刻，流水发送
                    await asyncio.sleep(wait)
                self._queue.popleft()
                if chunk is None:
                    # 该向待发字节已全部写出 -> 转发半关闭
                    try:
                        if self.writer.can_write_eof():
                            self.writer.write_eof()
                            await self.writer.drain()
                        else:
                            self.writer.close()
                    except (ConnectionError, OSError, RuntimeError) as exc:
                        # 半关闭转发失败也算该向异常（旁路据此不得判一致）
                        self._notify(self.on_error, exc)
                    else:
                        self._notify(self.on_eof_sent)
                    return
                self.writer.write(chunk)
                await self.writer.drain()
                self._queued -= len(chunk)
                self.stats.queued = self._queued
                if self.on_data is not None:
                    self.on_data(chunk)
                if self._queued < BUFFER_LIMIT:
                    self._space.set()
        except Exception as exc:
            self._notify(self.on_error, exc)
            raise


class CompareSession:
    """旁路比对：复制主链路上行给候选端，比两端下行流的摘要。

    独立任务运行；所有失败路径（连接失败、慢读、写错误、缓冲溢出、宽限
    超时）都只结束旁路并把状态置为 incomplete，绝不向主链路抛异常或背压。
    """

    def __init__(self, info: CompareInfo, cfg: ProxyConfig):
        self.info = info
        self.cfg = cfg
        self.reader: asyncio.StreamReader | None = None
        self.writer: asyncio.StreamWriter | None = None
        self._queue: deque[bytes] = deque()
        self._queued = 0
        self._eof = False
        self._done = asyncio.Event()
        self._more = asyncio.Event()
        self._main_down = asyncio.Event()
        self._task: asyncio.Task | None = None
        self._sum = hashlib.sha256()
        info.enabled = True
        info.status = CMP_PENDING
        info.candidate = f"{cfg.candidate_host}:{cfg.candidate_port}"
        info.started_at = time.time()

    # ---------- 主链路侧（同步、非阻塞） ----------

    def copy_upstream(self, chunk: bytes) -> None:
        """主链路上行读取回调：有界复制。溢出立即中止旁路。"""
        if self._done.is_set() or self._eof:
            return
        self.info.copied += len(chunk)
        if self._queued + len(chunk) > CANDIDATE_BUFFER_LIMIT:
            self.abort(f"旁路待发缓冲超过 64 KiB（{CANDIDATE_BUFFER_LIMIT} 字节）")
            return
        self._queue.append(chunk)
        self._queued += len(chunk)
        self.info.up.queued = self._queued
        if self._queued > self.info.up.peak:
            self.info.up.peak = self._queued
        self._more.set()

    def main_upstream_eof(self) -> None:
        """主链路上行 EOF（客户端半关闭）：旁路发完已复制数据后再 EOF。"""
        if not self._done.is_set():
            self._eof = True
            self._more.set()

    def main_upstream_error(self, exc: BaseException) -> None:
        """主链路上行读写出错：无法确认候选收到完整上行，不得判一致。"""
        self.abort(f"主端上行异常：{type(exc).__name__}: {exc}")

    def main_downlink_chunk(self, chunk: bytes) -> None:
        s = self.info.main_down
        s.bytes += len(chunk)
        self._sum.update(chunk)

    def main_downlink_eof(self) -> None:
        s = self.info.main_down
        if s.error is None:
            s.sha256 = self._sum.hexdigest()
            s.complete = True
        self._main_down.set()

    def main_downlink_error(self, exc: BaseException) -> None:
        s = self.info.main_down
        if s.error is None:
            s.error = f"{type(exc).__name__}: {exc}"
        self._main_down.set()

    def main_link_closed(self) -> None:
        """主连接已结束（两侧已关闭）。"""
        self._main_down.set()
        self._more.set()
        if not self._eof:
            # 没有见到上行 EOF，无法确认候选收到了完整上行、也无从转发 EOF
            self.abort("主连接在转发行 EOF 前结束，旁路不完整")

    # ---------- 生命周期 ----------

    def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name="compare")

    def abort(self, reason: str) -> None:
        """中止并标记不完整；幂等。只清理旁路自己的资源。"""
        if self._done.is_set():
            return
        self.info.status = CMP_INCOMPLETE
        self.info.reason = reason
        self._queue.clear()
        self._queued = 0
        self.info.up.queued = 0
        self.info.ended_at = time.time()
        self._done.set()
        self._more.set()
        task, self._task = self._task, None
        if task is not None and task is not asyncio.current_task() and not task.done():
            task.cancel()
        self._close_candidate()

    async def wait(self, timeout: float) -> bool:
        """等待旁路结束；超时返回 False（不抛异常）。"""
        try:
            await asyncio.wait_for(self._done.wait(), timeout=timeout)
            return True
        except asyncio.TimeoutError:
            return False

    async def _run(self) -> None:
        try:
            await self._connect()
            if self._done.is_set():
                return
            rx = asyncio.create_task(self._read_candidate(), name="compare-rx")
            try:
                await self._write_upstream()
                if not self._done.is_set():
                    # 上行写完/半关闭后，继续读到候选 EOF
                    await rx
                if not self._done.is_set():
                    # 两端都完整后才可比较：等主端下行流给出最终结果
                    await self._main_down.wait()
            finally:
                if not rx.done():
                    rx.cancel()
                    await asyncio.gather(rx, return_exceptions=True)
            if not self._done.is_set():
                self._finalize()
        except asyncio.CancelledError:
            if not self._done.is_set():
                self.abort("旁路被清理")
            raise
        except Exception as exc:
            self.abort(f"旁路异常 {type(exc).__name__}: {exc}")
        finally:
            self._close_candidate()
            if not self._done.is_set():
                self.abort("旁路未完成")
            elif self.info.ended_at is None:
                self.info.ended_at = time.time()

    async def _connect(self) -> None:
        if not self.cfg.candidate_host or not self.cfg.candidate_port:
            self.abort("未配置候选地址")
            return
        try:
            self.reader, self.writer = await asyncio.wait_for(
                asyncio.open_connection(self.cfg.candidate_host,
                                        self.cfg.candidate_port),
                timeout=CANDIDATE_CONNECT_TIMEOUT)
        except asyncio.TimeoutError:
            self.abort(f"连接候选端超时（>{CANDIDATE_CONNECT_TIMEOUT}s）")
        except Exception as exc:
            self.abort(f"连接候选端失败：{type(exc).__name__}: {exc}")
        if self._done.is_set():
            self._close_candidate()

    async def _write_upstream(self) -> None:
        """按复制顺序写出；全部发完后再 write_eof（未中止时）。"""
        while not self._done.is_set():
            if not self._queue:
                if self._eof:
                    break
                self._more.clear()
                if self._queue or self._eof or self._done.is_set():
                    continue
                await self._more.wait()  # 等主链路复制上行；不阻塞主链路
                continue
            chunk = self._queue[0]  # 写出成功前仍计入待发缓冲
            try:
                self.writer.write(chunk)
                await asyncio.wait_for(self.writer.drain(),
                                       timeout=CANDIDATE_CONNECT_TIMEOUT)
            except asyncio.TimeoutError:
                self.abort("候选端慢读：写超时")
                return
            except (ConnectionError, OSError, RuntimeError) as exc:
                self.abort(f"写候选端失败：{type(exc).__name__}: {exc}")
                return
            if self._done.is_set() or not self._queue:
                return  # 等待期间被中止（队列已被清空）
            self._queue.popleft()
            self._queued = max(0, self._queued - len(chunk))
            self.info.up.queued = self._queued
            self.info.up.bytes += len(chunk)
        if self._done.is_set():
            return
        # 复制的上行已全部发出 -> 转发 EOF
        try:
            if self.writer.can_write_eof():
                self.writer.write_eof()
                await asyncio.wait_for(self.writer.drain(),
                                       timeout=CANDIDATE_CONNECT_TIMEOUT)
            else:
                self.writer.close()
        except asyncio.TimeoutError:
            self.abort("候选端慢读：EOF 冲刷超时")
        except (ConnectionError, OSError, RuntimeError) as exc:
            self.abort(f"候选端上行半关闭失败：{type(exc).__name__}: {exc}")

    async def _read_candidate(self) -> None:
        """增量读取候选下行：只记字节数与 SHA-256，不保存全量响应。"""
        s = self.info.candidate_down
        h = hashlib.sha256()
        try:
            while True:
                chunk = await self.reader.read(READ_CHUNK)
                if not chunk:
                    break
                s.bytes += len(chunk)
                h.update(chunk)
            s.sha256 = h.hexdigest()
            s.complete = True
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            # 记下来交给 _finalize 统一判定为“不完整”；不向主链路抛出
            s.error = f"{type(exc).__name__}: {exc}"

    def _finalize(self) -> None:
        info = self.info
        main, cand = info.main_down, info.candidate_down
        if not main.complete:
            self.abort(f"主端下行异常：{main.error or '流不完整'}")
            return
        if not cand.complete or cand.error:
            self.abort(f"候选下行异常：{cand.error or '流不完整'}")
            return
        if main.bytes == cand.bytes and main.sha256 == cand.sha256:
            info.status = CMP_MATCH
            info.reason = None
        else:
            info.status = CMP_MISMATCH
            info.reason = (f"下行不一致：主端 {main.bytes} B / {main.sha256}；"
                           f"候选 {cand.bytes} B / {cand.sha256}")
        info.ended_at = time.time()
        self._done.set()

    def _close_candidate(self) -> None:
        w, self.writer = self.writer, None
        if w is not None:
            try:
                w.close()
            except Exception:
                pass


class Connection:
    """一条代理连接：客户端 <-> 两个 DirectionPump <-> 目标（+ 可选旁路）。"""

    def __init__(self, client_reader, client_writer, cfg: ProxyConfig, conn_id: int):
        self.client_reader = client_reader
        self.client_writer = client_writer
        self.target_reader = None
        self.target_writer = None
        peer = client_writer.get_extra_info("peername")
        client = f"{peer[0]}:{peer[1]}" if peer else "?"
        self.info = ConnInfo(id=conn_id, client=client, cfg=dataclasses.replace(cfg))
        self.compare: CompareSession | None = None

    def _capture_downlink(self, data: bytes):
        tail = self.info.downlink_tail + data
        self.info.downlink_tail = tail[-TAIL_KEEP:]
        if self.compare is not None:
            self.compare.main_downlink_chunk(data)

    async def run(self):
        info = self.info
        cfg = info.cfg
        # 与主目标连接并行建立候选连接，缩小旁路启动窗口，避免主链路快读
        # 在旁路尚未连上时就把 64 KiB 待发缓冲顶溢出。
        compare: CompareSession | None = None
        if cfg.compare_enabled:
            compare = CompareSession(info.compare, cfg)
            compare.start()
            self.compare = compare
        try:
            self.target_reader, self.target_writer = await asyncio.wait_for(
                asyncio.open_connection(cfg.target_host, cfg.target_port),
                timeout=CONNECT_TIMEOUT,
            )
        except Exception as exc:
            info.state = "error"
            info.error = f"connect target {cfg.target_host}:{cfg.target_port} failed: {exc}"
            if compare is not None:
                compare.abort("主连接建立失败，未发起旁路比对")
            self._close(self.client_writer)
            await self._wait_closed(self.client_writer)
            info.ended_at = time.time()
            if compare is not None:
                task = compare._task
                if task is not None and not task.done():
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
            return

        up = DirectionPump("up", self.client_reader, self.target_writer,
                           cfg.up_delay_ms / 1000.0, info.up,
                           on_read=(self.compare.copy_upstream if self.compare else None),
                           on_eof=(self.compare.main_upstream_eof if self.compare else None),
                           on_error=(self.compare.main_upstream_error if self.compare else None))
        down = DirectionPump("down", self.target_reader, self.client_writer,
                             cfg.down_delay_ms / 1000.0, info.down,
                             on_data=self._capture_downlink,
                             on_eof_sent=(self.compare.main_downlink_eof
                                          if self.compare else None),
                             on_error=(self.compare.main_downlink_error
                                       if self.compare else None))
        tasks = [asyncio.create_task(up.run()), asyncio.create_task(down.run())]
        try:
            await asyncio.gather(*tasks)
        except Exception as exc:
            info.state = "error"
            info.error = f"{type(exc).__name__}: {exc}"
            if self.compare is not None:
                self.compare.main_downlink_error(exc)
        finally:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            # 主连接结束：关闭两侧连接（队列已在 pump 内释放）
            self._close(self.client_writer)
            self._close(self.target_writer)
            await self._wait_closed(self.client_writer)
            await self._wait_closed(self.target_writer)
            if info.state == "active":
                info.state = "closed"
            info.ended_at = time.time()
            if self.compare is not None:
                try:
                    self.compare.main_link_closed()
                    await self._finish_compare()
                except asyncio.CancelledError:
                    self.compare.abort("主连接任务被取消")
                    raise

    async def _finish_compare(self):
        """主连接结束后最多再等 COMPARE_GRACE 秒；届时清理并标不完整。"""
        cmp = self.compare
        assert cmp is not None
        if not await cmp.wait(COMPARE_GRACE):
            cmp.abort(f"主连接结束后 {COMPARE_GRACE:g}s 内候选未结束")
        # 主端下行流的最终状态（EOF / 异常）必须参与判定
        if cmp.info.status == CMP_PENDING:
            cmp.abort("旁路状态未定")
        # 清理任务（禁止悬挂的旁路任务）
        task = cmp._task
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
    @staticmethod
    def _close(writer):
        if writer is not None:
            try:
                writer.close()
            except Exception:
                pass

    @staticmethod
    async def _wait_closed(writer):
        if writer is not None:
            try:
                await asyncio.wait_for(writer.wait_closed(), timeout=5)
            except Exception:
                pass


class DelayProxy:
    """监听 TCP 端口，为每条连接建立到目标的延迟管道。"""

    def __init__(self, cfg: ProxyConfig):
        self.cfg = cfg
        self.server: asyncio.Server | None = None
        self.listen_error: str | None = None
        self.active: dict[int, Connection] = {}
        self.history: deque[ConnInfo] = deque(maxlen=HISTORY_MAX)
        self._next_id = 1
        self._tasks: set[asyncio.Task] = set()
        self._lock = asyncio.Lock()

    @property
    def listening(self) -> bool:
        return self.server is not None and self.server.is_serving()

    async def apply_config(self, new_cfg: ProxyConfig):
        """应用新配置；仅影响之后建立的连接。监听地址/端口变化时重启监听器。"""
        async with self._lock:
            restart = (
                self.server is None
                or new_cfg.listen_host != self.cfg.listen_host
                or new_cfg.listen_port != self.cfg.listen_port
            )
            self.cfg = dataclasses.replace(new_cfg)
            if not restart:
                return
            if self.server is not None:
                self.server.close()
                await self.server.wait_closed()
                self.server = None
            self.listen_error = None
            try:
                self.server = await asyncio.start_server(
                    self._on_client, new_cfg.listen_host, new_cfg.listen_port)
            except Exception as exc:
                self.listen_error = str(exc)

    def _on_client(self, reader, writer):
        cfg = dataclasses.replace(self.cfg)  # 建立时快照配置
        conn = Connection(reader, writer, cfg, self._next_id)
        self._next_id += 1
        cid = conn.info.id
        self.active[cid] = conn
        task = asyncio.create_task(conn.run(), name=f"conn-{cid}")
        self._tasks.add(task)
        task.add_done_callback(lambda t: (self._tasks.discard(t), self._finish(cid)))

    def _finish(self, cid: int):
        conn = self.active.pop(cid, None)
        if conn is not None:
            if conn.info.compare.enabled and conn.info.compare.status == CMP_PENDING:
                conn.info.compare.status = CMP_INCOMPLETE
                conn.info.compare.reason = "连接结束但旁路未定"
            self.history.appendleft(conn.info)

    async def shutdown(self):
        if self.server is not None:
            self.server.close()
            await self.server.wait_closed()
            self.server = None
        tasks = list(self._tasks)
        for t in tasks:
            t.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    def snapshot(self) -> dict:
        def stream_dict(s: StreamSummary) -> dict:
            return dataclasses.asdict(s)

        def compare_dict(c: CompareInfo) -> dict:
            return {
                "enabled": c.enabled,
                "status": c.status,
                "reason": c.reason,
                "candidate": c.candidate,
                "copied": c.copied,
                "up": dataclasses.asdict(c.up),
                "main_down": stream_dict(c.main_down),
                "candidate_down": stream_dict(c.candidate_down),
                "started_at": c.started_at,
                "ended_at": c.ended_at,
            }

        def conn_dict(info: ConnInfo) -> dict:
            return {
                "id": info.id,
                "client": info.client,
                "state": info.state,
                "error": info.error,
                "target": f"{info.cfg.target_host}:{info.cfg.target_port}",
                "up_delay_ms": info.cfg.up_delay_ms,
                "down_delay_ms": info.cfg.down_delay_ms,
                "started_at": info.started_at,
                "ended_at": info.ended_at,
                "up": dataclasses.asdict(info.up),
                "down": dataclasses.asdict(info.down),
                "receipt": info.downlink_tail.decode("utf-8", "replace"),
                "compare": compare_dict(info.compare),
            }

        return {
            "listening": self.listening,
            "listen_error": self.listen_error,
            "config": dataclasses.asdict(self.cfg),
            "active": [conn_dict(c.info) for c in self.active.values()],
            "history": [conn_dict(i) for i in self.history],
        }
