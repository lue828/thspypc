"""板块通道注册保活：模仿真实客户端 94 页面打开期间的通道流量形态。

抓包结论（board94_20260909_capture.pcap，144s，2026-09-16 分析）：

- 同花顺客户端的板块指数（fu4）与成分股（shlv2/szlv2）通道上**没有
  tsi0= 心跳**（心跳只存在于 MAIN 车道，3s 一帧）；
- 真实客户端靠持续的业务流量保温：94 页面打开期间在成分股通道上高频
  重发 subreal/pageid 注册（每秒约 8--10 帧），服务端持续推送数据帧。

活网实验（2026-09-16，szlv2 成分股通道）：**注册帧本身续不了命**——
无论每 10s 喂 pageid 单帧还是 subreal×5+pageid 混合帧，服务端都在最后
一次业务查询后约 50s 强制 RST 连接（WinError 10053）。fu4 家族的闲置
回收器只认业务查询；真实客户端的通道 144s 存活是因为整页持续轮询，
不是因为注册帧。

因此本组件的实际职责是（按有效性排序）：

1. **死亡探测**：每 ``FEED_IDLE_SECONDS`` 向空闲通道发送一帧注册
   （bootstrap 二次注册阶段同款帧型，try_send 非阻塞抢锁、不读响应，
   绝不与业务请求争车道）。发送失败 = socket 已被服务端回收 → 立即
   关闭，下一次业务请求走全新建连（数秒拿全量数据），而不是把查询
   发到僵尸连接上等满超时（2026-09-16 之前的 45s 空列表故障形态）。
2. **注册流量**：在不对注册帧计费/计量的网关上维持会话状态（fu4 板块
   指数通道行为未单独实验，保持与真实客户端一致的帧型无害且可能保温）。
3. **长期闲置拆除**：空闲超过 ``MAX_IDLE_SECONDS`` 主动关闭，对应真实
   客户端离开 94 页面后的连接拆除。上限 15 分钟覆盖休市期前端 10 分钟
   一次的成分股心跳轮询（页面开着不拆；盘中 15s 轮询本身就在保温）。
"""
from __future__ import annotations

import logging
import threading
import time
from typing import Callable

from ..codecs.framing import encode_frame
from ..features.system_blocks_protocol import build_board_pageid_register
from ..models import AccountKind
from .._transport.connection import ConnectionRole
from .._transport.connection_manager import ConnectionManager

logger = logging.getLogger(__name__)


class BoardChannelKeepalive:
    """按真实客户端流量形态保温板块专用通道的守护线程。

    只服务板块三角色：``BOARD``（fu4 板块指数）、
    ``BOARD_CONSTITUENT_SH``/``_SZ``（成分股独立连接）。MAIN 家族
    （MAIN/KLINE_FAST/BSE_MAIN/L2）已有各自的心跳/探活路径，不在本组件
    职责内。
    """

    ROLES = (
        ConnectionRole.BOARD,
        ConnectionRole.BOARD_CONSTITUENT_SH,
        ConnectionRole.BOARD_CONSTITUENT_SZ,
    )
    # 轮询节拍；真实客户端最大自然空隙 14--23s，喂帧间隔取 10s 足够安全。
    POLL_SECONDS = 2.0
    FEED_IDLE_SECONDS = 10.0
    # 休市期前端成分股轮询间隔为 10 分钟（IDLE_POLL_MS），上限需覆盖它，
    # 否则每次轮询都要付一次重建成本；页面关闭后 15 分钟内拆除。
    MAX_IDLE_SECONDS = 900.0

    def __init__(
        self,
        manager_getter: "Callable[[], ConnectionManager | None]",
    ) -> None:
        self._manager_getter = manager_getter
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()
        # 每角色上次成功喂帧时刻：业务空闲判定基于 last_request，喂帧本身
        # 不刷新它，这里单独记录避免每个轮询节拍都发一帧。
        self._last_feed: dict[ConnectionRole, float] = {}

    def start(self) -> None:
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return
            self._stop.clear()
            self._thread = threading.Thread(
                target=self._run,
                name="ths-board-keepalive",
                daemon=True,
            )
            self._thread.start()
            logger.debug("板块通道保活线程已启动")

    def stop(self) -> None:
        self._stop.set()
        with self._lock:
            thread, self._thread = self._thread, None
        if thread is not None and thread.is_alive():
            thread.join(timeout=3)

    def _run(self) -> None:
        while not self._stop.wait(self.POLL_SECONDS):
            for role in self.ROLES:
                if self._stop.is_set():
                    return
                try:
                    self._service_role(role)
                except Exception as exc:  # noqa: BLE001 - 保活尽力而为
                    logger.debug(
                        "板块通道保活 %s 异常: %s", role.value, exc
                    )

    def _service_role(self, role: ConnectionRole) -> None:
        manager = self._manager_getter()
        if manager is None:
            return
        connection = manager.peek(role)
        if connection is None:
            return
        idle = connection.idle_seconds
        if idle >= self.MAX_IDLE_SECONDS:
            logger.info(
                "板块通道 %s 业务空闲 %.0f 秒，主动拆除（下次请求重建）",
                role.value,
                idle,
            )
            manager.close(role)
            return
        if idle < self.FEED_IDLE_SECONDS:
            return
        now = time.monotonic()
        if now - self._last_feed.get(role, 0.0) < self.FEED_IDLE_SECONDS:
            return
        level2 = manager.profile.kind is AccountKind.LEVEL2
        frame = encode_frame(
            build_board_pageid_register(level2, repeats=1)
        )
        try:
            sent = connection.try_send(frame)
        except OSError as exc:
            # 发送失败 = socket 已死；关闭让下次 acquire 重建，而不是把
            # 业务查询发到僵尸连接上等满超时。
            logger.info(
                "板块通道 %s 保活发送失败（%s），关闭待重建",
                role.value,
                exc,
            )
            manager.close(role)
            return
        if sent:
            self._last_feed[role] = now
            logger.debug(
                "板块通道 %s 空闲 %.0f 秒，已发送注册保活帧",
                role.value,
                idle,
            )
        # sent=False：业务请求正占着车道——业务流量本身就在保温，下个
        # 节拍再探测。
