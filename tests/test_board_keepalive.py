"""板块通道注册保活（模仿真实客户端 94 页面流量形态，2026-09-16）。"""

import time

from thspypc._client.board_keepalive import BoardChannelKeepalive
from thspypc._transport import ConnectionManager, ConnectionRole
from thspypc.codecs.framing import encode_frame
from thspypc.features.system_blocks_protocol import build_board_pageid_register
from thspypc.models import AccountKind, AccountProfile, Capability, Support


class FakeSocket:
    def __init__(self):
        self.sent = []
        self.timeout = None
        self.closed = False

    def settimeout(self, value):
        self.timeout = value

    def sendall(self, data):
        if self.closed:
            raise OSError("send on closed socket")
        self.sent.append(data)

    def close(self):
        self.closed = True


def _manager(kind, sock):
    capabilities = {Capability.BASIC_QUOTE: Support.YES}
    if kind is AccountKind.LEVEL2:
        capabilities[Capability.L2_MARKET_ACCESS] = Support.YES
    return ConnectionManager(
        AccountProfile(kind=kind, capabilities=capabilities),
        lambda _spec: sock,
    )


def _keepalive(manager):
    return BoardChannelKeepalive(lambda: manager)


def _adopt(manager, role, sock):
    manager.adopt(
        role, sock, capability=Capability.BASIC_QUOTE, owns_socket=True
    )
    return manager.peek(role)


def _set_idle(connection, seconds):
    connection._session.last_request = time.monotonic() - seconds


def test_idle_channel_gets_registration_frame():
    sock = FakeSocket()
    manager = _manager(AccountKind.LEVEL2, sock)
    connection = _adopt(manager, ConnectionRole.BOARD, sock)
    _set_idle(connection, 20.0)

    ka = _keepalive(manager)
    ka._service_role(ConnectionRole.BOARD)

    assert len(sock.sent) == 1
    frame = sock.sent[0]
    # FD 帧封包 + 尾部换行；body 是注册帧（L2=pageid 5716）
    assert frame == encode_frame(
        build_board_pageid_register(True, repeats=1)
    ) + b"\n"
    assert manager.peek(ConnectionRole.BOARD) is not None


def test_active_channel_not_fed():
    sock = FakeSocket()
    manager = _manager(AccountKind.LEVEL2, sock)
    connection = _adopt(manager, ConnectionRole.BOARD_CONSTITUENT_SH, sock)
    _set_idle(connection, 2.0)

    ka = _keepalive(manager)
    ka._service_role(ConnectionRole.BOARD_CONSTITUENT_SH)

    assert sock.sent == []


def test_feed_respects_interval_between_frames():
    sock = FakeSocket()
    manager = _manager(AccountKind.LEVEL2, sock)
    connection = _adopt(manager, ConnectionRole.BOARD, sock)
    _set_idle(connection, 30.0)

    ka = _keepalive(manager)
    ka._service_role(ConnectionRole.BOARD)
    ka._service_role(ConnectionRole.BOARD)  # 立即再来一轮：不应重复喂帧

    assert len(sock.sent) == 1


def test_business_traffic_resets_idle_and_skips_feeding():
    sock = FakeSocket()
    manager = _manager(AccountKind.STANDARD, sock)
    connection = _adopt(manager, ConnectionRole.BOARD, sock)

    ka = _keepalive(manager)
    # 业务请求刷新 last_request（MarketSession.request 发送后更新）
    with connection.request(b"\xfd\xfd\xfd\xfd00000001x", timeout=1.0):
        pass
    assert connection.idle_seconds < 5

    ka._service_role(ConnectionRole.BOARD)
    # 业务刚发过：只剩 1 帧业务数据，无保活帧
    assert len(sock.sent) == 1


def test_long_idle_channel_is_retired():
    sock = FakeSocket()
    manager = _manager(AccountKind.LEVEL2, sock)
    connection = _adopt(manager, ConnectionRole.BOARD_CONSTITUENT_SZ, sock)
    _set_idle(connection, BoardChannelKeepalive.MAX_IDLE_SECONDS + 60)

    ka = _keepalive(manager)
    ka._service_role(ConnectionRole.BOARD_CONSTITUENT_SZ)

    assert manager.peek(ConnectionRole.BOARD_CONSTITUENT_SZ) is None
    assert sock.closed


def test_send_failure_closes_channel_for_rebuild():
    sock = FakeSocket()
    manager = _manager(AccountKind.LEVEL2, sock)
    connection = _adopt(manager, ConnectionRole.BOARD, sock)
    _set_idle(connection, 20.0)
    sock.closed = True  # 模拟僵尸 socket：sendall 直接抛 OSError

    ka = _keepalive(manager)
    ka._service_role(ConnectionRole.BOARD)

    assert manager.peek(ConnectionRole.BOARD) is None


def test_missing_manager_or_channel_is_noop():
    ka = BoardChannelKeepalive(lambda: None)
    ka._service_role(ConnectionRole.BOARD)  # 不应抛错

    sock = FakeSocket()
    manager = _manager(AccountKind.LEVEL2, sock)
    ka2 = _keepalive(manager)
    ka2._service_role(ConnectionRole.BOARD)  # 无连接：不抛错、不建连
    assert sock.sent == []
