"""board_constituents 小市场组的单股行情补齐（2026-09-16 886028 修复）。"""

import pytest

from thspypc.client import THSClient
from thspypc.features.system_blocks import BlockStock
from thspypc.services.system_blocks import SystemBlocksError


@pytest.fixture
def client():
    return THSClient("offline-user", "offline-password", enable_heartbeat=False)


class _FakeBlocks:
    """直接命中稳定 ID 的本地板块替身。"""

    def __init__(self, stocks):
        self._stocks = stocks

    def constituents(self, block_id):
        if block_id == "886028":
            return self._stocks
        raise SystemBlocksError(block_id)


def _oxygen_members():
    # 血氧仪 886028：2 只沪市 + 3 只深市（示意）
    return [
        BlockStock(code="600839", market="17"),
        BlockStock(code="688130", market="17"),
        BlockStock(code="300003", market="33"),
        BlockStock(code="301290", market="33"),
        BlockStock(code="301552", market="33"),
    ]


def test_board_constituents_backfills_small_missing_group(client, monkeypatch):
    client._system_blocks = _FakeBlocks(_oxygen_members())
    # 板块通道（service 层已跳过 <3 只的沪组）只回深组三只
    monkeypatch.setattr(
        client,
        "_run_default_service",
        lambda capabilities, operation: [
            {"code": "301552", "dt10": 23.8, "dt6": 23.55, "dt19": 9151661.0},
            {"code": "301290", "dt10": 10.0, "dt6": 9.9, "dt19": 5000000.0},
            {"code": "300003", "dt10": 8.0, "dt6": 8.1, "dt19": 3000000.0},
        ],
    )
    monkeypatch.setattr(
        client,
        "stock_quote_fields",
        lambda codes, **kwargs: [
            {
                "code": "600839",
                "price": 4.05,
                "chg_pct": 1.2,
                "amount": 12345.0,
                "speed_4m": 0.05,
            },
            {
                "code": "688130",
                "price": 20.0,
                "chg_pct": -0.5,
                "amount": 6789.0,
                "speed_4m": None,
            },
        ],
    )

    rows = client.board_constituents(["886028"])

    by_code = {row["code"]: row for row in rows}
    assert set(by_code) == {
        "600839", "688130", "300003", "301290", "301552",
    }
    # 补齐行映射回 dt 键约定；chg_pct 直通、不被末尾 dt6/dt10 补算覆盖
    assert by_code["600839"]["dt10"] == 4.05
    assert by_code["600839"]["dt19"] == 12345.0
    assert by_code["600839"]["dt48"] == 0.05
    assert by_code["600839"]["chg_pct"] == 1.2
    assert by_code["688130"]["chg_pct"] == -0.5
    assert "dt48" not in by_code["688130"]
    # 板块通道行的 chg_pct 仍按 dt10/dt6 本地补算
    assert by_code["301552"]["chg_pct"] == pytest.approx(
        (23.8 / 23.55 - 1) * 100
    )


def test_board_constituents_skips_backfill_when_gap_too_large(
    client, monkeypatch,
):
    client._system_blocks = _FakeBlocks(
        [BlockStock(code=f"60000{i}", market="17") for i in range(9)]
    )
    monkeypatch.setattr(
        client, "_run_default_service",
        lambda capabilities, operation: [{"code": "300001", "dt10": 1.0}],
    )

    def _fail(codes, **kwargs):
        raise AssertionError("超过补齐上限时不应触发单股查询")

    monkeypatch.setattr(client, "stock_quote_fields", _fail)

    rows = client.board_constituents(["886028"])
    assert [row["code"] for row in rows] == ["300001"]


def test_board_constituents_backfill_failure_is_soft(client, monkeypatch):
    client._system_blocks = _FakeBlocks(_oxygen_members())
    monkeypatch.setattr(
        client,
        "_run_default_service",
        lambda capabilities, operation: [
            {"code": "301552", "dt10": 23.8, "dt6": 23.55},
        ],
    )

    def _boom(codes, **kwargs):
        raise RuntimeError("MAIN 不可用")

    monkeypatch.setattr(client, "stock_quote_fields", _boom)

    rows = client.board_constituents(["886028"])
    assert [row["code"] for row in rows] == ["301552"]


def test_backfill_constituent_gaps_noop_when_complete(client, monkeypatch):
    monkeypatch.setattr(
        client,
        "stock_quote_fields",
        lambda codes, **kwargs: (_ for _ in ()).throw(
            AssertionError("无缺口不应触发单股查询")
        ),
    )
    records = [{"code": "600839", "dt10": 1.0}]
    assert client._backfill_constituent_gaps(["600839"], records) is records
