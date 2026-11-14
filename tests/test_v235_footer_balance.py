"""v2.0.35 — 页脚 DeepSeek 官方余额字段（balance）。

覆盖三件事：
1. 渲染层：有值 → 双语 ``余额 ¥25.42`` / ``Balance ¥25.42``；无值 → 字段跳过。
2. 查询层：``/user/balance`` 响应解析（CNY 优先 / USD 兜底 / 缺字段返空）。
3. 门控：字段未声明不查询；provider 非 deepseek 或自定义 base_url 不查询。
"""

from __future__ import annotations

import pytest

from hermes_lark_streaming.cardkit.elements import _build_footer_elements
from hermes_lark_streaming.composer.balance import _format_balance
from hermes_lark_streaming.composer.finalize import (
    _deepseek_official_route,
    _footer_wants_balance,
)


class TestBalanceRendering:
    def test_balance_rendered_with_label(self) -> None:
        result = _build_footer_elements({"balance": "¥25.42"}, fields=[["balance"]])
        assert len(result) >= 2
        content = result[1]["content"]
        zh = result[1]["i18n_content"]["zh_cn"]
        assert "Balance ¥25.42" in content
        assert "余额 ¥25.42" in zh

    def test_balance_missing_skipped(self) -> None:
        assert _build_footer_elements({"balance": ""}, fields=[["balance"]]) == []

    def test_balance_appended_to_footer_row(self) -> None:
        result = _build_footer_elements(
            {"duration": 412, "model": "deepseek-flash", "balance": "¥25.42"},
            fields=[["status", "elapsed", "model", "balance"]],
        )
        content = result[1]["content"]
        assert "deepseek-flash" in content
        assert "¥25.42" in content


class TestBalanceParsing:
    def test_cny_preferred(self) -> None:
        payload = {
            "is_available": True,
            "balance_infos": [
                {"currency": "USD", "total_balance": "3.00"},
                {"currency": "CNY", "total_balance": "25.42"},
            ],
        }
        assert _format_balance(payload) == "¥25.42"

    def test_usd_fallback(self) -> None:
        payload = {"balance_infos": [{"currency": "USD", "total_balance": "1.5"}]}
        assert _format_balance(payload) == "$1.50"

    def test_missing_or_broken_payload(self) -> None:
        assert _format_balance({}) == ""
        assert _format_balance({"balance_infos": []}) == ""
        assert _format_balance({"balance_infos": [{"currency": "CNY"}]}) == ""
        assert _format_balance({"balance_infos": [{"currency": "CNY", "total_balance": "n/a"}]}) == ""


class TestBalanceGate:
    def test_field_declared(self) -> None:
        assert _footer_wants_balance([["status", "elapsed", "model", "balance"]])
        assert not _footer_wants_balance([["status", "elapsed", "model"]])

    @pytest.mark.parametrize(
        ("model_cfg", "expected"),
        [
            ({"provider": "cpa", "default": "sensenova-6.8-flash"}, False),
            ({"provider": "deepseek", "default": "deepseek-flash"}, True),
            (
                {"provider": "deepseek", "base_url": "https://proxy.example/v1"},
                False,
            ),
        ],
    )
    def test_official_route(self, monkeypatch: pytest.MonkeyPatch, model_cfg: dict, expected: bool) -> None:
        # 用假 gateway.run 模块替代真实导入：测试机不一定装 hermes 运行时，
        # 且真实模块 import 会触发重启动器（sys._hermes_relaunched）。
        import sys
        import types

        fake = types.ModuleType("gateway.run")
        fake._load_gateway_config = lambda: {"model": model_cfg}  # type: ignore[attr-defined]
        monkeypatch.setitem(sys.modules, "gateway.run", fake)
        assert _deepseek_official_route() is expected
