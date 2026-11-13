"""v2.0.36 回归测试 — 累积工具进度 >4000 字符漏成原生 post 消息修复.

2026-11-13 生产实测 3 起（10.5KB / 14.9KB / 20.4KB）：
  tool_preview_length=9999 + accumulate 模式下，插件 _on_progress 不回传 message_id，
  网关每次工具推进都全量重发越来越长的历史进度；一旦突破 adapter._upgrade_plain_to_progress
  的 4000 字符兜底上限，宽吸收放行落回 super().send()，把整坨工具日志吐成原生 post 散文本。
"""

from __future__ import annotations

import pytest

from hermes_lark_streaming.cardkit.cards import build_streaming_card_v2
from hermes_lark_streaming.composer.classify import CH_PROGRESS
from hermes_lark_streaming.composer.engine import FrameComposer
from hermes_lark_streaming.plugin.adapter import FeishuStreamingAdapter, _upgrade_plain_to_progress


class _FakeClient:
    def __init__(self) -> None:
        self.cards: dict[str, dict] = {}
        self._n = 0

    async def cardkit_create(self, card):
        self._n += 1
        self.cards[str(self._n)] = {"card": card, "seq": 0}
        return str(self._n)

    async def send_card_by_id_to_chat(self, chat_id, card_id):
        return f"om_card_{card_id}"

    async def reply_card_by_id(self, anchor, card_id):
        return f"om_card_{card_id}"

    async def cardkit_stream_element(self, card_id, element_id, content, sequence=0):
        self.cards[card_id]["seq"] = sequence

    async def cardkit_update(self, card_id, card, sequence=0):
        self.cards[card_id]["card"] = card
        self.cards[card_id]["seq"] = sequence


def _make_composer(client: _FakeClient | None = None) -> FrameComposer:
    c = client or _FakeClient()
    return FrameComposer(
        get_client=lambda: c,
        build_card=lambda **kw: build_streaming_card_v2(**kw),
        build_final_card=lambda **kw: {},
        config=None,
    )


class TestOversizedProgressUpgrade:
    async def test_15k_accumulate_snapshot_with_active_card_upgrades(self):
        comp = _make_composer()
        chat = "oc_oversized_1"
        first = _upgrade_plain_to_progress(comp, chat, "📚 skills_list...")
        assert first is not None
        await comp.handle_send(chat, "📚 skills_list...", None, None, first)

        # 构造 >15,000 字符真实形态累积快照（含 execute_code 里嵌套的 ``` 字面量）
        long_py = "import json; print('```check```'); " + ("x = 1; " * 2200)
        snapshot = "\n".join([
            "📚 skills_list...",
            "💻 terminal\n```\nls -la ~/.hermes/ && hermes curator status\n```",
            "🔎 搜索文件 · curator|skill_view|use_count",
            f"🐍 运行代码 {long_py}",
            "📖 读取文件 reader.py L1-85",
        ])
        assert len(snapshot) > 15000

        upgraded = _upgrade_plain_to_progress(comp, chat, snapshot, None)
        assert upgraded is not None
        assert upgraded.channel == CH_PROGRESS

    async def test_10k_bare_fence_continuation_group_with_active_card_upgrades(self):
        comp = _make_composer()
        chat = "oc_oversized_2"
        first = _upgrade_plain_to_progress(comp, chat, "💻 terminal\n```\nls -la\n```")
        await comp.handle_send(chat, "💻 terminal\n```\nls -la\n```", None, None, first)

        # 模拟 gateway _split_progress_groups 切出的续段：连续 terminal 裸围栏块开头（无语言 tag）+ 动词行 + 后续 terminal
        cmd_a = "echo start && " + ("--flag=val " * 420)
        cmd_b = "echo next && " + ("--opt=1 " * 420)
        snapshot = "\n".join([
            f"```\n{cmd_a}\n```",
            f"```\n{cmd_b}\n``` (×3)",
            "📖 读取文件 video_downloader.py L40-129",
        ])
        assert len(snapshot) > 8000

        upgraded = _upgrade_plain_to_progress(comp, chat, snapshot, None)
        assert upgraded is not None
        assert upgraded.channel == CH_PROGRESS

    def test_first_tool_over_4000_without_active_card_upgrades(self):
        comp = _make_composer()
        chat = "oc_first_huge"
        huge_term = "💻 terminal\n```\n" + ("echo hello; " * 450) + "\n```"
        assert len(huge_term) > 4000
        upgraded = _upgrade_plain_to_progress(comp, chat, huge_term, None)
        assert upgraded is not None
        assert upgraded.channel == CH_PROGRESS
        assert upgraded.tool_name == "terminal"

        huge_py = "🐍 运行代码 " + ("a = b + 1; " * 500)
        assert len(huge_py) > 4000
        upgraded2 = _upgrade_plain_to_progress(comp, chat, huge_py, None)
        assert upgraded2 is not None
        assert upgraded2.channel == CH_PROGRESS
        assert upgraded2.tool_name == "execute_code"

    def test_bare_fence_without_active_card_not_misabsorbed(self):
        comp = _make_composer()
        bare = "```\n" + ("x" * 4500) + "\n```"
        assert _upgrade_plain_to_progress(comp, "oc_no_card", bare, None) is None

    async def test_language_tagged_code_block_not_misabsorbed(self):
        comp = _make_composer()
        chat = "oc_lang_fence"
        first = _upgrade_plain_to_progress(comp, chat, "📚 列出技能")
        await comp.handle_send(chat, "📚 列出技能", None, None, first)

        py_reply = "```python\n" + ("print('hello')\n" * 400) + "```"
        assert len(py_reply) > 4000
        assert _upgrade_plain_to_progress(comp, chat, py_reply, None) is None

    async def test_long_emoji_prose_still_rejected(self):
        comp = _make_composer()
        chat = "oc_long_prose"
        first = _upgrade_plain_to_progress(comp, chat, "📚 列出技能")
        await comp.handle_send(chat, "📚 列出技能", None, None, first)

        long_prose = "✅ " + ("x" * 4500)
        assert _upgrade_plain_to_progress(comp, chat, long_prose, None) is None

        multiline_prose = "✅ Summary\n" + ("这是正文段落，不是工具进度行。\n" * 260)
        assert len(multiline_prose) > 4000
        assert _upgrade_plain_to_progress(comp, chat, multiline_prose, None) is None

    async def test_group_thread_metadata_progress_upgrades_but_prose_does_not(self):
        comp = _make_composer()
        chat = "oc_thread"
        thread_meta = {"thread_id": "om_root_1", "reply_to_message_id": "om_root_1"}
        snap = "📚 列出技能\n🐍 运行代码 " + ("k = 1; " * 700)
        assert len(snap) > 4000

        upgraded = _upgrade_plain_to_progress(comp, chat, snap, thread_meta)
        assert upgraded is not None and upgraded.channel == CH_PROGRESS

        prose = "这是群话题里一条超过300字的正常汇报正文：" + ("说明内容" * 120)
        assert _upgrade_plain_to_progress(comp, chat, prose, thread_meta) is None

    async def test_adapter_send_never_falls_back_to_native_on_15k_progress(self, monkeypatch):
        """端到端：累积工具快照从 <300 涨到 15000+ 时，adapter.send 全程走卡片，0 次 super().send 泄漏."""
        client = _FakeClient()
        comp = _make_composer(client)

        adapter = FeishuStreamingAdapter.__new__(FeishuStreamingAdapter)
        adapter._composer = comp
        adapter._card_config = None
        adapter._card_client = client
        adapter._app_id = "app"
        adapter._app_secret = "sec"
        adapter._inbound_anchor = {}
        adapter._session_keys = {}

        native_calls: list[str] = []

        async def _fake_super_send(self, chat_id, content, reply_to=None, metadata=None):
            native_calls.append(content)
            return "native_leaked"

        base_cls = FeishuStreamingAdapter.__bases__[0]
        monkeypatch.setattr(base_cls, "send", _fake_super_send)

        steps = [
            "📚 skills_list...",
            "💻 terminal\n```\nls -la ~/.hermes/\n```",
            "🐍 运行代码 " + ("a += 1; " * 600),
            "🔎 搜索文件 · " + ("pattern|" * 900),
            "📖 读取文件 adapter.py L750-870",
        ]
        accumulated = []
        for s in steps:
            accumulated.append(s)
            res = await adapter.send("oc_e2e_15k", "\n".join(accumulated), metadata=None)
            assert res.success is True
            assert res.message_id is None  # 契约：不向网关返回 card_message_id

        assert native_calls == []
        st = comp.active_state("oc_e2e_15k")
        assert st is not None
        assert len(st.status_lines) == 5
