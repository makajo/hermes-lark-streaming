"""v2.0.37 回归测试 — 中文生命周期/后台通知不再误建无限等待「...」流式卡.

生产取证（2026-11-13 agent.log + 飞书端两张截图）：
1. ``💾 自我改进回顾：技能 'hermes-gateway-diagnosis' 已修补 ...``
   （locales/zh.yaml:3534 ``display.review.summary_callback``，metadata 仅
   ``_interim_send=True``）：``_NOTICE_RES`` 原仅含英文 ``Self-improvement review:``，
   zh 文案漏检后落入 ``_is_progress_text``（💾 emoji 开头 ≤300 字）→ ``CH_PROGRESS``
   → ``_on_progress`` 无活跃卡时 ``early-create`` 流式卡（``streaming_mode=True``），
   卡尾一直「点点点」等待，4m 40s 后用户发 ``/restart`` 的回复被劫持填进该卡。
2. ``♻ 网关已成功重启。你的会话继续有效。``
   （locales/zh.yaml:861 ``gateway.startup.restarted``，metadata=None）：
   ``_NOTICE_RES`` 原仅含英文 ``Gateway restarted successfully/online``，zh 文案
   漏检后经 ``_upgrade_plain_to_progress`` 升格为 ``CH_PROGRESS`` → 无活跃卡时
   ``early-create`` 流式卡并永挂「点点点」，用户后续消息（"我刚刚重启了"）直接
   接管该卡，面板把「网关已成功重启」与后续 ``Terminal`` 混计为 ``2 个工具``。

修复：
- ``composer/classify.py::_NOTICE_RES`` 补全 en / zh / zh-hant 三语非对话通知规则
  （自我改进回顾、网关重启/上线/更新/停机/排空、长任务心跳、Cron 存储、DB 告警、
  卡住告警、审批超时/过期、后台任务、语音回显、目标/投递提示）；
- ``plugin/adapter.py::_send_impl`` 对命中 ``_is_notice_text`` 的 ``CH_INTERIM``
  帧禁止打 ``conversational_commentary`` 标（即便调用方传入了 ``reply_to``）。
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from hermes_lark_streaming.cardkit.cards import build_streaming_card_v2
from hermes_lark_streaming.composer.classify import (
    CH_FINAL,
    CH_INTERIM,
    CH_PLAIN,
    CH_PROGRESS,
    _is_notice_text,
    classify_send,
)
from hermes_lark_streaming.composer.engine import FrameComposer
from hermes_lark_streaming.plugin.adapter import FeishuStreamingAdapter, _upgrade_plain_to_progress

pytestmark = pytest.mark.filterwarnings("ignore::DeprecationWarning")


_ZH_SELF_IMPROVEMENT = (
    "💾 自我改进回顾：技能 'hermes-gateway-diagnosis' 已修补 "
    "(references/streaming-config.md) · 技能 'hermes-config-tuning' 已修补"
)
_ZH_GATEWAY_RESTARTED = "♻ 网关已成功重启。你的会话继续有效。"
_ZH_GATEWAY_ONLINE = "♻️ 网关已上线——Hermes 已恢复就绪。"


_ALL_ZH_ZHT_EN_NOTICES = (
    _ZH_SELF_IMPROVEMENT,
    "💾 自我改進檢閱：技能 'x' 已修補",
    "💾 Self-improvement review: Skill 'x' patched",
    "💾 Memory updated",
    "💾 记忆已更新",
    "💾 記憶已更新",
    _ZH_GATEWAY_RESTARTED,
    _ZH_GATEWAY_ONLINE,
    "♻ 閘道已成功重新啟動。你的工作階段將繼續。",
    "♻️ 閘道已上線 — Hermes 已恢復並準備就緒。",
    "♻ Gateway restarted successfully. Your session continues.",
    "♻️ Gateway online — Hermes is back and ready.",
    "♻ 正在重启网关。如果 60 秒内没有收到通知，请在控制台运行 `hermes gateway restart` 重启。",
    "⏳ 正在处理——5 分钟 — 第 3/500 轮迭代，正在使用 terminal",
    "⏳ 處理中 — 5 分鐘",
    "⏳ Working — 5 min",
    "✅ Hermes 更新已完成。",
    "✅ Hermes 更新成功完成。",
    "❌ Hermes 更新失败；之前的版本仍在运行。",
    "❌ Hermes 更新已超时（30 分钟）。",
    "✅ Hermes 更新已成功完成。",
    "❌ Hermes 更新已逾時（30 分鐘）。",
    "✅ Hermes update finished successfully.",
    "⚠️ Hermes 正在关闭——你当前的任务将被中断。",
    "⚠️ Hermes 正在重启——你当前的任务将被中断。",
    "⚠️ Hermes 正在關閉 — 你目前的任務將被中斷。",
    "⚠️ Hermes 正在重新啟動 — 你目前的任務將被中斷。",
    "⚠️ 定时任务 'daily' 因 Hermes 正在重启而被中断；本次运行没有结果。",
    "⚠️ 排程任務 'daily' 因 Hermes 正在重新啟動而中斷；本次沒有結果。",
    "⏳ 正在等待 1 个活跃代理结束后重启...",
    "⏳ 网关重启已在进行中……",
    "⏳ Draining 2 active agent(s) before restart...",
    "⚠️ Cron 存储 /root/.hermes/cron 自 10:00 起无法写入（disk full）。",
    "✅ Cron 存储 /root/.hermes/cron 已恢复可写；跳过了 1 次运行，每个任务将补跑一次。",
    "⚠️ 检测到会话数据库损坏。消息可能无法持久化。",
    "⚠️ 会话数据库不可用——消息可能无法保存，/resume 将为空。",
    "⚠️ Session database unavailable — messages may not be saved.",
    "⚠️ 我似乎卡住了（已 5 分钟没有活动）。",
    "⚠️ I seem to be stuck (no activity for 5 min).",
    "⌛ 审批已超时（5 分钟）——命令未运行。",
    "⚠️ 批准已过期（代理不再等待）。请让代理重试。",
    "✅ 后台任务已完成 — `sleep 10` (10s)",
    "❌ 后台任务失败 — `bad_cmd` (退出码 1)",
    "⏳ 后台任务仍在运行 — `sleep 60`",
    "🔄 后台任务已启动：「分析日志」\n任务 ID：bg_1",
    '🎙️ "帮我查一下今天的新闻"',
    "⏸ 目标已暂停：完成发布",
    "⚠️ 无法发送文件附件（report.pdf）。",
    "📬 feishu-streaming 尚未设置主频道。",
)


class TestMultilingualNoticeRecognition:
    @pytest.mark.parametrize("text", _ALL_ZH_ZHT_EN_NOTICES)
    def test_notice_matched(self, text: str) -> None:
        assert _is_notice_text(text) is True

    @pytest.mark.parametrize(
        "text",
        (
            "普通用户消息",
            "执行。先看依赖，再写 pipeline：",
            "💻 运行命令 uname -r",
            "📚 skills_list...",
            "🔍 搜索网页 · hermes",
            "🔧 更新技能 · hermes-gateway-diagnosis",
            "💬 思考第一段",
            '⚠️ Subagent failed — "job" after 1 min: error. Details: /agents',
            "⚠️ You've used $12.34 of your $20.00 cap",
            "🎙️ 生成语音 · 你好",
        ),
    )
    def test_non_notice_not_matched(self, text: str) -> None:
        assert _is_notice_text(text) is False

    def test_zh_self_improvement_with_interim_never_progress(self) -> None:
        cls = classify_send(_ZH_SELF_IMPROVEMENT, {"_interim_send": True})
        assert cls.channel == CH_INTERIM
        assert cls.kind == "advisory"

    def test_zh_gateway_restarted_never_upgrades_to_progress(self) -> None:
        assert _upgrade_plain_to_progress(None, "oc_c", _ZH_GATEWAY_RESTARTED, None) is None
        assert _upgrade_plain_to_progress(None, "oc_c", _ZH_GATEWAY_ONLINE, None) is None


class _FakeCardClient:
    def __init__(self) -> None:
        self.created_cards: list[dict] = []
        self.oneshot_cards: list[dict] = []
        self.streamed: list[tuple[str, str]] = []
        self.closed: list[str] = []
        self.replies: list[str] = []
        self._n = 0

    async def cardkit_create(self, card):
        self._n += 1
        cid = f"stream_card_{self._n}"
        self.created_cards.append(card)
        return cid

    async def send_card_by_id_to_chat(self, chat_id, card_id):
        return f"om_{card_id}"

    async def reply_card_by_id(self, anchor, card_id):
        self.replies.append(anchor)
        return f"om_reply_{card_id}"

    async def send_card_to_chat(self, chat_id, card):
        self.oneshot_cards.append(card)
        return f"om_oneshot_{len(self.oneshot_cards)}"

    async def reply_card(self, anchor, card):
        self.oneshot_cards.append(card)
        return f"om_oneshot_reply_{len(self.oneshot_cards)}"

    async def cardkit_stream_element(self, card_id, element_id, content, sequence=0):
        self.streamed.append((card_id, content))

    async def cardkit_update(self, card_id, card, sequence=0):
        return True

    async def cardkit_batch_update(self, card_id, actions, sequence=0):
        return True

    async def cardkit_close_streaming(self, card_id, sequence=0, summary=""):
        self.closed.append(card_id)


class _StubAdapter(FeishuStreamingAdapter):
    """绕过真实 Lark SDK 的最小适配器替身，直接驱动 _send_impl 与 FrameComposer。"""

    def __init__(self, client: _FakeCardClient) -> None:
        self._card_config = SimpleNamespace(
            oneshot_cards=True,
            streaming_panel_expanded=False,
            print_strategy="fast",
            print_step=10,
            watchdog_idle_sec=300.0,
            card_duration_sec=600,
            footer_fields=[["status", "elapsed", "model"]],
            footer_show_label=False,
        )
        self._card_client = client
        self._inbound_anchor = {}
        self._session_keys = {}
        self.native_sends: list[tuple[str, str, str | None, dict | None]] = []
        self._composer = FrameComposer(
            get_client=lambda: client,
            build_card=lambda **kw: build_streaming_card_v2(**kw),
            build_final_card=lambda **kw: {},
            config=self._card_config,
        )


def _patch_super_send(monkeypatch: pytest.MonkeyPatch, adapter: _StubAdapter) -> None:
    from hermes_lark_streaming.plugin import adapter as adapter_mod

    async def _fake_super_send(self, chat_id, content, reply_to=None, metadata=None):
        adapter.native_sends.append((chat_id, content, reply_to, metadata))
        return SimpleNamespace(success=True, message_id=f"om_native_{len(adapter.native_sends)}")

    # 替换 FeishuStreamingAdapter 基类的 send（测试沙箱内为 _MissingBuiltin）
    base = FeishuStreamingAdapter.__bases__[0]
    monkeypatch.setattr(base, "send", _fake_super_send, raising=False)
    assert adapter_mod is not None


class TestEndToEndNoOrphanCardOrHijack:
    @pytest.mark.asyncio
    async def test_zh_self_improvement_after_turn_never_opens_card_or_hijacks_next_turn(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        client = _FakeCardClient()
        adapter = _StubAdapter(client)
        _patch_super_send(monkeypatch, adapter)
        chat = "oc_user_1"

        # 1. 第一个正常回合完成并封卡
        await adapter._send_impl(chat, "第一轮回答", reply_to=None, metadata={"expect_edits": True, "reply_to_message_id": "om_u1"})
        st1 = adapter._composer.active_state(chat)
        assert st1 is not None
        await adapter.edit_message(chat, st1.card_message_id, "第一轮回答", finalize=True)
        assert adapter._composer.active_state(chat) is None
        assert len(client.created_cards) == 1

        # 2. 后台自我改进回顾完成 → 投递 zh 摘要（即使带了 reply_to 也不得当 commentary 建卡）
        await adapter._send_impl(
            chat, _ZH_SELF_IMPROVEMENT, reply_to="om_u1", metadata={"_interim_send": True},
        )
        assert len(client.created_cards) == 1, "自我改进回顾不得开启新流式卡（点点点）"
        assert adapter._composer.active_state(chat) is None, "不得留下未落定的占位卡状态"
        assert len(adapter.native_sends) == 1
        assert adapter.native_sends[0][1] == _ZH_SELF_IMPROVEMENT

        # 3. 下一条消息（如 /restart 的 notify 或普通新提问）走自己的新卡，不被旧等待卡劫持
        await adapter._send_impl(
            chat,
            "♻ 正在重启网关。如果 60 秒内没有收到通知，请在控制台运行 `hermes gateway restart` 重启。",
            reply_to="om_restart",
            metadata={"notify": True},
        )
        assert len(client.oneshot_cards) == 1
        assert len(client.created_cards) == 1

    @pytest.mark.asyncio
    async def test_zh_gateway_restarted_uses_static_oneshot_and_next_message_gets_fresh_card(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        client = _FakeCardClient()
        adapter = _StubAdapter(client)
        _patch_super_send(monkeypatch, adapter)
        chat = "oc_user_2"

        # 1. 网关重启后发送「网关已成功重启」（无 metadata）
        await adapter._send_impl(chat, _ZH_GATEWAY_RESTARTED, reply_to=None, metadata=None)
        assert len(client.created_cards) == 0, "重启通知不得开启 streaming_mode 卡"
        assert adapter._composer.active_state(chat) is None, "不得在 composer 注册活跃状态"
        assert len(client.oneshot_cards) == 1, "应走静态 oneshot 卡片（无点点点等待）"

        # 2. 用户随后发送新消息「我刚刚重启了」→ 工具进度与正文建一张全新流式卡
        adapter._inbound_anchor[chat] = ("om_u2", 9999999999.0)
        await adapter._send_impl(chat, "💻 terminal\n```\nsystemctl status hermes-gateway\n```", None, None)
        st = adapter._composer.active_state(chat)
        assert st is not None
        assert len(client.created_cards) == 1
        assert all("网关已成功重启" not in ln for ln in st.status_lines)
        assert len(st.status_lines) == 1

        await adapter._send_impl(
            chat, "已核验完成，重启已全部正常生效。", None,
            {"expect_edits": True, "reply_to_message_id": "om_u2"},
        )
        await adapter.edit_message(chat, st.card_message_id, "已核验完成，重启已全部正常生效。", finalize=True)
        assert st.finalized is True
        assert st.card_id in client.closed

    @pytest.mark.asyncio
    async def test_zh_working_heartbeat_during_active_turn_joins_panel(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        client = _FakeCardClient()
        adapter = _StubAdapter(client)
        _patch_super_send(monkeypatch, adapter)
        chat = "oc_user_3"

        await adapter._send_impl(chat, "💻 terminal\n```\nsleep 300\n```", None, None)
        st = adapter._composer.active_state(chat)
        assert st is not None

        await adapter._send_impl(chat, "⏳ 正在处理——5 分钟 — 第 3/500 轮迭代", None, {"_interim_send": True})
        assert len(client.created_cards) == 1
        assert any("正在处理——5 分钟" in ln for ln in st.status_lines)
        assert adapter.native_sends == []

    @pytest.mark.asyncio
    async def test_notify_flag_not_blocked_by_notice_check(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """带 notify=True 的合法定稿/命令回复（如 /restart ack）仍走 CH_FINAL → oneshot。"""
        cls = classify_send(
            "♻ 正在重启网关。如果 60 秒内没有收到通知，请在控制台运行 `hermes gateway restart` 重启。",
            {"notify": True},
        )
        assert cls.channel == CH_FINAL
        assert classify_send("💻 terminal...", {"_interim_send": True}).channel == CH_PROGRESS
        assert classify_send("plain", None).channel == CH_PLAIN
