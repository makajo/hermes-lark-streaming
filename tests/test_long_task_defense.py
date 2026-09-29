"""Long-running task and interim send defense tests.

Guards against:
1. Interim sends (heartbeats, progress notifications) prematurely flipping ctx["card_sent"] = True.
2. Disconnected or uncompleted card sessions causing Hermes's turn-final text response to be discarded.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from hermes_lark_streaming.controller import CardSession, StreamCardController
from hermes_lark_streaming.controller.mixin import COMPLETED, STREAMING
from hermes_lark_streaming.patching import _msg_ctx
from hermes_lark_streaming.patching.adapter import _dispatch_feishu_outbound, _is_interim_send
from hermes_lark_streaming.patching.gateway import _wrap_handle_message_with_agent
from hermes_lark_streaming.state.linear import UnifiedLinearState
from tests.test_controller import _make_session, _setup_ctrl


class TestInterimSendDetection:
    """Verify _is_interim_send correctly identifies interim sends."""

    def test_interim_send_flags(self) -> None:
        assert _is_interim_send({"_interim_send": True}) is True
        assert _is_interim_send({"interim": True}) is True
        assert _is_interim_send({"is_interim": True}) is True
        assert _is_interim_send({"heartbeat": True}) is True
        assert _is_interim_send({"notification_category": "progress"}) is True
        assert _is_interim_send({"notification_category": "heartbeat"}) is True
        assert _is_interim_send({"notification_category": "status"}) is True

    def test_non_interim_sends(self) -> None:
        assert _is_interim_send(None) is False
        assert _is_interim_send({}) is False
        assert _is_interim_send({"job_id": "cron_123"}) is False
        assert _is_interim_send({"notification_category": "final"}) is False
        assert _is_interim_send("string_metadata") is False


class TestInterimSendDoesNotFlipCardSent:
    """Verify interim sends suppress the bubble without setting ctx['card_sent'] = True."""

    @pytest.mark.asyncio
    async def test_interim_send_preserves_card_sent_false(self) -> None:
        ctrl = _setup_ctrl()
        session = _make_session("om_turn1")
        session.card_msg_id = "om_card_turn1"
        ctrl._sess_put("om_turn1", session)

        ctx = {
            "message_id": "om_turn1",
            "chat_id": "oc_test",
            "anchor_id": "om_turn1",
            "event_message_id": "om_turn1",
            "card_sent": False,
        }
        _msg_ctx.set(ctx)

        fake_adapter = SimpleNamespace()
        fake_passthrough = AsyncMock()

        with patch("hermes_lark_streaming.controller.get_controller", return_value=ctrl):
            res = await _dispatch_feishu_outbound(
                adapter=fake_adapter,
                chat_id="oc_test",
                content="⏳ Working — 3 min",
                passthrough=fake_passthrough,
                metadata={"_interim_send": True},
            )

        assert fake_passthrough.await_count == 0
        # CRUCIAL: card_sent must remain False!
        assert ctx["card_sent"] is False


class TestGatewayReplyFallback:
    """Verify _wrap_handle_message_with_agent never drops turn-final text replies if card unsealed."""

    @pytest.mark.asyncio
    async def test_release_gateway_reply_when_card_uncompleted(self) -> None:
        ctrl = _setup_ctrl()
        session = _make_session("om_task_msg")
        session.card_msg_id = "om_card_msg"
        session.state = STREAMING
        session.unified_state = UnifiedLinearState()
        session.unified_state.answer_text = ""  # No answer rendered on card!
        ctrl._sess_put("om_task_msg", session)

        orig = AsyncMock(return_value="Final agent detailed report (1361 chars)")
        wrapped = _wrap_handle_message_with_agent(orig)

        event = SimpleNamespace(message_id="om_task_msg", text="run task")
        source = SimpleNamespace(
            platform=SimpleNamespace(value="feishu"),
            chat_id="oc_test",
            thread_id=None,
        )
        self_mock = MagicMock()
        self_mock._reply_anchor_for_event = MagicMock(return_value="om_task_msg")

        with patch("hermes_lark_streaming.controller.get_controller", return_value=ctrl):
            result = await wrapped(self_mock, event, source)

        # Must release the 1361 chars text reply, NOT None!
        assert result == "Final agent detailed report (1361 chars)"

    @pytest.mark.asyncio
    async def test_suppress_gateway_reply_when_card_completed_with_answer(self) -> None:
        ctrl = _setup_ctrl()
        session = _make_session("om_task_msg2")
        session.card_msg_id = "om_card_msg2"
        session.state = COMPLETED
        session.unified_state = UnifiedLinearState()
        session.unified_state.answer_text = "Complete answer visible on card"
        ctrl._sess_put("om_task_msg2", session)

        orig = AsyncMock(return_value="Duplicate answer text")
        wrapped = _wrap_handle_message_with_agent(orig)

        event = SimpleNamespace(message_id="om_task_msg2", text="run task")
        source = SimpleNamespace(
            platform=SimpleNamespace(value="feishu"),
            chat_id="oc_test",
            thread_id=None,
        )
        self_mock = MagicMock()
        self_mock._reply_anchor_for_event = MagicMock(return_value="om_task_msg2")

        with patch("hermes_lark_streaming.controller.get_controller", return_value=ctrl):
            result = await wrapped(self_mock, event, source)

        # Successfully delivered on card -> suppress text duplicate!
        assert result is None
