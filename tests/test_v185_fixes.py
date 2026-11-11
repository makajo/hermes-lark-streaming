"""Tests for v1.8.5 fixes:
1. In-place fallback to partial_update_element when streaming closed (300309) — no card split.
2. Callback wrapping robust against AIAgent reuse and always setting _agent_ref.
3. Deferred patch poller logs DEBUG (not ERROR) on non-gateway processes.
4. on_feishu_normalize re-entrancy deduplication on identical MessageEvent.
"""

from __future__ import annotations

import asyncio
import sys
import time
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from hermes_lark_streaming.controller.core import StreamCardController
from hermes_lark_streaming.feishu import CARDKIT_STREAMING_CLOSED, FeishuAPIError
from hermes_lark_streaming.patching import _msg_ctx, _thread_local_ctx
from hermes_lark_streaming.patching.callbacks import _maybe_wrap_callbacks
from hermes_lark_streaming.patching.hooks import on_feishu_normalize
from hermes_lark_streaming.state.linear import UnifiedLinearState
from hermes_lark_streaming.state.phase import CardPhase
from hermes_lark_streaming.state.session import CardSession
try:
    from test_callback_interception import FakeAgent
    from test_controller import _make_session, _setup_ctrl
except ImportError:
    from tests.test_callback_interception import FakeAgent
    from tests.test_controller import _make_session, _setup_ctrl


# ══════════════════════════════════════════════════════════════════════
# 1. In-place fallback on 300309 streaming closed (no card split)
# ══════════════════════════════════════════════════════════════════════


class TestStreamingClosedInPlaceFallback:
    """Verify that CARDKIT_STREAMING_CLOSED (300309) falls back in-place
    to partial_update_element instead of splitting the card into a continuation card."""

    @pytest.mark.asyncio
    async def test_streaming_closed_falls_back_to_batch_update_in_flush(self) -> None:
        """When cardkit_stream_element raises 300309, _do_unified_flush immediately
        falls back to _fallback_write_answer (partial_update_element) on the SAME card."""
        ctrl = _setup_ctrl(linear=True)
        session = _make_session("om_msg_1", linear=True)
        session.card_id = "card_123"
        session.state = CardPhase.STREAMING
        session._creation_stages.add("answer")
        session.unified_state = UnifiedLinearState()
        session.unified_state.answer_text = "Hello world from agent"
        session.unified_state.answer_dirty = True
        ctrl._sess_put("om_msg_1", session)

        mock_client = MagicMock()
        mock_client.cardkit_stream_element = AsyncMock(
            side_effect=FeishuAPIError("ErrMsg: stream closed", code=CARDKIT_STREAMING_CLOSED)
        )
        mock_client.cardkit_batch_update = AsyncMock(return_value=None)
        ctrl._client = mock_client

        await ctrl._do_unified_flush(session)

        assert session._streaming_closed is True
        assert session.unified_state.answer_dirty is False
        assert mock_client.cardkit_batch_update.call_count == 1
        actions = mock_client.cardkit_batch_update.call_args[0][1]
        assert actions[0]["action"] == "partial_update_element"
        assert actions[0]["params"]["element_id"] == "answer_content"
        assert actions[0]["params"]["partial_element"]["content"] == "Hello world from agent"

    @pytest.mark.asyncio
    async def test_subsequent_flush_uses_fallback_directly(self) -> None:
        """Once _streaming_closed is True, subsequent flushes bypass stream_element
        and write directly via _fallback_write_answer."""
        ctrl = _setup_ctrl(linear=True)
        session = _make_session("om_msg_2", linear=True)
        session.card_id = "card_456"
        session.state = CardPhase.STREAMING
        session._creation_stages.add("answer")
        session._streaming_closed = True
        session.unified_state = UnifiedLinearState()
        session.unified_state.answer_text = "Updated text chunk"
        session.unified_state.answer_dirty = True
        ctrl._sess_put("om_msg_2", session)

        mock_client = MagicMock()
        mock_client.cardkit_stream_element = AsyncMock()
        mock_client.cardkit_batch_update = AsyncMock(return_value=None)
        ctrl._client = mock_client

        await ctrl._do_unified_flush(session)

        # stream_element must NOT be called at all
        assert mock_client.cardkit_stream_element.call_count == 0
        assert mock_client.cardkit_batch_update.call_count == 1
        assert session.unified_state.answer_dirty is False

    def test_on_answer_does_not_split_card_on_streaming_closed(self) -> None:
        """on_answer must NOT trigger _maybe_reactivate_for_continuation when
        _streaming_closed is True; it must accumulate into the original card."""
        ctrl = _setup_ctrl(linear=True)
        session = _make_session("om_msg_3", linear=True)
        session.card_id = "card_789"
        session.state = CardPhase.STREAMING
        session._streaming_closed = True
        session.unified_state = UnifiedLinearState()
        session.unified_state.answer_text = "First chunk"
        ctrl._sess_put("om_msg_3", session)

        ctrl.on_answer(message_id="om_msg_3", text=" and second chunk")

        # Session must remain the same, text appended, no continuation session created
        assert session.unified_state.answer_text == "First chunk and second chunk"
        assert ctrl._resolve_continuation_id("om_msg_3") is None
        assert ctrl._sess_get("om_msg_3-cont-1") is None


# ══════════════════════════════════════════════════════════════════════
# 2. Callback wrapping: agent reuse and _agent_ref preservation
# ══════════════════════════════════════════════════════════════════════


class TestCallbacksWrapAgentReuseAndRef:
    """Verify that _maybe_wrap_callbacks sets _agent_ref unconditionally
    and wraps newly rebound callbacks on reused agent instances."""

    def test_agent_ref_set_even_when_callbacks_already_wrapped(self) -> None:
        """When an agent's callbacks are already wrapped, _maybe_wrap_callbacks
        must still populate ctx['_agent_ref'] = agent."""
        _msg_ctx.set({"event_message_id": "om_test_eid"})
        agent = FakeAgent()

        # First wrap
        _maybe_wrap_callbacks(agent)
        ctx = _msg_ctx.get()
        assert ctx is not None
        assert ctx.get("_agent_ref") is agent

        # Clear _agent_ref to simulate new turn context
        new_ctx: dict[str, Any] = {"event_message_id": "om_test_eid_turn2"}
        _msg_ctx.set(new_ctx)

        # Second wrap on the SAME agent instance
        _maybe_wrap_callbacks(agent)
        assert new_ctx.get("_agent_ref") is agent

    def test_newly_attached_callback_is_wrapped(self) -> None:
        """If Hermes attaches a new callback (e.g. reasoning_callback) to an
        already-wrapped agent, calling _maybe_wrap_callbacks wraps it properly."""
        _msg_ctx.set({"event_message_id": "om_test_eid"})
        agent = FakeAgent()

        # Wrap initially without reasoning_callback
        agent.reasoning_callback = None
        _maybe_wrap_callbacks(agent)
        first_stream = agent.stream_delta_callback
        assert getattr(first_stream, "_hls_wrapper", False) is True

        # Now attach a new unwrapped reasoning_callback
        orig_reasoning = MagicMock()
        agent.reasoning_callback = orig_reasoning

        _maybe_wrap_callbacks(agent)

        # stream_delta_callback must remain the SAME wrapper (not re-wrapped)
        assert agent.stream_delta_callback is first_stream
        # reasoning_callback must now be wrapped
        assert agent.reasoning_callback is not orig_reasoning
        assert getattr(agent.reasoning_callback, "_hls_wrapper", False) is True


# ══════════════════════════════════════════════════════════════════════
# 3. Deferred patch poller non-gateway log level
# ══════════════════════════════════════════════════════════════════════


class TestDeferredPollerNonGatewayQuiet:
    """Verify that unresolved deferred patch targets log DEBUG in non-gateway
    processes (CLI/doctor) and ERROR only in gateway processes."""

    def test_non_gateway_process_logs_debug(self) -> None:
        from hermes_lark_streaming.patching import _maybe_start_deferred_poller
        import hermes_lark_streaming.patching as patch_mod

        # Reset flag
        patch_mod._deferred_poller_started = False

        with patch.object(sys, "argv", ["/usr/local/bin/hermes", "doctor"]), patch(
            "hermes_lark_streaming.patching._logger"
        ) as mock_logger, patch("time.sleep", side_effect=[None, None]), patch(
            "time.monotonic", side_effect=[0.0, 10.0, 70.0, 80.0]
        ):
            _maybe_start_deferred_poller(
                need_gw=True,
                need_cl=False,
                need_cron=False,
                need_adapter=False,
                need_agent=False,
                need_hook=False,
            )
            # Give background thread a moment to execute
            time.sleep(0.1)

            # mock_logger.error must NOT be called with the 60s failure message
            for call in mock_logger.error.call_args_list:
                msg = call[0][0] if call[0] else ""
                assert "deferred patch targets NOT resolved within 60s" not in msg


# ══════════════════════════════════════════════════════════════════════
# 4. on_feishu_normalize re-entrancy deduplication
# ══════════════════════════════════════════════════════════════════════


class TestOnFeishuNormalizeReentrancy:
    """Verify that re-entrant calls with the exact same MessageEvent are
    deduplicated and do not invoke record_inbound or log duplicate lines."""

    def test_reentrant_call_on_same_event_is_deduplicated(self) -> None:
        source = SimpleNamespace(platform=SimpleNamespace(value="feishu"), thread_id=None)
        event = SimpleNamespace(message_id="om_event_test_1", raw_message={})

        with patch("hermes_lark_streaming.aowen.record_inbound") as mock_record, patch(
            "hermes_lark_streaming.patching.hooks.get_controller"
        ) as mock_get_ctrl, patch("hermes_lark_streaming.patching.hooks._logger") as mock_logger:
            mock_ctrl = MagicMock()
            mock_ctrl.enabled = True
            mock_get_ctrl.return_value = mock_ctrl

            # First call
            on_feishu_normalize(message_id="om_event_test_1", source=source, event=event)
            assert mock_record.call_count == 1
            assert getattr(event, "_hls_normalized", False) is True

            # Second re-entrant call (SessionAuthority admission / ingress replay)
            on_feishu_normalize(message_id="om_event_test_1", source=source, event=event)
            # record_inbound must NOT be called again
            assert mock_record.call_count == 1
