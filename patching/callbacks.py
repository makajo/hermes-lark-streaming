"""Callback wrapping for AIAgent streaming callbacks."""

from __future__ import annotations

from typing import Any

from . import (
    _msg_ctx,
    _thread_local_ctx,
    _logger,
    _get_event_message_id,
)

def _resolve_eid(fallback_eid: str | None) -> str | None:
    """Re-resolve the current event_message_id from _msg_ctx at call time."""
    _eid = _get_event_message_id()
    if _eid:
        return _eid
    if fallback_eid:
        try:
            from ..controller import get_controller
            ctrl = get_controller()
            sess = ctrl._sess_get(fallback_eid)
            if sess is not None and not sess.is_terminal_phase:
                return fallback_eid
        except Exception:
            pass
    return None

def _maybe_wrap_callbacks(agent) -> None:
    """Replace streaming callbacks on *agent* with wrappers that also fire
    Feishu CardKit updates.  Skips silently when outside a Feishu message
    context (i.e. no event_message_id in context)."""
    _logger.debug("HLS: _maybe_wrap_callbacks invoked, has_stream=%s, eid_lookup=%s", bool(getattr(agent, "stream_delta_callback", None)), bool(_get_event_message_id()))

    eid = _get_event_message_id()
    if not eid:
        _logger.debug("HLS: skip — no event_message_id in ctx")
        return  # Not in a hermes-lark-streaming context — skip

    # Store agent reference immediately so it is NEVER lost on double-wrap or agent reuse
    ctx = _msg_ctx.get()
    if ctx is not None:
        ctx["_agent_ref"] = agent
        _thread_local_ctx.data = dict(ctx)

    # Scoped consumed-length tracking per agent instance
    _stream_consumed_len = getattr(agent, "_hls_stream_consumed_len", None)
    if _stream_consumed_len is None:
        _stream_consumed_len = {}
        setattr(agent, "_hls_stream_consumed_len", _stream_consumed_len)

    def _cleanup_consumed_len(_eid: str) -> None:
        """Remove consumed-length tracking for a completed message."""
        _stream_consumed_len.pop(_eid, None)

    # ── STREAM: wrap stream_delta_callback ──
    _current_stream = getattr(agent, "stream_delta_callback", None)
    if not (getattr(_current_stream, "_hls_wrapper", False) is True):
        if _current_stream:
            _orig_stream = _current_stream

            def _answer_wrapper(text, *args, **kwargs):
                _eid = _resolve_eid(eid)
                if not _eid:
                    return _orig_stream(text, *args, **kwargs)
                try:
                    from .hooks import on_answer_delta

                    if text and on_answer_delta(message_id=_eid, text=text):
                        # Record total consumed length for dedup with interim_assistant_callback
                        _stream_consumed_len[_eid] = _stream_consumed_len.get(_eid, 0) + len(text)
                        return
                except Exception:
                    _logger.debug("HLS: answer_wrapper exception", exc_info=True)
                return _orig_stream(text, *args, **kwargs)

            agent.stream_delta_callback = _answer_wrapper
            setattr(agent.stream_delta_callback, "_hls_wrapper", True)
            _logger.debug("HLS: _maybe_wrap_callbacks stream_delta_callback wrapped")
        else:
            # Fix: Create our own stream_delta_callback that routes answer tokens to
            def _answer_wrapper_synthetic(text, *args, **kwargs):
                # Handle None — stream boundary signal from conversation_loop
                # (tool boundary flush / end-of-stream). Just ignore it.
                if text is None:
                    return
                _eid = _resolve_eid(eid)
                if not _eid:
                    return
                try:
                    from .hooks import on_answer_delta

                    if text and on_answer_delta(message_id=_eid, text=text):
                        _stream_consumed_len[_eid] = _stream_consumed_len.get(_eid, 0) + len(text)
                        return
                except Exception:
                    _logger.debug("HLS: answer_wrapper_synthetic exception", exc_info=True)

            agent.stream_delta_callback = _answer_wrapper_synthetic
            setattr(agent.stream_delta_callback, "_hls_wrapper", True)

    # ── INTERIM: wrap interim_assistant_callback ──
    _current_interim = getattr(agent, "interim_assistant_callback", None)
    if _current_interim and not (getattr(_current_interim, "_hls_wrapper", False) is True):
        _orig_interim = _current_interim

        def _thinking_wrapper(text, *args, **kwargs):
            _eid = _resolve_eid(eid)
            if not _eid:
                return _orig_interim(text, *args, **kwargs)
            try:
                # ── already_streamed passthrough (Hermes hint) ──
                already_streamed = kwargs.get("already_streamed", False)
                if already_streamed:
                    # v1.3.2 fix (P3-02): clean up consumed-length tracking for
                    _cleanup_consumed_len(_eid)
                    return _orig_interim(text, *args, **kwargs)

                # ── Length-based dedup ──
                consumed_len = _stream_consumed_len.get(_eid, 0)
                if text and consumed_len > 0 and len(text) <= consumed_len:
                    # v1.3.2 fix (P3-02): same cleanup — text fully consumed,
                    # message streaming is done.
                    _cleanup_consumed_len(_eid)
                    return _orig_interim(text, *args, **kwargs)

                if text:
                    from .hooks import on_thinking_delta
                    consumed = on_thinking_delta(message_id=_eid, text=text)
                    if consumed:
                        return
            except Exception:
                _logger.debug("HLS: thinking_wrapper exception", exc_info=True)
            return _orig_interim(text, *args, **kwargs)

        agent.interim_assistant_callback = _thinking_wrapper
        setattr(agent.interim_assistant_callback, "_hls_wrapper", True)
        _logger.debug("HLS: _maybe_wrap_callbacks interim_assistant_callback wrapped")

    # ── TOOL: wrap tool_progress_callback ──
    _current_tool = getattr(agent, "tool_progress_callback", None)
    if _current_tool and not (getattr(_current_tool, "_hls_wrapper", False) is True):
        _orig_tool = _current_tool

        def _tool_wrapper(event_type, tool_name=None, preview=None, *args, **kwargs):
            _eid = _resolve_eid(eid)
            if not _eid:
                return _orig_tool(event_type, tool_name, preview, *args, **kwargs)
            try:
                from .hooks import on_tool_updated

                if event_type in ("tool.started", "tool.completed"):
                    if on_tool_updated(
                        message_id=_eid,
                        tool_name=tool_name or "",
                        status="started" if event_type == "tool.started" else "completed",
                        detail=preview or "",
                    ):
                        return
            except Exception:
                _logger.debug("HLS: tool_wrapper exception", exc_info=True)
            return _orig_tool(event_type, tool_name, preview, *args, **kwargs)

        agent.tool_progress_callback = _tool_wrapper
        setattr(agent.tool_progress_callback, "_hls_wrapper", True)

    # ── REASONING: wrap reasoning_callback ──
    _current_reasoning = getattr(agent, "reasoning_callback", None)
    if _current_reasoning and not (getattr(_current_reasoning, "_hls_wrapper", False) is True):
        _orig_reasoning = _current_reasoning

        def _reasoning_wrapper(text, *args, **kwargs):
            _eid = _resolve_eid(eid)
            if not _eid:
                # No ctx — call original if present
                if _orig_reasoning and not (getattr(_orig_reasoning, "_hls_wrapper", False) is True):
                    return _orig_reasoning(text, *args, **kwargs)
                return
            try:
                from .hooks import on_reasoning_delta

                if text:
                    on_reasoning_delta(message_id=_eid, text=text)
            except Exception:
                _logger.debug("HLS: reasoning_wrapper exception", exc_info=True)
            if _orig_reasoning and not (getattr(_orig_reasoning, "_hls_wrapper", False) is True):
                return _orig_reasoning(text, *args, **kwargs)

        agent.reasoning_callback = _reasoning_wrapper
        setattr(agent.reasoning_callback, "_hls_wrapper", True)

    # ── BACKGROUND_REVIEW: wrap background_review_callback ──
    _current_bg = getattr(agent, "background_review_callback", None)
    if _current_bg and not (getattr(_current_bg, "_hls_wrapper", False) is True):
        _orig_bg = _current_bg

        def _bg_wrapper(message, *args, **kwargs):
            _eid = _resolve_eid(eid)
            if not _eid:
                return _orig_bg(message, *args, **kwargs)
            try:
                from .hooks import on_background_review_message

                deferred = on_background_review_message(
                    message_id=_eid,
                    text=message,
                    sender=_orig_bg,
                )
                if deferred:
                    return
            except Exception:
                _logger.debug("HLS: bg_wrapper exception", exc_info=True)
            return _orig_bg(message, *args, **kwargs)

        agent.background_review_callback = _bg_wrapper
        setattr(agent.background_review_callback, "_hls_wrapper", True)
