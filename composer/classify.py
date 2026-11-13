"""V2 帧分类器 — 把 hermes 出站帧分类为卡片可消费的通道.

输入契约（全部为 hermes v0.21.x 官方 metadata / 参数，无任何私有 hack）：

- ``metadata["expect_edits"]=True``  → answer 流式帧（后续会被 edit_message 更新）
- ``metadata["_interim_send"]=True`` → mid-turn 状态帧（interim 播报 / progress 气泡首条）
- ``metadata["notify"]=True``        → turn 定稿帧（原生流式 final 语义）
- ``edit_message(..., finalize=True)`` → 定稿编辑
- progress 文本模板（hermes locales，稳定 i18n key 渲染产物）:
    thinking 前缀 ``💬 {text}``；工具行 ``{emoji} {tool}...`` / ``{emoji} {tool}："{preview}"``
    v2.0.6 补真实模板（``agent/display.py`` + ``run_turn_runner``）：
    tool_head + 围栏代码块（terminal）、中文/英文动词行（``💻 运行命令 ...``）、
    裸围栏块（连续 terminal 调用省略 header）、去重后缀 ``(×N)``

设计红线：分类器**永不抛异常**——任何解析失败都降级为最保守的通道（interim / plain），
消息内容永不丢失（最坏情况走原生文本路径）。
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Any

# v2.0.5：进度行解析下沉到 cardkit.progline 叶子模块（cards.py 渲染面板时需要
# 同一套解析，原地对拷会漂移；从 classify 导入又会 cardkit←composer 循环）。
# re-export 既有公开名（_THINKING_PREFIX / parse_progress_line）；
# _EMOJI_RE 供 _is_progress_text 启发式使用。
from ..cardkit.progline import (
    _EMOJI_RE,
    _THINKING_PREFIX,
    parse_progress_line,
)  # noqa: F401 — re-export 供既有导入方使用

_logger = logging.getLogger("hermes_lark_streaming.composer")

# ── 通道常量 ────────────────────────────────────────────────────────────────
CH_STREAM = "stream"      # answer 流式帧（send 首条 / edit 后续）
CH_PROGRESS = "progress"  # 工具进度行 / thinking 气泡（progress lane）
CH_INTERIM = "interim"    # interim 播报（非工具非思考的中途文本）
CH_FINAL = "final"        # turn 定稿
CH_PLAIN = "plain"        # 普通消息（cron / 后台 / 无标记发送）→ 原生路径


# ── v2.0.8 / v2.0.34 / v2.0.37 非对话通知识别 ───────────────────────────────
# hermes 的后台通知（background_review / 长任务心跳 / 更新通告 / 启动重启 /
# 停机通告 / Cron 存储 / DB 告警 / 卡住告警 / 审批超时 / 后台任务 / 语音回显）
# 经 _send_status_text(_interim_send=True) 或 adapter.send(metadata=None) 投递
# （_non_conversational 标记仅 Discord 生效，见 gateway/run.py）。
# 这类文本带 emoji 且常 ≤300 字 → 若未先命中本清单会落入 _is_progress_text →
# CH_PROGRESS → _on_progress 无卡分支 early-create 流式卡（streaming_mode +
# spinner「点点点」永挂），并在卡未落定前劫持用户下一条消息的回复/工具流。
# 归入本清单后：
# - _interim_send 路径 → CH_INTERIM advisory（活跃卡并入面板 / 无卡原生文本）；
# - plain 无标记路径 → 不升格 CH_PROGRESS，保持 CH_PLAIN（无活跃卡走静态
#   oneshot 定稿卡或原生文本），永不建流式占位卡。
# 锚定 i18n 模板特征短语，en + zh + zh-hant 三语覆盖（locales/*.yaml 逐字节核验）。
_NOTICE_RES: tuple[re.Pattern[str], ...] = (
    # display.review.summary_callback / summary_cli（en / zh / zh-hant）
    re.compile(
        r"^\s*💾\s*(?:Self-improvement review|自我改进回顾|自我改進檢閱)\s*[:：]\s*\S",
        re.IGNORECASE,
    ),
    # 旧版 / memory_notifications: on / verbose 形态
    re.compile(
        r"^\s*💾\s+(?:Memory\s+updated|记忆已更新|記憶已更新|"
        r"(?:Memory|Skill|User\s+profile|记忆|技能|用户画像|記憶|使用者設定檔)已更新|"
        r"Skill\s+['\"].+?['\"]\s+(?:created|updated|improved|patched)|"
        r"(?:技能|User\s+profile|用户画像|使用者設定檔)\s+['\"].+?['\"]\s+"
        r"(?:created|updated|improved|patched|已(?:创建|重写|修补|写入|移除|删除|建立|重寫|修補|寫入|移除|刪除)))",
        re.IGNORECASE,
    ),
    # gateway.progress.working_heartbeat（en / zh / zh-hant）
    re.compile(r"^\s*⏳\s+Working\s+—\s+\d+\s+min(?:\s|$)", re.IGNORECASE),
    re.compile(r"^\s*⏳\s+(?:正在处理|處理中)\s*(?:——|—)\s*\d+\s*(?:分钟|分鐘)(?:\s|$|—|，|,)"),
    # gateway.update.*（en / zh / zh-hant）
    re.compile(
        r"^\s*(?:✅|❌)\s+Hermes\s+(?:update\s+(?:finished|failed|timed out)|"
        r"更新(?:已完成|成功完成|已成功完成|完成|失败|失敗|已超时|已逾時))",
        re.IGNORECASE,
    ),
    re.compile(
        r"^\s*☤\s+(?:Starting Hermes update|\*\*Update needs your input|"
        r"\*\*更新需要你的输入|\*\*更新需要你的輸入|正在启动 Hermes 更新|正在啟動 Hermes 更新)",
        re.IGNORECASE,
    ),
    # gateway.startup.* / gateway.restart.*（en / zh / zh-hant）
    re.compile(r"^\s*♻\uFE0F?\s+Gateway\s+(?:restarted successfully|online\b)", re.IGNORECASE),
    re.compile(r"^\s*♻\uFE0F?\s+(?:网关已成功重启|网关已上线|閘道已成功重新啟動|閘道已上線)"),
    re.compile(r"^\s*♻\uFE0F?\s+(?:Restarting gateway|正在重启网关|正在重新啟動閘道)", re.IGNORECASE),
    # gateway.shutdown.*（en / zh / zh-hant）
    re.compile(r"^\s*⚠\uFE0F?\s+Hermes\s+is\s+(?:shutting\s+down|restarting)\b", re.IGNORECASE),
    re.compile(r"^\s*⚠\uFE0F?\s+Hermes\s+(?:正在关闭|正在重启|正在關閉|正在重新啟動)\s*(?:——|—)"),
    re.compile(r"^\s*⚠\uFE0F?\s+(?:Scheduled\s+job|定时任务|排程任務)\s+'", re.IGNORECASE),
    # gateway.draining / gateway.busy.drain_* / gateway.restart.in_progress
    re.compile(
        r"^\s*⏳\s+(?:Draining\s+\d+|正在等待\s*\d+\s*个活跃代理|正在等待\s*\d+\s*個活躍代理|"
        r"Gateway\s+(?:is\s+)?(?:restarting|shutting down|restart already in progress)|"
        r"网关(?:正在重启|正在关闭|重启已在进行中)|閘道(?:正在重新啟動|正在關閉|重新啟動已在進行中))",
        re.IGNORECASE,
    ),
    # gateway.cron_store.* / gateway.startup.db_* / session_stall / progress.stuck_warning
    re.compile(r"^\s*(?:⚠\uFE0F?|✅)\s+Cron\s+(?:store\s+|存储|儲存區)", re.IGNORECASE),
    re.compile(
        r"^\s*⚠\uFE0F?\s+(?:Session database\b|检测到会话数据库|会话数据库|偵測到工作階段資料庫|工作階段資料庫)",
        re.IGNORECASE,
    ),
    re.compile(r"^\s*⚠\uFE0F?\s+(?:I seem to be stuck\b|我似乎卡住了)", re.IGNORECASE),
    # gateway.exec_approval.timed_out_notice / gateway.approval_expired
    re.compile(r"^\s*⌛\s+(?:Approval timed out|审批已超时|審批已逾時|批准已逾時)", re.IGNORECASE),
    re.compile(r"^\s*⚠\uFE0F?\s+(?:Approval expired|批准已过期|批准已逾期)", re.IGNORECASE),
    # gateway.background.*
    re.compile(
        r"^\s*(?:✅|❌|⏳|🔄)\s+(?:Background task\s+(?:started|finished|failed|still running|complete)|"
        r"Your background task\s+\"|The background task couldn't start|"
        r"后台任务(?:已启动|已完成|失败|仍在运行|完成|无法启动)|你的后台任务|"
        r"背景任務(?:已啟動|已完成|失敗|仍在執行|完成|無法啟動)|你的背景任務)",
        re.IGNORECASE,
    ),
    # gateway.voice.transcript_echo_short（🎙️ "..."）
    re.compile(r'^\s*🎙\uFE0F?\s+"[\s\S]*"\s*$'),
    # gateway.goal.* / heartbeat.* / notify.*
    re.compile(
        r"^\s*(?:⏸|▶)\uFE0F?\s+(?:Goal|Heartbeat|目标|心跳|目標)\s*(?:paused|resumed|已暂停|已恢复|已暫停|已恢復)",
        re.IGNORECASE,
    ),
    re.compile(
        r"^\s*⚠\uFE0F?\s+(?:Couldn't deliver the |Message delivery failed after multiple attempts|"
        r"无法发送(?:音频|视频|文件|图片)附件|多次尝试后消息仍发送失败|"
        r"無法傳送(?:音訊|視訊|檔案|圖片)附件|多次嘗試後訊息仍無法送達)",
        re.IGNORECASE,
    ),
    re.compile(r"^\s*📬\s+(?:No home channel is set|\S+\s+(?:尚未设置主频道|尚未設定主頻道))", re.IGNORECASE),
)


def _is_notice_text(content: str) -> bool:
    """hermes 后台非对话通知（💾 Self-improvement review 等）→ 永不进卡片通道."""
    text = (content or "").strip()
    if not text:
        return False
    return any(rx.match(text) for rx in _NOTICE_RES)


@dataclass
class ClassifyResult:
    """一帧的分类结论。"""

    channel: str
    kind: str = "text"          # 细分：answer / thinking / tool / advisory / plain
    tool_name: str = ""         # kind=tool 时尽力提取
    thinking_text: str = ""     # kind=thinking 时剥离前缀后的原文
    raw_content: str = ""
    meta: dict[str, Any] = field(default_factory=dict)


def _strip_internal_meta(metadata: dict[str, Any] | None) -> dict[str, Any]:
    """剥离 Gateway 内部标记（官方注释：adapters strip it before the wire）。"""
    if not metadata:
        return {}
    meta = dict(metadata)
    meta.pop("_interim_send", None)
    return meta


# ── v2.0.15 平台通知轨识别（子代理失败通知 >300 字符漏洞，Task 21/22）────────
# hermes 子代理失败/超时通知经 _deliver_platform_notice 直发 adapter.send
#（gateway/run_notifications.py:208），metadata 无任何标记（飞书 thread_id=None
# 时为 None）→ CH_PLAIN，只能靠文本识别。官方行格式（tools/delegate_tool_progress.py
# format_subagent_failure_line:64-93，f-string 硬编码前缀）：
#   ``⚠️ Subagent failed — "{goal}" after {t}: {reason}. Details: /agents, …``
#   ``⚠️ Subagent timed out … without finishing. …``
# reason 经 _clean_error_text cap 200 字符（:38-45），整行最长 ≈359 字符——超出
# 工具行默认 300 上限（_is_progress_text），致「长通知 + 活跃卡在场」漏到原生
# post 文本（⚠️ emoji 行本应升格进面板）。警告族 AgentNotice（credits_tracker.py
# 唯一生产方）行全部 <100 字符，默认上限已覆盖，无需特判。
# 前缀锚定 + 600 防御上限（与围栏块一致）；⚠ 容忍带/不带 U+FE0F 变体。
# hermes 该行硬编码英文（无 i18n，locales 全库无「子代理失败」通知文案），
# 不猜测中文变体。
_PLATFORM_NOTICE_RES: tuple[re.Pattern[str], ...] = (
    re.compile(r"^⚠\uFE0F?\s+Subagent\s+(?:failed|timed out)\b"),
)


def _is_progress_text(content: str) -> bool:
    """启发式：progress lane 文本 = thinking 气泡 或 emoji 开头的短工具行.

    v2.0.6：裸围栏代码块（`` ``` `` 开头，连续 terminal 调用省略 header 的
    真实形态）也判为 progress；上限放宽到 600（块含命令正文比单行预览长）。
    v2.0.15：hermes 平台通知轨（⚠️ Subagent failed/timed out 固定前缀，
    官方 f-string 硬编码）豁免 300 上限——reason cap 200 使整行可达 ~359，
    超 300 会漏成原生 post；前缀特异性高（hermes 专属词），误吸收风险≈0。"""
    text = (content or "").strip()
    if not text:
        return False
    if text.startswith(_THINKING_PREFIX):
        return True
    if text.startswith("```"):
        return len(text) <= 600  # 命令块远短于 600；长文本不可能是进度块
    if any(rx.match(text) for rx in _PLATFORM_NOTICE_RES):
        return len(text) <= 600  # 通知行实测最长 ~359；600 防御未来文案变长
    if len(text) > 300:  # 工具行都很短；长文本不可能是工具行
        return False
    return bool(_EMOJI_RE.match(text))


# v2.0.1 公开别名：适配器层上下文升格规则需要复用进度启发式
# （edit-transport 平台的进度气泡首条是裸 send，无任何标记，需在活跃卡片期
# 结合启发式升格 CH_PROGRESS；详见 adapter.send 注释）。
is_progress_text = _is_progress_text


def classify_send(
    content: str,
    metadata: dict[str, Any] | None,
) -> ClassifyResult:
    """分类一个 send() 帧。"""
    try:
        meta = metadata or {}
        interim = bool(meta.get("_interim_send"))
        expect_edits = bool(meta.get("expect_edits"))
        notify = bool(meta.get("notify"))

        # 1) answer 流首帧：显式 expect_edits 且不是内部状态帧
        if expect_edits and not interim:
            return ClassifyResult(CH_STREAM, kind="answer", raw_content=content, meta=_strip_internal_meta(meta))

        # 2) mid-turn 状态帧（progress 气泡首条 / interim 播报共用 _interim_send 标记）
        if interim:
            # v2.0.8：后台非对话通知（💾 Self-improvement review 等）→ interim
            # 语义（活跃卡并入面板 / 无卡原生文本），永不触发建卡（跑马灯根因）。
            if _is_notice_text(content):
                return ClassifyResult(CH_INTERIM, kind="advisory", raw_content=content,
                                      meta=_strip_internal_meta(meta))
            if _is_progress_text(content):
                kind, tool, clean = parse_progress_line(content)
                return ClassifyResult(CH_PROGRESS, kind=kind, tool_name=tool, thinking_text=clean,
                                      raw_content=content, meta=_strip_internal_meta(meta))
            return ClassifyResult(CH_INTERIM, kind="advisory", raw_content=content, meta=_strip_internal_meta(meta))

        # 3) 定稿帧
        if notify:
            return ClassifyResult(CH_FINAL, kind="final", raw_content=content, meta=_strip_internal_meta(meta))

        # 4) 兜底：无任何标记 → 原生路径（cron / 后台 / 外部投递）
        return ClassifyResult(CH_PLAIN, kind="plain", raw_content=content, meta=_strip_internal_meta(meta))
    except Exception:
        _logger.debug("classify_send fallback to plain", exc_info=True)
        return ClassifyResult(CH_PLAIN, kind="plain", raw_content=content or "")


def classify_edit(
    content: str,
    finalize: bool,
) -> ClassifyResult:
    """分类一个 edit_message() 帧。

    edit 无 metadata 参数 → 通道归属由 engine 的 message_id 注册表决定，
    这里只判定 finalize 语义。
    """
    return ClassifyResult(
        CH_FINAL if finalize else CH_STREAM,
        kind="final" if finalize else "answer",
        raw_content=content or "",
    )
