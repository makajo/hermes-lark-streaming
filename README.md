<h1 align="center">hermes-lark-streaming (Hardened Fork)</h1>

<p align="center">
  <img src="https://img.shields.io/badge/Project-Hardened%20Fork-blue" alt="Hardened Fork">
  <a href="https://opensource.org/licenses/MIT"><img src="https://img.shields.io/badge/License-MIT-4caf50.svg" alt="License: MIT"></a>
  <img src="https://img.shields.io/badge/python-3.11+-3776AB.svg" alt="Python 3.11+">
  <img src="https://img.shields.io/badge/version-1.8.0--patch4-ff9800.svg" alt="Version">
  <img src="https://img.shields.io/badge/Hermes%20Agent-%3E%3D0.20.0-6f42c1.svg" alt="Hermes Agent">
</p>

<p align="center">
English | <a href="README.zh-CN.md">中文版</a>
</p>

Feishu / Lark CardKit v2.0 native streaming cards plugin for [Hermes Agent](https://github.com/NousResearch/hermes-agent).

This project is a **Hardened Fork** maintained from [Aowen-Nowor/hermes-lark-streaming](https://github.com/Aowen-Nowor/hermes-lark-streaming) v1.8.0, featuring critical fixes for gateway startup deadlocks, strict multi-platform (Feishu + QQBot/NapCat/Telegram) isolation, unhandled exception card sealing, and non-reply message fallbacks.

---

## Core Fixes & Features in this Fork

| Area | Problem in Upstream | Solution in this Fork |
|------|-------------------|----------------------|
| **Multi-Platform Isolation** | Upstream did not strictly verify message platform origins. When running multiple platforms (e.g. Feishu + QQBot/NapCat), QQ messages could leak into Feishu contexts and trigger incorrect card behaviors. | Implemented `_is_feishu_platform(source)` guard across all entry hooks. Non-Feishu messages are passed through untouched without interference. |
| **Gateway Anti-Deadlock** | Synchronous `from gateway.run import ...` during plugin registration risked Python import lock deadlocks during gateway startup. | Replaced synchronous imports with lazy inspection via `sys.modules.get("gateway.run")`, completely resolving circular import deadlocks. |
| **Non-Reply Fallback** | Messages without a `reply_anchor_id` (e.g. cron task notifications, proactive alerts, system events) caused 400 Bad Request errors when calling reply APIs. | Extended `FeishuClient` with `send_card_by_id_to_chat` and `send_text_to_chat` to gracefully fall back to direct chat delivery. |
| **Safe Card Sealing on Error** | If an unhandled agent exception occurred during reasoning or tool execution, the Feishu card stayed permanently stuck in "Processing..." status. | Added cleanup and automatic card sealing on unhandled exceptions to ensure card lifecycles are always properly finalized. |
| **Delayed Patch Status Sync** | In delayed patch mode, `_patch_status` was not updated upon successful patch execution, leaving the `/aowen status` dashboard out of sync. | Added status sync logic so that the diagnostic dashboard immediately turns green (`✓`) upon delayed patch activation. |

---

## Preview

<table align="center">
  <tr>
    <td><img src="assets/screenshots/img1.png" width="200px" /></td>
    <td><img src="assets/screenshots/img2.png" width="200px" /></td>
    <td><img src="assets/screenshots/img3.png" width="200px" /></td>
    <td><img src="assets/screenshots/img4.png" width="200px" /></td>
  </tr>
</table>

- **Native CardKit 2.0 Streaming**: Smooth typewriter effect (default 4 chars/step) with real-time token streaming.
- **Unified Chronological Panel**: Interleaved reasoning rounds and tool steps displayed in order of occurrence.
- **200-Element Overflow Guard**: Smart folding and seal-time element trimming to comply with Feishu's 200-element hard limit.
- **Rich Status Footer**: Elapsed time, model name, estimated cost, token counts, and cache statistics.

---

## Quick Start

### 1. Installation
Run via the Hermes CLI:

```bash
hermes plugins install https://github.com/makajo/hermes-lark-streaming
```
Type `Y` when prompted to enable the plugin.

### 2. Restart Gateway
```bash
systemctl restart hermes-gateway
# or via CLI
hermes gateway restart
```

### 3. Verification
Verify plugin loading via CLI:
```bash
hermes plugins list
grep hermes_lark_streaming ~/.hermes/logs/agent.log
```
Or send `/aowen status` directly in a Feishu chat with your bot to confirm all patch statuses are green (`✓`).

---

## Configuration

Custom configuration options reside in `~/.hermes/config.yaml`.

### General Streaming Options
```yaml
hermes_lark_streaming:
  panel_expanded: false            # Keep panel expanded in finalized cards (default: false)
  streaming_panel_expanded: false  # Keep panel expanded during streaming (default: false)
  print_strategy: delay            # Typing strategy: delay (smooth) or fast (instant)
  print_step: 4                    # Characters per step (1–10, default: 4)
  flush_interval_ms: 200           # Update push interval in ms (70–2000, default: 200)
  card_ttl_sec: 600               # Card alive timeout in seconds
  max_tool_steps: 20               # Max tool steps in panel (default: 20)
  max_reasoning_rounds: 20         # Max reasoning rounds in panel (default: 20)

  footer:
    show_label: false
    fields:
      - [status, elapsed, model, cost, compression_exhausted]
```

### Enabling Reasoning / Thinking Display
To display the model's chain-of-thought in the top collapsible panel:
```yaml
display:
  platforms:
    feishu:
      show_reasoning: true  # Set to true to display reasoning rounds
```
> **Note**: This setting is hot-reloaded automatically and does not require restarting the gateway.

---

## Commands (`/aowen`)

Send `/aowen` commands directly in Feishu (zero LLM token consumption):

| Command | Description |
|---------|-------------|
| `/aowen help` | Show help menu |
| `/aowen status` | View plugin status, patch health, and active configuration |
| `/aowen monitor` | Inspect card creation, stream updates, and API metrics |
| `/aowen monitor reset` | Reset monitoring counters |
| `/aowen config reload` | Reload configuration from `config.yaml` |

---

## Uninstallation

```bash
# 1. Clean up injected configuration
python3 ~/.hermes/plugins/hermes-lark-streaming/__main__.py cleanup

# 2. Uninstall plugin directory
hermes plugins uninstall hermes-lark-streaming

# 3. Restart gateway
systemctl restart hermes-gateway
```

---

## Acknowledgments

- Forked from [Aowen-Nowor/hermes-lark-streaming](https://github.com/Aowen-Nowor/hermes-lark-streaming).
- Thanks to Aowen-Nowor and the original author [Cheerwhy](https://github.com/Cheerwhy) for their foundation.
- Licensed under the [MIT License](LICENSE).
