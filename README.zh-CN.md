<h1 align="center">hermes-lark-streaming (Hardened Fork)</h1>

<p align="center">
  <img src="https://img.shields.io/badge/Project-Hardened%20Fork-blue" alt="Hardened Fork">
  <a href="https://opensource.org/licenses/MIT"><img src="https://img.shields.io/badge/License-MIT-4caf50.svg" alt="License: MIT"></a>
  <img src="https://img.shields.io/badge/python-3.11+-3776AB.svg" alt="Python 3.11+">
  <img src="https://img.shields.io/badge/version-1.8.0--patch4-ff9800.svg" alt="Version">
  <img src="https://img.shields.io/badge/Hermes%20Agent-%3E%3D0.20.0-6f42c1.svg" alt="Hermes Agent">
</p>

<p align="center">
<a href="README.md">English</a> | 中文版
</p>

为 [Hermes Agent](https://github.com/NousResearch/hermes-agent) 提供飞书 / Lark CardKit v2.0 原生流式消息卡片插件。

本项目是基于 [Aowen-Nowor/hermes-lark-streaming](https://github.com/Aowen-Nowor/hermes-lark-streaming) v1.8.0 维护的**高可用与多平台兼容增强分支（Hardened Fork）**，针对网关启动死锁、多平台（飞书 + QQ / NapCat / Telegram）消息串扰、异常封卡与无锚点消息降级等生产痛点进行了深度修复。

---

## 本分支核心修复与特性 (Fixes & Features)

| 修复领域 | 问题与风险 | 本分支解决方案 |
|---------|-----------|----------------|
| **跨平台严格隔离** | 原版未严格校验消息来源平台，多平台网关（如飞书与 QQ / NapCat 共存）运行时，QQ 消息会被写入飞书上下文并触发钩子，引发卡片误发或串扰。 | 注入全链路 `_is_feishu_platform(source)` 守卫，非飞书消息一律原样直通，绝不介入、污染其他平台。 |
| **网关启动防死锁** | 启动时同步 `from gateway.run import ...`，在插件扫描与网关主线程并发执行时极易触发 Python 模块导入锁死循环。 | 移除同步强导入，改用 `sys.modules.get("gateway.run")` 惰性安全探测，彻底解除死锁风险。 |
| **非回复消息降级** | 无 `reply_anchor_id` 的场景（如定时任务通知、主动推送、特殊系统消息）直接调用卡片回复接口会报 400 失败。 | 扩展客户端支持 `send_card_by_id_to_chat` 与 `send_text_to_chat`，无回复锚点时自动优雅降级为会话直发。 |
| **异常安全封卡** | Agent 在思考或工具调用中发生未捕获异常时，飞书卡片会永久停留在“正在处理 / 思考中”状态，三个点动画无法消除。 | 在未处理异常出口增加卡片兜底封口（Seal），确保卡片生命周期始终正常终结。 |
| **延迟打补丁状态同步** | 当网关启动走延迟补丁轮询线程时，补丁成功打上后未回写 `_patch_status`，导致 `/aowen status` 面板显示状态滞后。 | 补充状态同步逻辑，补丁激活后状态即刻转绿（`✓`）。 |

---

## 效果预览

<table align="center">
  <tr>
    <td><img src="assets/screenshots/img1.png" width="200px" /></td>
    <td><img src="assets/screenshots/img2.png" width="200px" /></td>
    <td><img src="assets/screenshots/img3.png" width="200px" /></td>
    <td><img src="assets/screenshots/img4.png" width="200px" /></td>
  </tr>
</table>

- **CardKit 2.0 原生流式打字**：平滑打字机动画（默认 4 字符/步），支持流式更新。
- **单一时间线执行面板**：思考过程（Reasoning Rounds）与工具调用（Tool Calls）按实际发生时序交错折叠排布。
- **元素上限智能防护**：严格遵守飞书 200 元素硬上限，自动折叠超限轮次并具备封口安全裁剪网。
- **完整状态页脚**：呈现回复状态、模型名称、耗时、预估费用与 Token/缓存统计。

---

## 快速安装

### 1. 安装插件
在终端执行以下命令（或直接让 Hermes 执行）：

```bash
hermes plugins install https://github.com/makajo/hermes-lark-streaming
```
提示时输入 `Y` 启用插件。

### 2. 重启网关
```bash
systemctl restart hermes-gateway
# 或使用 CLI 重启
hermes gateway restart
```

### 3. 验证运行状态
在终端运行验证：
```bash
hermes plugins list
# 查看日志确认打补丁成功
grep hermes_lark_streaming ~/.hermes/logs/agent.log
```
或直接在飞书私聊中向机器人发送：
```text
/aowen status
```
看到各补丁项全为绿色 `✓` 即可。

---

## 配置说明

所有自定义配置位于 `~/.hermes/config.yaml`。

### 常用流式与面板配置
```yaml
hermes_lark_streaming:
  panel_expanded: false            # 完成态卡片面板是否保持展开（默认 false 折叠）
  streaming_panel_expanded: false  # 流式生成中面板是否自动展开（默认 false）
  print_strategy: delay            # 打字策略：delay（丝滑打字机，推荐）或 fast（即时）
  print_step: 4                    # 每次渲染字符数（1~10，默认 4）
  flush_interval_ms: 200           # 插件更新推送间隔（70~2000ms，默认 200）
  card_ttl_sec: 600               # 卡片存活超时（秒）
  max_tool_steps: 20               # 统一面板最多显示的工具步骤数（默认 20）
  max_reasoning_rounds: 20         # 统一面板最多显示的推理轮次数（默认 20）

  footer:
    show_label: false              # 是否显示字段文字标签
    fields:
      - [status, elapsed, model, cost, compression_exhausted]
```

### 开启思考链（Reasoning / Thinking）展示
若希望在顶部的折叠面板中展示模型的完整思维链：
```yaml
display:
  platforms:
    feishu:
      show_reasoning: true  # 设为 true 即可在执行面板内展示思考过程
```
> **提示**：该配置支持热重载，修改后自动生效，无需重启网关。

---

## 管理命令 (`/aowen`)

在飞书中发送以下命令直接与插件交互（无需消耗 LLM Token）：

| 命令 | 说明 |
|------|------|
| `/aowen help` | 显示帮助菜单 |
| `/aowen status` | 查看插件运行状态、补丁生效情况及当前配置 |
| `/aowen monitor` | 查看卡片创建、流式更新、API 调用与错误码统计 |
| `/aowen monitor reset` | 重置监控计数器 |
| `/aowen config reload` | 重新加载 `config.yaml` 运行时配置 |

---

## 卸载与还原

若需卸载该插件：
```bash
# 1. 自动清理注入配置
python3 ~/.hermes/plugins/hermes-lark-streaming/__main__.py cleanup

# 2. 卸载插件目录
hermes plugins uninstall hermes-lark-streaming

# 3. 重启网关
systemctl restart hermes-gateway
```

---

## 致谢与溯源

- 本项目 Fork 自 [Aowen-Nowor/hermes-lark-streaming](https://github.com/Aowen-Nowor/hermes-lark-streaming)。
- 感谢原作者 Aowen-Nowor 及初始贡献者 [Cheerwhy](https://github.com/Cheerwhy) 的卓越工作。
- 遵循 [MIT 开源许可证](LICENSE)。
