# 自定义模型（models.json）接入指南

wb-proxy 支持把 WorkBuddy 桌面端里配置的「自定义模型」（自备 OpenAI 兼容上游）合并进网关，供 Codex 等客户端通过本地代理直接调用，**不消耗 WorkBuddy 账号池积分**。

## 链路

```
Codex → http://127.0.0.1:8788/v1 → wb-proxy → 自定义模型上游（直连）
                                     ↘ 内置 WorkBuddy 模型 → 官方账号池（原路径不变）
```

## 配置来源

配置文件与 WorkBuddy 桌面端共用：

```
%USERPROFILE%\.workbuddy\models.json
```

JSON 数组，每个条目至少需要 `id`、`url`（http/https）、`apiKey`；能力字段（`supportsToolCall` / `supportsImages` / `supportsReasoning`、`maxInputTokens`、`maxOutputTokens`、`reasoning`）可选。桌面端里加过自定义模型的话，这个文件已经存在，无需手工编辑。

**热生效**：文件按 `(mtime, size)` 变化自动重读，改完即生效，无需重启。

## 路由规则

- 模型名命中 `models.json` 的请求走自定义上游：
  - 上游始终以 `stream=true` 调用（长生成不会被 header 超时掐断）；客户端要流式则 SSE 透传，要非流式则网关聚合成单个 chat.completion / response 对象返回。
  - `POST /v1/chat/completions`：SSE 透传或流聚合。
  - `POST /v1/responses`：复用网关的 Responses↔Chat 翻译层，返回 Responses SSE 事件或聚合 response 对象。
  - 上游 URL = 配置的 `url` + `/chat/completions`（标准 OpenAI 协议；`useCustomProtocol: false` 的条目均如此）。
  - 条目声明了 `reasoning.defaultEffort` 且客户端未指定 effort 时，自动带上默认档位（客户端显式指定优先）。
- 未命中 `models.json` 的模型走原有内置路由，行为完全不变。
- 同名冲突：`models.json` 优先。若把自定义模型命名为内置模型 id，将覆盖内置路由。
- 每个客户端 API Key 的「模型白名单」限制对自定义模型同样生效。

## 安全边界

- `apiKey` 只在发往该条目自身上游的 `Authorization: Bearer` 头中出现；不写日志、不进 usage 记录、不出现在 `/v1/models` 响应或任何错误信息里。
- `/v1/models` 中自定义模型只暴露 `id`、能力标志与规格元数据（`owned_by: "custom"`），不暴露上游 URL。
- 只允许配置中明确列出的 http/https 上游；URL 内嵌凭据（`user:pass@`）、query、fragment 会被拒绝，**重定向一律不跟随**（3xx 报 502），密钥不会被带去别的主机。
- 自定义模型请求绝不经过官方账号池端点；两个通道完全隔离。

## 安全失败行为

| 情况 | 行为 |
|---|---|
| 文件不存在 | 自定义模型不出现在清单；请求按内置路由处理（通常 503 无账号），网关正常运行 |
| JSON 损坏 / 校验失败 | 同上，日志记录一次原因（`custom models: config rejected: ...`） |
| 单个条目非法（缺 apiKey、ftp:// 等） | 仅跳过该条目，其余条目正常 |
| 未知模型名 | 走内置路由，不会伪装成功 |
| 上游 4xx/5xx | 状态码透传给客户端（错误体中的密钥会被打码） |
| 上游超时 | 连接阶段超时 → 504；流中空闲超时 → 断流并记录 `upstream_aborted` |
| 上游 3xx | 502（不跟随重定向） |
| 空流（200 但无事件） | 流式返回带内 `error` 事件；非流式返回 502，不伪造成正常完成 |
| 上游返回 200 但内容不是 SSE/补全 | 502 |

内置 WorkBuddy 模型路径不受以上任何分支影响。

## 诊断方法

1. `GET /v1/models`：确认自定义模型出现且 `owned_by` 为 `custom`。
2. 看板/终端日志：每个请求有一行 `custom: model=<id> upstream=<host> stream=<bool> msgs=<n>`；失败行是 `request error: ...`（错误体已脱敏）。
3. usage 流水（`/usage` 或 usage.jsonl）：自定义模型按普通模型记 token/耗时，`account` 列为空即代表未走官方账号池。
4. 配置被拒时看 `custom models:` 开头的日志行。

## 禁用与回滚

- **临时禁用单个模型**：从 `models.json` 删除该条目（或桌面端删除），保存即生效。
- **整体禁用**：把 `models.json` 改名或清空为 `[]`；所有自定义模型立即从清单消失，不再有自定义路由。
- **回滚代码**：本功能只新增 `wb_custom_models.py`、`tests/_test_custom_models.py`、`docs/custom-models.md`，并在 `wb_proxy.py` 加了带 `wb_custom_models` / `custom` 标记的少量插入点；`git revert` 或还原这几个文件即可，内置链路无任何行为改动。

## 已知限制

- `/v1/responses` 不支持 `previous_response_id`（网关无状态，与内置模型一致）。
- 自定义上游不享受网关的 web_search/web_fetch 代跑与 WorkBuddy 专属清洗；模型声明的工具原样透传给上游。
- 上游必须讲标准 OpenAI Chat Completions 协议（含流式）；不兼容的协议会在请求时报上游原始错误。
- 配置文件损坏期间清单为空；修复文件后自动恢复。
