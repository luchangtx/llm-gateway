# 统一 LLM 网关 (llm-gateway)

把你散落在各家的 LLM 渠道（baseUrl + key + 协议类型）聚合成**一个 OpenAI 兼容入口**：对外只有一个地址、一个 key，模型列表里能看到所有渠道的所有模型，并且**自动识别每个模型的能力**（视觉 / 思考 / 上下文长度 / 最大输出）。

- **零依赖**：只用 Node 内置模块（需 Node ≥ 18.17），`npm start` 即跑
- **协议互转**：客户端永远说 OpenAI 方言，Anthropic / Gemini / OpenAI Responses 渠道由网关自动转换（含流式、工具调用、图片）
- **能力识别四级来源**：手动覆盖 > 实测探测 > 渠道元数据 > 内置知识库 > 名称推断
- **故障转移**：同名模型在多个渠道时，一个渠道挂了自动切下一个
- **Web 后台**：渠道管理、能力一览、在线调试、请求日志、设置（网关 key 自定义/轮换、转发参数）、明亮/暗色双主题

## 快速开始

```bash
cd llm-gateway
npm start          # 首次启动会自动生成 config.json 和网关 key
```

启动后：

```
后台管理:  http://localhost:8787/
OpenAI 兼容 base_url: http://localhost:8787/v1
网关 key: sk-gw-xxxxxxxx        （自动生成，见启动横幅）
```

**首次配置不需要任何凭证**：第一次打开后台会自动弹出「首次设置」向导（仅限本机访问、仅出现一次），直接用自动生成的 key 进入，或填一个自定义 key。之后在后台「渠道」里添加你的渠道 → 「模型与能力」里就能看到聚合后的模型表。

常用命令：

```bash
npm start        # 启动服务（key 会打印在启动横幅里）
npm run show-key # 查看当前网关 key
npm run reset-key -- <新key>   # 重置 key（运行中服务自动热加载，无需重启）
```

### 三种典型接入方式

```bash
# curl
curl http://localhost:8787/v1/chat/completions \
  -H "Authorization: Bearer <网关key>" \
  -H "Content-Type: application/json" \
  -d '{"model":"zhipu/glm-4.6","messages":[{"role":"user","content":"你好"}]}'
```

```python
# Python openai SDK
client = OpenAI(base_url="http://localhost:8787/v1", api_key="<网关key>")
client.chat.completions.create(model="gemini/gemini-2.5-flash", messages=[...])
```

Cherry Studio / LobeChat / Cline 等客户端：API Host 填 `http://localhost:8787`，Key 填网关 key，模型从列表里选。

## 配置（config.json）

```jsonc
{
  "port": 8787,
  "gateway_key": "sk-gw-...",            // 对外唯一的 key
  "routing": {
    "failover": true,                     // 同名模型多渠道自动故障转移
    "timeout_ms": 120000,                 // 非流式请求总超时
    "idle_timeout_ms": 60000,             // 流式空闲超时
    "first_byte_timeout_ms": 60000        // 流式首包超时（上游迟迟不出第一个字就中断）
  },
  "capabilities": {
    "refresh_minutes": 720,               // 模型列表定时刷新间隔
    "expose_prefixed_ids": true,          // /v1/models 是否同时暴露 渠道名/模型名
    "overrides": {                        // 手动覆盖能力（最高优先级）
      "zhipu/glm-4.6": { "context": 200000 }
    }
  },
  "channels": [
    {
      "name": "zhipu",                    // 用于 "zhipu/glm-4.6" 这种精确指定
      "base_url": "https://open.bigmodel.cn/api/paas/v4",
      "api_key": "xxx",
      "protocol": "openai",               // openai | anthropic | gemini
      "enabled": true,
      "priority": 10,                     // 越小越优先
      "models": [],                       // 手动模型列表；为空则自动从 /models 拉取
      "model_aliases": { "gpt-4o": "glm-4.6" }   // 别名: 对外名称 -> 上游真实名
    }
  ]
}
```

### Base URL 怎么填

| 协议 | 填法示例 | 说明 |
|---|---|---|
| openai（Chat Completions） | `https://api.openai.com/v1`、`https://open.bigmodel.cn/api/paas/v4`、`https://dashscope.aliyuncs.com/compatible-mode/v1` | 只填到版本号为止，网关自动拼 `/chat/completions`；只填域名时自动补 `/v1` |
| openai（Responses API） | `https://api.openai.com` | 走 `/v1/responses`；网关自动做 Chat ↔ Responses 双向转换 |
| anthropic | `https://api.anthropic.com` | 自动拼 `/v1/messages` |
| gemini | `https://generativelanguage.googleapis.com` | 自动拼 `/v1beta/models/...` |

## Responses API 方言

OpenAI 现在有两套 API 形态：经典的 **Chat Completions**（`/v1/chat/completions`）和较新的 **Responses API**（`/v1/responses`）。网关这样处理：

- **两种方言全模型互通**：统一入口是 Chat Completions（`/v1/chat/completions`）；同时开放 Responses 方言入口（`/v1/responses`，Codex CLI 等客户端直连）。**任何一个模型两种方言都能调**：
  - 渠道是 Responses API → Chat 客户端的请求自动转成 Responses（system → `instructions`、工具扁平化、`reasoning_effort` → `reasoning.effort`），响应/流式事件转回 chunk；
  - 渠道是 chat/anthropic/gemini → Responses 客户端的请求自动转成 Chat（`instructions` → system、`function_call` 历史 → tool 消息、`input` → messages、图片部件互转），响应转回标准 `response` 对象（含 `output` 数组、`usage` 映射、`incomplete/max_output_tokens` 状态）。
  - 同一模型既有 Responses 渠道又有 chat 渠道时，优先原生透传，失败自动切换（含跨方言故障转移）。转换请求默认 `store: false`（不留存会话）；`previous_response_id` 等有状态能力仅在原生 Responses 渠道上可用。

## 模型命名与路由

- `渠道名/模型名`：精确指定，如 `zhipu/glm-4.6`、`anthropic/claude-sonnet-4-5`，**不会故障转移**
- 直接写 `模型名`：在所有启用渠道里找同名模型，按 `priority` 排序，失败自动切换下一个渠道
- 别名：配置 `model_aliases` 后，`gpt-4o` 这样的名字可以映射到任意渠道的真实模型

## 模型能力自动识别

每个模型条目的能力字段：`vision`（视觉）、`reasoning`（思考）、`thinking_param`（思考参数风格）、`context`（上下文长度）、`max_output`（最大输出）。按以下优先级合并，后台里每个字段都标了来源：

1. **手动覆盖**（override）：后台模型页「覆盖」按钮，或 config 的 `capabilities.overrides`
2. **实测探测**（probe）：后台「🧪 探测」按钮。视觉探测发一张 1x1 PNG（几十字节）；思考探测尝试带 `reasoning_effort` 的最小请求。结果缓存 7 天
3. **渠道元数据**（provider）：OpenRouter（`context_length`、`input_modalities`）、vLLM（`max_model_len`）、Gemini（`inputTokenLimit`）等模型列表自带的信息
4. **内置知识库**（kb）：GPT / Claude / Gemini / GLM / DeepSeek / Qwen / Kimi / Grok / Doubao 等常见家族的近似值（见 `src/kb.json`）
5. **名称推断**（name）：`-128k` 等命名长度、`vl`/`vision` 视觉后缀

> 知识库数值是公开资料的近似值，且各家可能调整过版本——以「探测」实测和「覆盖」为准。

## 网关 Key 管理

在后台「设置」页可以：

- **自定义 key**：在输入框填自己的 key（≥ 8 字符）保存，立即生效
- **随机生成**：本地生成一个 `sk-gw-` 前缀的随机 key 填入
- **一键轮换**：服务端立即生成并启用新 key，旧 key 即刻失效
- 保存/轮换后，当前后台页面会自动用新 key 重新登录；**所有已接入的客户端需要同步更换**

**多 Key（多人接入）**：设置页支持在主 Key 之外添加多个「附加 Keys」——每个 key 可命名备注（如"朋友A / Cline"）、随时停用或删除，全部立即生效；日志会记录每个请求用的是哪个 key。适合把网关分享给别人用但保留单独吊销的能力。

**忘记 key 怎么办？** 网页里修改 key 需要先用有效 key 登录（否则任何人都能改你的 key）。如果浏览器里没有有效 key：

```bash
npm run show-key               # 查看当前 key
npm run reset-key              # 或自动生成新 key
npm run reset-key -- my-key-123  # 或指定新 key
```

运行中的网关会自动热加载新 key（约 1 秒，无需重启）。服务每次启动时也会在控制台横幅里打印当前 key。

> 首次设置向导的安全边界：仅允许本机（localhost）发起、要求专用请求头（跨站网页无法伪造）、且只在从未登录过之前可用。完成后修改 key 一律需要现有 key 鉴权，或走上面的命令行重置。

转发参数（端口、故障转移开关、超时、模型列表刷新间隔）也在「设置」页调整；端口修改需重启。安全默认：网关只监听 `127.0.0.1`（仅本机访问）；如需局域网设备访问，把 config.json 的 `host` 改为 `"0.0.0.0"` 并重启，同时建议配合防火墙/反代使用。

## API

客户端端点（OpenAI 兼容，鉴权：`Authorization: Bearer <网关key>`）：

| 端点 | 说明 |
|---|---|
| `GET /v1/models` | 聚合所有渠道的模型，附带能力字段 |
| `POST /v1/chat/completions` | 对话补全，支持 `stream`、工具调用、图片、`reasoning_effort` |
| `POST /v1/responses` | OpenAI Responses API 原生透传（仅路由到 Responses 渠道） |
| `POST /v1/completions` `/v1/embeddings` | 仅 OpenAI 兼容协议渠道透传 |
| `GET /v1/models/{id}` | 单模型能力详情 |

管理端点（同样的鉴权）：`/admin/overview` `/admin/channels`(GET/PUT) `/admin/channels/{name}/test` `/admin/refresh` `/admin/models` `/admin/probe`(POST) `/admin/override`(PUT/DELETE) `/admin/logs`。

响应头 `x-gateway-channel` 会告诉你是哪个渠道实际服务的（日志页也能看到）。

## 协议转换覆盖范围

| 能力 | OpenAI 渠道 | Anthropic 渠道 | Gemini 渠道 |
|---|---|---|---|
| 流式输出 | 字节级透传 | SSE 事件流 → OpenAI chunk | SSE → OpenAI chunk |
| system / 多轮 | 透传 | 提取为顶层 system | systemInstruction |
| 图片（data URL / http） | 透传 | base64 / url 块 | inline_data（http 自动抓取转 base64） |
| 工具调用（含流式增量） | 透传 | tool_use / tool_result 互转 | functionCall / functionResponse 互转 |
| 思考（reasoning_effort） | 透传 | thinking budget（low/med/high → 2048/8192/16384） | thinkingConfig budget |
| 思考内容回传 | 透传 | delta.reasoning_content | thought parts → delta.reasoning_content |
| stop / temperature / top_p / max_tokens | 透传 | stop_sequences 等 | generationConfig |
| usage 统计 | 透传 | input/output tokens 映射 | prompt/candidates/thoughts 映射 |

## 开发与测试

```bash
npm test    # mock 上游 + 单元/集成测试（协议转换、流式、故障转移、探测、鉴权）
```

目录结构：

```
src/
  index.js          HTTP 服务、路由、鉴权、故障转移
  registry.js       模型注册表（拉取/缓存/索引/路由解析）
  capabilities.js   能力合并引擎
  kb.json           内置模型能力知识库
  probe.js          活体探测（视觉/思考）
  adapters/         openai(透传) / anthropic / gemini 协议转换
  upstream.js       URL 拼接、协议鉴权头、超时 fetch
  config.js  log.js  sse.js
public/index.html   Web 管理后台（单文件，无依赖）
```

## 常见问题

- **某渠道不提供模型列表接口**：编辑渠道，手写「模型列表」（每行一个）即可。
- **模型名在各渠道不同**：用 `model_aliases` 映射，如 `"gpt-4o": "glm-4.6"`。
- **探测会花钱吗**：会消耗极少量 token（每模型 2 个小请求），默认结果缓存 7 天，也可手动覆盖不探测。
- **修改端口 / 渠道**：渠道改动即时生效；端口改完需重启。
- **安全提示**：网关只有 key 鉴权，无配额/限速。请勿把端口暴露到公网；如需远程使用，建议套一层反代加 TLS 并限制来源。
