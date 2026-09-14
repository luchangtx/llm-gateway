// 集成测试：mock 上游 -> 网关 -> OpenAI 客户端请求
import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import { mockOpenAI, mockAnthropic, mockGemini, mockResponses, mockHanging, listen } from './mocks.js';

// ---- 1. 启动 mock 上游 ----
const oa = mockOpenAI();
const bad = mockOpenAI({ failModels: ['dup-model'] });
const anth = mockAnthropic();
const gm = mockGemini();
const orc = mockResponses();
const hang = mockHanging();
const [oaPort, badPort, anthPort, gmPort, orcPort, hangPort] = await Promise.all([
  listen(oa.server), listen(bad.server), listen(anth.server), listen(gm.server), listen(orc.server), listen(hang.server)
]);

// ---- 2. 写临时配置（必须在导入网关模块前设置环境变量） ----
const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'gw-test-'));
const GW_KEY = 'test-gw-key-123';
fs.writeFileSync(path.join(tmp, 'config.json'), JSON.stringify({
  port: 0,
  gateway_key: GW_KEY,
  routing: { failover: true, timeout_ms: 10000, idle_timeout_ms: 5000, first_byte_timeout_ms: 1000 },
  capabilities: { refresh_minutes: 0, probe_concurrency: 2, expose_prefixed_ids: true, overrides: {} },
  channels: [
    { name: 'oa', base_url: `http://127.0.0.1:${oaPort}`, api_key: 'sk-oa-secret', protocol: 'openai', enabled: true, priority: 5, models: [], model_aliases: { 'gpt-4o-easy': 'mock-gpt' } },
    { name: 'bad', base_url: `http://127.0.0.1:${badPort}`, api_key: 'sk-bad', protocol: 'openai', enabled: true, priority: 1, models: ['dup-model'], model_aliases: {} },
    { name: 'anth', base_url: `http://127.0.0.1:${anthPort}`, api_key: 'sk-ant-secret', protocol: 'anthropic', enabled: true, priority: 10, models: [], model_aliases: {} },
    { name: 'gm', base_url: `http://127.0.0.1:${gmPort}`, api_key: 'AIza-gm-secret', protocol: 'gemini', enabled: true, priority: 10, models: [], model_aliases: {} },
    { name: 'orr', base_url: `http://127.0.0.1:${orcPort}`, api_key: 'sk-orc-secret', protocol: 'openai', api_style: 'responses', enabled: true, priority: 10, models: [], model_aliases: {} },
    { name: 'hg', base_url: `http://127.0.0.1:${hangPort}`, api_key: 'sk-hang', protocol: 'openai', enabled: true, priority: 99, models: ['hang-model'], model_aliases: {} }
  ]
}));
process.env.GATEWAY_CONFIG = path.join(tmp, 'config.json');
process.env.GATEWAY_DATA = path.join(tmp, 'data');

// ---- 3. 导入网关并启动 ----
const { config } = await import('../src/config.js');
const registry = await import('../src/registry.js');
const gwIndex = await import('../src/index.js');
const { toAnthropicRequest, fromAnthropicResponse } = await import('../src/adapters/anthropic.js');
const { toGeminiRequest, fromGeminiResponse } = await import('../src/adapters/gemini.js');
const { toResponsesRequest, fromResponsesResponse, responsesToChatBody, chatToResponsesResponse, chatChunksToResponsesEvents } = await import('../src/adapters/responses.js');
const { kbLookup, nameLookup, providerMetaFromModelEntry } = await import('../src/capabilities.js');

config.port = 0;
const server = await gwIndex.start({ silent: true });
await registry.refreshAll({ force: true });
const GW = `http://127.0.0.1:${server.address().port}`;
const AUTH = { authorization: `Bearer ${GW_KEY}`, 'content-type': 'application/json' };

async function post(p, body, headers = AUTH) {
  return fetch(GW + p, { method: 'POST', headers, body: JSON.stringify(body) });
}

async function collectSSE(res) {
  const reader = res.body.getReader();
  const dec = new TextDecoder();
  let buf = '', raw = '';
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    buf += dec.decode(value, { stream: true });
    raw += buf; // no-op, keep simple
  }
  return buf;
}

function parseSSE(text) {
  const events = [];
  let done = false;
  for (const block of text.split('\n\n')) {
    const line = block.split('\n').find((l) => l.startsWith('data:'));
    if (!line) continue;
    const data = line.slice(5).trim();
    if (data === '[DONE]') { done = true; continue; }
    try { events.push(JSON.parse(data)); } catch { /* ignore */ }
  }
  return { events, done };
}

// ================= 单元测试：能力引擎 =================
test('kb: gemini-2.5-flash 识别为视觉+思考+1M 上下文', () => {
  const { caps } = kbLookup('gemini-2.5-flash');
  assert.equal(caps.vision, true);
  assert.equal(caps.reasoning, true);
  assert.equal(caps.context, 1048576);
});

test('kb: moonshot-v1-128k 从名称提取上下文', () => {
  const caps = nameLookup('moonshot-v1-128k');
  assert.equal(caps.context, 128000);
});

test('provider 元数据: context_length / max_output_tokens', () => {
  const meta = providerMetaFromModelEntry({ context_length: 200000, max_output_tokens: 128000 }, 'openai');
  assert.equal(meta.context, 200000);
  assert.equal(meta.max_output, 128000);
});

test('能力三态: 未知模型不再冒充"不支持"', async () => {
  const { buildCaps } = await import('../src/capabilities.js');
  const unknown = buildCaps({ model: 'mimo-v2.5-totally-unknown', key: 'k1' });
  assert.equal(unknown.caps.vision, undefined);       // 未知 ≠ false
  assert.equal(unknown.caps.reasoning, undefined);
  assert.equal(unknown.caps.context, 32768);          // 数值型仍有默认
  const known = buildCaps({ model: 'gemini-2.5-flash', key: 'k2' });
  assert.equal(known.caps.vision, true);
});

// ================= 单元测试：Anthropic 转换 =================
test('toAnthropicRequest: system 提取 / 图片 base64 / 工具映射 / thinking 预算', () => {
  const body = {
    messages: [
      { role: 'system', content: 'Be helpful' },
      { role: 'user', content: [{ type: 'text', text: '看图' }, { type: 'image_url', image_url: { url: 'data:image/png;base64,AAAA' } }] },
      { role: 'assistant', tool_calls: [{ id: 'call_1', type: 'function', function: { name: 'get_weather', arguments: '{"city":"北京"}' } }] },
      { role: 'tool', tool_call_id: 'call_1', content: '晴 25 度' }
    ],
    tools: [{ type: 'function', function: { name: 'get_weather', description: '查天气', parameters: { type: 'object', properties: { city: { type: 'string' } } } } }],
    tool_choice: 'required',
    reasoning_effort: 'high'
  };
  const out = toAnthropicRequest(body, { upstreamModel: 'claude-mock', caps: {} });
  assert.equal(out.system, 'Be helpful');
  assert.equal(out.model, 'claude-mock');
  const userMsg = out.messages.find((m) => m.role === 'user');
  const img = userMsg.content.find((b) => b.type === 'image');
  assert.deepEqual(img.source, { type: 'base64', media_type: 'image/png', data: 'AAAA' });
  const toolResultMsg = out.messages.find((m) => m.content?.[0]?.type === 'tool_result');
  assert.equal(toolResultMsg.content[0].tool_use_id, 'call_1');
  assert.equal(out.tools[0].name, 'get_weather');
  assert.deepEqual(out.tool_choice, { type: 'any' });
  assert.equal(out.thinking.type, 'enabled');
  assert.equal(out.thinking.budget_tokens, 16384);
  assert.ok(out.max_tokens >= 16384 + 2048);
});

test('toGeminiRequest: systemInstruction / functionResponse 名称回填 / schema 清洗', async () => {
  const body = {
    messages: [
      { role: 'system', content: 'sys' },
      { role: 'user', content: '天气如何' },
      { role: 'assistant', tool_calls: [{ id: 'call_9', type: 'function', function: { name: 'get_weather', arguments: '{"city":"北京"}' } }] },
      { role: 'tool', tool_call_id: 'call_9', content: '{"temp":25}' }
    ],
    tools: [{ type: 'function', function: { name: 'f', parameters: { $schema: 'x', type: 'object', additionalProperties: false, properties: { q: { type: 'string' } } } } }],
    tool_choice: 'auto',
    reasoning_effort: 'low'
  };
  const out = await toGeminiRequest(body, { caps: {} });
  assert.deepEqual(out.systemInstruction, { parts: [{ text: 'sys' }] });
  const modelTurn = out.contents.find((c) => c.role === 'model');
  assert.deepEqual(modelTurn.parts[0].functionCall, { name: 'get_weather', args: { city: '北京' } });
  const userTurns = out.contents.filter((c) => c.role === 'user');
  const fnResp = userTurns[userTurns.length - 1].parts[0].functionResponse;
  assert.equal(fnResp.name, 'get_weather'); // 通过 tool_call_id 回填函数名
  assert.deepEqual(fnResp.response, { temp: 25 });
  assert.equal(out.tools[0].functionDeclarations[0].parameters.$schema, undefined);
  assert.equal(out.toolConfig.functionCallingConfig.mode, 'AUTO');
  assert.equal(out.generationConfig.thinkingConfig.thinkingBudget, 1024);
});

test('fromAnthropicResponse: tool_use -> tool_calls / finish_reason', () => {
  const out = fromAnthropicResponse({
    id: 'msg_x',
    content: [{ type: 'text', text: '查一下' }, { type: 'tool_use', id: 'toolu_1', name: 'get_weather', input: { city: '北京' } }],
    stop_reason: 'tool_use',
    usage: { input_tokens: 11, output_tokens: 4 }
  }, 'anth/claude-mock');
  assert.equal(out.model, 'anth/claude-mock');
  assert.equal(out.choices[0].finish_reason, 'tool_calls');
  assert.equal(out.choices[0].message.tool_calls[0].function.name, 'get_weather');
  assert.deepEqual(JSON.parse(out.choices[0].message.tool_calls[0].function.arguments), { city: '北京' });
  assert.deepEqual(out.usage, { prompt_tokens: 11, completion_tokens: 4, total_tokens: 15, cached_tokens: null });
});

test('fromGeminiResponse: thought -> reasoning_content / functionCall -> tool_calls', () => {
  const out = fromGeminiResponse({
    candidates: [{ content: { parts: [{ text: 'think...', thought: true }, { text: '答案' }, { functionCall: { name: 'f', args: { a: 1 } } }] }, finishReason: 'STOP' }],
    usageMetadata: { promptTokenCount: 5, candidatesTokenCount: 9, thoughtsTokenCount: 3, totalTokenCount: 17 }
  }, 'gm/gemini-mock');
  assert.equal(out.choices[0].message.reasoning_content, 'think...');
  assert.equal(out.choices[0].message.content, '答案');
  assert.equal(out.choices[0].message.tool_calls[0].function.name, 'f');
  assert.equal(out.choices[0].finish_reason, 'tool_calls');
  assert.equal(out.usage.completion_tokens, 12); // candidatesTokenCount + thoughts
  assert.equal(out.usage.completion_tokens_details.reasoning_tokens, 3);
});

// ================= 单元测试：Responses API 转换 =================
test('toResponsesRequest: instructions / function_call 历史 / 工具扁平化 / reasoning', () => {
  const out = toResponsesRequest({
    messages: [
      { role: 'system', content: 'sys prompt' },
      { role: 'user', content: '天气如何' },
      { role: 'assistant', tool_calls: [{ id: 'call_9', type: 'function', function: { name: 'get_weather', arguments: '{"city":"北京"}' } }] },
      { role: 'tool', tool_call_id: 'call_9', content: '{"temp":25}' }
    ],
    tools: [{ type: 'function', function: { name: 'get_weather', description: '查天气', parameters: { type: 'object', properties: { city: { type: 'string' } } } } }],
    tool_choice: { type: 'function', function: { name: 'get_weather' } },
    max_tokens: 500,
    reasoning_effort: 'low'
  }, { upstreamModel: 'mock-resp' });
  assert.equal(out.model, 'mock-resp');
  assert.equal(out.instructions, 'sys prompt');
  assert.equal(out.max_output_tokens, 500);
  assert.equal(out.store, false);
  assert.deepEqual(out.reasoning, { effort: 'low', summary: 'auto' });
  const fnCall = out.input.find((i) => i.type === 'function_call');
  assert.deepEqual(fnCall, { type: 'function_call', call_id: 'call_9', name: 'get_weather', arguments: '{"city":"北京"}' });
  const fnOut = out.input.find((i) => i.type === 'function_call_output');
  assert.equal(fnOut.call_id, 'call_9');
  assert.equal(fnOut.output, '{"temp":25}');
  assert.deepEqual(out.tools[0], { type: 'function', name: 'get_weather', description: '查天气', parameters: { type: 'object', properties: { city: { type: 'string' } } } });
  assert.deepEqual(out.tool_choice, { type: 'function', name: 'get_weather' });
});

test('fromResponsesResponse: message/reasoning/function_call -> chat completion', () => {
  const out = fromResponsesResponse({
    id: 'resp_x', status: 'completed',
    output: [
      { type: 'reasoning', summary: [{ type: 'summary_text', text: '想一想' }] },
      { type: 'message', role: 'assistant', content: [{ type: 'output_text', text: '答案' }] },
      { type: 'function_call', call_id: 'call_3', name: 'f', arguments: '{"a":1}' }
    ],
    usage: { input_tokens: 7, output_tokens: 9, total_tokens: 16, output_tokens_details: { reasoning_tokens: 4 } }
  }, 'orr/mock-resp');
  assert.equal(out.model, 'orr/mock-resp');
  assert.equal(out.choices[0].message.content, '答案');
  assert.equal(out.choices[0].message.reasoning_content, '想一想');
  assert.equal(out.choices[0].message.tool_calls[0].id, 'call_3');
  assert.equal(out.choices[0].finish_reason, 'tool_calls');
  assert.deepEqual(out.usage.completion_tokens_details, { reasoning_tokens: 4 });
});

test('responsesToChatBody: instructions/input 项回填/工具嵌套化/reasoning', () => {
  const out = responsesToChatBody({
    instructions: 'sys',
    input: [
      '你好',
      { role: 'assistant', content: [{ type: 'output_text', text: '需要查天气' }] },
      { type: 'function_call', call_id: 'call_1', name: 'get_weather', arguments: '{"city":"北京"}' },
      { type: 'function_call_output', call_id: 'call_1', output: '{"temp":25}' },
      { role: 'user', content: [{ type: 'input_text', text: '继续' }] }
    ],
    tools: [{ type: 'function', name: 'get_weather', description: '查天气', parameters: { type: 'object', properties: {} } }],
    tool_choice: { type: 'function', name: 'get_weather' },
    max_output_tokens: 512,
    reasoning: { effort: 'medium' }
  }, { upstreamModel: 'mock-gpt' });
  assert.equal(out.model, 'mock-gpt');
  assert.equal(out.messages[0].role, 'system');
  assert.equal(out.messages[1].content, '你好');
  assert.equal(out.messages[2].tool_calls[0].id, 'call_1');
  assert.equal(out.messages[3].role, 'tool');
  assert.equal(out.messages[3].tool_call_id, 'call_1');
  assert.equal(out.messages[4].content, '继续');
  assert.deepEqual(out.tools[0], { type: 'function', function: { name: 'get_weather', description: '查天气', parameters: { type: 'object', properties: {} } } });
  assert.deepEqual(out.tool_choice, { type: 'function', function: { name: 'get_weather' } });
  assert.equal(out.max_tokens, 512);
  assert.equal(out.reasoning_effort, 'medium');
});

test('chatToResponsesResponse: output 项与 usage 映射', () => {
  const out = chatToResponsesResponse({
    id: 'cmpl-1', created: 123,
    choices: [{ index: 0, finish_reason: 'tool_calls', message: { role: 'assistant', content: '查一下', tool_calls: [{ id: 'call_9', type: 'function', function: { name: 'get_weather', arguments: '{"city":"北京"}' } }] } }],
    usage: { prompt_tokens: 7, completion_tokens: 9, total_tokens: 16, cached_tokens: 2, completion_tokens_details: { reasoning_tokens: 3 } }
  }, 'oa/mock-gpt');
  assert.equal(out.object, 'response');
  assert.equal(out.status, 'completed');
  assert.equal(out.model, 'oa/mock-gpt');
  assert.equal(out.output[0].type, 'message');
  assert.equal(out.output[0].content[0].text, '查一下');
  assert.equal(out.output[1].type, 'function_call');
  assert.equal(out.output[1].call_id, 'call_9');
  assert.equal(out.usage.input_tokens, 7);
  assert.equal(out.usage.output_tokens, 9);
  assert.equal(out.usage.input_tokens_details.cached_tokens, 2);
  assert.equal(out.usage.output_tokens_details.reasoning_tokens, 3);
});

test('fromResponsesResponse: incomplete/max_output_tokens -> length', () => {
  const out = fromResponsesResponse({
    status: 'incomplete', incomplete_details: { reason: 'max_output_tokens' },
    output: [{ type: 'message', role: 'assistant', content: [{ type: 'output_text', text: '截断' }] }],
    usage: { input_tokens: 1, output_tokens: 10, total_tokens: 11 }
  }, 'orr/mock-resp');
  assert.equal(out.choices[0].finish_reason, 'length');
});

test('usage: 从原始 SSE 文本解析 usage（含缓存命中/思考）', async () => {
  const { parseUsageFromText } = await import('../src/usage.js');
  const text = 'data: {"choices":[{"delta":{"content":"hi"}}]}\n\n' +
    'data: {"id":"x","choices":[],"usage":{"prompt_tokens":120,"completion_tokens":30,' +
    '"prompt_tokens_details":{"cached_tokens":80},"completion_tokens_details":{"reasoning_tokens":5}}}\n\n';
  const u = parseUsageFromText(text);
  assert.equal(u.prompt_tokens, 120);
  assert.equal(u.cached_tokens, 80);
  assert.equal(u.reasoning_tokens, 5);
});

// ================= 集成测试 =================
test('bootstrap: 首次设置免验证（仅本机 + 专用头，一次性）', async () => {
  // 缺少专用头 → 拒绝（防跨站伪造）
  const noHdr = await fetch(`${GW}/admin/bootstrap-status`);
  assert.equal(noHdr.status, 403);
  // 首次设置可用
  const st = await fetch(`${GW}/admin/bootstrap-status`, { headers: { 'x-gateway-bootstrap': '1' } });
  assert.equal(st.status, 200);
  assert.equal((await st.json()).first_setup, true);
  // 设置自定义 key
  const bp = await fetch(`${GW}/admin/bootstrap`, {
    method: 'POST', headers: { 'x-gateway-bootstrap': '1', 'content-type': 'application/json' },
    body: JSON.stringify({ gateway_key: 'bootstrap-key-123' })
  });
  assert.equal(bp.status, 200);
  // 新 key 立即生效
  const ok = await fetch(`${GW}/v1/models`, { headers: { authorization: 'Bearer bootstrap-key-123' } });
  assert.equal(ok.status, 200);
  // 完成后再次 bootstrap 被拒绝
  const again = await fetch(`${GW}/admin/bootstrap`, {
    method: 'POST', headers: { 'x-gateway-bootstrap': '1', 'content-type': 'application/json' }, body: '{}'
  });
  assert.equal(again.status, 403);
  // 恢复原 key 供后续用例使用
  const restore = await fetch(`${GW}/admin/config`, {
    method: 'PUT', headers: { authorization: 'Bearer bootstrap-key-123', 'content-type': 'application/json' },
    body: JSON.stringify({ gateway_key: GW_KEY })
  });
  assert.equal(restore.status, 200);
});

test('鉴权: 错误 key 返回 401', async () => {
  const res = await fetch(`${GW}/v1/models`, { headers: { authorization: 'Bearer wrong' } });
  assert.equal(res.status, 401);
});

test('/v1/models 聚合所有渠道 + 能力字段', async () => {
  const res = await fetch(`${GW}/v1/models`, { headers: AUTH });
  const j = await res.json();
  const ids = j.data.map((m) => m.id);
  for (const id of ['oa/mock-gpt', 'oa/glm-4.6', 'oa/gpt-4o-easy', 'bad/dup-model', 'anth/claude-mock', 'gm/gemini-mock', 'orr/mock-resp', 'mock-gpt', 'claude-mock']) {
    assert.ok(ids.includes(id), `缺少 ${id}`);
  }
  const glm = j.data.find((m) => m.id === 'oa/glm-4.6');
  assert.equal(glm.context_length, 200000); // 渠道元数据
  assert.equal(glm.max_output_tokens, 128000);
  const gmMock = j.data.find((m) => m.id === 'gm/gemini-mock');
  assert.equal(gmMock.context_length, 123456); // Gemini inputTokenLimit
  // embedding 模型被过滤
  assert.ok(!ids.some((i) => i.includes('embedding')));
  const claude = j.data.find((m) => m.id === 'anth/claude-mock');
  assert.equal(claude.capabilities.vision, true); // 来自知识库
});

test('chat 非流式经 anthropic 渠道: system/图片/工具 正确转换', async () => {
  anth.requests.length = 0;
  const res = await post('/v1/chat/completions', {
    model: 'anth/claude-mock',
    messages: [
      { role: 'system', content: 'Be helpful' },
      { role: 'user', content: [{ type: 'text', text: '看图' }, { type: 'image_url', image_url: { url: 'data:image/png;base64,iVBORw0KGgo=' } }] }
    ],
    max_tokens: 100
  });
  assert.equal(res.headers.get('x-gateway-channel'), 'anth');
  const j = await res.json();
  assert.equal(j.object, 'chat.completion');
  assert.equal(j.model, 'anth/claude-mock');
  assert.equal(j.choices[0].message.content, 'Hello from claude');
  assert.deepEqual(j.usage, { prompt_tokens: 10, completion_tokens: 5, total_tokens: 15, cached_tokens: 4 });
  // 校验上游收到的请求
  const up = anth.requests.find((r) => r.path === '/v1/messages');
  assert.ok(up, 'anthropic mock 未收到请求');
  assert.equal(up.headers['x-api-key'], 'sk-ant-secret');
  assert.equal(up.body.system, 'Be helpful');
  assert.equal(up.body.max_tokens, 100);
  const block = up.body.messages[0].content.find((b) => b.type === 'image');
  assert.equal(block.source.type, 'base64');
});

test('chat 流式经 gemini 渠道: thought/内容/usage/[DONE]', async () => {
  const res = await post('/v1/chat/completions', {
    model: 'gm/gemini-mock',
    messages: [{ role: 'user', content: 'hi' }],
    stream: true,
    stream_options: { include_usage: true }
  });
  assert.equal(res.headers.get('x-gateway-channel'), 'gm');
  const { events, done } = parseSSE(await collectSSE(res));
  assert.ok(done, '缺少 [DONE]');
  const content = events.map((e) => e.choices?.[0]?.delta?.content || '').join('');
  assert.equal(content, 'Hello');
  const reasoning = events.map((e) => e.choices?.[0]?.delta?.reasoning_content || '').join('');
  assert.equal(reasoning, 'pondering');
  assert.equal(events[0].choices[0].delta.role, 'assistant');
  const finish = events.map((e) => e.choices?.[0]?.finish_reason).filter(Boolean);
  assert.deepEqual(finish, ['stop']);
  const usageChunk = events.find((e) => e.usage && e.choices.length === 0);
  assert.ok(usageChunk, '缺少 usage chunk');
  assert.equal(usageChunk.usage.prompt_tokens, 7);
  const gmReq = gm.requests.find((r) => r.path.includes(':streamGenerateContent'));
  assert.ok(gmReq);
  assert.equal(gmReq.headers['x-goog-api-key'], 'AIza-gm-secret');
});

test('chat 流式经 openai 渠道: 字节级透传', async () => {
  const res = await post('/v1/chat/completions', {
    model: 'oa/mock-gpt', messages: [{ role: 'user', content: 'hi' }], stream: true
  });
  assert.equal(res.headers.get('x-gateway-channel'), 'oa');
  const text = await collectSSE(res);
  assert.ok(text.includes('"content":"Raw "'));
  assert.ok(text.trimEnd().endsWith('data: [DONE]'));
  const req = oa.requests.find((r) => r.path === '/v1/chat/completions');
  assert.equal(req.body.model, 'mock-gpt'); // 去掉渠道前缀
  assert.equal(req.headers.authorization, 'Bearer sk-oa-secret');
});

test('故障转移: bad 渠道 500 后自动切到 oa 渠道', async () => {
  const res = await post('/v1/chat/completions', {
    model: 'dup-model', messages: [{ role: 'user', content: 'hi' }]
  });
  assert.equal(res.status, 200);
  assert.equal(res.headers.get('x-gateway-channel'), 'oa');
  assert.equal(res.headers.get('x-gateway-attempt'), '2');
  const j = await res.json();
  assert.equal(j.choices[0].message.content, 'MockGPT reply');
});

test('别名: oa/gpt-4o-easy -> 上游 mock-gpt', async () => {
  oa.requests.length = 0;
  const res = await post('/v1/chat/completions', {
    model: 'oa/gpt-4o-easy', messages: [{ role: 'user', content: 'hi' }]
  });
  const j = await res.json();
  assert.equal(j.model, 'oa/gpt-4o-easy'); // 回显客户端请求名
  assert.equal(oa.requests[0].body.model, 'mock-gpt'); // 上游收到真实名
});

test('未知模型返回 404 + 相似建议', async () => {
  const res = await post('/v1/chat/completions', {
    model: 'glm-4.5', messages: [{ role: 'user', content: 'hi' }]
  });
  assert.equal(res.status, 404);
  const j = await res.json();
  assert.match(j.error.message, /不存在/);
  assert.match(j.error.message, /glm-4\.6/); // 相似模型提示
});

test('admin: 渠道 key 打码 + KEEP 保留原 key', async () => {
  const res = await fetch(`${GW}/admin/channels`, { headers: AUTH });
  const j = await res.json();
  const oaCh = j.channels.find((c) => c.name === 'oa');
  assert.equal(oaCh.api_key, '__KEEP__');
  assert.equal(oaCh.api_key_masked, 'sk-oa****cret');
  // 原样 PUT 回去，key 不丢
  const put = await fetch(`${GW}/admin/channels`, {
    method: 'PUT', headers: AUTH,
    body: JSON.stringify({ channels: j.channels })
  });
  assert.equal(put.status, 200);
  assert.equal(config.channels.find((c) => c.name === 'oa').api_key, 'sk-oa-secret');
});

test('admin: 弹窗内拉取模型列表（不落盘，KEEP 自动复用已存 key）', async () => {
  const res = await post('/admin/channels/probe-models', {
    name: 'oa', base_url: `http://127.0.0.1:${oaPort}`, api_key: '__KEEP__', protocol: 'openai'
  });
  const j = await res.json();
  assert.equal(j.ok, true);
  assert.ok(j.models.includes('mock-gpt'));
  assert.ok(j.models.includes('glm-4.6'));
  // 缺 key 且渠道不存在 → 明确报错
  const bad = await post('/admin/channels/probe-models', {
    name: 'no-such-channel', base_url: `http://127.0.0.1:${oaPort}`, api_key: '', protocol: 'openai'
  });
  assert.equal(bad.status, 400);
});

test('admin: 探测 anthropic 渠道模型能力（实测）', async () => {
  const res = await post('/admin/probe', { channel: 'anth', model: 'claude-mock' });
  const j = await res.json();
  const r = j.results[0];
  assert.equal(r.vision, true); // mock 忽略图片返回 200
  assert.equal(r.reasoning, true); // mock 接受 thinking 参数
});

test('admin: 能力覆盖生效', async () => {
  const put = await fetch(`${GW}/admin/override`, {
    method: 'PUT', headers: AUTH,
    body: JSON.stringify({ key: 'oa/mock-gpt', caps: { context: 999999 } })
  });
  assert.equal(put.status, 200);
  const res = await fetch(`${GW}/v1/models/mock-gpt`, { headers: AUTH });
  const j = await res.json();
  assert.equal(j.context_length, 999999);
  assert.equal(j.capabilities.context, 999999);
});

test('admin: 网关 key 轮换 + 自定义 + 校验', async () => {
  const oldKey = config.gateway_key;
  // 校验：过短 key 拒绝
  const short = await fetch(`${GW}/admin/config`, { method: 'PUT', headers: AUTH, body: JSON.stringify({ gateway_key: 'abc' }) });
  assert.equal(short.status, 400);
  // 自定义 key 生效
  const put = await fetch(`${GW}/admin/config`, { method: 'PUT', headers: AUTH, body: JSON.stringify({ gateway_key: 'my-custom-key-123' }) });
  assert.equal(put.status, 200);
  assert.equal(config.gateway_key, 'my-custom-key-123');
  // 旧 key 立即失效
  const oldAuth = await fetch(`${GW}/v1/models`, { headers: { authorization: `Bearer ${oldKey}` } });
  assert.equal(oldAuth.status, 401);
  // 新 key 可用
  const newAuth = await fetch(`${GW}/v1/models`, { headers: { authorization: 'Bearer my-custom-key-123' } });
  assert.equal(newAuth.status, 200);
  // 服务端轮换（注意：此时有效 key 已是自定义 key，需用新 key 调用）
  const rot = await fetch(`${GW}/admin/rotate-key`, {
    method: 'POST', headers: { authorization: `Bearer ${config.gateway_key}`, 'content-type': 'application/json' }
  });
  const rj = await rot.json();
  assert.equal(rot.status, 200);
  assert.match(rj.gateway_key, /^sk-gw-/);
  assert.notEqual(rj.gateway_key, 'my-custom-key-123');
  // 自定义 key 已失效
  const stale = await fetch(`${GW}/v1/models`, { headers: { authorization: 'Bearer my-custom-key-123' } });
  assert.equal(stale.status, 401);
  // 校验：非法端口拒绝（用轮换后的 key）
  const badPort = await fetch(`${GW}/admin/config`, {
    method: 'PUT', headers: { authorization: `Bearer ${config.gateway_key}`, 'content-type': 'application/json' },
    body: JSON.stringify({ port: 99999 })
  });
  assert.equal(badPort.status, 400);
  // 恢复原 key，保证后续用例的固定 AUTH 头可用
  const restore = await fetch(`${GW}/admin/config`, {
    method: 'PUT', headers: { authorization: `Bearer ${config.gateway_key}`, 'content-type': 'application/json' },
    body: JSON.stringify({ gateway_key: GW_KEY })
  });
  assert.equal(restore.status, 200);
  assert.equal(config.gateway_key, GW_KEY);
});

test('流式首包超时: 上游挂起时快速 504 中断（failover 后）', async () => {
  const t0 = Date.now();
  const res = await post('/v1/chat/completions', {
    model: 'hang-model', messages: [{ role: 'user', content: 'hi' }], stream: true
  });
  const ms = Date.now() - t0;
  assert.equal(res.status, 504);
  assert.ok(ms < 5000, `应约 1s 返回，实际 ${ms}ms`);
  const j = await res.json();
  assert.match(j.error.message, /首包/);
});

test('dashboard: 首页 HTML 可访问', async () => {
  const res = await fetch(`${GW}/`);
  assert.equal(res.status, 200);
  const html = await res.text();
  assert.ok(html.includes('统一 LLM 网关'));
});

// ================= 集成测试：Responses API 渠道 =================
test('chat 非流式经 openai-responses 渠道: 请求互转 + 响应回转', async () => {
  orc.requests.length = 0;
  const res = await post('/v1/chat/completions', {
    model: 'orr/mock-resp',
    messages: [
      { role: 'system', content: 'sys' },
      { role: 'user', content: 'hi' },
      { role: 'assistant', tool_calls: [{ id: 'call_1', type: 'function', function: { name: 'get_weather', arguments: '{"city":"北京"}' } }] },
      { role: 'tool', tool_call_id: 'call_1', content: '{"temp":25}' }
    ],
    tools: [{ type: 'function', function: { name: 'get_weather', parameters: { type: 'object', properties: { city: { type: 'string' } } } } }],
    max_tokens: 300
  });
  assert.equal(res.headers.get('x-gateway-channel'), 'orr');
  const j = await res.json();
  assert.equal(j.object, 'chat.completion');
  assert.equal(j.model, 'orr/mock-resp');
  assert.equal(j.choices[0].message.content, 'Resp reply');
  assert.equal(j.choices[0].message.reasoning_content, 'resp think');
  assert.equal(j.choices[0].message.tool_calls[0].function.name, 'get_weather');
  assert.equal(j.choices[0].finish_reason, 'tool_calls');
  assert.equal(j.usage.prompt_tokens, 8);
  // 校验上游收到的 Responses 请求
  const up = orc.requests.find((r) => r.path === '/v1/responses');
  assert.ok(up, 'responses mock 未收到请求');
  assert.equal(up.headers.authorization, 'Bearer sk-orc-secret');
  assert.equal(up.body.model, 'mock-resp');
  assert.equal(up.body.instructions, 'sys');
  assert.equal(up.body.max_output_tokens, 300);
  assert.equal(up.body.store, false);
  const fnOut = up.body.input.find((i) => i.type === 'function_call_output');
  assert.equal(fnOut.call_id, 'call_1');
  assert.equal(up.body.tools[0].type, 'function'); // 扁平化（非 chat 的嵌套结构）
});

test('chat 流式经 openai-responses 渠道: 事件流 -> OpenAI chunk', async () => {
  const res = await post('/v1/chat/completions', {
    model: 'orr/mock-resp',
    messages: [{ role: 'user', content: 'hi' }],
    stream: true,
    stream_options: { include_usage: true }
  });
  assert.equal(res.headers.get('x-gateway-channel'), 'orr');
  const { events, done } = parseSSE(await collectSSE(res));
  assert.ok(done, '缺少 [DONE]');
  const content = events.map((e) => e.choices?.[0]?.delta?.content || '').join('');
  assert.equal(content, 'Hi there');
  const reasoning = events.map((e) => e.choices?.[0]?.delta?.reasoning_content || '').join('');
  assert.equal(reasoning, 'resp think');
  const argChunks = events.flatMap((e) => e.choices?.[0]?.delta?.tool_calls || []);
  assert.equal(argChunks[0].id, 'call_7');
  assert.equal(argChunks.map((t) => t.function?.arguments || '').join(''), '{"city":"北京"}');
  assert.deepEqual(events.map((e) => e.choices?.[0]?.finish_reason).filter(Boolean), ['tool_calls']);
  const usageChunk = events.find((e) => e.usage && e.choices.length === 0);
  assert.equal(usageChunk.usage.prompt_tokens, 5);
});

test('/v1/responses 原生透传: Responses 客户端直连', async () => {
  orc.requests.length = 0;
  const res = await post('/v1/responses', {
    model: 'mock-resp',
    input: '画一张图',
    max_output_tokens: 256
  });
  assert.equal(res.status, 200);
  assert.equal(res.headers.get('x-gateway-channel'), 'orr');
  const j = await res.json();
  assert.equal(j.object, 'response');
  assert.equal(j.model, 'mock-resp'); // 回显客户端请求名
  assert.equal(orc.requests[0].body.model, 'mock-resp'); // 上游收到原名
  assert.equal(orc.requests[0].body.input, '画一张图'); // 原样透传
});

test('/v1/responses 转换: chat 渠道非流式', async () => {
  oa.requests.length = 0;
  const res = await post('/v1/responses', { model: 'oa/mock-gpt', input: 'hi', max_output_tokens: 128 });
  assert.equal(res.status, 200);
  assert.equal(res.headers.get('x-gateway-channel'), 'oa');
  const j = await res.json();
  assert.equal(j.object, 'response');
  assert.equal(j.status, 'completed');
  const msg = j.output.find((o) => o.type === 'message');
  assert.equal(msg.content[0].text, 'MockGPT reply');
  assert.equal(j.usage.input_tokens, 9);
  assert.equal(j.usage.output_tokens, 4);
  assert.equal(j.usage.input_tokens_details.cached_tokens, 3);
  // 上游收到的是 chat completions 请求
  assert.equal(oa.requests[0].path, '/v1/chat/completions');
  assert.equal(oa.requests[0].body.messages[0].content, 'hi');
  assert.equal(oa.requests[0].body.max_tokens, 128);
});

test('/v1/responses 转换: anthropic 渠道（Responses 客户端接入 claude）', async () => {
  const res = await post('/v1/responses', { model: 'anth/claude-mock', input: [{ role: 'user', content: [{ type: 'input_text', text: 'hi' }] }] });
  assert.equal(res.status, 200);
  assert.equal(res.headers.get('x-gateway-channel'), 'anth');
  const j = await res.json();
  assert.equal(j.status, 'completed');
  assert.equal(j.output.find((o) => o.type === 'message').content[0].text, 'Hello from claude');
});

test('/v1/responses 转换: chat 渠道流式（Responses 事件流）', async () => {
  const res = await post('/v1/responses', { model: 'oa/mock-gpt', input: 'hi', stream: true });
  assert.equal(res.status, 200);
  assert.equal(res.headers.get('x-gateway-channel'), 'oa');
  const text = await collectSSE(res);
  const events = [];
  for (const block of text.split('\n\n')) {
    const evLine = block.split('\n').find((l) => l.startsWith('event:'));
    const dataLine = block.split('\n').find((l) => l.startsWith('data:'));
    if (evLine && dataLine) events.push({ event: evLine.slice(6).trim(), data: JSON.parse(dataLine.slice(5).trim()) });
  }
  assert.equal(events[0].event, 'response.created');
  const deltas = events.filter((e) => e.event === 'response.output_text.delta').map((e) => e.data.delta).join('');
  assert.equal(deltas, 'Raw stream');
  const done = events.find((e) => e.event === 'response.completed');
  assert.ok(done, '缺少 response.completed');
  assert.equal(done.data.response.usage.input_tokens, 3);
  assert.equal(done.data.response.usage.output_tokens, 2);
});

test('用量统计与持久化: tokens/缓存入日志与看板', async () => {
  // 非流式（mock: prompt 9 / completion 4 / cached 3）
  await post('/v1/chat/completions', { model: 'oa/mock-gpt', messages: [{ role: 'user', content: 'hi' }] });
  const l1 = (await (await fetch(`${GW}/admin/logs?limit=5`, { headers: AUTH })).json()).logs
    .find((l) => l.model === 'oa/mock-gpt' && !l.stream);
  assert.ok(l1, '日志缺该请求');
  assert.equal(l1.prompt_tokens, 9);
  assert.equal(l1.cached_tokens, 3);
  // 流式（raw 透传解析: prompt 3 / completion 2 / cached 1）
  await post('/v1/chat/completions', { model: 'oa/mock-gpt', messages: [{ role: 'user', content: 'hi' }], stream: true });
  const l2 = (await (await fetch(`${GW}/admin/logs?limit=5`, { headers: AUTH })).json()).logs
    .find((l) => l.model === 'oa/mock-gpt' && l.stream);
  assert.ok(l2, '日志缺流式请求');
  assert.equal(l2.prompt_tokens, 3);
  assert.equal(l2.cached_tokens, 1);
  // 看板聚合
  const u = await (await fetch(`${GW}/admin/usage`, { headers: AUTH })).json();
  assert.ok(u.totals.requests >= 2, '看板请求数异常');
  assert.ok(u.totals.prompt >= 12, '看板输入 tokens 异常');
  assert.ok(u.totals.cached >= 4, '看板缓存 tokens 异常');
  assert.ok(u.byChannel.some((c) => c.name === 'oa' && c.prompt >= 12), '按渠道聚合异常');
  assert.ok(u.byModel.some((m) => m.name === 'oa/mock-gpt' && m.prompt >= 12), '按模型聚合异常');
  // 持久化文件
  assert.ok(fs.existsSync(path.join(tmp, 'data', 'usage.json')), 'usage.json 未生成');
  assert.ok(fs.existsSync(path.join(tmp, 'data', 'requests.jsonl')), 'requests.jsonl 未生成');
});

test('admin: 多 Key 管理（添加/启停/删除）', async () => {
  // 添加
  const add = await post('/admin/keys', { name: '朋友A' });
  assert.equal(add.status, 200);
  const entry = (await add.json()).entry;
  assert.match(entry.key, /^sk-gw-/);
  // 新 key 立即可用
  const ok1 = await fetch(`${GW}/v1/models`, { headers: { authorization: `Bearer ${entry.key}` } });
  assert.equal(ok1.status, 200);
  // 停用 → 立即失效
  await fetch(`${GW}/admin/keys`, { method: 'PUT', headers: AUTH, body: JSON.stringify({ key: entry.key, enabled: false }) });
  const ok2 = await fetch(`${GW}/v1/models`, { headers: { authorization: `Bearer ${entry.key}` } });
  assert.equal(ok2.status, 401);
  // 重新启用 → 恢复
  await fetch(`${GW}/admin/keys`, { method: 'PUT', headers: AUTH, body: JSON.stringify({ key: entry.key, enabled: true }) });
  const ok3 = await fetch(`${GW}/v1/models`, { headers: { authorization: `Bearer ${entry.key}` } });
  assert.equal(ok3.status, 200);
  // 自定义 key
  const custom = await post('/admin/keys', { name: '朋友B', key: 'my-friend-key-999' });
  assert.equal(custom.status, 200);
  const ok4 = await fetch(`${GW}/v1/models`, { headers: { authorization: 'Bearer my-friend-key-999' } });
  assert.equal(ok4.status, 200);
  // 删除 → 失效
  await fetch(`${GW}/admin/keys`, { method: 'DELETE', headers: AUTH, body: JSON.stringify({ key: entry.key }) });
  const ok5 = await fetch(`${GW}/v1/models`, { headers: { authorization: `Bearer ${entry.key}` } });
  assert.equal(ok5.status, 401);
  // 主 Key 不受影响
  const primary = await fetch(`${GW}/v1/models`, { headers: AUTH });
  assert.equal(primary.status, 200);
});

// ---- 清理：关闭所有服务器，避免测试进程挂住 ----
test.after(() => {
  server.closeAllConnections?.();
  server.close();
  for (const s of [oa.server, bad.server, anth.server, gm.server, orc.server, hang.server]) {
    s.closeAllConnections?.();
    s.close();
  }
});
