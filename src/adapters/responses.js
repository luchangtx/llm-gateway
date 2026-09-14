// OpenAI(Chat Completions) 客户端  ->  OpenAI Responses API(/v1/responses) 上游 的双向转换
// 以及 /v1/responses 原生入口的透传
import crypto from 'node:crypto';
import { openAIChunk, pumpSSE } from '../sse.js';
import { endpointFor, upstreamFetch, UpstreamError, chatTimeoutOpts } from '../upstream.js';
import { genId, textOf } from './util.js';

export function toResponsesRequest(body, { upstreamModel }) {
  const instructions = [];
  const input = [];

  for (const m of body.messages || []) {
    const role = m.role === 'developer' ? 'system' : m.role;
    if (role === 'system') { instructions.push(textOf(m.content)); continue; }
    if (role === 'tool') {
      // 工具结果：output 必须是字符串
      const out = textOf(m.content);
      input.push({ type: 'function_call_output', call_id: m.tool_call_id || 'call_unknown', output: out });
      continue;
    }
    if (role === 'assistant') {
      const text = textOf(m.content);
      if (text) input.push({ role: 'assistant', content: [{ type: 'output_text', text }] });
      for (const tc of m.tool_calls || []) {
        if (tc.type !== 'function' && !tc.function) continue;
        const args = typeof tc.function.arguments === 'string' ? tc.function.arguments : JSON.stringify(tc.function.arguments ?? {});
        input.push({ type: 'function_call', call_id: tc.id || `call_${crypto.randomUUID().slice(0, 8)}`, name: tc.function.name, arguments: args });
      }
      continue;
    }
    // user
    if (typeof m.content === 'string') { input.push({ role: 'user', content: m.content }); continue; }
    if (Array.isArray(m.content)) {
      const parts = [];
      for (const p of m.content) {
        if (!p) continue;
        if (typeof p === 'string') { parts.push({ type: 'input_text', text: p }); continue; }
        if (p.type === 'text') parts.push({ type: 'input_text', text: p.text || '' });
        else if (p.type === 'image_url' || p.type === 'input_image') {
          const url = p.image_url?.url ?? p.image_url ?? p.url;
          if (typeof url === 'string' && url) parts.push({ type: 'input_image', image_url: url });
        }
      }
      if (parts.length) input.push({ role: 'user', content: parts });
    }
  }

  const out = { model: upstreamModel, input };
  const sys = instructions.filter(Boolean).join('\n\n');
  if (sys) out.instructions = sys;

  const maxTok = body.max_completion_tokens ?? body.max_tokens;
  if (maxTok !== undefined && maxTok !== null) out.max_output_tokens = maxTok;
  if (body.temperature !== undefined && body.temperature !== null) out.temperature = body.temperature;
  if (body.top_p !== undefined && body.top_p !== null) out.top_p = body.top_p;
  if (body.parallel_tool_calls !== undefined && body.parallel_tool_calls !== null) out.parallel_tool_calls = body.parallel_tool_calls;
  if (body.stream) out.stream = true;
  // 网关场景默认不存储会话（OpenAI Responses 默认 store=true），客户端可显式传 store 覆盖
  out.store = body.store ?? false;
  // 注：Responses API 不支持 stop 序列，静默丢弃

  const tools = (body.tools || []).filter((t) => t && (t.type === 'function' ? true : !!t.function)).map((t) => {
    const f = t.function || t;
    return {
      type: 'function',
      name: f.name,
      description: f.description || '',
      parameters: f.parameters && typeof f.parameters === 'object' ? f.parameters : { type: 'object', properties: {} }
    };
  });
  if (tools.length) out.tools = tools;

  const choice = body.tool_choice;
  if (choice === 'auto' || choice === 'none' || choice === 'required') out.tool_choice = choice;
  else if (choice && typeof choice === 'object' && choice.function?.name) out.tool_choice = { type: 'function', name: choice.function.name };

  if (body.reasoning_effort) out.reasoning = { effort: body.reasoning_effort, summary: 'auto' };
  return out;
}

function mapFinish(resp, sawFunctionCall) {
  if (resp.status === 'incomplete') {
    const reason = resp.incomplete_details?.reason;
    if (reason === 'max_output_tokens') return 'length';
    if (reason === 'content_filter') return 'content_filter';
    return 'stop';
  }
  return sawFunctionCall ? 'tool_calls' : 'stop';
}

export function fromResponsesResponse(resp, clientModel) {
  let text = '';
  const reasoningParts = [];
  const toolCalls = [];
  for (const item of resp.output || []) {
    if (item.type === 'message') {
      for (const p of item.content || []) {
        if (p.type === 'output_text') text += p.text || '';
        else if (p.type === 'refusal') text += p.refusal || '';
      }
    } else if (item.type === 'reasoning') {
      for (const s of item.summary || []) {
        if (s.type === 'summary_text' && s.text) reasoningParts.push(s.text);
      }
    } else if (item.type === 'function_call') {
      toolCalls.push({
        id: item.call_id || item.id,
        type: 'function',
        function: { name: item.name, arguments: item.arguments || '{}' }
      });
    }
  }
  const message = { role: 'assistant', content: text || null };
  if (reasoningParts.length) message.reasoning_content = reasoningParts.join('');
  if (toolCalls.length) message.tool_calls = toolCalls;
  const usage = resp.usage || {};
  const reasoningTokens = usage.output_tokens_details?.reasoning_tokens ?? 0;
  const cachedTokens = usage.input_tokens_details?.cached_tokens ?? null;
  return {
    id: resp.id || genId(),
    object: 'chat.completion',
    created: Math.floor(Date.now() / 1000),
    model: clientModel,
    choices: [{ index: 0, message, finish_reason: mapFinish(resp, toolCalls.length > 0) }],
    usage: {
      prompt_tokens: usage.input_tokens ?? 0,
      completion_tokens: usage.output_tokens ?? 0,
      total_tokens: usage.total_tokens ?? (usage.input_tokens ?? 0) + (usage.output_tokens ?? 0),
      cached_tokens: cachedTokens,
      ...(reasoningTokens ? { completion_tokens_details: { reasoning_tokens: reasoningTokens } } : {})
    }
  };
}

/** Responses SSE 事件流 -> OpenAI chunk 迭代器 */
export async function* responsesStreamToChat(body, { clientModel, includeUsage = false, usageRef = {}, signal }) {
  const id = genId();
  let started = false;
  let finish = null;
  let usage = null;
  let sawFunctionCall = false;
  const toolIdxByOutput = new Map();
  let nextTool = 0;

  const emit = (delta, fin = null) => {
    const d = started ? delta : { role: 'assistant', content: '', ...delta };
    started = true;
    return openAIChunk({ id, model: clientModel, delta: d, finish: fin });
  };

  for await (const ev of pumpSSE(body, { signal })) {
    if (ev.done) break;
    let j;
    try { j = JSON.parse(ev.data); } catch { continue; }
    const type = j.type || ev.event;
    switch (type) {
      case 'response.output_text.delta':
        if (j.delta) yield emit({ content: j.delta });
        break;
      case 'response.reasoning_summary_text.delta':
      case 'response.reasoning_text.delta':
        if (j.delta) yield emit({ reasoning_content: j.delta });
        break;
      case 'response.refusal.delta':
        if (j.delta) yield emit({ content: j.delta });
        break;
      case 'response.output_item.added': {
        const item = j.item || {};
        if (item.type === 'function_call') {
          sawFunctionCall = true;
          const idx = nextTool++;
          toolIdxByOutput.set(j.output_index ?? idx, idx);
          yield emit({ tool_calls: [{ index: idx, id: item.call_id || item.id, type: 'function', function: { name: item.name || '', arguments: '' } }] });
        }
        break;
      }
      case 'response.function_call_arguments.delta': {
        const idx = toolIdxByOutput.get(j.output_index ?? 0) ?? 0;
        if (j.delta) yield emit({ tool_calls: [{ index: idx, function: { arguments: j.delta } }] });
        break;
      }
      case 'response.completed':
      case 'response.incomplete': {
        const r = j.response || {};
        usage = r.usage || usage;
        finish = mapFinish(r, sawFunctionCall);
        break;
      }
      case 'response.failed':
        usageRef.error = j.response?.error?.message || '上游 response failed';
        break;
      case 'error':
        usageRef.error = j.message || j.error?.message || '上游流错误';
        break;
      default:
        break;
    }
  }

  yield emit({}, finish);
  const p = usage?.input_tokens ?? 0;
  const c = usage?.output_tokens ?? 0;
  usageRef.prompt_tokens = p;
  usageRef.completion_tokens = c;
  usageRef.total_tokens = usage?.total_tokens ?? p + c;
  usageRef.cached_tokens = usage?.input_tokens_details?.cached_tokens ?? null;
  usageRef.reasoning_tokens = usage?.output_tokens_details?.reasoning_tokens ?? null;
  if (includeUsage) {
    yield {
      id, object: 'chat.completion.chunk', created: Math.floor(Date.now() / 1000), model: clientModel,
      choices: [],
      usage: { prompt_tokens: p, completion_tokens: c, total_tokens: usage?.total_tokens ?? p + c, cached_tokens: usage?.input_tokens_details?.cached_tokens ?? null }
    };
  }
  if (usageRef.error) throw new UpstreamError(502, usageRef.error);
}

export async function forward({ channel, route, body, clientModel, signal }) {
  const stream = !!body.stream;
  const upstreamBody = toResponsesRequest(body, { upstreamModel: route.upstream });
  const url = endpointFor(channel, 'responses');
  const res = await upstreamFetch(channel, url, { method: 'POST', body: upstreamBody, ...chatTimeoutOpts(stream), signal });
  if (!res.ok) throw await UpstreamError.fromResponse(res);
  if (!stream) {
    const json = await res.json();
    if (json.status === 'failed' || json.error) {
      throw new UpstreamError(502, json.error?.message || json.failure_reason || '上游 response failed');
    }
    return { kind: 'json', status: 200, json: fromResponsesResponse(json, clientModel) };
  }
  const usageRef = {};
  const iterator = responsesStreamToChat(res.body, { clientModel, includeUsage: !!body.stream_options?.include_usage, usageRef, signal });
  return { kind: 'sse', status: 200, iterator, usageRef };
}

/** /v1/responses 原生透传：Responses 客户端 -> Responses 渠道（同方言直转） */
export async function forwardNative({ channel, route, body, clientModel, signal }) {
  const stream = !!body.stream;
  const upstreamBody = { ...body, model: route.upstream };
  const url = endpointFor(channel, 'responses');
  const res = await upstreamFetch(channel, url, { method: 'POST', body: upstreamBody, ...chatTimeoutOpts(stream), signal });
  if (!res.ok) throw await UpstreamError.fromResponse(res);
  if (!stream) {
    const json = await res.json();
    if (json && typeof json === 'object') json.model = clientModel;
    return { kind: 'json', status: res.status, json };
  }
  return { kind: 'raw', status: res.status, body: res.body, headers: res.headers };
}

// ================= 反向转换：Responses 方言客户端 -> chat 型渠道 =================

function rid() { return crypto.randomUUID().replace(/-/g, '').slice(0, 20); }

function textFromChatContent(content) {
  if (typeof content === 'string') return content;
  if (Array.isArray(content)) {
    return content.filter((p) => p && (p.type === 'text' || typeof p === 'string'))
      .map((p) => (typeof p === 'string' ? p : p.text || '')).join('\n');
  }
  return '';
}

/** Responses 请求体 -> Chat Completions 请求体 */
export function responsesToChatBody(native, { upstreamModel } = {}) {
  const messages = [];
  if (native.instructions) messages.push({ role: 'system', content: String(native.instructions) });

  const input = native.input;
  if (typeof input === 'string') {
    if (input) messages.push({ role: 'user', content: input });
  } else if (Array.isArray(input)) {
    for (const item of input) {
      if (!item) continue;
      if (typeof item === 'string') { messages.push({ role: 'user', content: item }); continue; }
      if (item.type === 'function_call') {
        messages.push({
          role: 'assistant',
          tool_calls: [{ id: item.call_id || item.id || `call_${rid()}`, type: 'function', function: { name: item.name, arguments: item.arguments || '{}' } }]
        });
        continue;
      }
      if (item.type === 'function_call_output') {
        messages.push({
          role: 'tool',
          tool_call_id: item.call_id || 'call_unknown',
          content: typeof item.output === 'string' ? item.output : JSON.stringify(item.output ?? '')
        });
        continue;
      }
      const role = item.role === 'developer' ? 'system' : item.role;
      if (role === 'system') { messages.push({ role: 'system', content: textFromChatContent(item.content) }); continue; }
      if (role === 'assistant') {
        const text = textFromChatContent(item.content);
        if (text) messages.push({ role: 'assistant', content: text });
        continue;
      }
      if (role === 'user') {
        if (typeof item.content === 'string') { messages.push({ role: 'user', content: item.content }); continue; }
        const parts = Array.isArray(item.content) ? item.content.map((p) => {
          if (!p) return null;
          if (typeof p === 'string') return { type: 'text', text: p };
          if (p.type === 'input_text') return { type: 'text', text: p.text || '' };
          if (p.type === 'input_image') return { type: 'image_url', image_url: { url: p.image_url || p.url } };
          return null;
        }).filter(Boolean) : [];
        if (parts.length) {
          // 纯文本时归一为字符串，兼容性最好
          const onlyText = parts.every((p) => p.type === 'text');
          messages.push({ role: 'user', content: onlyText ? parts.map((p) => p.text).join('\n') : parts });
        }
      }
    }
  }

  const out = { model: upstreamModel, messages };
  if (native.max_output_tokens != null) out.max_tokens = native.max_output_tokens;
  if (native.temperature != null) out.temperature = native.temperature;
  if (native.top_p != null) out.top_p = native.top_p;
  if (native.parallel_tool_calls != null) out.parallel_tool_calls = native.parallel_tool_calls;
  if (native.stream != null) out.stream = !!native.stream;
  if (native.stream_options) out.stream_options = native.stream_options;
  if (native.stop) out.stop = native.stop;

  const tools = (native.tools || []).filter((t) => t && (t.type === 'function' ? !!t.name : !!t.function?.name));
  if (tools.length) {
    out.tools = tools.map((t) => t.type === 'function'
      ? { type: 'function', function: { name: t.name, description: t.description || '', parameters: t.parameters && typeof t.parameters === 'object' ? t.parameters : { type: 'object', properties: {} } } }
      : t);
  }
  const choice = native.tool_choice;
  if (choice === 'auto' || choice === 'none' || choice === 'required') out.tool_choice = choice;
  else if (choice && typeof choice === 'object' && choice.type === 'function' && choice.name) {
    out.tool_choice = { type: 'function', function: { name: choice.name } };
  }
  if (native.reasoning?.effort) out.reasoning_effort = native.reasoning.effort;
  return out;
}

/** Chat Completions 响应 -> Responses 响应 */
export function chatToResponsesResponse(chat, model) {
  const ch = chat.choices?.[0] || {};
  const m = ch.message || {};
  const output = [];
  if (m.reasoning_content) {
    output.push({ id: 'rs_' + rid(), type: 'reasoning', summary: [{ type: 'summary_text', text: m.reasoning_content }] });
  }
  const text = typeof m.content === 'string' ? m.content : '';
  if (text) output.push({ id: 'msg_' + rid(), type: 'message', role: 'assistant', content: [{ type: 'output_text', text, annotations: [] }] });
  for (const tc of m.tool_calls || []) {
    output.push({ id: 'fc_' + rid(), type: 'function_call', call_id: tc.id, name: tc.function?.name, arguments: tc.function?.arguments || '{}' });
  }
  const u = chat.usage || {};
  const status = ch.finish_reason === 'length' || ch.finish_reason === 'content_filter' ? 'incomplete' : 'completed';
  return {
    id: 'resp_' + (chat.id || rid()),
    object: 'response',
    created_at: chat.created || Math.floor(Date.now() / 1000),
    status,
    ...(status === 'incomplete' ? { incomplete_details: { reason: ch.finish_reason === 'length' ? 'max_output_tokens' : 'content_filter' } } : {}),
    model,
    output,
    usage: {
      input_tokens: u.prompt_tokens ?? 0,
      output_tokens: u.completion_tokens ?? 0,
      total_tokens: u.total_tokens ?? (u.prompt_tokens ?? 0) + (u.completion_tokens ?? 0),
      input_tokens_details: { cached_tokens: u.cached_tokens ?? u.prompt_tokens_details?.cached_tokens ?? 0 },
      output_tokens_details: { reasoning_tokens: u.completion_tokens_details?.reasoning_tokens ?? 0 }
    },
    error: null
  };
}

/** Chat Completions chunk 流 -> Responses SSE 事件流 */
export async function* chatChunksToResponsesEvents(chunks, { model }) {
  const id = 'resp_' + rid();
  const emit = (event, data) => ({ event, data: { type: event, ...data } });
  yield emit('response.created', { response: { id, object: 'response', status: 'in_progress', model } });
  let outIdx = 0;
  let msgAdded = false;
  let rsAdded = false;
  const tools = new Map();
  let finish = null;
  let usage = null;

  for await (const chunk of chunks) {
    if (chunk.usage) usage = chunk.usage;
    const c0 = chunk.choices?.[0];
    const d = c0?.delta || {};
    if (c0?.finish_reason) finish = c0.finish_reason;
    if (d.reasoning_content) {
      if (!rsAdded) {
        rsAdded = true;
        yield emit('response.output_item.added', { output_index: outIdx, item: { type: 'reasoning', id: 'rs_' + rid(), summary: [] } });
        outIdx++;
      }
      yield emit('response.reasoning_summary_text.delta', { delta: d.reasoning_content });
      continue;
    }
    if (d.content) {
      if (!msgAdded) {
        msgAdded = true;
        yield emit('response.output_item.added', { output_index: outIdx, item: { type: 'message', role: 'assistant', id: 'msg_' + rid(), content: [] } });
        yield emit('response.content_part.added', { item_id: 'msg_' + rid(), part: { type: 'output_text', text: '', annotations: [] } });
        outIdx++;
      }
      yield emit('response.output_text.delta', { delta: d.content });
      continue;
    }
    for (const tc of d.tool_calls || []) {
      const idx = tc.index ?? 0;
      if (!tools.has(idx)) {
        tools.set(idx, { outIndex: outIdx, callId: tc.id, name: tc.function?.name || '' });
        yield emit('response.output_item.added', { output_index: outIdx, item: { type: 'function_call', id: 'fc_' + rid(), call_id: tc.id, name: tc.function?.name || '', arguments: '' } });
        outIdx++;
      }
      if (tc.function?.arguments) {
        yield emit('response.function_call_arguments.delta', { output_index: tools.get(idx).outIndex, delta: tc.function.arguments });
      }
    }
  }

  if (msgAdded) {
    yield emit('response.output_text.done', {});
    yield emit('response.output_item.done', {});
  }
  yield emit('response.completed', {
    response: {
      id, object: 'response', status: finish === 'length' || finish === 'content_filter' ? 'incomplete' : 'completed',
      model,
      usage: {
        input_tokens: usage?.prompt_tokens ?? 0,
        output_tokens: usage?.completion_tokens ?? 0,
        total_tokens: usage?.total_tokens ?? 0,
        input_tokens_details: { cached_tokens: usage?.cached_tokens ?? usage?.prompt_tokens_details?.cached_tokens ?? 0 },
        output_tokens_details: { reasoning_tokens: usage?.completion_tokens_details?.reasoning_tokens ?? 0 }
      },
      error: null
    }
  });
}
