// OpenAI(Chat Completions) 客户端  ->  Anthropic(/v1/messages) 上游 的双向转换
import crypto from 'node:crypto';
import { openAIChunk, pumpSSE } from '../sse.js';
import { endpointFor, upstreamFetch, UpstreamError, chatTimeoutOpts } from '../upstream.js';

function genId() { return 'chatcmpl-' + crypto.randomUUID(); }

function parseArgs(args) {
  if (!args) return {};
  if (typeof args === 'object') return args;
  try { return args.trim() ? JSON.parse(args) : {}; } catch { return {}; }
}

function textOf(content) {
  if (typeof content === 'string') return content;
  if (Array.isArray(content)) {
    return content.filter((p) => p && (p.type === 'text' || typeof p === 'string'))
      .map((p) => (typeof p === 'string' ? p : p.text || '')).join('\n');
  }
  return '';
}

function parseDataUrl(url) {
  const m = /^data:([^;,]+)?(;base64)?,(.*)$/s.exec(url);
  if (!m) return null;
  const mediaType = m[1] || 'application/octet-stream';
  if (!m[2]) return null; // 非 base64 data url 不支持
  return { mediaType, data: m[3] };
}

function imageBlock(imageUrl) {
  if (typeof imageUrl !== 'string' || !imageUrl) return null;
  if (imageUrl.startsWith('data:')) {
    const parsed = parseDataUrl(imageUrl);
    if (!parsed) throw new UpstreamError(400, '图片 data URL 必须是 base64 编码');
    return { type: 'image', source: { type: 'base64', media_type: parsed.mediaType, data: parsed.data } };
  }
  if (/^https?:\/\//i.test(imageUrl)) {
    return { type: 'image', source: { type: 'url', url: imageUrl } };
  }
  throw new UpstreamError(400, `不支持的图片地址: ${String(imageUrl).slice(0, 80)}`);
}

function userBlocks(content) {
  if (typeof content === 'string') return content ? [{ type: 'text', text: content }] : [];
  if (!Array.isArray(content)) return [];
  const blocks = [];
  for (const part of content) {
    if (!part) continue;
    if (typeof part === 'string') { blocks.push({ type: 'text', text: part }); continue; }
    if (part.type === 'text') blocks.push({ type: 'text', text: part.text || '' });
    else if (part.type === 'image_url') {
      const b = imageBlock(part.image_url?.url ?? part.image_url);
      if (b) blocks.push(b);
    } else if (part.type === 'input_image') {
      const b = imageBlock(part.image_url || part.url);
      if (b) blocks.push(b);
    }
  }
  return blocks.filter((b) => b.type !== 'text' || b.text !== '');
}

function assistantBlocks(msg) {
  const blocks = [];
  const text = textOf(msg.content);
  if (text) blocks.push({ type: 'text', text });
  for (const tc of msg.tool_calls || []) {
    if (tc.type !== 'function' && !tc.function) continue;
    blocks.push({ type: 'tool_use', id: tc.id || `toolu_${crypto.randomUUID().slice(0, 8)}`, name: tc.function.name, input: parseArgs(tc.function.arguments) });
  }
  return blocks;
}

const EFFORT_BUDGET = { minimal: 1024, low: 2048, medium: 8192, high: 16384, xhigh: 32000 };

export function toAnthropicRequest(body, { upstreamModel, caps = {} }) {
  const systemParts = [];
  const messages = [];
  let toolIdToName = {};

  const pushToolResult = (msg) => {
    const text = textOf(msg.content) || ' ';
    const block = { type: 'tool_result', tool_use_id: msg.tool_call_id || 'toolu_unknown', content: [{ type: 'text', text }] };
    const last = messages[messages.length - 1];
    if (last && last._toolResult) last.content.push(block);
    else messages.push({ role: 'user', _toolResult: true, content: [block] });
  };

  for (const m of body.messages || []) {
    const role = m.role === 'developer' ? 'system' : m.role;
    if (role === 'system') { systemParts.push(textOf(m.content)); continue; }
    if (role === 'tool') { pushToolResult(m); continue; }
    if (role === 'assistant') {
      for (const tc of m.tool_calls || []) toolIdToName[tc.id] = tc.function?.name;
      const blocks = assistantBlocks(m);
      if (blocks.length) messages.push({ role: 'assistant', content: blocks });
      continue;
    }
    const blocks = userBlocks(m.content);
    if (blocks.length) messages.push({ role: 'user', content: blocks });
  }

  // Anthropic 要求 user/assistant 交替：合并连续同角色
  const merged = [];
  for (const msg of messages) {
    const clean = { role: msg.role, content: msg.content };
    const last = merged[merged.length - 1];
    if (last && last.role === clean.role) last.content = [...last.content, ...clean.content];
    else merged.push(clean);
  }
  for (const m of merged) delete m._toolResult;

  const out = {
    model: upstreamModel,
    messages: merged,
    max_tokens: body.max_completion_tokens ?? body.max_tokens
  };
  const sys = systemParts.filter(Boolean).join('\n\n');
  if (sys) out.system = sys;

  if (out.max_tokens === undefined || out.max_tokens === null) {
    out.max_tokens = Math.min(caps.max_output || 8192, 8192);
  }

  if (body.temperature !== undefined && body.temperature !== null) out.temperature = body.temperature;
  if (body.top_p !== undefined && body.top_p !== null) out.top_p = body.top_p;
  if (body.stop) out.stop_sequences = Array.isArray(body.stop) ? body.stop : [body.stop];

  const fns = (body.tools || []).filter((t) => t && (t.type === 'function' ? true : !!t.function || !!t.name));
  const mappedTools = fns.map((t) => {
    const f = t.function || t;
    return { name: f.name, description: f.description || '', input_schema: f.parameters && typeof f.parameters === 'object' ? f.parameters : { type: 'object', properties: {} } };
  });
  const choice = body.tool_choice;
  if (choice === 'none') {
    // Anthropic 无法在带工具时强制不调用：直接不传 tools
  } else {
    if (mappedTools.length) out.tools = mappedTools;
    if (choice === 'auto') out.tool_choice = { type: 'auto' };
    else if (choice === 'required') out.tool_choice = { type: 'any' };
    else if (choice && typeof choice === 'object' && choice.function?.name) out.tool_choice = { type: 'tool', name: choice.function.name };
  }

  const effort = body.reasoning_effort;
  const thinkingAllowed = caps.reasoning !== false && caps.thinking_param !== 'none';
  if (effort && thinkingAllowed) {
    const budget = EFFORT_BUDGET[effort] ?? (Number.isFinite(+effort) ? +effort : 2048);
    const budgetTokens = Math.max(1024, Math.min(budget, 64000));
    out.thinking = { type: 'enabled', budget_tokens: budgetTokens };
    out.max_tokens = Math.max(out.max_tokens, budgetTokens + 2048);
    if (out.temperature !== undefined && out.temperature !== 1) delete out.temperature; // thinking 模式下只允许默认温度
  }
  return out;
}

function mapStopReason(reason) {
  switch (reason) {
    case 'end_turn': case 'stop_sequence': return 'stop';
    case 'max_tokens': return 'length';
    case 'tool_use': return 'tool_calls';
    case 'refusal': return 'content_filter';
    default: return reason ? 'stop' : null;
  }
}

export function fromAnthropicResponse(msg, clientModel) {
  let text = '';
  const reasoningParts = [];
  const toolCalls = [];
  for (const block of msg.content || []) {
    if (block.type === 'text') text += block.text || '';
    else if (block.type === 'thinking') reasoningParts.push(block.thinking || '');
    else if (block.type === 'tool_use') {
      toolCalls.push({
        id: block.id, type: 'function',
        function: { name: block.name, arguments: JSON.stringify(block.input ?? {}) }
      });
    }
  }
  const message = { role: 'assistant', content: text || null };
  if (reasoningParts.length) message.reasoning_content = reasoningParts.join('');
  if (toolCalls.length) message.tool_calls = toolCalls;
  return {
    id: msg.id || genId(),
    object: 'chat.completion',
    created: Math.floor(Date.now() / 1000),
    model: clientModel,
    choices: [{ index: 0, message, finish_reason: mapStopReason(msg.stop_reason) }],
    usage: {
      prompt_tokens: msg.usage?.input_tokens ?? 0,
      completion_tokens: msg.usage?.output_tokens ?? 0,
      total_tokens: (msg.usage?.input_tokens ?? 0) + (msg.usage?.output_tokens ?? 0),
      cached_tokens: msg.usage?.cache_read_input_tokens ?? null
    }
  };
}

/** Anthropic SSE -> OpenAI chunk 迭代器；usageRef 用于外部收集用量 */
export async function* anthropicStreamToOpenAI(body, { clientModel, includeUsage = false, usageRef = {}, signal }) {
  const id = genId();
  const blockToolIndex = {};
  let nextTool = 0;
  let promptTokens = 0, completionTokens = 0, cachedRead = null;
  let finish = null;
  let streamError = null;
  let emitted = false;

  const emit = (delta, fin = null) => {
    emitted = true;
    return openAIChunk({ id, model: clientModel, delta, finish: fin });
  };

  for await (const ev of pumpSSE(body, { signal })) {
    if (ev.done) break;
    let j;
    try { j = JSON.parse(ev.data); } catch { continue; }
    switch (j.type) {
      case 'message_start':
        promptTokens = j.message?.usage?.input_tokens ?? 0;
        cachedRead = j.message?.usage?.cache_read_input_tokens ?? null;
        yield emit({ role: 'assistant', content: '' });
        break;
      case 'content_block_start': {
        const b = j.content_block || {};
        if (b.type === 'tool_use') {
          const idx = nextTool++;
          blockToolIndex[j.index] = idx;
          yield emit({ tool_calls: [{ index: idx, id: b.id, type: 'function', function: { name: b.name, arguments: '' } }] });
        }
        break;
      }
      case 'content_block_delta': {
        const d = j.delta || {};
        if (d.type === 'text_delta' && d.text) yield emit({ content: d.text });
        else if (d.type === 'thinking_delta' && d.thinking) yield emit({ reasoning_content: d.thinking });
        else if (d.type === 'input_json_delta') {
          const idx = blockToolIndex[j.index] ?? 0;
          if (d.partial_json) yield emit({ tool_calls: [{ index: idx, function: { arguments: d.partial_json } }] });
        }
        break;
      }
      case 'message_delta':
        if (j.delta?.stop_reason) finish = mapStopReason(j.delta.stop_reason);
        if (j.usage?.output_tokens) completionTokens = j.usage.output_tokens;
        break;
      case 'message_stop':
        // 结束
        break;
      case 'error':
        streamError = j.error?.message || 'upstream stream error';
        break;
      default:
        break;
    }
    if (streamError) break;
  }

  if (!emitted && !streamError) yield emit({ role: 'assistant', content: '' });
  yield emit({}, finish);
  usageRef.prompt_tokens = promptTokens;
  usageRef.completion_tokens = completionTokens;
  usageRef.total_tokens = promptTokens + completionTokens;
  usageRef.cached_tokens = cachedRead;
  if (streamError) usageRef.error = streamError;
  if (includeUsage) {
    yield {
      id, object: 'chat.completion.chunk', created: Math.floor(Date.now() / 1000), model: clientModel,
      choices: [],
      usage: { prompt_tokens: promptTokens, completion_tokens: completionTokens, total_tokens: promptTokens + completionTokens, cached_tokens: cachedRead }
    };
  }
  if (streamError) throw new UpstreamError(502, streamError);
}

export async function forward({ channel, route, body, clientModel, caps, signal }) {
  const stream = !!body.stream;
  const upstreamBody = toAnthropicRequest(body, { upstreamModel: route.upstream, caps });
  if (stream) upstreamBody.stream = true;
  const url = endpointFor(channel, 'chat');
  const res = await upstreamFetch(channel, url, { method: 'POST', body: upstreamBody, ...chatTimeoutOpts(stream), signal });
  if (!res.ok) throw await UpstreamError.fromResponse(res);
  if (!stream) {
    const json = await res.json();
    return { kind: 'json', status: 200, json: fromAnthropicResponse(json, clientModel) };
  }
  const usageRef = {};
  const iterator = anthropicStreamToOpenAI(res.body, { clientModel, includeUsage: !!body.stream_options?.include_usage, usageRef, signal });
  return { kind: 'sse', status: 200, iterator, usageRef };
}
