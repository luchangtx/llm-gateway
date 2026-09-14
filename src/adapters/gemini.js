// OpenAI(Chat Completions) 客户端  ->  Gemini(generateContent) 上游 的双向转换
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
  if (!m || !m[2]) return null;
  return { mimeType: m[1] || 'application/octet-stream', data: m[3] };
}

const imgCache = new Map();

async function fetchImageAsInline(url) {
  if (imgCache.has(url)) return imgCache.get(url);
  const res = await fetch(url, { signal: AbortSignal.timeout(20000) });
  if (!res.ok) throw new UpstreamError(400, `抓取图片失败(${res.status}): ${url.slice(0, 120)}`);
  const buf = Buffer.from(await res.arrayBuffer());
  if (buf.length > 20 * 1024 * 1024) throw new UpstreamError(400, '图片超过 20MB，Gemini 渠道不支持');
  const mime = (res.headers.get('content-type') || 'image/png').split(';')[0];
  const inline = { inline_data: { mime_type: mime, data: buf.toString('base64') } };
  imgCache.set(url, inline);
  return inline;
}

async function userParts(content) {
  if (typeof content === 'string') return content ? [{ text: content }] : [];
  if (!Array.isArray(content)) return [];
  const parts = [];
  for (const part of content) {
    if (!part) continue;
    if (typeof part === 'string') { parts.push({ text: part }); continue; }
    if (part.type === 'text') parts.push({ text: part.text || '' });
    else if (part.type === 'image_url' || part.type === 'input_image') {
      const url = part.image_url?.url ?? part.image_url ?? part.url;
      if (typeof url !== 'string') continue;
      if (url.startsWith('data:')) {
        const parsed = parseDataUrl(url);
        if (!parsed) throw new UpstreamError(400, '图片 data URL 必须是 base64 编码');
        parts.push({ inline_data: { mime_type: parsed.mimeType, data: parsed.data } });
      } else if (/^https?:\/\//i.test(url)) {
        parts.push(await fetchImageAsInline(url));
      } else {
        throw new UpstreamError(400, `不支持的图片地址: ${url.slice(0, 80)}`);
      }
    }
  }
  return parts.filter((p) => p.text !== undefined || p.inline_data);
}

function cleanSchema(node, depth = 0) {
  if (node === null || typeof node !== 'object' || depth > 12) return { type: 'string' };
  if (Array.isArray(node)) return node.map((n) => cleanSchema(n, depth + 1));
  const out = {};
  let t = node.type;
  if (Array.isArray(t)) {
    if (t.includes('null')) out.nullable = true;
    t = t.find((x) => x !== 'null') || 'string';
  }
  if (t) out.type = String(t).toLowerCase();
  if (node.description) out.description = String(node.description).slice(0, 1000);
  if (Array.isArray(node.enum) && node.enum.length) out.enum = node.enum;
  if (out.type === 'string' && node.format) out.format = node.format;
  if (node.nullable) out.nullable = true;
  if (out.type === 'array' && node.items) out.items = cleanSchema(node.items, depth + 1);
  if (node.properties && typeof node.properties === 'object') {
    out.properties = {};
    for (const [k, v] of Object.entries(node.properties)) out.properties[k] = cleanSchema(v, depth + 1);
  }
  if (Array.isArray(node.required)) out.required = node.required.filter((x) => typeof x === 'string');
  return out;
}

const EFFORT_BUDGET = { minimal: 0, low: 1024, medium: 8192, high: 24576, xhigh: 32768 };

export async function toGeminiRequest(body, { caps = {} } = {}) {
  const systemParts = [];
  const contents = [];
  let pendingFunctionResponses = [];
  let toolIdToName = {};

  const flushResponses = () => {
    if (pendingFunctionResponses.length) {
      contents.push({ role: 'user', parts: pendingFunctionResponses });
      pendingFunctionResponses = [];
    }
  };

  for (const m of body.messages || []) {
    const role = m.role === 'developer' ? 'system' : m.role;
    if (role === 'system') { systemParts.push(textOf(m.content)); continue; }
    if (role === 'tool') {
      const name = toolIdToName[m.tool_call_id] || 'function';
      const respText = textOf(m.content) || '{}';
      let resp;
      try { resp = JSON.parse(respText); } catch { resp = { result: respText }; }
      if (resp === null || typeof resp !== 'object') resp = { result: resp };
      pendingFunctionResponses.push({ functionResponse: { name, response: resp } });
      continue;
    }
    flushResponses();
    if (role === 'assistant') {
      for (const tc of m.tool_calls || []) toolIdToName[tc.id] = tc.function?.name;
      const parts = [];
      const text = textOf(m.content);
      if (text) parts.push({ text });
      for (const tc of m.tool_calls || []) {
        if (tc.type !== 'function' && !tc.function) continue;
        parts.push({ functionCall: { name: tc.function.name, args: parseArgs(tc.function.arguments) } });
      }
      if (parts.length) contents.push({ role: 'model', parts });
      continue;
    }
    const parts = await userParts(m.content);
    if (parts.length) contents.push({ role: 'user', parts });
  }
  flushResponses();

  const out = { contents };
  const sys = systemParts.filter(Boolean).join('\n\n');
  if (sys) out.systemInstruction = { parts: [{ text: sys }] };

  const gen = {};
  if (body.temperature !== undefined && body.temperature !== null) gen.temperature = body.temperature;
  if (body.top_p !== undefined && body.top_p !== null) gen.topP = body.top_p;
  if (body.stop) gen.stopSequences = Array.isArray(body.stop) ? body.stop : [body.stop];
  const maxTok = body.max_completion_tokens ?? body.max_tokens;
  if (maxTok !== undefined && maxTok !== null) gen.maxOutputTokens = maxTok;

  const effort = body.reasoning_effort;
  const thinkingAllowed = caps.reasoning !== false && caps.thinking_param !== 'none';
  if (effort && thinkingAllowed) {
    const budget = EFFORT_BUDGET[effort] ?? (Number.isFinite(+effort) ? +effort : 8192);
    gen.thinkingConfig = { thinkingBudget: budget };
  }
  if (Object.keys(gen).length) out.generationConfig = gen;

  const fns = (body.tools || []).filter((t) => t && (t.type === 'function' ? true : !!t.function));
  if (fns.length) {
    out.tools = [{
      functionDeclarations: fns.map((t) => {
        const f = t.function || t;
        return { name: f.name, description: f.description || '', parameters: cleanSchema(f.parameters || { type: 'object' }) };
      })
    }];
  }
  const choice = body.tool_choice;
  if (choice === 'none') out.toolConfig = { functionCallingConfig: { mode: 'NONE' } };
  else if (choice === 'auto') out.toolConfig = { functionCallingConfig: { mode: 'AUTO' } };
  else if (choice === 'required') out.toolConfig = { functionCallingConfig: { mode: 'ANY' } };
  else if (choice && typeof choice === 'object' && choice.function?.name) {
    out.toolConfig = { functionCallingConfig: { mode: 'ANY', allowedFunctionNames: [choice.function.name] } };
  }
  return out;
}

function mapFinish(reason, hasToolCall) {
  if (hasToolCall) return 'tool_calls';
  switch (reason) {
    case 'STOP': return 'stop';
    case 'MAX_TOKENS': return 'length';
    case 'SAFETY': case 'RECITATION': case 'PROHIBITED_CONTENT': case 'BLOCKLIST': case 'SPII': return 'content_filter';
    case 'MALFORMED_FUNCTION_CALL': return 'stop';
    case undefined: case null: return null;
    default: return 'stop';
  }
}

export function fromGeminiResponse(json, clientModel) {
  const cand = json.candidates?.[0] || {};
  const parts = cand.content?.parts || [];
  let text = '';
  const reasoningParts = [];
  const toolCalls = [];
  for (const p of parts) {
    if (p.functionCall) {
      toolCalls.push({
        id: `call_${crypto.randomUUID().slice(0, 8)}`,
        type: 'function',
        function: { name: p.functionCall.name, arguments: JSON.stringify(p.functionCall.args ?? {}) }
      });
      continue;
    }
    if (typeof p.text !== 'string') continue;
    if (p.thought) reasoningParts.push(p.text);
    else text += p.text;
  }
  const message = { role: 'assistant', content: text || null };
  if (reasoningParts.length) message.reasoning_content = reasoningParts.join('');
  if (toolCalls.length) message.tool_calls = toolCalls;
  const usage = json.usageMetadata || {};
  const reasoningTokens = usage.thoughtsTokenCount ?? 0;
  return {
    id: json.responseId || genId(),
    object: 'chat.completion',
    created: Math.floor(Date.now() / 1000),
    model: clientModel,
    choices: [{ index: 0, message, finish_reason: mapFinish(cand.finishReason, toolCalls.length > 0) }],
    usage: {
      prompt_tokens: usage.promptTokenCount ?? 0,
      completion_tokens: (usage.candidatesTokenCount ?? 0) + reasoningTokens,
      total_tokens: usage.totalTokenCount ?? 0,
      cached_tokens: usage.cachedContentTokenCount ?? null,
      completion_tokens_details: reasoningTokens ? { reasoning_tokens: reasoningTokens } : undefined
    }
  };
}

/** Gemini SSE(每个 data 是完整 GenerateContentResponse) -> OpenAI chunk 迭代器 */
export async function* geminiStreamToOpenAI(body, { clientModel, includeUsage = false, usageRef = {}, signal }) {
  const id = genId();
  let nextTool = 0;
  let finish = null;
  let usageMeta = null;
  let hadContent = false;
  let streamError = null;
  let started = false;

  const emit = (delta, fin = null) => {
    const d = started ? delta : { role: 'assistant', content: '', ...delta };
    started = true;
    return openAIChunk({ id, model: clientModel, delta: d, finish: fin });
  };

  for await (const ev of pumpSSE(body, { signal })) {
    if (ev.done) break;
    let j;
    try { j = JSON.parse(ev.data); } catch { continue; }
    if (j.error) { streamError = j.error.message || 'upstream stream error'; break; }
    const cand = j.candidates?.[0];
    const parts = cand?.content?.parts || [];
    for (const p of parts) {
      if (p.functionCall) {
        hadContent = true;
        yield emit({ tool_calls: [{ index: nextTool++, id: `call_${crypto.randomUUID().slice(0, 8)}`, type: 'function', function: { name: p.functionCall.name, arguments: JSON.stringify(p.functionCall.args ?? {}) } }] });
        continue;
      }
      if (typeof p.text !== 'string' || p.text === '') continue;
      hadContent = true;
      if (p.thought) yield emit({ reasoning_content: p.text });
      else yield emit({ content: p.text });
    }
    if (cand?.finishReason) finish = mapFinish(cand.finishReason, false);
    if (j.usageMetadata) usageMeta = j.usageMetadata;
  }

  if (finish === null && hadContent) finish = 'stop';
  yield emit({}, finish);
  const prompt = usageMeta?.promptTokenCount ?? 0;
  const reasoning = usageMeta?.thoughtsTokenCount ?? 0;
  const completion = (usageMeta?.candidatesTokenCount ?? 0) + reasoning;
  usageRef.prompt_tokens = prompt;
  usageRef.completion_tokens = completion;
  usageRef.total_tokens = usageMeta?.totalTokenCount ?? prompt + completion;
  usageRef.cached_tokens = usageMeta?.cachedContentTokenCount ?? null;
  usageRef.reasoning_tokens = reasoning;
  if (streamError) usageRef.error = streamError;
  if (includeUsage) {
    yield {
      id, object: 'chat.completion.chunk', created: Math.floor(Date.now() / 1000), model: clientModel,
      choices: [],
      usage: { prompt_tokens: prompt, completion_tokens: completion, total_tokens: usageMeta?.totalTokenCount ?? prompt + completion, cached_tokens: usageMeta?.cachedContentTokenCount ?? null }
    };
  }
  if (streamError) throw new UpstreamError(502, streamError);
}

export async function forward({ channel, route, body, clientModel, caps, signal }) {
  const stream = !!body.stream;
  const upstreamBody = await toGeminiRequest(body, { caps });
  const url = endpointFor(channel, stream ? 'chat_stream' : 'chat', route.upstream);
  const res = await upstreamFetch(channel, url, { method: 'POST', body: upstreamBody, ...chatTimeoutOpts(stream), signal });
  if (!res.ok) throw await UpstreamError.fromResponse(res);
  if (!stream) {
    const json = await res.json();
    return { kind: 'json', status: 200, json: fromGeminiResponse(json, clientModel) };
  }
  const usageRef = {};
  const iterator = geminiStreamToOpenAI(res.body, { clientModel, includeUsage: !!body.stream_options?.include_usage, usageRef, signal });
  return { kind: 'sse', status: 200, iterator, usageRef };
}
