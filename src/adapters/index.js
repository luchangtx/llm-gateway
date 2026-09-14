// OpenAI 兼容渠道：原样透传（保留 reasoning_effort / enable_thinking 等厂商私有字段）
import { endpointFor, upstreamFetch, UpstreamError, chatTimeoutOpts } from '../upstream.js';

export async function forward({ channel, route, body, clientModel, signal, objectMode = false }) {
  const stream = !!body.stream;
  const upstreamBody = { ...body, model: route.upstream };
  // 客户端没要求用量时注入 include_usage，用于网关计费统计；不认这个字段的渠道会自动去掉重试
  let injectedUsage = false;
  if (stream && !upstreamBody.stream_options?.include_usage) {
    upstreamBody.stream_options = { ...(upstreamBody.stream_options || {}), include_usage: true };
    injectedUsage = true;
  }
  const url = endpointFor(channel, 'chat');
  let res = await upstreamFetch(channel, url, { method: 'POST', body: upstreamBody, ...chatTimeoutOpts(stream), signal });
  if (!res.ok && injectedUsage) {
    const err = await UpstreamError.fromResponse(res);
    if (!/stream_options|include_usage|unknown|unrecognized|unexpected/i.test(err.message)) throw err;
    delete upstreamBody.stream_options;
    res = await upstreamFetch(channel, url, { method: 'POST', body: upstreamBody, ...chatTimeoutOpts(stream), signal });
  }
  if (!res.ok) throw await UpstreamError.fromResponse(res);
  if (!stream) {
    const json = await res.json();
    if (json && typeof json === 'object') json.model = clientModel; // 回显客户端请求的模型名
    return { kind: 'json', status: res.status, json };
  }
  if (objectMode) {
    // 转换路径需要逐 chunk 处理：把原始 SSE 解析成 OpenAI chunk 对象流
    const { pumpSSE } = await import('../sse.js');
    const usageRef = {};
    async function* chunks() {
      for await (const ev of pumpSSE(res.body, { signal })) {
        if (ev.done) break;
        let j;
        try { j = JSON.parse(ev.data); } catch { continue; }
        if (j.error) { usageRef.error = j.error.message; break; }
        const u = j.usage;
        if (u) {
          usageRef.prompt_tokens = u.prompt_tokens ?? usageRef.prompt_tokens;
          usageRef.completion_tokens = u.completion_tokens ?? usageRef.completion_tokens;
          usageRef.cached_tokens = u.prompt_tokens_details?.cached_tokens ?? usageRef.cached_tokens;
          usageRef.reasoning_tokens = u.completion_tokens_details?.reasoning_tokens ?? usageRef.reasoning_tokens;
        }
        yield j;
      }
      if (usageRef.error) throw new UpstreamError(502, usageRef.error);
    }
    return { kind: 'sse', status: 200, iterator: chunks(), usageRef };
  }
  return { kind: 'raw', status: res.status, body: res.body, headers: res.headers };
}

/** OpenAI 兼容端点的通用透传（/v1/completions、/v1/embeddings） */
export async function forwardRawEndpoint({ channel, body, endpoint, signal }) {
  const url = endpointFor(channel, endpoint);
  const stream = !!body?.stream;
  const res = await upstreamFetch(channel, url, { method: 'POST', body, ...chatTimeoutOpts(stream), signal });
  if (!res.ok) throw await UpstreamError.fromResponse(res);
  if (!stream) {
    const json = await res.json();
    return { kind: 'json', status: res.status, json };
  }
  return { kind: 'raw', status: res.status, body: res.body };
}

export async function forwardChat(opts) {
  const ch = opts.channel;
  if (ch.protocol === 'openai' && ch.api_style === 'responses') {
    const mod = await import('./responses.js');
    return mod.forward(opts);
  }
  if (ch.protocol === 'anthropic') {
    const mod = await import('./anthropic.js');
    return mod.forward(opts);
  }
  if (ch.protocol === 'gemini') {
    const mod = await import('./gemini.js');
    return mod.forward(opts);
  }
  return forward(opts);
}

/** /v1/responses 原生入口：仅转发到 Responses API 风格的渠道 */
export async function forwardResponsesNative(opts) {
  const mod = await import('./responses.js');
  return mod.forwardNative(opts);
}
