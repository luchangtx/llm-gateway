// 上游 HTTP 工具：URL 拼接、协议鉴权头、带超时的 fetch
import { config } from './config.js';

/** 智能拼接：base 无路径时补 /v1（OpenAI 习惯），有路径则直接接；已带完整路径则原样使用 */
export function joinUrl(base, endpoint) {
  if (!base) return base;
  if (base.endsWith('/' + endpoint)) return base;
  let u;
  try { u = new URL(base); } catch { return `${base.replace(/\/+$/, '')}/${endpoint}`; }
  const p = u.pathname.replace(/\/+$/, '');
  if (!p || p === '') {
    const root = `${u.origin}/v1`;
    return endpoint ? `${root}/${endpoint}` : root;
  }
  return `${u.origin}${p}/${endpoint}`;
}

export function endpointFor(channel, kind, model) {
  const b = channel.base_url.replace(/\/+$/, '');
  if (channel.protocol === 'anthropic') {
    if (kind === 'chat') return joinUrl(b, 'messages');
    if (kind === 'models') return joinUrl(b, 'models');
  }
  if (channel.protocol === 'gemini') {
    const root = b;
    const hasPath = (() => { try { return !!new URL(b).pathname.replace(/\/+$/, ''); } catch { return true; } })();
    const apiRoot = hasPath ? root : `${root}/v1beta`;
    if (kind === 'chat') return `${apiRoot}/models/${encodeURIComponent(model)}:generateContent`;
    if (kind === 'chat_stream') return `${apiRoot}/models/${encodeURIComponent(model)}:streamGenerateContent?alt=sse`;
    if (kind === 'models') return `${apiRoot}/models?pageSize=200`;
  }
  // openai 兼容
  if (kind === 'chat') return joinUrl(b, 'chat/completions');
  if (kind === 'responses') return joinUrl(b, 'responses');
  if (kind === 'completions') return joinUrl(b, 'completions');
  if (kind === 'embeddings') return joinUrl(b, 'embeddings');
  if (kind === 'models') return joinUrl(b, 'models');
  return joinUrl(b, kind);
}

export function authHeaders(channel) {
  const h = { 'content-type': 'application/json' };
  if (channel.protocol === 'anthropic') {
    h['x-api-key'] = channel.api_key;
    h['anthropic-version'] = '2023-06-01';
  } else if (channel.protocol === 'gemini') {
    h['x-goog-api-key'] = channel.api_key;
  } else {
    h['authorization'] = `Bearer ${channel.api_key}`;
  }
  for (const [k, v] of Object.entries(channel.extra_headers || {})) h[k.toLowerCase()] = v;
  return h;
}

/** 聊天请求的超时参数：流式只限首包时长，非流式限总时长 */
export function chatTimeoutOpts(stream) {
  return stream
    ? { headerTimeoutMs: config.routing.first_byte_timeout_ms || 60000 }
    : { timeoutMs: config.routing.timeout_ms || 120000 };
}

export async function upstreamFetch(channel, url, { method = 'GET', body, timeoutMs = 0, headerTimeoutMs = 0, signal } = {}) {
  // timeoutMs > 0：整个请求的总时长上限（非流式）
  if (timeoutMs > 0) {
    const signals = [];
    if (signal) signals.push(signal);
    signals.push(AbortSignal.timeout(timeoutMs));
    return await fetch(url, {
      method,
      headers: authHeaders(channel),
      body: body === undefined ? undefined : JSON.stringify(body),
      signal: AbortSignal.any(signals)
    });
  }
  // headerTimeoutMs > 0：只限制"等到响应头"的时长（流式），超时后中断，避免上游挂死导致无限等待
  if (headerTimeoutMs > 0) {
    const ctrl = new AbortController();
    const onAbort = () => ctrl.abort(signal?.reason);
    signal?.addEventListener('abort', onAbort, { once: true });
    let timedOut = false;
    const timer = setTimeout(() => {
      timedOut = true;
      ctrl.abort(new Error(`等待上游首包超过 ${Math.round(headerTimeoutMs / 1000)}s`));
    }, headerTimeoutMs);
    try {
      return await fetch(url, {
        method,
        headers: authHeaders(channel),
        body: body === undefined ? undefined : JSON.stringify(body),
        signal: ctrl.signal
      });
    } catch (e) {
      if (timedOut) throw new UpstreamError(504, `上游 ${Math.round(headerTimeoutMs / 1000)}s 未返回首包，已中断（可在设置里调整首包超时）`);
      throw e;
    } finally {
      clearTimeout(timer);
    }
  }
  return await fetch(url, {
    method,
    headers: authHeaders(channel),
    body: body === undefined ? undefined : JSON.stringify(body),
    signal
  });
}

export class UpstreamError extends Error {
  constructor(status, message, body) {
    super(message);
    this.status = status;
    this.upstreamBody = body;
  }

  static async fromResponse(res) {
    let text = '';
    try { text = await res.text(); } catch { /* ignore */ }
    let msg = '';
    try {
      const j = JSON.parse(text);
      msg = j?.error?.message || j?.message || j?.[0]?.error?.message || j?.error?.code || '';
      if (!msg && Array.isArray(j?.errors)) msg = j.errors.map((e) => e.message).join('; ');
    } catch { /* not json */ }
    if (!msg) msg = text.slice(0, 300) || `HTTP ${res.status}`;
    return new UpstreamError(res.status, `[${res.status}] ${msg}`, text);
  }
}
