// 统一 LLM 网关 — 主服务
import http from 'node:http';
import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

import { config, startConfigWatch } from './config.js';
import { logRequest, flushLogs } from './log.js';
import { getUsage, parseUsageFromText } from './usage.js';
import { sseChunk, SSE_DONE } from './sse.js';
import * as registry from './registry.js';
import { forwardChat, forwardRawEndpoint, forwardResponsesNative } from './adapters/index.js';
import { responsesToChatBody, chatToResponsesResponse, chatChunksToResponsesEvents } from './adapters/responses.js';
import { UpstreamError } from './upstream.js';
import { probeChannel, probeAll, probeIds } from './probe.js';

const pkg = JSON.parse(fs.readFileSync(path.join(config.ROOT, 'package.json'), 'utf8'));
const VERSION = pkg.version || '1.0.0';

const ROOT = config.ROOT;
const PUBLIC_DIR = path.join(ROOT, 'public');

// ---------- 通用工具 ----------
function json(res, status, obj, extraHeaders = {}) {
  const body = JSON.stringify(obj);
  res.writeHead(status, {
    'content-type': 'application/json; charset=utf-8',
    'access-control-allow-origin': '*',
    ...extraHeaders
  });
  res.end(body);
}

function openAIError(res, status, message, type = 'gateway_error') {
  json(res, status, { error: { message, type, code: status } });
}

function readBody(req, limitMB = 64) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let size = 0;
    req.on('data', (c) => {
      size += c.length;
      if (size > limitMB * 1024 * 1024) { reject(new UpstreamError(413, '请求体过大')); req.destroy(); return; }
      chunks.push(c);
    });
    req.on('end', () => resolve(Buffer.concat(chunks)));
    req.on('error', reject);
  });
}

function authOk(req, url) {
  const h = req.headers['authorization'] || '';
  const bearer = h.startsWith('Bearer ') ? h.slice(7) : null;
  const key = bearer || req.headers['x-api-key'] || url.searchParams.get('key') || '';
  const a = Buffer.from(String(key));
  const b = Buffer.from(String(config.gateway_key));
  if (a.length === b.length && crypto.timingSafeEqual(a, b)) return { name: '主 Key', primary: true };
  for (const k of config.keys || []) {
    if (k.enabled === false) continue;
    const kb = Buffer.from(String(k.key));
    if (a.length === kb.length && crypto.timingSafeEqual(a, kb)) return { name: k.name, primary: false };
  }
  return null;
}

/** 鉴权成功后标记首次设置已完成（只写盘一次） */
function authed(req, url) {
  const match = authOk(req, url);
  if (match && !config.setup_done) {
    config.setup_done = true;
    config.save();
  }
  return match;
}

// ---------- 首次设置（免验证引导）：仅本机 + 专用头，防止跨站滥用 ----------
function localOnly(req, res) {
  const ra = req.socket.remoteAddress || '';
  const loopback = ra === '127.0.0.1' || ra === '::1' || ra === '::ffff:127.0.0.1';
  const host = String(req.headers.host || '');
  const localHost = /^(localhost|127\.0\.0\.1|\[::1\])(:\d+)?$/i.test(host);
  if (!loopback || !localHost) {
    openAIError(res, 403, '首次设置仅允许在本机通过 localhost 访问');
    return false;
  }
  // 自定义头会触发 CORS 预检且我们不放行，跨站页面无法伪造
  if (req.headers['x-gateway-bootstrap'] !== '1') {
    openAIError(res, 403, '缺少 x-gateway-bootstrap 头');
    return false;
  }
  return true;
}

async function handleBootstrap(req, res, p) {
  if (!localOnly(req, res)) return;
  if (req.method === 'GET' && p === '/admin/bootstrap-status') {
    return json(res, 200, { first_setup: !config.setup_done, version: VERSION });
  }
  if (req.method !== 'POST' || p !== '/admin/bootstrap') return openAIError(res, 405, 'Method Not Allowed');
  if (config.setup_done) {
    return openAIError(res, 403, '首次设置已完成。修改 key 请登录后在设置页进行，或在服务器上运行 npm run show-key 查看 / npm run reset-key 重置');
  }
  let body = {};
  try { body = JSON.parse((await readBody(req)).toString('utf8') || '{}'); } catch { return openAIError(res, 400, '请求体解析失败'); }
  if (body.gateway_key !== undefined && body.gateway_key !== null && body.gateway_key !== '') {
    const k = String(body.gateway_key).trim();
    if (k.length < 8) return openAIError(res, 400, '网关 key 至少 8 个字符');
    config.gateway_key = k;
  }
  config.setup_done = true;
  config.save();
  console.log(`[bootstrap] 首次设置完成，网关 key: ${config.gateway_key}`);
  return json(res, 200, { ok: true, gateway_key: config.gateway_key });
}

function maskKey(k) {
  if (!k) return '';
  if (k.length <= 8) return '****';
  return k.slice(0, 5) + '****' + k.slice(-4);
}

const KEEP = '__KEEP__';

function maskChannel(c) {
  return { ...c, api_key: KEEP, api_key_masked: maskKey(c.api_key) };
}

function unmaskChannels(incoming) {
  const old = new Map(config.channels.map((c) => [c.name, c]));
  return incoming.map((c, i) => {
    if (c.api_key === KEEP || !c.api_key) {
      const prev = old.get(c.name);
      c.api_key = prev?.api_key || '';
    }
    return c;
  });
}

// ---------- 聊天转发（含故障转移） ----------

/** 字节级透传：首个 chunk 到手后才写响应头，因此首包失败仍可触发上层故障转移 */
async function pipeRaw(res, stream, route, { reqId, clientGone, idleMs }) {
  const reader = stream.getReader();
  let first;
  try {
    first = await reader.read();
  } catch (e) {
    throw new UpstreamError(502, `上游流读取失败: ${e.message}`);
  }
  if (first.done) throw new UpstreamError(502, '上游流提前结束');
  res.writeHead(200, {
    'content-type': 'text/event-stream; charset=utf-8',
    'cache-control': 'no-cache',
    'connection': 'keep-alive',
    'x-accel-buffering': 'no',
    'access-control-allow-origin': '*',
    'x-gateway-channel': route.channel,
    'x-gateway-attempt': String(route.attemptNo || 1),
    'x-request-id': reqId
  });
  res.write(Buffer.from(first.value));
  let bytes = first.value.byteLength;
  let streamError = null;
  const dec = new TextDecoder();
  let rawText = dec.decode(first.value, { stream: true });
  try {
    while (true) {
      let timer;
      const gate = new Promise((_, rej) => { timer = setTimeout(() => rej(new Error(`上游空闲超过 ${idleMs}ms`)), idleMs); });
      let r;
      try { r = await Promise.race([reader.read(), gate]); } finally { clearTimeout(timer); }
      if (r.done) break;
      if (clientGone.value) break;
      res.write(Buffer.from(r.value));
      bytes += r.value.byteLength;
      rawText += dec.decode(r.value, { stream: true });
    }
  } catch (e) {
    if (!clientGone.value) {
      streamError = `[渠道 ${route.channel}] 流中断: ${e.message}`;
      res.write(sseChunk({ error: { message: streamError, type: 'upstream_error', code: 502 } }));
      res.write(SSE_DONE);
    }
  }
  if (!clientGone.value) res.end();
  return { bytes, error: streamError, usage: parseUsageFromText(rawText) };
}

/** 从 usage 对象（各协议已归一化）提取日志/统计字段 */
function usageFields(u) {
  if (!u) return {};
  return {
    prompt_tokens: u.prompt_tokens ?? u.input_tokens ?? null,
    completion_tokens: u.completion_tokens ?? u.output_tokens ?? null,
    cached_tokens: u.cached_tokens ?? u.prompt_tokens_details?.cached_tokens ?? null,
    reasoning_tokens: u.reasoning_tokens ?? u.completion_tokens_details?.reasoning_tokens ?? null
  };
}

async function handleChat(req, res, url) {
  const reqId = crypto.randomUUID().slice(0, 8);
  const t0 = Date.now();
  let body;
  try {
    const raw = await readBody(req);
    body = JSON.parse(raw.toString('utf8') || '{}');
  } catch (e) {
    return openAIError(res, e?.status || 400, `请求体解析失败: ${e.message}`);
  }
  if (!body.model) return openAIError(res, 400, '缺少 model 字段。可用模型见 GET /v1/models，或指定 渠道名/模型名');
  if (!Array.isArray(body.messages) || !body.messages.length) return openAIError(res, 400, '缺少 messages 字段');

  const { routes, notFound, suggestions } = registry.resolveRoutes(body.model);
  if (!routes.length) {
    const hint = suggestions.length ? `相似模型: ${suggestions.join(', ')}` : '可用模型见 GET /v1/models';
    return openAIError(res, 404, `模型 "${notFound}" 不存在（或在所有渠道都不可用）。${hint}`);
  }

  const stream = !!body.stream;
  const attempts = config.routing.failover ? routes : [routes[0]];
  const clientModel = body.model;
  const clientGone = { value: false };
  req.on('close', () => { clientGone.value = true; });
  const abort = new AbortController();
  req.on('close', () => { if (!res.writableEnded) abort.abort(new Error('client disconnected')); });

  let lastErr = null;
  let attemptNo = 0;

  for (const route of attempts) {
    attemptNo++;
    if (clientGone.value) return;
    const channel = registry.getChannel(route.channel);
    if (!channel) continue;
    const entry = registry.entryWithCaps(route);
    const tAttempt = Date.now();
    try {
      const out = await forwardChat({
        channel, route, body: { ...body, model: clientModel }, clientModel,
        caps: entry.caps, signal: abort.signal
      });

      if (out.kind === 'json') {
        logRequest({
          req_id: reqId, model: clientModel, key_name: req._keyName, channel: route.channel, protocol: route.protocol,
          status: out.status, ms: Date.now() - tAttempt, stream: false, attempt: attemptNo,
          ...usageFields(out.json?.usage)
        });
        return json(res, out.status, out.json, {
          'x-gateway-channel': route.channel,
          'x-gateway-attempt': String(attemptNo),
          'x-request-id': reqId
        });
      }

      if (out.kind === 'sse') {
        // 先拿第一个 chunk 再写头 —— 便于首包失败时故障转移
        const first = await out.iterator.next();
        if (first.done) throw new UpstreamError(502, '上游流提前结束');
        res.writeHead(200, {
          'content-type': 'text/event-stream; charset=utf-8',
          'cache-control': 'no-cache',
          'connection': 'keep-alive',
          'x-accel-buffering': 'no',
          'access-control-allow-origin': '*',
          'x-gateway-channel': route.channel,
          'x-gateway-attempt': String(attemptNo),
          'x-request-id': reqId
        });
        res.write(sseChunk(first.value));
        try {
          for await (const chunk of out.iterator) {
            if (clientGone.value) break;
            res.write(sseChunk(chunk));
          }
        } catch (e) {
          if (!clientGone.value) {
            const msg = `[渠道 ${route.channel}] 流中断: ${e.message}`;
            res.write(sseChunk({ error: { message: msg, type: 'upstream_error', code: 502 } }));
          }
        }
        if (!clientGone.value) { res.write(SSE_DONE); res.end(); }
        logRequest({
          req_id: reqId, model: clientModel, key_name: req._keyName, channel: route.channel, protocol: route.protocol,
          status: 200, ms: Date.now() - tAttempt, stream: true, attempt: attemptNo,
          ...usageFields(out.usageRef),
          error: out.usageRef?.error
        });
        return;
      }

      if (out.kind === 'raw') {
        // OpenAI 兼容渠道：字节级透传（首个 chunk 到手后再写头，首包失败可故障转移）
        const r = await pipeRaw(res, out.body, { ...route, attemptNo }, { reqId, clientGone, idleMs: config.routing.idle_timeout_ms });
        logRequest({
          req_id: reqId, model: clientModel, key_name: req._keyName, channel: route.channel, protocol: route.protocol,
          status: 200, ms: Date.now() - tAttempt, stream: true, attempt: attemptNo,
          bytes: r.bytes, error: r.error, ...usageFields(r.usage)
        });
        return;
      }
    } catch (e) {
      lastErr = e;
      const status = e?.status || 0;
      const retryable = status === 0 || status === 401 || status === 403 || status === 404 ||
        status === 408 || status === 429 || status >= 500 || /timeout|abort|fetch failed|ECONN/i.test(e?.message || '');
      const canRetry = attempts.length > 1 && attemptNo < attempts.length && !clientGone.value;
      logRequest({
        req_id: reqId, model: clientModel, key_name: req._keyName, channel: route.channel, protocol: route.protocol,
        status: status || 502, ms: Date.now() - tAttempt, stream, attempt: attemptNo,
        error: e.message, failover: retryable && canRetry
      });
      if (retryable && canRetry) {
        console.warn(`[chat] ${reqId} 渠道 ${route.channel} 失败(${e.message})，尝试下一个渠道...`);
        continue;
      }
      break;
    }
  }

  if (clientGone.value) return;
  const status = lastErr?.status && lastErr.status >= 400 ? lastErr.status : 502;
  openAIError(res, status, `所有渠道均失败: ${lastErr?.message || 'unknown'}`, 'upstream_error');
}

// ---------- /v1/responses 原生入口（Responses 方言客户端 -> Responses 渠道透传） ----------
async function handleResponses(req, res, url) {
  const reqId = crypto.randomUUID().slice(0, 8);
  let body;
  try {
    body = JSON.parse((await readBody(req)).toString('utf8') || '{}');
  } catch (e) {
    return openAIError(res, e?.status || 400, `请求体解析失败: ${e.message}`);
  }
  if (!body.model) return openAIError(res, 400, '缺少 model 字段。可用模型见 GET /v1/models');

  const { routes, notFound, suggestions } = registry.resolveRoutes(body.model);
  if (!routes.length) {
    const hint = suggestions.length ? `相似模型: ${suggestions.join(', ')}` : '可用模型见 GET /v1/models';
    return openAIError(res, 404, `模型 "${notFound}" 不存在（或在所有渠道都不可用）。${hint}`);
  }

  // Responses 方言渠道优先原生透传；chat/anthropic/gemini 渠道自动做 Responses<->Chat 转换
  const isNative = (r) => r.protocol === 'openai' && registry.getChannel(r.channel)?.api_style === 'responses';
  const ordered = [...routes].sort((a, b) => (isNative(b) ? 1 : 0) - (isNative(a) ? 1 : 0));

  const clientModel = body.model;
  const clientGone = { value: false };
  req.on('close', () => { clientGone.value = true; });
  const abort = new AbortController();
  req.on('close', () => { if (!res.writableEnded) abort.abort(new Error('client disconnected')); });

  const attempts = config.routing.failover ? ordered : [ordered[0]];
  let lastErr = null;
  let attemptNo = 0;
  for (const route of attempts) {
    attemptNo++;
    if (clientGone.value) return;
    const channel = registry.getChannel(route.channel);
    if (!channel) continue;
    const entry = registry.entryWithCaps(route);
    const tAttempt = Date.now();
    const logBase = { req_id: reqId, model: clientModel, key_name: req._keyName, channel: route.channel, protocol: isNative(route) ? 'responses' : route.protocol, endpoint: '/v1/responses', attempt: attemptNo };
    try {
      if (isNative(route)) {
        const out = await forwardResponsesNative({ channel, route, body: { ...body, model: clientModel }, clientModel, signal: abort.signal });
        if (out.kind === 'json') {
          logRequest({ ...logBase, status: out.status, ms: Date.now() - tAttempt, stream: false, ...usageFields(out.json?.usage) });
          return json(res, out.status, out.json, {
            'x-gateway-channel': route.channel,
            'x-gateway-attempt': String(attemptNo),
            'x-request-id': reqId
          });
        }
        const r = await pipeRaw(res, out.body, { ...route, attemptNo }, { reqId, clientGone, idleMs: config.routing.idle_timeout_ms });
        logRequest({ ...logBase, status: 200, ms: Date.now() - tAttempt, stream: true, bytes: r.bytes, error: r.error, ...usageFields(r.usage) });
        return;
      }

      // 转换路径：Responses 请求 -> chat 渠道 -> Responses 响应/事件
      const chatBody = responsesToChatBody({ ...body, model: clientModel });
      if (chatBody.stream) chatBody.stream_options = { ...(chatBody.stream_options || {}), include_usage: true };
      const out = await forwardChat({ channel, route, body: chatBody, clientModel, caps: entry.caps, signal: abort.signal, objectMode: true });
      if (out.kind === 'json') {
        const resp = chatToResponsesResponse(out.json, clientModel);
        logRequest({ ...logBase, status: 200, ms: Date.now() - tAttempt, stream: false, ...usageFields(out.json?.usage) });
        return json(res, 200, resp, {
          'x-gateway-channel': route.channel,
          'x-gateway-attempt': String(attemptNo),
          'x-request-id': reqId
        });
      }
      // 流式：先拿到首个非 created 事件再写头，便于首包失败时故障转移
      const events = chatChunksToResponsesEvents(out.iterator, { model: clientModel });
      const buffered = [];
      let first = null;
      while (true) {
        const r = await events.next();
        if (r.done) break;
        if (r.value.event === 'response.created') { buffered.push(r.value); continue; }
        first = r.value;
        break;
      }
      if (!first) throw new UpstreamError(502, '上游流提前结束');
      res.writeHead(200, {
        'content-type': 'text/event-stream; charset=utf-8',
        'cache-control': 'no-cache',
        'connection': 'keep-alive',
        'x-accel-buffering': 'no',
        'access-control-allow-origin': '*',
        'x-gateway-channel': route.channel,
        'x-gateway-attempt': String(attemptNo),
        'x-request-id': reqId
      });
      const write = (ev) => res.write(`event: ${ev.event}\ndata: ${JSON.stringify(ev.data)}\n\n`);
      for (const ev of buffered) write(ev);
      write(first);
      try {
        while (true) {
          const r = await events.next();
          if (r.done) break;
          if (clientGone.value) break;
          write(r.value);
        }
      } catch (e) {
        if (!clientGone.value) {
          res.write(`event: error\ndata: ${JSON.stringify({ type: 'error', code: 'upstream_error', message: `[渠道 ${route.channel}] 流中断: ${e.message}`, param: null })}\n\n`);
        }
      }
      if (!clientGone.value) res.end();
      logRequest({ ...logBase, status: 200, ms: Date.now() - tAttempt, stream: true, ...usageFields(out.usageRef), error: out.usageRef?.error });
      return;
    } catch (e) {
      lastErr = e;
      const status = e?.status || 0;
      const retryable = status === 0 || status === 401 || status === 403 || status === 404 ||
        status === 408 || status === 429 || status >= 500 || /timeout|abort|fetch failed|ECONN/i.test(e?.message || '');
      const canRetry = attempts.length > 1 && attemptNo < attempts.length && !clientGone.value;
      logRequest({
        req_id: reqId, model: clientModel, key_name: req._keyName, channel: route.channel, protocol: route.protocol,
        status: status || 502, ms: Date.now() - tAttempt, stream: !!body.stream, attempt: attemptNo,
        error: e.message, failover: retryable && canRetry, endpoint: '/v1/responses'
      });
      if (retryable && canRetry) {
        console.warn(`[responses] ${reqId} 渠道 ${route.channel} 失败(${e.message})，尝试下一个渠道...`);
        continue;
      }
      break;
    }
  }
  if (clientGone.value) return;
  const status = lastErr?.status && lastErr.status >= 400 ? lastErr.status : 502;
  openAIError(res, status, `所有渠道均失败: ${lastErr?.message || 'unknown'}`, 'upstream_error');
}

// ---------- Admin ----------
async function handleAdmin(req, res, url) {
  const p = url.pathname;
  const method = req.method;

  if (method === 'GET' && p === '/admin/overview') {
    return json(res, 200, { stats: registry.stats(), config: { port: config.port, failover: config.routing.failover, gateway_key: config.gateway_key } });
  }

  if (method === 'GET' && p === '/admin/channels') {
    return json(res, 200, { channels: config.channels.map(maskChannel) });
  }

  if (method === 'PUT' && p === '/admin/channels') {
    const body = JSON.parse((await readBody(req)).toString('utf8') || '{}');
    if (!Array.isArray(body.channels)) return openAIError(res, 400, 'channels 必须是数组');
    config.saveChannels(unmaskChannels(body.channels));
    // 模型列表后台并行刷新，保存立即返回（否则要串行等所有渠道的 /models）
    registry.refreshAll({ force: true }).catch(() => {});
    return json(res, 200, { ok: true, refreshing: true, channels: config.channels.map(maskChannel) });
  }

  if (method === 'POST' && p === '/admin/channels/probe-models') {
    // 渠道弹窗内"拉取模型列表"：用表单里未保存的配置直接探测，不落盘
    const body = JSON.parse((await readBody(req)).toString('utf8') || '{}');
    if (!body.base_url) return openAIError(res, 400, '请先填写 Base URL');
    let apiKey = body.api_key;
    if (!apiKey || apiKey === '__KEEP__') {
      const prev = config.channels.find((c) => c.name === body.name);
      if (!prev?.api_key) return openAIError(res, 400, '请先填写该渠道的 API Key 再拉取');
      apiKey = prev.api_key;
    }
    const temp = {
      name: String(body.name || 'probe'),
      base_url: String(body.base_url).replace(/\/+$/, ''),
      api_key: String(apiKey),
      protocol: config.PROTOCOLS.includes(body.protocol) ? body.protocol : 'openai',
      enabled: true,
      models: [],
      model_aliases: {},
      extra_headers: {}
    };
    const t0 = Date.now();
    try {
      const models = await registry.fetchChannelModels(temp);
      return json(res, 200, { ok: true, models: models.map((m) => m.id), latency_ms: Date.now() - t0 });
    } catch (e) {
      return json(res, 200, { ok: false, error: e.message });
    }
  }

  if (method === 'POST' && p.startsWith('/admin/channels/') && p.endsWith('/test')) {
    const name = decodeURIComponent(p.slice('/admin/channels/'.length, -'/test'.length));
    const result = await registry.testChannel(name);
    return json(res, 200, result);
  }

  if (method === 'POST' && p === '/admin/refresh') {
    const n = await registry.refreshAll({ force: true });
    return json(res, 200, { ok: true, models: n, stats: registry.stats() });
  }

  if (method === 'GET' && p === '/admin/models') {
    return json(res, 200, registry.adminList());
  }

  if (method === 'POST' && p === '/admin/probe') {
    const body = JSON.parse((await readBody(req)).toString('utf8') || '{}');
    if (Array.isArray(body.ids)) {
      // 按模型 id 列表探测（后台「探测能力（按筛选）」用）
      const results = await probeIds(body.ids, {
        kinds: body.kinds || ['vision', 'thinking'],
        concurrency: body.concurrency || config.capabilities.probe_concurrency
      });
      return json(res, 200, { ok: true, results });
    }
    if (body.channel) {
      const results = await probeChannel({
        channelName: body.channel, model: body.model || null,
        kinds: body.kinds || ['vision', 'thinking'], skipCachedDays: body.skip_cached_days ?? 7
      });
      return json(res, 200, { ok: true, results });
    }
    const results = await probeAll({ kinds: body.kinds || ['vision', 'thinking'], concurrency: body.concurrency || config.capabilities.probe_concurrency });
    return json(res, 200, { ok: true, results });
  }

  if (method === 'PUT' && p === '/admin/override') {
    const body = JSON.parse((await readBody(req)).toString('utf8') || '{}');
    if (!body.key) return openAIError(res, 400, '缺少 key（渠道/模型 或 模型名）');
    config.capabilities.overrides[body.key] = { ...(config.capabilities.overrides[body.key] || {}), ...(body.caps || {}) };
    config.save();
    return json(res, 200, { ok: true, overrides: config.capabilities.overrides });
  }

  if (method === 'DELETE' && p.startsWith('/admin/override/')) {
    const key = decodeURIComponent(p.slice('/admin/override/'.length));
    delete config.capabilities.overrides[key];
    config.save();
    return json(res, 200, { ok: true });
  }

  if (method === 'GET' && p === '/admin/usage') {
    const q = url.searchParams;
    flushLogs(); // 打开看板时强制落盘一次
    return json(res, 200, getUsage({
      from: q.get('from') || null,
      to: q.get('to') || null,
      channel: q.get('channel') || null,
      model: q.get('model') || null
    }));
  }

  if (method === 'GET' && p === '/admin/logs') {
    const { recentLogs } = await import('./log.js');
    return json(res, 200, { logs: recentLogs(+url.searchParams.get('limit') || 200) });
  }

  if (method === 'GET' && p === '/admin/config') {
    return json(res, 200, {
      port: config.port,
      gateway_key: config.gateway_key,
      routing: config.routing,
      capabilities: config.capabilities,
      config_path: config.CONFIG_PATH
    });
  }

  // ---------- 附加 Keys 管理（多人接入） ----------
  if (method === 'GET' && p === '/admin/keys') {
    return json(res, 200, { keys: config.keys || [] });
  }

  if (method === 'POST' && p === '/admin/keys') {
    const body = JSON.parse((await readBody(req)).toString('utf8') || '{}');
    const key = body.key ? String(body.key).trim() : 'sk-gw-' + crypto.randomBytes(24).toString('base64url');
    if (key.length < 8) return openAIError(res, 400, 'key 至少 8 个字符');
    if (key === config.gateway_key) return openAIError(res, 400, '不能与主 Key 相同');
    if ((config.keys || []).some((k) => k.key === key)) return openAIError(res, 400, 'key 已存在');
    const entry = { key, name: String(body.name || '').slice(0, 40) || `key-${(config.keys || []).length + 1}`, enabled: true, created_at: Date.now() };
    config.keys = [...(config.keys || []), entry];
    config.save();
    return json(res, 200, { ok: true, entry });
  }

  if (method === 'PUT' && p === '/admin/keys') {
    const body = JSON.parse((await readBody(req)).toString('utf8') || '{}');
    const k = (config.keys || []).find((x) => x.key === body.key);
    if (!k) return openAIError(res, 404, 'key 不存在');
    if (body.name !== undefined) k.name = String(body.name).slice(0, 40) || k.name;
    if (body.enabled !== undefined) k.enabled = !!body.enabled;
    config.save();
    return json(res, 200, { ok: true, entry: k });
  }

  if (method === 'DELETE' && p === '/admin/keys') {
    const body = JSON.parse((await readBody(req)).toString('utf8') || '{}');
    const before = config.keys || [];
    const after = before.filter((x) => x.key !== body.key);
    if (after.length === before.length) return openAIError(res, 404, 'key 不存在');
    config.keys = after;
    config.save();
    return json(res, 200, { ok: true });
  }

  if (method === 'POST' && p === '/admin/rotate-key') {
    config.gateway_key = 'sk-gw-' + crypto.randomBytes(24).toString('base64url');
    config.save();
    console.log(`[config] 网关 key 已轮换: ${config.gateway_key}`);
    return json(res, 200, { ok: true, gateway_key: config.gateway_key });
  }

  if (method === 'PUT' && p === '/admin/config') {
    const body = JSON.parse((await readBody(req)).toString('utf8') || '{}');
    if (body.gateway_key !== undefined) {
      const k = String(body.gateway_key).trim();
      if (k.length < 8) return openAIError(res, 400, '网关 key 至少 8 个字符');
      config.gateway_key = k;
    }
    if (body.port !== undefined) {
      const pt = +body.port;
      if (!Number.isInteger(pt) || pt < 1 || pt > 65535) return openAIError(res, 400, '端口必须是 1-65535 的整数');
      config.port = pt;
    }
    if (body.routing) {
      const r = body.routing;
      config.routing = {
        failover: r.failover !== false,
        timeout_ms: Number.isFinite(+r.timeout_ms) && +r.timeout_ms >= 1000 ? +r.timeout_ms : config.routing.timeout_ms,
        idle_timeout_ms: Number.isFinite(+r.idle_timeout_ms) && +r.idle_timeout_ms >= 1000 ? +r.idle_timeout_ms : config.routing.idle_timeout_ms,
        first_byte_timeout_ms: Number.isFinite(+r.first_byte_timeout_ms) && +r.first_byte_timeout_ms >= 1000 ? +r.first_byte_timeout_ms : config.routing.first_byte_timeout_ms
      };
    }
    if (body.capabilities) {
      const c = body.capabilities;
      config.capabilities = {
        ...config.capabilities,
        ...c,
        refresh_minutes: Number.isFinite(+c.refresh_minutes) && +c.refresh_minutes >= 0 ? +c.refresh_minutes : config.capabilities.refresh_minutes,
        overrides: { ...(c.overrides || config.capabilities.overrides) }
      };
    }
    config.save();
    return json(res, 200, {
      ok: true,
      config: { port: config.port, gateway_key: config.gateway_key, routing: config.routing, capabilities: config.capabilities },
      note: '端口修改需重启生效；网关 key 修改后客户端需同步更新'
    });
  }

  return openAIError(res, 404, '未知管理接口');
}

// ---------- 入口路由 ----------
const dashboardHTML = () => {
  try { return fs.readFileSync(path.join(PUBLIC_DIR, 'index.html')); } catch {
    return '<h1>llm-gateway</h1><p>public/index.html 缺失</p>';
  }
};

async function route(req, res) {
  const url = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
  const p = url.pathname;

  if (req.method === 'OPTIONS') {
    res.writeHead(204, {
      'access-control-allow-origin': '*',
      'access-control-allow-methods': 'GET, POST, PUT, DELETE, OPTIONS',
      'access-control-allow-headers': 'authorization, content-type, x-api-key',
      'access-control-max-age': '86400'
    });
    return res.end();
  }

  if (p === '/' || p === '/index.html') {
    res.writeHead(200, { 'content-type': 'text/html; charset=utf-8' });
    return res.end(dashboardHTML());
  }

  if (p === '/healthz') {
    return json(res, 200, { ok: true, version: VERSION, uptime_s: Math.round(process.uptime()), ...registry.stats() });
  }

  if (p === '/admin/bootstrap-status' || p === '/admin/bootstrap') {
    return handleBootstrap(req, res, p);
  }

  if (p.startsWith('/admin/')) {
    const km = authed(req, url);
    if (!km) return openAIError(res, 401, '网关 key 无效（Authorization: Bearer <gateway_key>）');
    req._keyName = km.name;
    return handleAdmin(req, res, url);
  }

  if (p.startsWith('/v1/')) {
    const km = authed(req, url);
    if (!km) return openAIError(res, 401, '网关 key 无效（Authorization: Bearer <gateway_key>）');
    req._keyName = km.name;
    const method = req.method;

    if (method === 'GET' && p === '/v1/models') {
      return json(res, 200, registry.listOpenAI());
    }

    if (method === 'GET' && p.startsWith('/v1/models/')) {
      const id = decodeURIComponent(p.slice('/v1/models/'.length));
      const e = registry.getEntry(id) || registry.resolveRoutes(id).routes[0];
      if (!e) return openAIError(res, 404, `模型 "${id}" 不存在`);
      const full = registry.entryWithCaps(e);
      return json(res, 200, {
        id: e.id, object: 'model', owned_by: e.channel, context_length: full.caps.context,
        max_output_tokens: full.caps.max_output, capabilities: full.caps, sources: full.sources
      });
    }

    if (method === 'POST' && p === '/v1/chat/completions') {
      return handleChat(req, res, url);
    }

    if (method === 'POST' && p === '/v1/responses') {
      return handleResponses(req, res, url);
    }

    if (method === 'POST' && (p === '/v1/completions' || p === '/v1/embeddings')) {
      const kind = p === '/v1/completions' ? 'completions' : 'embeddings';
      let body;
      try {
        body = JSON.parse((await readBody(req)).toString('utf8') || '{}');
      } catch (e) {
        return openAIError(res, 400, `请求体解析失败: ${e.message}`);
      }
      const { routes } = registry.resolveRoutes(body.model || '');
      if (!routes.length) return openAIError(res, 404, `模型 "${body.model}" 不存在`);
      for (const route of routes) {
        const channel = registry.getChannel(route.channel);
        if (!channel || channel.protocol !== 'openai') continue; // 该端点仅 OpenAI 兼容渠道支持
        try {
          const out = await forwardRawEndpoint({ channel, body: { ...body, model: route.upstream }, endpoint: kind, signal: req.abortSignal });
          return json(res, out.status, out.json, { 'x-gateway-channel': route.channel });
        } catch (e) { continue; }
      }
      return openAIError(res, 502, `${p} 仅支持 OpenAI 兼容协议的渠道`);
    }

    return openAIError(res, 404, `未知端点 ${p}`);
  }

  return openAIError(res, 404, 'Not Found');
}

// ---------- 启动 ----------
const server = http.createServer((req, res) => {
  route(req, res).catch((e) => {
    console.error('[server] 未处理错误:', e);
    if (!res.headersSent) openAIError(res, 500, `网关内部错误: ${e.message}`);
    else try { res.end(); } catch { /* ignore */ }
  });
});

export async function start({ silent = false } = {}) {
  startConfigWatch();
  await new Promise((resolve) => server.listen(config.port, config.host || '127.0.0.1', resolve));
  if (!silent) {
    const host = config.host === '127.0.0.1' ? 'localhost' : config.host;
    const base = `http://${host}:${server.address().port}`;
    console.log('----------------------------------------------');
    console.log(`  统一 LLM 网关 v${VERSION} 已启动${config.host === '127.0.0.1' ? '（仅本机可访问）' : `（监听 ${config.host}）`}`);
    console.log(`  后台管理:  ${base}/`);
    console.log(`  OpenAI 兼容 base_url: ${base}/v1`);
    console.log(`  网关 key: ${config.gateway_key}`);
    console.log(`  渠道数: ${config.channels.filter((c) => c.enabled).length} | 配置: ${config.CONFIG_PATH}`);
    console.log('----------------------------------------------');
  }
  registry.refreshAll().catch((e) => console.warn('[registry] 初始刷新失败:', e.message));
  const minutes = config.capabilities.refresh_minutes;
  if (minutes > 0) {
    const t = setInterval(() => registry.refreshAll().catch(() => {}), minutes * 60000);
    t.unref();
  }
  return server;
}

const isMain = process.argv[1] && import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href;
if (isMain) {
  start().catch((e) => { console.error('启动失败:', e); process.exit(1); });
}

process.on('SIGINT', () => { try { flushLogs(); } catch { /* ignore */ } server.close(() => process.exit(0)); setTimeout(() => process.exit(0), 1500).unref(); });
process.on('exit', () => { try { flushLogs(); } catch { /* ignore */ } });
