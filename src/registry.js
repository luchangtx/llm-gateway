// 模型注册表：拉取各渠道模型列表 -> 合并能力 -> 建索引 -> 路由解析
import fs from 'node:fs';
import path from 'node:path';
import { config, onConfigChange } from './config.js';
import { endpointFor, upstreamFetch } from './upstream.js';
import { buildCaps, providerMetaFromModelEntry, probeEntry } from './capabilities.js';

const CACHE_FILE = path.join(config.DATA_DIR, 'registry.json');

let entries = [];        // 每个 (渠道, 模型) 一条
let byId = new Map();
let byModel = new Map(); // 对外模型名 -> entries[](按优先级)
let lastRefresh = 0;
let refreshing = null;

function loadCache() {
  try {
    const raw = JSON.parse(fs.readFileSync(CACHE_FILE, 'utf8'));
    if (Array.isArray(raw.entries)) {
      entries = raw.entries;
      reindex();
      lastRefresh = raw.saved_at || 0;
    }
  } catch { /* ignore */ }
}
function saveCache() {
  try { fs.writeFileSync(CACHE_FILE, JSON.stringify({ saved_at: lastRefresh, entries }, null, 2)); } catch { /* ignore */ }
}

function reindex() {
  byId = new Map();
  byModel = new Map();
  for (const e of entries) {
    byId.set(e.id, e);
    if (!byModel.has(e.model)) byModel.set(e.model, []);
    byModel.get(e.model).push(e);
  }
  for (const list of byModel.values()) {
    list.sort((a, b) => (a.priority - b.priority) || a.channel.localeCompare(b.channel));
  }
}

function extractOpenAIModels(json) {
  const list = Array.isArray(json?.data) ? json.data : Array.isArray(json) ? json : [];
  return list.map((m) => (typeof m === 'string' ? { id: m } : m)).filter((m) => m && m.id);
}

function extractAnthropicModels(json) {
  const list = Array.isArray(json?.data) ? json.data : [];
  return list.map((m) => ({ id: m.id, display_name: m.display_name }));
}

function extractGeminiModels(json) {
  const list = Array.isArray(json?.models) ? json.models : [];
  return list
    .filter((m) => (m.supportedGenerationMethods || []).includes('generateContent'))
    .filter((m) => !/embedding|aqa|tts|imagen|image-generation|veo|native-audio|live/i.test(m.name))
    .map((m) => ({ id: m.name.replace(/^models\//, ''), providerMeta: m }));
}

export async function fetchChannelModels(channel, signal) {
  if (channel.models.length > 0) {
    // 手动指定模型列表（不提供 /models 的渠道）
    return channel.models.map((id) => ({ id }));
  }
  const url = endpointFor(channel, 'models');
  const res = await upstreamFetch(channel, url, { timeoutMs: 20000, signal });
  if (!res.ok) {
    const text = await res.text().catch(() => '');
    throw new Error(`HTTP ${res.status} ${text.slice(0, 200)}`);
  }
  const json = await res.json();
  if (channel.protocol === 'anthropic') return extractAnthropicModels(json);
  if (channel.protocol === 'gemini') return extractGeminiModels(json);
  return extractOpenAIModels(json);
}

function buildChannelEntries(ch, models, out, pushed) {
  const addEntry = (clientName, upstreamName, meta = {}) => {
    const id = `${ch.name}/${clientName}`;
    if (pushed.has(id)) return;
    pushed.add(id);
    out.push({
      id,
      channel: ch.name,
      model: clientName,
      upstream: upstreamName,
      protocol: ch.protocol,
      api_style: ch.api_style || 'chat',
      priority: ch.priority,
      providerMeta: meta
    });
  };
  for (const m of models) {
    addEntry(m.id, m.id, providerMetaFromModelEntry(m.providerMeta || m, ch.protocol));
  }
  // 别名: 对外暴露 alias 名 -> 转发上游真实名（也允许别名指向列表之外的模型）
  for (const [alias, upstream] of Object.entries(ch.model_aliases || {})) {
    if (!alias || !upstream || alias === upstream) continue;
    addEntry(alias, upstream);
  }
}

export async function refreshAll({ force = false } = {}) {
  if (refreshing) return refreshing;
  refreshing = (async () => {
    const next = [];
    const enabled = config.channels.filter((c) => c.enabled);
    // 各渠道并行拉取，总耗时 ≈ 最慢的渠道
    const results = await Promise.allSettled(enabled.map((ch) => fetchChannelModels(ch)));
    const pushed = new Set();
    results.forEach((r, i) => {
      const ch = enabled[i];
      if (r.status === 'fulfilled') {
        buildChannelEntries(ch, r.value, next, pushed);
      } else {
        console.warn(`[registry] 渠道 ${ch.name} 模型列表获取失败: ${r.reason?.message || r.reason}`);
        // 保留旧缓存，避免该渠道暂时不可达导致模型全部消失
        for (const e2 of entries) {
          if (e2.channel === ch.name) next.push(e2);
        }
      }
    });
    entries = next;
    lastRefresh = Date.now();
    reindex();
    saveCache();
    console.log(`[registry] 已刷新 ${entries.length} 个模型（${new Set(entries.map((e) => e.channel)).size} 个渠道）`);
    return entries.length;
  })();
  try { return await refreshing; } finally { refreshing = null; }
}

/** 解析客户端请求的 model -> 按优先级排列的转发路由 */
export function resolveRoutes(modelStr) {
  const m = String(modelStr || '').trim();
  if (!m) return { routes: [], notFound: m, suggestions: [] };
  const chNames = new Set(config.channels.map((c) => c.name));
  const slash = m.indexOf('/');
  if (slash > 0) {
    const chName = m.slice(0, slash);
    const rest = m.slice(slash + 1);
    if (chNames.has(chName)) {
      const e = entries.find((x) => x.channel === chName && (x.model === rest || x.upstream === rest));
      if (e) return { routes: [e], notFound: null, suggestions: [] };
      return { routes: [], notFound: m, suggestions: suggestionsFor(rest) };
    }
  }
  const list = byModel.get(m) || [];
  if (list.length) return { routes: list, notFound: null, suggestions: [] };
  return { routes: [], notFound: m, suggestions: suggestionsFor(m) };
}

function suggestionsFor(model) {
  const lower = String(model).toLowerCase();
  if (lower.length < 4) return [];
  const prefix = lower.slice(0, 5);
  const hits = [...byModel.keys()].filter((n) => n.toLowerCase().includes(prefix));
  return hits.slice(0, 8);
}

export function getEntry(id) { return byId.get(id); }
export function getChannel(name) { return config.channels.find((c) => c.name === name); }

/** 组装带能力的模型条目（管理接口用） */
export function entryWithCaps(e) {
  const key = `${e.channel}/${e.model}`;
  const probe = probeEntry(key);
  const { caps, sources, kb_matched } = buildCaps({ model: e.model, key, providerMeta: e.providerMeta || {}, probeData: probe });
  return { ...e, caps, sources, kb_matched, probed_at: probe?.probed_at || null };
}

export function listOpenAI() {
  const seen = new Set();
  const data = [];
  const push = (id, e) => {
    if (seen.has(id)) return;
    seen.add(id);
    const full = entryWithCaps(e);
    data.push({
      id,
      object: 'model',
      created: Math.floor((lastRefresh || Date.now()) / 1000),
      owned_by: e.channel,
      context_length: full.caps.context,
      max_output_tokens: full.caps.max_output,
      capabilities: {
        vision: full.caps.vision,
        reasoning: full.caps.reasoning,
        thinking_param: full.caps.thinking_param,
        levels: full.caps.levels
      }
    });
  };
  // 裸模型名（多渠道同名时指向最高优先级渠道）
  for (const [name] of byModel) push(name, byModel.get(name)[0]);
  if (config.capabilities.expose_prefixed_ids) for (const e of entries) push(e.id, e);
  return { object: 'list', data };
}

export function adminList() {
  return {
    last_refresh: lastRefresh,
    channels: config.channels.map((c) => ({
      name: c.name, protocol: c.protocol, enabled: c.enabled, priority: c.priority,
      base_url: c.base_url, has_key: !!c.api_key, manual_models: c.models.length,
      aliases: Object.keys(c.model_aliases || {}).length
    })),
    models: entries.map((e) => {
      const full = entryWithCaps(e);
      return {
        id: e.id,
        model: e.model,
        upstream: e.upstream,
        channel: e.channel,
        protocol: e.protocol,
        api_style: e.api_style || 'chat',
        priority: e.priority,
        caps: full.caps,
        sources: full.sources,
        kb_matched: full.kb_matched,
        probed_at: full.probed_at,
        provider_meta: e.providerMeta || {}
      };
    })
  };
}

export async function testChannel(channelName) {
  const ch = getChannel(channelName);
  if (!ch) return { ok: false, error: '渠道不存在' };
  const t0 = Date.now();
  try {
    const models = await fetchChannelModels(ch);
    return { ok: true, model_count: models.length, sample: models.slice(0, 10).map((m) => m.id), latency_ms: Date.now() - t0 };
  } catch (e) {
    return { ok: false, error: e.message, latency_ms: Date.now() - t0 };
  }
}

export function stats() {
  return {
    last_refresh: lastRefresh,
    entries: entries.length,
    unique_models: byModel.size,
    channels: config.channels.filter((c) => c.enabled).length
  };
}

loadCache();
// config.json 热加载（如 CLI 修改渠道）后自动刷新模型列表
onConfigChange(() => { refreshAll().catch(() => {}); });
