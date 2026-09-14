import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { fileURLToPath } from 'node:url';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const CONFIG_PATH = process.env.GATEWAY_CONFIG || path.join(ROOT, 'config.json');
const DATA_DIR = process.env.GATEWAY_DATA || path.join(ROOT, 'data');

const PROTOCOLS = ['openai', 'anthropic', 'gemini'];

function normalizeKeys(raw) {
  if (!Array.isArray(raw)) return [];
  const seen = new Set();
  const out = [];
  for (const k of raw) {
    if (!k || typeof k.key !== 'string' || k.key.length < 8) continue;
    if (seen.has(k.key)) continue;
    seen.add(k.key);
    out.push({
      key: k.key,
      name: String(k.name || '').slice(0, 40) || '未命名',
      enabled: k.enabled !== false,
      created_at: Number.isFinite(+k.created_at) ? +k.created_at : Date.now()
    });
  }
  return out;
}

function normalizeChannel(raw, i) {
  const c = raw || {};
  // 'openai-responses' 是 UI 下拉框里的组合值，展开为 protocol + api_style
  let protocol = PROTOCOLS.includes(c.protocol) ? c.protocol : 'openai';
  let api_style = c.api_style === 'responses' ? 'responses' : 'chat';
  if (c.protocol === 'openai-responses') { protocol = 'openai'; api_style = 'responses'; }
  return {
    name: String(c.name || `channel-${i + 1}`).trim(),
    base_url: String(c.base_url || '').replace(/\/+$/, ''),
    api_key: String(c.api_key || ''),
    protocol,
    api_style,
    enabled: c.enabled !== false,
    priority: Number.isFinite(+c.priority) ? +c.priority : 10,
    // 非空数组 = 手动指定模型列表（部分渠道不提供 /models 接口时使用）
    models: Array.isArray(c.models) ? c.models.map(String).filter(Boolean) : [],
    // 别名: 对外暴露的名称 -> 上游真实名称
    model_aliases: c.model_aliases && typeof c.model_aliases === 'object' ? { ...c.model_aliases } : {},
    extra_headers: c.extra_headers && typeof c.extra_headers === 'object' ? { ...c.extra_headers } : {}
  };
}

function load() {
  fs.mkdirSync(DATA_DIR, { recursive: true });
  let raw = {};
  if (fs.existsSync(CONFIG_PATH)) {
    try {
      raw = JSON.parse(fs.readFileSync(CONFIG_PATH, 'utf8'));
    } catch (e) {
      throw new Error(`config.json 解析失败: ${e.message}`);
    }
  } else {
    const example = path.join(ROOT, 'config.example.json');
    if (fs.existsSync(example)) raw = JSON.parse(fs.readFileSync(example, 'utf8'));
  }
  const cfg = {
    port: Number.isFinite(+raw.port) ? +raw.port : 8787,
    host: typeof raw.host === 'string' && raw.host ? raw.host : '127.0.0.1',
    gateway_key: String(raw.gateway_key || ''),
    keys: normalizeKeys(raw.keys),
    setup_done: !!raw.setup_done,
    routing: {
      failover: raw.routing?.failover !== false,
      timeout_ms: Number.isFinite(+raw.routing?.timeout_ms) ? +raw.routing?.timeout_ms : 120000,
      idle_timeout_ms: Number.isFinite(+raw.routing?.idle_timeout_ms) ? +raw.routing?.idle_timeout_ms : 60000,
      first_byte_timeout_ms: Number.isFinite(+raw.routing?.first_byte_timeout_ms) ? +raw.routing?.first_byte_timeout_ms : 60000
    },
    capabilities: {
      refresh_minutes: Number.isFinite(+raw.capabilities?.refresh_minutes) ? +raw.capabilities?.refresh_minutes : 720,
      probe_concurrency: Math.max(1, +raw.capabilities?.probe_concurrency || 2),
      expose_prefixed_ids: raw.capabilities?.expose_prefixed_ids !== false,
      overrides: raw.capabilities?.overrides && typeof raw.capabilities.overrides === 'object' ? { ...raw.capabilities.overrides } : {}
    },
    channels: Array.isArray(raw.channels) ? raw.channels.map(normalizeChannel) : []
  };
  if (!cfg.gateway_key) {
    cfg.gateway_key = 'sk-gw-' + crypto.randomBytes(24).toString('base64url');
    save(cfg);
    console.log(`[config] 已自动生成网关 key: ${cfg.gateway_key}`);
  }
  return cfg;
}

function save(cfg) {
  fs.writeFileSync(CONFIG_PATH, JSON.stringify(cfg, null, 2) + '\n');
}

export const config = { ...load(), ROOT, DATA_DIR, CONFIG_PATH, PROTOCOLS };
config.save = () => save(config);
config.saveChannels = (channels) => {
  config.channels = channels.map(normalizeChannel);
  save(config);
};

// ---------- 配置文件热加载（让 CLI 重置 key 等改动即时生效，端口修改仍需重启） ----------
const changeListeners = new Set();
export function onConfigChange(fn) { changeListeners.add(fn); return () => changeListeners.delete(fn); }

export function startConfigWatch() {
  let lastApplied = '';
  let lastMtime = 0;
  try { lastMtime = fs.statSync(CONFIG_PATH).mtimeMs; } catch { /* ignore */ }
  fs.watchFile(CONFIG_PATH, { interval: 1200 }, () => {
    let st;
    try { st = fs.statSync(CONFIG_PATH); } catch { return; }
    if (st.mtimeMs === lastMtime) return;
    lastMtime = st.mtimeMs;
    let raw;
    try { raw = JSON.parse(fs.readFileSync(CONFIG_PATH, 'utf8')); } catch { return; } // 写入中间态
    try {
      if (raw.gateway_key) config.gateway_key = String(raw.gateway_key);
      if (raw.keys !== undefined) config.keys = normalizeKeys(raw.keys);
      if (raw.setup_done !== undefined) config.setup_done = !!raw.setup_done;
      if (raw.routing) config.routing = { ...config.routing, ...raw.routing };
      if (raw.capabilities) config.capabilities = { ...config.capabilities, ...raw.capabilities };
      if (Array.isArray(raw.channels)) config.channels = raw.channels.map(normalizeChannel);
      const applied = JSON.stringify([config.gateway_key, config.routing, config.capabilities, config.channels]);
      if (applied === lastApplied) return;
      lastApplied = applied;
      console.log('[config] 检测到 config.json 变更，已热加载（端口修改需重启）');
      for (const fn of changeListeners) {
        try { fn(); } catch { /* ignore */ }
      }
    } catch { /* ignore */ }
  });
}
