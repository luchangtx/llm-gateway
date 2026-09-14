import fs from 'node:fs';
import path from 'node:path';
import { config } from './config.js';

import kbRaw from './kb.json' with { type: 'json' };

// 能力字段合并优先级：override > probe > provider > kb > name > default
const SRC_PRIORITY = ['override', 'probe', 'provider', 'kb', 'name', 'default'];
const FIELDS = ['vision', 'reasoning', 'thinking_param', 'levels', 'context', 'max_output'];

const PROBE_FILE = path.join(config.DATA_DIR, 'capabilities.json');

let probeCache = {};
try { probeCache = JSON.parse(fs.readFileSync(PROBE_FILE, 'utf8')); } catch { probeCache = {}; }

export function saveProbeCache() {
  try { fs.writeFileSync(PROBE_FILE, JSON.stringify(probeCache, null, 2)); } catch (e) { console.error('[probe] 保存失败', e.message); }
}

function better(a, b) {
  if (a === undefined || a === null) return false;
  if (b === undefined || b === null) return true;
  return SRC_PRIORITY.indexOf(a) < SRC_PRIORITY.indexOf(b);
}

/** 知识库匹配：按顺序第一个命中生效 */
export function kbLookup(model) {
  for (const entry of kbRaw.models) {
    let re;
    try { re = new RegExp(entry.match, 'i'); } catch { continue; }
    if (!re.test(model)) continue;
    const caps = {};
    if (entry.vision !== undefined) caps.vision = entry.vision;
    if (entry.reasoning !== undefined) caps.reasoning = entry.reasoning;
    if (entry.thinking_param !== undefined) caps.thinking_param = entry.thinking_param;
    if (entry.context !== undefined) caps.context = entry.context === 'name_k' || entry.context === 'name_m' ? null : entry.context;
    if (entry.max_output !== undefined) caps.max_output = entry.max_output;
    return { caps, source: 'kb', matched: entry.match };
  }
  return { caps: {}, source: 'kb', matched: null };
}

/** 从模型名推断（如 -128k、vl/vision 后缀） */
export function nameLookup(model) {
  const caps = {};
  const k = model.match(/-(\d+)k\b/i);
  if (k) caps.context = +k[1] * 1000;
  const m = model.match(/-(\d+)m\b/i);
  if (m) caps.context = +m[1] * 1000000;
  if (/vl|vision|internvl|minicpm-v|llava|-4v|4v-/i.test(model)) caps.vision = true;
  return caps;
}

// vision / reasoning 无来源时保持 undefined（UI 显示"未知"），不冒充"不支持"
const DEFAULTS = { context: 32768, max_output: 8192 };

function lookupOverride(key, model) {
  const ov = config.capabilities.overrides || {};
  const direct = ov[key] || ov[model];
  return direct && typeof direct === 'object' ? direct : null;
}

/**
 * 合并出一个模型条目的能力。
 * providerMeta: 渠道模型列表里带回来的元数据（OpenRouter/vLLM/Gemini 等）
 * probeData:    实测缓存条目 { vision, reasoning, probed_at }
 */
export function buildCaps({ model, key, providerMeta = {}, probeData = null }) {
  const out = {};
  const sources = {};
  const apply = (field, value, source) => {
    if (value === undefined || value === null || value === '') return;
    if (!better(source, sources[field])) return;
    out[field] = value;
    sources[field] = source;
  };

  // kb
  const kb = kbLookup(model);
  for (const f of FIELDS) apply(f, kb.caps[f], 'kb');
  // 名称推断（只补 kb 没给的字段）
  const nameCaps = nameLookup(model);
  for (const f of ['vision', 'context']) apply(f, nameCaps[f], 'name');
  // 渠道元数据
  if (providerMeta.context) apply('context', providerMeta.context, 'provider');
  if (providerMeta.max_output) apply('max_output', providerMeta.max_output, 'provider');
  if (providerMeta.vision !== undefined) apply('vision', providerMeta.vision, 'provider');
  if (providerMeta.reasoning !== undefined) apply('reasoning', providerMeta.reasoning, 'provider');
  // 实测
  if (probeData) {
    if (probeData.vision !== undefined && probeData.vision !== null) apply('vision', probeData.vision, 'probe');
    if (probeData.reasoning !== undefined && probeData.reasoning !== null) apply('reasoning', probeData.reasoning, 'probe');
    if (probeData.levels) apply('levels', probeData.levels, 'probe');
  }
  // 手动覆盖（最优先）
  const ov = lookupOverride(key, model);
  if (ov) for (const f of FIELDS) apply(f, ov[f], 'override');

  // 默认值
  for (const [f, v] of Object.entries(DEFAULTS)) {
    if (out[f] === undefined) { out[f] = v; sources[f] = 'default'; }
  }
  if (out.reasoning && out.thinking_param == null) {
    // 知道能思考但不知道参数风格：由适配器按渠道协议给默认
    out.thinking_param = 'auto';
    sources.thinking_param = 'kb';
  }
  return { caps: out, sources, kb_matched: kb.matched };
}

export function probeEntry(key) { return probeCache[key] || null; }
export function setProbe(key, data) {
  probeCache[key] = { ...(probeCache[key] || {}), ...data, probed_at: Date.now() };
  saveProbeCache();
}
export function allProbeCache() { return probeCache; }

/** 归一化渠道 /models 返回里的能力元数据 */
export function providerMetaFromModelEntry(entry, protocol) {
  const meta = {};
  if (!entry || typeof entry !== 'object') return meta;
  const num = (v) => (Number.isFinite(+v) && +v > 0 ? +v : null);
  const ctx = num(entry.context_length) || num(entry.max_model_len) || num(entry.context_window) || num(entry.max_context_length);
  if (ctx) meta.context = ctx;
  const out = num(entry.max_output_tokens) || num(entry.max_tokens_out) || num(entry.outputTokenLimit) || num(entry.max_completion_tokens);
  if (out) meta.max_output = out;
  const modality = entry.architecture?.input_modalities || entry.input_modalities;
  if (Array.isArray(modality)) meta.vision = modality.includes('image');
  const sp = entry.supported_parameters || entry.supportedGenerationMethods;
  if (Array.isArray(sp)) {
    if (sp.includes('reasoning')) meta.reasoning = true;
    if (protocol === 'gemini') {
      meta._methods = sp;
      if (num(entry.inputTokenLimit)) meta.context = num(entry.inputTokenLimit);
    }
  }
  return meta;
}

export function fmtTokens(n) {
  if (!Number.isFinite(+n) || +n <= 0) return '—';
  n = +n;
  if (n >= 1000000) return (n % 1000000 === 0 ? n / 1000000 : (n / 1000000).toFixed(1)) + 'M';
  if (n >= 1000) return Math.round(n / 1000) + 'K';
  return String(n);
}
