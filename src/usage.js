// 用量统计持久化（data/usage.json）+ 请求日志持久化辅助（JSONL）
import fs from 'node:fs';
import path from 'node:path';
import { config } from './config.js';

const USAGE_FILE = path.join(config.DATA_DIR, 'usage.json');
export const LOG_FILE = path.join(config.DATA_DIR, 'requests.jsonl');
const LOG_ROTATED = LOG_FILE + '.1';
const MAX_LOG_BYTES = 8 * 1024 * 1024;

function pad(n) { return String(n).padStart(2, '0'); }
function dayKey(t) {
  const d = new Date(t);
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`;
}

function emptyAgg() { return { requests: 0, ok: 0, fail: 0, prompt: 0, completion: 0, reasoning: 0, cached: 0 }; }

function addTo(a, e) {
  a.requests += 1;
  if ((e.status || 0) < 400) a.ok += 1; else a.fail += 1;
  a.prompt += e.prompt_tokens || 0;
  a.completion += e.completion_tokens || 0;
  a.reasoning += e.reasoning_tokens || 0;
  a.cached += e.cached_tokens || 0;
}

let data = { version: 1, totals: emptyAgg(), days: {} };
try {
  const raw = JSON.parse(fs.readFileSync(USAGE_FILE, 'utf8'));
  if (raw && raw.days && raw.totals) data = raw;
} catch { /* 首次运行 */ }

let dirty = false;
let saveTimer = null;

function saveNow() {
  try {
    fs.mkdirSync(config.DATA_DIR, { recursive: true });
    fs.writeFileSync(USAGE_FILE, JSON.stringify(data, null, 1));
    dirty = false;
  } catch (e) { console.error('[usage] 保存失败:', e.message); }
}

function scheduleSave() {
  dirty = true;
  if (saveTimer) return;
  saveTimer = setTimeout(() => { saveTimer = null; if (dirty) saveNow(); }, 2000);
  saveTimer.unref?.();
}

/** 记录一次请求到聚合统计（含失败/无 usage 的请求） */
export function recordUsage(e) {
  const day = dayKey(e.t || Date.now());
  if (!data.days[day]) data.days[day] = { totals: emptyAgg(), channels: {}, models: {} };
  const d = data.days[day];
  addTo(data.totals, e);
  addTo(d.totals, e);
  if (e.channel) {
    if (!d.channels[e.channel]) d.channels[e.channel] = emptyAgg();
    addTo(d.channels[e.channel], e);
  }
  if (e.model) {
    if (!d.models[e.model]) d.models[e.model] = emptyAgg();
    addTo(d.models[e.model], e);
  }
  scheduleSave();
}

export function flushUsage() { if (dirty) saveNow(); }

/**
 * 聚合查询：按时间范围 + 渠道/模型过滤。
 * 过滤优先级：model > channel > 全部。
 */
export function getUsage({ from = null, to = null, channel = null, model = null } = {}) {
  const merge = () => emptyAgg();
  const totals = merge();
  const byChannel = {};
  const byModel = {};
  const byDay = [];
  const days = Object.keys(data.days).sort().filter((day) => (!from || day >= from) && (!to || day <= to));
  for (const day of days) {
    const d = data.days[day];
    const scope = model ? d.models[model] : channel ? d.channels[channel] : d.totals;
    const dayAgg = merge();
    if (scope) for (const [k, v] of Object.entries(scope)) dayAgg[k] += v;
    for (const [k, v] of Object.entries(dayAgg)) totals[k] += v;
    byDay.push({ day, ...dayAgg });
    if (!model) {
      for (const [ch, agg] of Object.entries(d.channels)) {
        if (!byChannel[ch]) byChannel[ch] = merge();
        for (const [k, v] of Object.entries(agg)) byChannel[ch][k] += v;
      }
    }
    if (!channel) {
      for (const [m, agg] of Object.entries(d.models)) {
        if (!byModel[m]) byModel[m] = merge();
        for (const [k, v] of Object.entries(agg)) byModel[m][k] += v;
      }
    }
  }
  const toList = (obj) => Object.entries(obj).map(([name, a]) => ({ name, ...a })).sort((a, b) => (b.prompt + b.completion) - (a.prompt + a.completion));
  return {
    totals,
    byChannel: toList(byChannel),
    byModel: toList(byModel),
    byDay,
    range: { from: from || null, to: to || null, channel: channel || null, model: model || null }
  };
}

// ---------- 请求日志 JSONL 持久化 ----------

let appendCount = 0;

export function appendLogLine(entry) {
  try {
    fs.mkdirSync(config.DATA_DIR, { recursive: true });
    if (appendCount % 500 === 0) {
      // 偶尔检查大小，超过 8MB 轮转一代
      try {
        const st = fs.statSync(LOG_FILE);
        if (st.size > MAX_LOG_BYTES) {
          try { fs.rmSync(LOG_ROTATED, { force: true }); } catch { /* ignore */ }
          fs.renameSync(LOG_FILE, LOG_ROTATED);
        }
      } catch { /* 文件还不存在 */ }
    }
    fs.appendFileSync(LOG_FILE, JSON.stringify(entry) + '\n');
    appendCount++;
  } catch { /* 日志写失败不影响主流程 */ }
}

/** 启动时从磁盘恢复最近 limit 条日志到内存 */
export function loadRecentLogs(limit = 500) {
  const lines = [];
  for (const f of [LOG_ROTATED, LOG_FILE]) {
    try {
      const text = fs.readFileSync(f, 'utf8');
      for (const line of text.split('\n')) {
        const s = line.trim();
        if (!s) continue;
        try { lines.push(JSON.parse(s)); } catch { /* 跳过损坏行 */ }
      }
    } catch { /* 文件不存在 */ }
  }
  lines.sort((a, b) => (a.t || 0) - (b.t || 0));
  return lines.slice(-limit);
}

/** 从流式透传的原始文本中提取最后一个 usage 对象（OpenAI/Responses/Gemini/Anthropic 风格都认） */
function extractBalancedObject(text, keyIdx) {
  const open = text.indexOf('{', keyIdx);
  if (open < 0) return null;
  let depth = 0, inStr = false, esc = false;
  for (let i = open; i < text.length; i++) {
    const c = text[i];
    if (inStr) {
      if (esc) esc = false;
      else if (c === '\\') esc = true;
      else if (c === '"') inStr = false;
      continue;
    }
    if (c === '"') inStr = true;
    else if (c === '{') depth++;
    else if (c === '}') {
      depth--;
      if (depth === 0) {
        try { return JSON.parse(text.slice(open, i + 1)); } catch { return null; }
      }
    }
  }
  return null;
}

function normalizeUsage(u) {
  if (!u || typeof u !== 'object') return null;
  const prompt = u.prompt_tokens ?? u.input_tokens ?? null;
  const completion = u.completion_tokens ?? u.output_tokens ?? null;
  if (prompt == null && completion == null) return null;
  return {
    prompt_tokens: prompt,
    completion_tokens: completion,
    cached_tokens: u.prompt_tokens_details?.cached_tokens ?? u.input_tokens_details?.cached_tokens
      ?? u.cache_read_input_tokens ?? u.cached_tokens ?? u.cachedContentTokenCount ?? null,
    reasoning_tokens: u.completion_tokens_details?.reasoning_tokens ?? u.output_tokens_details?.reasoning_tokens
      ?? u.thoughtsTokenCount ?? null
  };
}

export function parseUsageFromText(text) {
  for (const key of ['"usage"', '"usageMetadata"', '"usage":']) {
    let idx = text.lastIndexOf(key);
    while (idx >= 0) {
      const obj = extractBalancedObject(text, idx);
      const u = normalizeUsage(obj);
      if (u) return u;
      idx = text.lastIndexOf(key, idx - 1);
    }
    if (idx >= 0) break;
  }
  return null;
}
