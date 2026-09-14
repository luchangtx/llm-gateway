// 能力活体探测：用最小请求实测视觉 / 思考能力（会产生极少量 token 消耗）
import { forwardChat as adaptersForward } from './adapters/index.js';
import { getChannel, adminList } from './registry.js';
import { setProbe, probeEntry } from './capabilities.js';

export const TINY_PNG =
  'data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg==';

function msgMatch(err, re) {
  return re.test(String(err?.message || ''));
}

function isEmbedding(model) {
  return /embedding|embed|rerank|moderation|whisper|tts|dall-e|image|audio|aqa/i.test(model);
}

/** 视觉探测：发一张 1x1 PNG，2xx 即支持 */
async function probeVision(route, channel, clientModel) {
  const body = {
    model: clientModel,
    messages: [{ role: 'user', content: [{ type: 'text', text: 'Reply with OK only.' }, { type: 'image_url', image_url: { url: TINY_PNG } }] }],
    max_tokens: 10
  };
  try {
    await adaptersForward({ channel, route, body, clientModel, caps: {} });
    return true;
  } catch (e) {
    if (e?.status === 400) return false; // 不支持视觉的模型通常对图片输入报 400
    return null;
  }
}

/** 思考探测：尝试带 reasoning 参数请求，观察是否被接受 / 响应是否含思考字段 */
async function probeThinking(route, channel, clientModel) {
  const messages = [{ role: 'user', content: 'What is 2+2? Reply with the number only.' }];

  if (channel.protocol === 'openai') {
    // 策略1: max_completion_tokens + reasoning_effort
    // 策略2: max_tokens + reasoning_effort
    // 策略3: 纯请求，看响应是否带 reasoning_content
    const attempts = [
      { max_completion_tokens: 4000, reasoning_effort: 'low' },
      { max_tokens: 4000, reasoning_effort: 'low' },
      { max_tokens: 4000 }
    ];
    for (let i = 0; i < attempts.length; i++) {
      try {
        const out = await adaptersForward({ channel, route, body: { model: clientModel, messages, ...attempts[i] }, clientModel, caps: {} });
        if (out.kind !== 'json') return i < 2 ? true : null;
        const m = out.json?.choices?.[0]?.message || {};
        const reasoningTokens = out.json?.usage?.completion_tokens_details?.reasoning_tokens ?? 0;
        if (i < 2) return { reasoning: true, levels: ['low', 'medium', 'high'] };
        if (m.reasoning_content || m.reasoning || reasoningTokens > 0) return { reasoning: true, levels: null };
        return false;
      } catch (e) {
        if (i === 0 && msgMatch(e, /max_completion_tokens|unknown|unsupported|unrecognized/i)) continue;
        if (i === 1 && msgMatch(e, /reasoning/i)) continue;
        return null;
      }
    }
    return null;
  }

  // anthropic / gemini：适配器会把 reasoning_effort 映射为 thinking / thinkingConfig
  try {
    await adaptersForward({
      channel, route,
      body: { model: clientModel, messages, max_tokens: 4096, reasoning_effort: 'low' },
      clientModel, caps: {}
    });
    return { reasoning: true, levels: ['low', 'medium', 'high'] };
  } catch (e) {
    if (msgMatch(e, /thinking|budget|reasoning/i) && e?.status === 400) return false;
    return null;
  }
}

/** 对单个模型条目执行探测并写缓存 */
async function probeEntryLive(entry, channelDef, kinds) {
  const item = { id: entry.id, vision: null, reasoning: null, levels: null };
  const route = { upstream: entry.upstream };
  const clientModel = entry.id;
  if (kinds.includes('vision') && !isEmbedding(entry.model)) {
    item.vision = await probeVision(route, channelDef, clientModel);
  }
  if (kinds.includes('thinking') && !isEmbedding(entry.model)) {
    const t = await probeThinking(route, channelDef, clientModel);
    if (typeof t === 'boolean') { item.reasoning = t; item.levels = null; }
    else if (t) { item.reasoning = t.reasoning; item.levels = t.levels; }
  }
  setProbe(entry.id, { vision: item.vision, reasoning: item.reasoning, levels: item.levels });
  return item;
}

async function runPool(targets, kinds, concurrency) {
  const out = [];
  const queue = [...targets];
  await Promise.all(Array.from({ length: Math.max(1, Math.min(concurrency, targets.length || 1)) }, async () => {
    while (queue.length) {
      const t = queue.shift();
      try {
        out.push(await probeEntryLive(t.entry, t.channel, kinds));
      } catch (e) {
        out.push({ id: t.entry.id, error: e.message });
      }
    }
  }));
  return out;
}

/** 按 id（渠道/模型）并发探测，用于后台「探测能力（按筛选）」 */
export async function probeIds(ids, { kinds = ['vision', 'thinking'], concurrency = 2 } = {}) {
  const list = adminList();
  const targets = ids
    .map((id) => list.models.find((m) => m.id === id))
    .filter(Boolean)
    .map((entry) => ({ entry, channel: getChannel(entry.channel) }));
  return runPool(targets, kinds, concurrency);
}

/** 探测指定渠道的指定模型（或全部模型），带缓存跳过 */
export async function probeChannel({ channelName, model = null, kinds = ['vision', 'thinking'], skipCachedDays = 7 } = {}) {
  const list = adminList();
  const channel = list.channels.find((c) => c.name === channelName);
  if (!channel) throw new Error(`渠道不存在: ${channelName}`);
  const entries = list.models.filter(
    (m) => m.channel === channelName && (!model || m.model === model || m.id === model)
  );
  if (!entries.length) throw new Error(`渠道 ${channelName} 下没有匹配的模型: ${model || '*'}`);

  const results = [];
  const now = Date.now();
  const freshMs = skipCachedDays * 86400000;
  const stale = [];
  for (const entry of entries) {
    const cached = probeEntry(entry.id);
    if (cached?.probed_at && now - cached.probed_at < freshMs) {
      results.push({ id: entry.id, vision: cached.vision ?? null, reasoning: cached.reasoning ?? null, levels: cached.levels ?? null, cached: true });
    } else {
      stale.push(entry);
    }
  }
  const channelDef = getChannel(channelName);
  const fresh = (await runPool(stale.map((e) => ({ entry: e, channel: channelDef })), kinds, 1)).map((r) => ({ ...r, cached: false }));
  results.push(...fresh);
  return results;
}

export async function probeAll({ kinds, concurrency = 2, channels = null } = {}) {
  const list = adminList();
  const names = list.channels.filter((c) => c.enabled && (!channels || channels.includes(c.name))).map((c) => c.name);
  const all = [];
  const queue = [...names];
  await Promise.all(Array.from({ length: Math.max(1, concurrency) }, async () => {
    while (queue.length) {
      const name = queue.shift();
      try {
        const r = await probeChannel({ channelName: name, kinds, skipCachedDays: 7 });
        all.push(...r);
      } catch (e) {
        all.push({ id: `${name}/*`, error: e.message });
      }
    }
  }));
  return all;
}
