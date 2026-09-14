// SSE 读写小工具（零依赖）

/** 把 upstream 的 body (web ReadStream) 逐事件解析为 { event, data }，自动处理跨 chunk 断行 */
export async function* readSSE(body, { signal } = {}) {
  const decoder = new TextDecoder();
  let buf = '';
  const reader = body.getReader();
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buf += decoder.decode(value, { stream: true });
      let idx;
      while ((idx = buf.indexOf('\n\n')) >= 0) {
        const rawEvent = buf.slice(0, idx);
        buf = buf.slice(idx + 2);
        const parsed = parseEvent(rawEvent);
        if (parsed) yield parsed;
      }
    }
    if (buf.trim()) {
      const parsed = parseEvent(buf);
      if (parsed) yield parsed;
    }
  } finally {
    try { await reader.cancel(); } catch { /* ignore */ }
  }
}

function parseEvent(raw) {
  const lines = raw.split('\n');
  let event = null;
  const dataLines = [];
  for (const line of lines) {
    if (line.startsWith('event:')) event = line.slice(6).trim();
    else if (line.startsWith('data:')) dataLines.push(line.slice(5).replace(/^ /, ''));
  }
  if (!dataLines.length) return null;
  const data = dataLines.join('\n');
  if (data === '[DONE]') return { event, data: '[DONE]', done: true };
  return { event, data, done: false };
}

/** 带空闲看门狗的事件泵：超过 idleMs 没有任何事件就报错（用于流式中断检测） */
export async function* pumpSSE(body, { idleMs = 60000, signal } = {}) {
  const it = readSSE(body, { signal })[Symbol.asyncIterator]();
  while (true) {
    let timer;
    const gate = new Promise((_, rej) => {
      timer = setTimeout(() => rej(new Error(`上游流空闲超过 ${idleMs}ms`)), idleMs);
    });
    let r;
    try {
      r = await Promise.race([it.next(), gate]);
    } finally {
      clearTimeout(timer);
    }
    if (r.done) return;
    yield r.value;
  }
}

export function sseChunk(obj) {
  return `data: ${JSON.stringify(obj)}\n\n`;
}

export const SSE_DONE = 'data: [DONE]\n\n';

export function openAIChunk({ id, model, delta = {}, finish = null, usage = null, emptyChoices = false }) {
  const chunk = {
    id,
    object: 'chat.completion.chunk',
    created: Math.floor(Date.now() / 1000),
    model,
    choices: emptyChoices ? [] : [{ index: 0, delta, finish_reason: finish }]
  };
  if (usage) chunk.usage = usage;
  return chunk;
}
