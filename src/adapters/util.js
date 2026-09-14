// 适配器共享小工具
import crypto from 'node:crypto';

export function genId() { return 'chatcmpl-' + crypto.randomUUID(); }

export function parseArgs(args) {
  if (!args) return {};
  if (typeof args === 'object') return args;
  try { return args.trim() ? JSON.parse(args) : {}; } catch { return {}; }
}

export function textOf(content) {
  if (typeof content === 'string') return content;
  if (Array.isArray(content)) {
    return content.filter((p) => p && (p.type === 'text' || typeof p === 'string'))
      .map((p) => (typeof p === 'string' ? p : p.text || '')).join('\n');
  }
  return '';
}

export function parseDataUrl(url) {
  const m = /^data:([^;,]+)?(;base64)?,(.*)$/s.exec(url);
  if (!m || !m[2]) return null;
  return { mediaType: m[1] || 'application/octet-stream', data: m[3] };
}
