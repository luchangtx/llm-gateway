// 请求日志：内存环形缓冲（最近 500 条，供日志页快速读取）+ JSONL 持久化（重启不丢）
import fs from 'node:fs';
import { loadRecentLogs, appendLogLine, recordUsage, flushUsage } from './usage.js';

const MAX = 500;
const ring = [];
const listeners = new Set();

// 启动时从磁盘恢复
try {
  for (const entry of loadRecentLogs(MAX)) ring.push(entry);
} catch { /* ignore */ }

export function logRequest(entry) {
  const item = { t: Date.now(), ...entry };
  ring.push(item);
  if (ring.length > MAX) ring.shift();
  try { appendLogLine(item); } catch { /* ignore */ }
  try { recordUsage(item); } catch { /* ignore */ }
  for (const fn of listeners) {
    try { fn(item); } catch { /* ignore */ }
  }
}

export function recentLogs(limit = 200) {
  return ring.slice(-limit).reverse();
}

export function onLog(fn) { listeners.add(fn); return () => listeners.delete(fn); }

export function flushLogs() { flushUsage(); }
