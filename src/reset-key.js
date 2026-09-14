#!/usr/bin/env node
// 重置网关 key：node src/reset-key.js [新key]（不传则自动生成）
// 运行中的网关会在 ~1 秒内自动热加载新 key，无需重启。
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { fileURLToPath } from 'node:url';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const CONFIG_PATH = process.env.GATEWAY_CONFIG || path.join(ROOT, 'config.json');

const newKey = process.argv[2] || 'sk-gw-' + crypto.randomBytes(24).toString('base64url');
if (newKey.length < 8) {
  console.error('✗ key 至少 8 个字符');
  process.exit(1);
}

let cfg = {};
try { cfg = JSON.parse(fs.readFileSync(CONFIG_PATH, 'utf8')); } catch { cfg = {}; }
cfg.gateway_key = newKey;
fs.mkdirSync(path.dirname(CONFIG_PATH), { recursive: true });
fs.writeFileSync(CONFIG_PATH, JSON.stringify(cfg, null, 2) + '\n');

console.log(`✓ 网关 key 已重置为: ${newKey}`);
console.log(`  配置文件: ${CONFIG_PATH}`);
console.log('  运行中的服务会自动热加载（约 1 秒），无需重启；请同步更新所有客户端。');
