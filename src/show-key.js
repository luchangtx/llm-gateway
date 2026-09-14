#!/usr/bin/env node
// 查看当前网关 key：npm run show-key
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const CONFIG_PATH = process.env.GATEWAY_CONFIG || path.join(ROOT, 'config.json');

try {
  const cfg = JSON.parse(fs.readFileSync(CONFIG_PATH, 'utf8'));
  if (!cfg.gateway_key) {
    console.log('config.json 里没有设置 gateway_key（服务首次启动时会自动生成）');
  } else {
    console.log(`当前网关 key: ${cfg.gateway_key}`);
    console.log(`配置文件: ${CONFIG_PATH}`);
  }
} catch (e) {
  console.error(`读取失败: ${e.message}（${CONFIG_PATH}）`);
  process.exit(1);
}
