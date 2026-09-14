// 测试用 mock 上游：OpenAI 兼容 / Anthropic / Gemini
import http from 'node:http';

function sse(res, chunks) {
  res.writeHead(200, { 'content-type': 'text/event-stream' });
  for (const c of chunks) res.write(c);
  res.end();
}

export function mockOpenAI({ failModels = [] } = {}) {
  const requests = [];
  const server = http.createServer((req, res) => {
    let raw = '';
    req.on('data', (c) => (raw += c));
    req.on('end', () => {
      const body = raw ? JSON.parse(raw) : {};
      requests.push({ path: req.url, headers: req.headers, body });
      if (req.method === 'GET' && req.url === '/v1/models') {
        return json(res, 200, { data: [{ id: 'mock-gpt' }, { id: 'glm-4.6', context_length: 200000, max_output_tokens: 128000 }, { id: 'dup-model' }] });
      }
      if (req.method === 'POST' && req.url === '/v1/chat/completions') {
        if (failModels.includes(body.model)) return json(res, 500, { error: { message: 'mock upstream exploded' } });
        if (body.stream) {
          return sse(res, [
            `data: ${JSON.stringify({ id: 'cmpl-1', object: 'chat.completion.chunk', model: body.model, choices: [{ index: 0, delta: { role: 'assistant', content: '' }, finish_reason: null }] })}\n\n`,
            `data: ${JSON.stringify({ id: 'cmpl-1', object: 'chat.completion.chunk', model: body.model, choices: [{ index: 0, delta: { content: 'Raw ' }, finish_reason: null }] })}\n\n`,
            `data: ${JSON.stringify({ id: 'cmpl-1', object: 'chat.completion.chunk', model: body.model, choices: [{ index: 0, delta: { content: 'stream' }, finish_reason: null }] })}\n\n`,
            `data: ${JSON.stringify({ id: 'cmpl-1', object: 'chat.completion.chunk', model: body.model, choices: [{ index: 0, delta: {}, finish_reason: 'stop' }] })}\n\n`,
            `data: ${JSON.stringify({ id: 'cmpl-1', object: 'chat.completion.chunk', model: body.model, choices: [], usage: { prompt_tokens: 3, completion_tokens: 2, total_tokens: 5, prompt_tokens_details: { cached_tokens: 1 } } })}\n\n`,
            'data: [DONE]\n\n'
          ]);
        }
        return json(res, 200, {
          id: 'cmpl-1', object: 'chat.completion', model: body.model,
          choices: [{ index: 0, message: { role: 'assistant', content: 'MockGPT reply' }, finish_reason: 'stop' }],
          usage: { prompt_tokens: 9, completion_tokens: 4, total_tokens: 13, prompt_tokens_details: { cached_tokens: 3 } }
        });
      }
      json(res, 404, { error: { message: 'no such mock path: ' + req.url } });
    });
  });
  return { server, requests };
}

export function mockAnthropic() {
  const requests = [];
  const server = http.createServer((req, res) => {
    let raw = '';
    req.on('data', (c) => (raw += c));
    req.on('end', () => {
      const body = raw ? JSON.parse(raw) : {};
      requests.push({ path: req.url, headers: req.headers, body });
      if (req.method === 'GET' && req.url === '/v1/models') {
        return json(res, 200, { data: [{ id: 'claude-mock', display_name: 'Claude Mock' }] });
      }
      if (req.method === 'POST' && req.url === '/v1/messages') {
        if (body.stream) {
          return sse(res, [
            'event: message_start\ndata: {"type":"message_start","message":{"id":"msg_1","usage":{"input_tokens":12}}}\n\n',
            'event: content_block_start\ndata: {"type":"content_block_start","index":0,"content_block":{"type":"text"}}\n\n',
            'event: content_block_delta\ndata: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Hi"}}\n\n',
            'event: content_block_stop\ndata: {"type":"content_block_stop","index":0}\n\n',
            'event: content_block_start\ndata: {"type":"content_block_start","index":1,"content_block":{"type":"tool_use","id":"toolu_1","name":"get_weather"}}\n\n',
            'event: content_block_delta\ndata: {"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"{\\"city\\":\\"Bei"}}\n\n',
            'event: content_block_delta\ndata: {"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"jing"}}\n\n',
            'event: content_block_stop\ndata: {"type":"content_block_stop","index":1}\n\n',
            'event: message_delta\ndata: {"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":7}}\n\n',
            'event: message_stop\ndata: {"type":"message_stop"}\n\n'
          ]);
        }
        return json(res, 200, {
          id: 'msg_1', type: 'message', role: 'assistant',
          content: [{ type: 'text', text: 'Hello from claude' }],
          stop_reason: 'end_turn',
          usage: { input_tokens: 10, output_tokens: 5, cache_read_input_tokens: 4 }
        });
      }
      json(res, 404, { error: { message: 'no such mock path: ' + req.url } });
    });
  });
  return { server, requests };
}

export function mockGemini() {
  const requests = [];
  const server = http.createServer((req, res) => {
    let raw = '';
    req.on('data', (c) => (raw += c));
    req.on('end', () => {
      const body = raw ? JSON.parse(raw) : {};
      requests.push({ path: req.url, headers: req.headers, body });
      if (req.method === 'GET' && req.url.startsWith('/v1beta/models')) {
        return json(res, 200, {
          models: [
            { name: 'models/gemini-mock', supportedGenerationMethods: ['generateContent'], inputTokenLimit: 123456, outputTokenLimit: 8192 },
            { name: 'models/text-embedding-mock', supportedGenerationMethods: ['embedContent'] }
          ]
        });
      }
      if (req.method === 'POST' && req.url.includes(':generateContent')) {
        return json(res, 200, {
          responseId: 'resp-1',
          candidates: [{ content: { parts: [{ text: 'Gemini says hi' }] }, finishReason: 'STOP' }],
          usageMetadata: { promptTokenCount: 7, candidatesTokenCount: 3, totalTokenCount: 10 }
        });
      }
      if (req.method === 'POST' && req.url.includes(':streamGenerateContent')) {
        return sse(res, [
          `data: ${JSON.stringify({ candidates: [{ content: { parts: [{ text: 'pondering', thought: true }] } }] })}\n\n`,
          `data: ${JSON.stringify({ candidates: [{ content: { parts: [{ text: 'He' }] } }] })}\n\n`,
          `data: ${JSON.stringify({ candidates: [{ content: { parts: [{ text: 'llo' }] } }] })}\n\n`,
          `data: ${JSON.stringify({ candidates: [{ content: { parts: [] }, finishReason: 'STOP' }], usageMetadata: { promptTokenCount: 7, candidatesTokenCount: 4, thoughtsTokenCount: 2, totalTokenCount: 13 } })}\n\n`
        ]);
      }
      json(res, 404, { error: { message: 'no such mock path: ' + req.url } });
    });
  });
  return { server, requests };
}

export function mockResponses() {
  const requests = [];
  const server = http.createServer((req, res) => {
    let raw = '';
    req.on('data', (c) => (raw += c));
    req.on('end', () => {
      const body = raw ? JSON.parse(raw) : {};
      requests.push({ path: req.url, headers: req.headers, body });
      if (req.method === 'GET' && req.url === '/v1/models') {
        return json(res, 200, { data: [{ id: 'mock-resp' }] });
      }
      if (req.method === 'POST' && req.url === '/v1/responses') {
        if (body.stream) {
          const ev = (event, data) => `event: ${event}\ndata: ${JSON.stringify({ type: event, ...data })}\n\n`;
          return sse(res, [
            ev('response.created', { response: { id: 'resp_1' } }),
            ev('response.reasoning_summary_text.delta', { delta: 'resp think' }),
            ev('response.output_text.delta', { delta: 'Hi ' }),
            ev('response.output_text.delta', { delta: 'there' }),
            ev('response.output_item.added', { output_index: 3, item: { type: 'function_call', call_id: 'call_7', name: 'get_weather', arguments: '' } }),
            ev('response.function_call_arguments.delta', { output_index: 3, delta: '{"city":"北' }),
            ev('response.function_call_arguments.delta', { output_index: 3, delta: '京"}' }),
            ev('response.completed', { response: { id: 'resp_1', status: 'completed', usage: { input_tokens: 5, output_tokens: 9, total_tokens: 14 } } })
          ]);
        }
        return json(res, 200, {
          id: 'resp_1', object: 'response', status: 'completed', model: body.model,
          output: [
            { type: 'reasoning', summary: [{ type: 'summary_text', text: 'resp think' }] },
            { type: 'message', role: 'assistant', content: [{ type: 'output_text', text: 'Resp reply' }] },
            { type: 'function_call', call_id: 'call_9', name: 'get_weather', arguments: '{"city":"北京"}' }
          ],
          usage: { input_tokens: 8, output_tokens: 6, total_tokens: 14, output_tokens_details: { reasoning_tokens: 2 } }
        });
      }
      json(res, 404, { error: { message: 'no such mock path: ' + req.url } });
    });
  });
  return { server, requests };
}

export function mockHanging() {
  // 接受连接但永远不响应 —— 用于测试流式首包超时
  const server = http.createServer(() => { /* 故障挂起 */ });
  return { server, requests: [] };
}

function json(res, status, obj) {
  res.writeHead(status, { 'content-type': 'application/json' });
  res.end(JSON.stringify(obj));
}

export function listen(server) {
  return new Promise((resolve) => server.listen(0, '127.0.0.1', () => resolve(server.address().port)));
}
