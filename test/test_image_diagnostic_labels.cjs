// Run with: node test/test_image_diagnostic_labels.cjs
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const ts = require('../web/node_modules/typescript');
const source = fs.readFileSync(path.join(__dirname, '../web/src/lib/image-diagnostic-labels.ts'), 'utf8');
const code = ts.transpileModule(source, { compilerOptions: { module: ts.ModuleKind.CommonJS } }).outputText;
const context = { exports: {} };
vm.runInNewContext(code, context);
const { diagnosticPhaseLabel: phase, diagnosticStatusLabel: status, diagnosticReasonLabel: reason } = context.exports;
const backend = fs.readFileSync(path.join(__dirname, '../services/image_diagnostics.py'), 'utf8');
const phases = backend.match(/TRACE_PHASES = frozenset\(\{([\s\S]*?)\}\)/)[1].matchAll(/'([^']+)'/g);
for (const [, name] of phases) {
  assert.notEqual(phase(name), '其他阶段', `Missing Chinese label: ${name}`);
  assert.match(phase(name), /[\u3400-\u9fff]/);
}
assert.equal(status({ phase: 'sse_stream', status: 'failed', error_type: 'GeneratorExit' }), '已停止读取');
assert.equal(status({ phase: 'sse_stream', status: 'failed', error_type: 'TimeoutError' }), '失败');
assert.equal(status({ status: 'running' }), '进行中');
assert.equal(reason({ action: 'generation_result_uncertain', reason: 'result_poll_timeout' }), '生成结果尚未确定 / 图片结果补查超时');
assert.equal(reason({ error_type: 'ImageDeadlineExceeded' }), '图片请求总预算耗尽');
assert.equal(reason({ error_type: 'UnmappedError' }), '其他异常');
console.log('Chinese labels cover every diagnostic phase, historical GeneratorExit, and timeout decisions.');
assert.equal(reason({ action: 'result_poll_failed', reason: 'result_resolve_failed' }), '图片结果补查失败 / 图片结果补查或解析失败');
assert.equal(reason({ error_type: 'ImageResultResolveError' }), '图片结果补查或解析失败');

const performance = fs.readFileSync(path.join(__dirname, '../services/image_performance.py'), 'utf8');
const components = performance.match(/COMPONENTS = frozenset\(\{([\s\S]*?)\}\)/)[1].matchAll(/'([^']+)'/g);
for (const [, name] of components) {
  assert.notEqual(context.exports.performanceComponentLabel(name), '其他性能项', `Missing Chinese performance label: ${name}`);
  assert.match(context.exports.performanceComponentLabel(name), /[\u3400-\u9fff]/);
}
const stages = performance.match(/STAGES = frozenset\(\{([\s\S]*?)\}\)/)[1].matchAll(/'([^']+)'/g);
for (const [, name] of stages) assert.notEqual(phase(name), '其他阶段', `Missing performance stage label: ${name}`);
console.log('Chinese labels cover every performance component and stage.');
