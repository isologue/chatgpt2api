// Run with: node test/test_image_urls.cjs (uses the project's existing TypeScript compiler).
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const ts = require('../web/node_modules/typescript');
const source = fs.readFileSync(path.join(__dirname, '../web/src/lib/image-urls.ts'), 'utf8');
const code = ts.transpileModule(source, { compilerOptions: { module: ts.ModuleKind.CommonJS } }).outputText;
const context = { exports: {}, URL };
vm.runInNewContext(code, context);
const { getManagedImageUrl: image, getManagedThumbnailUrl: thumbnail } = context.exports;
const rel = '2026/10/29/1793260713_cd0109f187ffe7537025e2f2f153ab92.png';
const local = `http://127.0.0.1:3005/images/${rel}`;
assert.equal(image(local), `/images/${rel}`);
assert.equal(thumbnail(local), `/image-thumbnails/${rel}`);
assert.equal(image(`https://old.example/images/${rel}`), `/images/${rel}`);
assert.equal(image(`/images/${rel}`), `/images/${rel}`);
assert.equal(thumbnail(`/images/${rel}`), `/image-thumbnails/${rel}`);
assert.equal(thumbnail(local, 'http://127.0.0.1:8000'), `http://127.0.0.1:8000/image-thumbnails/${rel}`);
assert.equal(thumbnail(`http://127.0.0.1:8000/images/${rel}`, 'http://127.0.0.1:8000'), `http://127.0.0.1:8000/image-thumbnails/${rel}`);
assert.equal(image(`https://old.example/prefix/images/${rel}?v=1`), `/prefix/images/${rel}?v=1`);
for (const value of ['data:image/png;base64,AAAA', 'blob:https://app.test/123', 'https://cdn.test/photo.png?signature=x', 'https://cdn.test/images/2026/10/29/photo.png', 'https://cdn.test/images/icon.png', 'not a url', 'javascript:alert(1)']) {
  assert.equal(image(value), value);
  assert.equal(thumbnail(value), value);
}
console.log('Image URL regression checks passed (loopback, old host, dev API, subpath, third-party, data/blob).');
