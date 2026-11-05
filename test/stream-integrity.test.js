'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { Readable, Writable } = require('node:stream');
const { pipeline } = require('node:stream/promises');
const crypto = require('node:crypto');
const { createIntegrityMeter } = require('../systems/stream-integrity');
test('hashes streamed chunks without holding the full upload', async () => {
  const input = [Buffer.from('hello'), Buffer.from('world')];
  const meter = createIntegrityMeter({ maxBytes: 20 });
  const output = [];
  await pipeline(Readable.from(input), meter, new Writable({ write(chunk, enc, cb) { output.push(chunk); cb(); } }));
  assert.equal(Buffer.concat(output).toString(), 'helloworld');
  assert.deepEqual(meter.result(), { size: 10, sha256: crypto.createHash('sha256').update('helloworld').digest('hex') });
});
test('rejects transfers beyond configured limit', async () => {
  const meter = createIntegrityMeter({ maxBytes: 2 });
  await assert.rejects(pipeline(Readable.from([Buffer.from('abc')]), meter, new Writable({ write(c,e,cb) { cb(); } })), /TOO_BIG/);
});
