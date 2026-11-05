'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { createDownloadLifecycle } = require('../systems/download-lifecycle');
test('reservation must persist before being acknowledged', async () => {
  const record = { used: false };
  const lifecycle = createDownloadLifecycle({ record, shouldConsume: () => true, persist: async () => { throw Error('Drive unavailable'); }, onCompleted: async () => {}, onInterrupted: async () => {} });
  await assert.rejects(lifecycle.reserve(), /Drive unavailable/);
  assert.equal(record.used, false);
  assert.equal(lifecycle.state, 'ready');
});
test('interrupted download releases token only once', async () => {
  const record = { used: false }; let interruptions = 0;
  const lifecycle = createDownloadLifecycle({ record, shouldConsume: () => true, persist: async () => {}, onCompleted: async () => {}, onInterrupted: async () => { interruptions++; } });
  assert.equal(await lifecycle.reserve(), true);
  assert.equal(record.used, true);
  assert.equal(await lifecycle.interrupted(), true);
  assert.equal(await lifecycle.interrupted(), false);
  assert.equal(record.used, false);
  assert.equal(interruptions, 1);
});
test('completed download cannot subsequently be interrupted', async () => {
  const record = { used: false }; let completed = 0;
  const lifecycle = createDownloadLifecycle({ record, shouldConsume: () => true, persist: async () => {}, onCompleted: async () => { completed++; }, onInterrupted: async () => { throw Error('unexpected'); } });
  await lifecycle.reserve();
  await lifecycle.completed();
  assert.equal(await lifecycle.interrupted(), false);
  assert.equal(completed, 1);
});
