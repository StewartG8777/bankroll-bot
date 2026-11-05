'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { createSerializedStorageQueue } = require('../systems/serialized-storage-queue');
const { verifiedOfflineRollback } = require('../systems/verified-drive-rollback');
test('serializes concurrent writes without overlapping', async () => {
  const q = createSerializedStorageQueue(), order = [];
  const a = q.enqueue(async () => { order.push('a-start'); await new Promise(r => setTimeout(r, 10)); order.push('a-end'); return 1; });
  const b = q.enqueue(async () => { order.push('b'); return 2; });
  assert.deepEqual(await Promise.all([a,b]), [1,2]);
  assert.deepEqual(order, ['a-start','a-end','b']);
  assert.equal(q.status().pending,0);
});
test('a failed write does not poison the serialized queue', async () => {
  const q = createSerializedStorageQueue();
  await assert.rejects(q.enqueue(async () => { throw Error('failed'); }), /failed/);
  assert.equal(await q.enqueue(async () => 'next'), 'next');
});
test('maintenance drains earlier writes, blocks new writes and unlocks on failure', async () => {
  const q = createSerializedStorageQueue(), order = [];
  const write = q.enqueue(async () => { order.push('write'); });
  const gate = q.withMaintenance(async () => {
    order.push('maintenance');
    assert.equal(q.status().maintenance,true);
    await assert.rejects(q.enqueue(async () => {}), /maintenance/);
    throw new Error('failed rollback');
  });
  await write;
  await assert.rejects(gate,/failed rollback/);
  assert.deepEqual(order,['write','maintenance']);
  assert.equal(q.status().maintenance,false);
  await q.enqueue(async () => order.push('after'));
  assert.deepEqual(order,['write','maintenance','after']);
});
test('offline rollback refuses unconfirmed or online operations', async () => {
  const q = createSerializedStorageQueue();
  const store = {verifyGeneration(){ throw Error('should not be called'); }};
  await assert.rejects(verifiedOfflineRollback({queue:q,store,generation:'g',assertStopped:async()=>true}),/confirmation/);
  await assert.rejects(verifiedOfflineRollback({queue:q,store,generation:'g',confirm:true,assertStopped:async()=>false}),/stopped/);
});
test('offline rollback verifies target and saves a safety generation before applying', async () => {
  const q = createSerializedStorageQueue(), order = [];
  const store = {
    async verifyGeneration(id) { order.push('verify:'+id); return {files:3}; },
    async snapshot() { order.push('safety');return {generation:'before'}; },
    async restoreGeneration(id) { order.push('restore:'+id);return {generation:id}; },
    async promoteGeneration(id) { order.push('promote:'+id);return {generation:id}; }
  };
  const result = await verifiedOfflineRollback({queue:q,store,generation:'older',confirm:true,assertStopped:async()=>true});
  assert.deepEqual(order,['verify:older','safety','restore:older','promote:older']);
  assert.equal(result.safetyGeneration,'before');
  assert.equal(result.promoted.generation,'older');
  assert.equal(q.status().maintenance,false);
});
