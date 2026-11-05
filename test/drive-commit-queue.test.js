'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { createDriveCommitQueue } = require('../systems/drive-commit-queue');
test('serializes writes to the same key', async () => {
  const events = [];
  const queue = createDriveCommitQueue({ write: async (_, value) => {
    events.push('start'+value);
    await new Promise(resolve => setImmediate(resolve));
    events.push('end'+value);
  }});
  await Promise.all([queue.enqueue('vault',1),queue.enqueue('vault',2)]);
  await queue.flush();
  assert.deepEqual(events,['start1','end1','start2','end2']);
});
test('does not report a failed commit as durable', async () => {
  const queue = createDriveCommitQueue({ write: async () => { throw new Error('offline'); } });
  await assert.rejects(queue.enqueue('vault','x'), /offline/);
  await assert.rejects(queue.flush(), /Uncommitted/);
});
test('new successful commit supersedes earlier failed state', async () => {
  let attempts=0;
  const queue = createDriveCommitQueue({ write: async () => { if (++attempts === 1) throw new Error('offline'); } });
  await assert.rejects(queue.enqueue('vault','old'));
  await queue.enqueue('vault','new');
  await queue.flush();
});
