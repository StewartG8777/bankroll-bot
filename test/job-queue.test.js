'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { createJobQueue } = require('../systems/job-queue');
test('limits concurrency and resolves all jobs', async () => {
  const q = createJobQueue({ concurrency: 1 });
  let active = 0, peak = 0;
  const task = async () => { active++; peak = Math.max(peak, active); await new Promise(r => setTimeout(r, 5)); active--; return 1; };
  assert.deepEqual(await Promise.all([q.enqueue(task), q.enqueue(task), q.enqueue(task)]), [1,1,1]);
  assert.equal(peak, 1); assert.equal(q.status().completed, 3);
});
test('retries recoverable failures and reports terminal failures', async () => {
  const q = createJobQueue({ retryDelayMs: 1 });
  let attempts = 0;
  assert.equal(await q.enqueue(async () => { if (++attempts < 2) throw Error('retry'); return 'ok'; }), 'ok');
  assert.equal(q.status().retried, 1);
  await assert.rejects(q.enqueue(async () => { throw Error('failed'); }, { retries: 0 }), /failed/);
  assert.equal(q.status().failed, 1);
});
