'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { Readable } = require('stream');
const { destinationFor, createDriveObjectStore } = require('../systems/drive-object-store');

test('non-payment file routing', () => {
  assert.equal(destinationFor('vault/example.bin'), 'VAULT');
  assert.equal(destinationFor('files/order.zip'), 'CUSTOMER DELIVERIES');
  assert.equal(destinationFor('deliveries/order.zip'), 'CUSTOMER DELIVERIES');
  assert.equal(destinationFor('tmp/job.zip'), 'TEMPORARY PROCESSING');
  assert.equal(destinationFor('vault.json'), 'DATABASE');
  assert.equal(destinationFor('notifications.json'), 'LOGS');
});
test('payment files and traversal are rejected', () => {
  for (const name of ['monzo_payments.json', 'monzo_review.json', 'stripe_balance_history.json', 'stripe_balance_snapshots.json', 'webhook_events.json', '../vault/file', '/vault/file', 'vault/../file', 'vault\\file']) {
    assert.throws(() => destinationFor(name));
  }
});
test('upload and direct stream download', async () => {
  const uploaded = [];
  const drive = { files: {
    create: async args => { uploaded.push(args); return { data: { id: 'file-1' } }; },
    list: async () => ({ data: { files: [{ id: 'file-1', createdTime: '2026-11-05T18:00:00Z', size: '3', appProperties: { bankrollObjectPath: 'vault/test.bin', bankrollObjectVersion: '1' } }] } }),
    get: async () => ({ data: Readable.from([Buffer.from('abc')]) }),
  } };
  const store = createDriveObjectStore({ drive, folders: { VAULT: { id: 'folder-vault' } } });
  await store.put('vault/test.bin', Buffer.from('abc'));
  assert.equal(uploaded[0].requestBody.parents[0], 'folder-vault');
  const result = await store.get('vault/test.bin');
  const chunks = [];
  for await (const chunk of result.stream) chunks.push(chunk);
  assert.equal(Buffer.concat(chunks).toString(), 'abc');
});
