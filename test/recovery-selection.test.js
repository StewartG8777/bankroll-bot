'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { selectRecoveryPoint } = require('../systems/recovery-selection');
const points = [
  { id:'old', createdAt:'2026-10-01T00:00:00Z' },
  { id:'new', createdAt:'2026-11-01T00:00:00Z' },
  { id:'middle', createdAt:'2026-10-15T00:00:00Z' }
];
test('latest recovery point is selected by timestamp', () => assert.equal(selectRecoveryPoint(points).id, 'new'));
test('recovery can target an exact historical point', () => assert.equal(selectRecoveryPoint(points, {id:'old'}).id, 'old'));
test('point in time recovery selects newest eligible snapshot', () => assert.equal(selectRecoveryPoint(points, {at:'2026-10-20T00:00:00Z'}).id, 'middle'));
test('missing points and invalid dates fail safely', () => {
  assert.equal(selectRecoveryPoint(points, {id:'missing'}), null);
  assert.equal(selectRecoveryPoint(points, {at:'2026-09-01T00:00:00Z'}), null);
  assert.throws(() => selectRecoveryPoint(points, {at:'invalid'}), /Invalid/);
});
