'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
test('ShareX uploader uses binary POST and reads returned Bankroll URL', () => {
  const cfg = JSON.parse(fs.readFileSync(path.join(__dirname, '..', 'sharex', 'Bankroll.sxcu'), 'utf8'));
  assert.equal(cfg.RequestMethod, 'POST');
  assert.equal(cfg.Body, 'Binary');
  assert.equal(cfg.URL, '$json:url$');
  assert.match(cfg.RequestURL, /\/sharex\/upload$/);
  assert.ok(cfg.Headers['X-ShareX-Token']);
});
