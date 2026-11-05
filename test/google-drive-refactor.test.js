'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const create = require('../systems/google-drive');
const system = create({ crypto, Buffer });
test('Google OAuth recovery encrypts version 2 with stable Render encryption key', () => {
  const previous = process.env.BANKROLL_BACKUP_ENCRYPTION_KEY;
  process.env.BANKROLL_BACKUP_ENCRYPTION_KEY='unit-test-stable-secret-not-for-production';
  try {
    const payload={client:{client_id:'client'},token:{refresh_token:'token'}};
    const blob=system.encryptGoogleAuthBundle(payload);
    assert.equal(JSON.parse(blob.toString()).version,2);
    assert.deepEqual(system.decryptGoogleAuthBundle(blob),payload);
    assert.ok(!blob.toString('utf8').includes('refresh_token'));
  } finally {
    if(previous===undefined)delete process.env.BANKROLL_BACKUP_ENCRYPTION_KEY;
    else process.env.BANKROLL_BACKUP_ENCRYPTION_KEY=previous;
  }
});
test('version 2 fails closed when stable encryption key is missing', () => {
  const previous=process.env.BANKROLL_BACKUP_ENCRYPTION_KEY;
  delete process.env.BANKROLL_BACKUP_ENCRYPTION_KEY;
  try {assert.throws(()=>system.encryptGoogleAuthBundle({client:{}}),/BANKROLL_BACKUP_ENCRYPTION_KEY/);}
  finally {if(previous!==undefined)process.env.BANKROLL_BACKUP_ENCRYPTION_KEY=previous;}
});
test('legacy version 1 bundles remain recoverable while original token exists', () => {
  const prevKey=process.env.BANKROLL_BACKUP_ENCRYPTION_KEY;
  const prevDiscord=process.env.DISCORD_TOKEN;
  delete process.env.BANKROLL_BACKUP_ENCRYPTION_KEY;
  process.env.DISCORD_TOKEN='unit-test-old-discord-token';
  try {
    const iv=crypto.randomBytes(12);
    const key=crypto.createHash('sha256').update(process.env.DISCORD_TOKEN).digest();
    const cipher=crypto.createCipheriv('aes-256-gcm',key,iv);
    const payload={client:{client_id:'old'}};
    const body=Buffer.concat([cipher.update(JSON.stringify(payload)),cipher.final()]);
    const blob=Buffer.from(JSON.stringify({
      version:1,iv:iv.toString('base64'),tag:cipher.getAuthTag().toString('base64'),data:body.toString('base64')
    }));
    assert.deepEqual(system.decryptGoogleAuthBundle(blob),payload);
  } finally {
    if(prevKey===undefined)delete process.env.BANKROLL_BACKUP_ENCRYPTION_KEY;else process.env.BANKROLL_BACKUP_ENCRYPTION_KEY=prevKey;
    if(prevDiscord===undefined)delete process.env.DISCORD_TOKEN;else process.env.DISCORD_TOKEN=prevDiscord;
  }
});

test('legacy version 1 recovery tries the original Discord token even after a stable key is installed', () => {
  const prevKey=process.env.BANKROLL_BACKUP_ENCRYPTION_KEY;
  const prevToken=process.env.DISCORD_TOKEN;
  process.env.BANKROLL_BACKUP_ENCRYPTION_KEY='current-new-stable-key';
  process.env.DISCORD_TOKEN='historical-valid-discord-token';
  try {
    const iv=crypto.randomBytes(12);
    const oldKey=crypto.createHash('sha256').update(process.env.DISCORD_TOKEN).digest();
    const cipher=crypto.createCipheriv('aes-256-gcm',oldKey,iv);
    const bundle={client:{client_id:'historical'},token:{refresh_token:'r'}};
    const bytes=Buffer.concat([cipher.update(JSON.stringify(bundle)),cipher.final()]);
    const envelope=Buffer.from(JSON.stringify({version:1,iv:iv.toString('base64'),tag:cipher.getAuthTag().toString('base64'),data:bytes.toString('base64')}));
    assert.deepEqual(system.decryptGoogleAuthBundle(envelope),bundle);
  } finally {
    if(prevKey===undefined)delete process.env.BANKROLL_BACKUP_ENCRYPTION_KEY;
    else process.env.BANKROLL_BACKUP_ENCRYPTION_KEY=prevKey;
    if(prevToken===undefined)delete process.env.DISCORD_TOKEN;
    else process.env.DISCORD_TOKEN=prevToken;
  }
});
