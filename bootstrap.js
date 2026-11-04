'use strict';

// Bankroll storage bootstrap. This file deliberately runs before index.js is
// loaded, because index.js reads JSON databases at module evaluation time.
require('dotenv').config();
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { Client, GatewayIntentBits, Partials } = require('discord.js');

const TOKEN = process.env.DISCORD_TOKEN;
const DATA_DIR = path.resolve(process.env.DATA_DIR || path.join(__dirname, 'data'));
const BACKUP_CHANNEL_ID = process.env.BACKUP_CHANNEL_ID || '';
const PRIMARY_REQUIRED = process.env.BANKROLL_DRIVE_PRIMARY !== '0';
const AUTH_NAME = 'google-drive-auth.enc';
const FOLDER_NAMES = ['BACKUPS', 'DATABASE', 'VAULT'];

function log(message) { console.log('[Bankroll bootstrap] ' + message); }
function authKey() {
  const secret = process.env.BANKROLL_BACKUP_ENCRYPTION_KEY || TOKEN;
  if (!secret) throw new Error('No server secret is available to decrypt the Google Drive credentials.');
  return crypto.createHash('sha256').update(String(secret)).digest();
}
function decryptBundle(buffer) {
  const envelope = JSON.parse(buffer.toString('utf8'));
  if (envelope.version !== 1) throw new Error('Unsupported encrypted Google credential backup version.');
  const decipher = crypto.createDecipheriv('aes-256-gcm', authKey(), Buffer.from(envelope.iv, 'base64'));
  decipher.setAuthTag(Buffer.from(envelope.tag, 'base64'));
  return JSON.parse(Buffer.concat([
    decipher.update(Buffer.from(envelope.data, 'base64')), decipher.final(),
  ]).toString('utf8'));
}
function safeRelative(value) {
  if (typeof value !== 'string' || !value.trim()) throw new Error('Empty path in Drive manifest.');
  const clean = value.replace(/\\/g, '/').replace(/^\/+/, '');
  if (clean.split('/').some((part) => !part || part === '.' || part === '..')) throw new Error('Unsafe path in Drive manifest.');
  if (/^(?:google-oauth\.json|google-drive-token\.json|\.env)$/i.test(clean)) throw new Error('Credential path is forbidden in Drive recovery.');
  return clean;
}
async function findBackupChannel(client) {
  if (BACKUP_CHANNEL_ID) {
    const channel = await client.channels.fetch(BACKUP_CHANNEL_ID).catch(() => null);
    if (channel?.isTextBased?.() && channel.messages?.fetch) return channel;
  }
  for (const guild of client.guilds.cache.values()) {
    let channels = guild.channels.cache;
    if (!channels.size) channels = await guild.channels.fetch().catch(() => channels);
    const found = channels.find((item) => item?.name?.toLowerCase() === 'bankroll-backup' && item.isTextBased?.() && item.messages?.fetch);
    if (found) return found;
  }
  return null;
}
async function recoverOAuth(client) {
  const channel = await findBackupChannel(client);
  if (!channel) throw new Error('Cannot find #bankroll-backup to recover the encrypted Google Drive credentials.');
  let before;
  for (let page = 0; page < 100; page++) {
    const messages = await channel.messages.fetch({ limit: 100, ...(before ? { before } : {}) });
    if (!messages.size) break;
    const ordered = [...messages.values()].sort((a, b) => b.createdTimestamp - a.createdTimestamp);
    for (const message of ordered) {
      if (message.author?.id !== client.user.id) continue;
      const attachment = message.attachments.find((item) => item.name === AUTH_NAME);
      if (!attachment) continue;
      const response = await fetch(attachment.url, { signal: AbortSignal.timeout(30000) });
      if (!response.ok) throw new Error('Could not download encrypted Drive credentials (HTTP ' + response.status + ').');
      const bundle = decryptBundle(Buffer.from(await response.arrayBuffer()));
      const config = bundle.client?.web || bundle.client?.installed || bundle.client;
      if (!config?.client_id || !config?.client_secret || !bundle.token) throw new Error('The encrypted Google OAuth bundle is incomplete.');
      return { config, token: bundle.token };
    }
    before = ordered[ordered.length - 1]?.id;
    if (!before || messages.size < 100) break;
  }
  throw new Error('No encrypted Google OAuth recovery attachment was found in #bankroll-backup.');
}
async function downloadVerified(drive, entry, destination) {
  if (!entry?.fileId || !/^[a-f0-9]{64}$/i.test(String(entry.sha256 || '')) ||
      !Number.isSafeInteger(Number(entry.size)) || Number(entry.size) < 0) {
    throw new Error('Drive recovery manifest contains an invalid file record.');
  }
  await fs.promises.mkdir(path.dirname(destination), { recursive: true });
  const temp = destination + '.bootstrap-' + crypto.randomBytes(8).toString('hex');
  try {
    const response = await drive.files.get({ fileId: entry.fileId, alt: 'media', supportsAllDrives: true }, { responseType: 'stream' });
    const hash = crypto.createHash('sha256');
    let size = 0;
    const { Transform } = require('stream');
    const { pipeline } = require('stream/promises');
    const meter = new Transform({ transform(chunk, enc, callback) { size += chunk.length; hash.update(chunk); callback(null, chunk); } });
    await pipeline(response.data, meter, fs.createWriteStream(temp, { flags: 'wx', mode: 0o600 }));
    if (size !== Number(entry.size) || hash.digest('hex') !== entry.sha256) throw new Error('Drive recovery checksum failed for ' + path.basename(destination) + '.');
    await fs.promises.rename(temp, destination);
  } catch (error) {
    await fs.promises.unlink(temp).catch(() => {});
    throw error;
  }
}
async function restoreDriveSnapshot(tokenBundle) {
  const { google } = require('googleapis');
  const clientConfig = tokenBundle.config;
  const redirect = (clientConfig.redirect_uris || [])[0] || 'http://localhost';
  const oauth = new google.auth.OAuth2(clientConfig.client_id, clientConfig.client_secret, redirect);
  oauth.setCredentials(tokenBundle.token);
  const drive = google.drive({ version: 'v3', auth: oauth });
  const folders = {};
  for (const name of FOLDER_NAMES) {
    const page = await drive.files.list({
      q: "name = '" + name + "' and mimeType = 'application/vnd.google-apps.folder' and trashed = false",
      pageSize: 100, fields: 'files(id,name)', supportsAllDrives: true, includeItemsFromAllDrives: true,
    });
    const matches = page.data.files || [];
    if (matches.length !== 1) throw new Error('Expected exactly one Google Drive folder named ' + name + '; found ' + matches.length + '.');
    folders[name] = matches[0];
  }
  const manifests = [];
  let pageToken;
  do {
    const page = await drive.files.list({
      q: "'" + folders.BACKUPS.id + "' in parents and name contains 'bankroll-recovery-' and trashed = false",
      pageSize: 1000, pageToken, orderBy: 'modifiedTime desc',
      fields: 'nextPageToken,files(id,name,modifiedTime)', supportsAllDrives: true, includeItemsFromAllDrives: true,
    });
    manifests.push(...(page.data.files || []));
    pageToken = page.data.nextPageToken;
  } while (pageToken);
  manifests.sort((a, b) => new Date(b.modifiedTime || 0) - new Date(a.modifiedTime || 0));
  if (!manifests.length) throw new Error('No versioned Bankroll recovery manifest exists in Google Drive BACKUPS.');
  let manifest;
  for (const candidate of manifests) {
    const response = await drive.files.get({ fileId: candidate.id, alt: 'media', supportsAllDrives: true }, { responseType: 'stream' });
    const chunks = [];
    for await (const chunk of response.data) chunks.push(Buffer.from(chunk));
    const parsed = JSON.parse(Buffer.concat(chunks).toString('utf8'));
    if (parsed.format === 'bankroll-drive-layout-v2' && Array.isArray(parsed.databaseFiles) && Array.isArray(parsed.vaultFiles)) {
      manifest = parsed;
      log('Selected recovery generation ' + candidate.name + ' (' + parsed.createdAt + ').');
      break;
    }
  }
  if (!manifest) throw new Error('No supported Bankroll v2 recovery generation was found.');

  let restoredDatabase = 0, restoredVault = 0, preservedExisting = 0;
  // Existing Render files are never overwritten during bootstrap. This preserves
  // the live instance's latest invoice/payment records during the first cutover.
  for (const entry of manifest.databaseFiles) {
    const name = String(entry.name || '');
    if (!/^[a-z0-9_.-]+\.json$/i.test(name)) throw new Error('Unsafe database filename in recovery manifest.');
    const destination = path.join(DATA_DIR, name);
    if (fs.existsSync(destination)) { preservedExisting++; continue; }
    await downloadVerified(drive, entry, destination);
    restoredDatabase++;
  }
  for (const entry of manifest.vaultFiles) {
    const relative = safeRelative(entry.path);
    if (!(relative.startsWith('vault/') || relative.startsWith('files/'))) throw new Error('Unexpected vault path in recovery manifest.');
    const destination = path.resolve(DATA_DIR, relative);
    if (!destination.startsWith(DATA_DIR + path.sep)) throw new Error('Vault path escapes the data directory.');
    if (fs.existsSync(destination)) { preservedExisting++; continue; }
    await downloadVerified(drive, entry, destination);
    restoredVault++;
  }
  log('Recovery complete: ' + restoredDatabase + ' database files and ' + restoredVault + ' vault files restored; ' + preservedExisting + ' existing Render files preserved.');
  return { restoredDatabase, restoredVault, preservedExisting };
}
async function main() {
  if (!TOKEN) throw new Error('DISCORD_TOKEN is required for secure Google credential recovery.');
  fs.mkdirSync(DATA_DIR, { recursive: true });
  const localDatabases = fs.readdirSync(DATA_DIR).filter((name) => /^[a-z0-9_.-]+\.json$/i.test(name) && !/^google-/i.test(name));
  const client = new Client({ intents: [GatewayIntentBits.Guilds, GatewayIntentBits.GuildMessages], partials: [Partials.Channel] });
  try {
    await client.login(TOKEN);
    log('Connected to Discord to recover Drive credentials before loading databases.');
    const bundle = await recoverOAuth(client);
    const hasAnyLocalDb = localDatabases.length > 0;
    if (hasAnyLocalDb) log('Existing Render database files detected; they will be preserved and only missing files imported.');
    await restoreDriveSnapshot(bundle);
  } finally {
    client.destroy();
  }
  // Only now may index.js evaluate and load the application databases.
  require('./index.js');
}
main().catch((error) => {
  console.error('[Bankroll bootstrap] FATAL: ' + (error.stack || error.message || error));
  if (process.env.BANKROLL_DRIVE_PRIMARY !== '0') process.exitCode = 1;
  else {
    console.error('[Bankroll bootstrap] BANKROLL_DRIVE_PRIMARY=0; starting in legacy local-storage mode.');
    require('./index.js');
  }
});
