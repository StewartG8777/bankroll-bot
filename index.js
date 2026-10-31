require('dotenv').config();
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const express = require('express');
const Stripe = require('stripe');
const AdmZip = require('adm-zip'); // npm install adm-zip (used by /vault edit)
const { pipeline } = require('stream/promises');
const { Readable, Transform } = require('stream');
const {
  Client,
  GatewayIntentBits,
  Partials,
  EmbedBuilder,
  ActionRowBuilder,
  ButtonBuilder,
  ButtonStyle,
  SlashCommandBuilder,
  InteractionContextType,
  ApplicationIntegrationType,
  AttachmentBuilder,
  ChannelType,
  PermissionFlagsBits,
} = require('discord.js');

const {
  DISCORD_TOKEN,
  STRIPE_SECRET_KEY,
  STRIPE_WEBHOOK_SECRET,
  OWNER_ID,
  ALLOWED_USER_IDS = '',
  CURRENCY = 'gbp',
  SUCCESS_URL = 'https://stripe.com',
  LOG_WEBHOOK_URL = '',
  LOG_CHANNEL_ID = '',
  PORT = 3000,
  PUBLIC_URL = '',
  BACKUP_CHANNEL_ID = '',
  MONZO_ME_URL = '',
  MONZO_PAYMENT_DETAILS = '',
  MONZO_WEBHOOK_TOKEN = '',
  MONZO_USERNAME = '',
  MONZO_ACCOUNT_ID = '',
  MONZO_ACCESS_TOKEN = '',
  DISCORD_GUILD_ID = '',
} = process.env;

const MONZO_LINK = (MONZO_ME_URL || (MONZO_USERNAME ? `https://monzo.me/${MONZO_USERNAME.replace(/^\/+|\/+$/g, '')}` : '')).replace(/\/+$/, '');

const stripe = new Stripe(STRIPE_SECRET_KEY);
const client = new Client({
  intents: [GatewayIntentBits.Guilds, GatewayIntentBits.DirectMessages],
  partials: [Partials.Channel],
});

// Who is allowed to run the commands (owner is always allowed)
const allowed = new Set(
  [OWNER_ID, ...ALLOWED_USER_IDS.split(',').map((s) => s.trim())].filter(Boolean)
);

// Select a writable data directory before any vault/database folders are created.
// Render Free does not provide persistent disks; if DATA_DIR points at an unavailable
// mount (for example /var/data), fall back to ephemeral local storage and log a warning.
const requestedDataDir = process.env.DATA_DIR || path.join(__dirname, 'data');
let DATA_DIR_STATUS = 'configured';
function selectWritableDataDir() {
  const candidates = [...new Set([requestedDataDir, path.join(__dirname, 'data'), path.join('/tmp', 'bankroll-data')].map((p) => path.resolve(p)))];
  let lastError = null;
  for (const candidate of candidates) {
    try {
      fs.mkdirSync(candidate, { recursive: true });
      const probe = path.join(candidate, `.bankroll-write-test-${process.pid}`);
      fs.writeFileSync(probe, 'ok');
      fs.unlinkSync(probe);
      if (candidate !== requestedDataDir) {
        DATA_DIR_STATUS = `fallback storage in use (${candidate}); requested ${requestedDataDir} was not writable`;
        console.warn(`DATA_DIR ${requestedDataDir} is not writable; using ${candidate}. Files may not persist across redeploys.`);
      } else {
        DATA_DIR_STATUS = `using ${candidate}`;
      }
      return candidate;
    } catch (e) {
      lastError = e;
    }
  }
  throw new Error(`No writable data directory found: ${lastError?.message || 'unknown error'}`);
}
const DATA_DIR = selectWritableDataDir();
const NOTIFICATIONS_FILE = path.join(DATA_DIR, 'notifications.json');
let notificationHistory = [];
try {
  const savedNotifications = JSON.parse(fs.readFileSync(NOTIFICATIONS_FILE, 'utf8'));
  notificationHistory = Array.isArray(savedNotifications) ? savedNotifications.slice(-100) : [];
} catch {}
let restoreDone = false;
let backupTimer = null;
function saveNotifications() {
  try {
    atomicWrite(NOTIFICATIONS_FILE, JSON.stringify(notificationHistory.slice(-100), null, 2));
  } catch (e) {
    console.error('Could not save notification history:', e.message);
  }
  scheduleBackup();
}
function recordNotification(type, title, detail, status = 'info') {
  notificationHistory.push({
    id: crypto.randomUUID(),
    type,
    title: short(String(title || 'Notification'), 100),
    detail: short(String(detail || ''), 800),
    status,
    at: Date.now(),
  });
  if (notificationHistory.length > 100) notificationHistory = notificationHistory.slice(-100);
  saveNotifications();
}

const GREEN = 0x2ecc40;
const GREY = 0x95a5a6;
const RED = 0xe74c3c;
const symbols = { gbp: '£', usd: '$', eur: '€' };
const money = (amount, cur) =>
  `${symbols[cur.toLowerCase()] ?? cur.toUpperCase() + ' '}${amount.toFixed(2)}`;
const short = (text, n) => (text && text.length > n ? text.slice(0, n - 1) + '…' : text || '—');

// Posts a copy of important events to a private server channel (via a channel webhook).
async function getLogChannel() {
  if (LOG_CHANNEL_ID) return client.channels.fetch(LOG_CHANNEL_ID).catch(() => null);
  for (const g of client.guilds.cache.values()) {
    const c = g.channels.cache.find((x) => x.name === 'bankroll-log' && x.isTextBased());
    if (c) return c;
  }
  return null;
}

async function logToServer(payload) {
  try {
    const ch = await getLogChannel();
    if (ch) {
      try {
        await ch.send({
          content: payload.content || undefined,
          embeds: payload.embeds,
          files: payload.files?.map((f) => new AttachmentBuilder(f.buffer, { name: f.name })),
          allowedMentions: { parse: [] },
        });
      } catch (e) {
        if (!payload.files?.length) throw e;
        // The file made the post fail (for example too big): log the embed without it.
        console.error('Log post with file failed, retrying without it:', e.message);
        await ch.send({
          content: payload.content || undefined,
          embeds: payload.embeds,
          allowedMentions: { parse: [] },
        });
      }
      return;
    }
  } catch (e) {
    console.error('Log channel failed:', e.message);
  }
  if (!LOG_WEBHOOK_URL) return;
  try {
    const body = {
      username: 'Bankroll Log',
      allowed_mentions: { parse: [] },
      ...(payload.content ? { content: payload.content } : {}),
      ...(payload.embeds ? { embeds: payload.embeds.map((e) => (e.toJSON ? e.toJSON() : e)) } : {}),
    };
    const res = await fetch(LOG_WEBHOOK_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    if (!res.ok) console.error('Log webhook failed:', res.status, (await res.text()).slice(0, 200));
  } catch (e) {
    console.error('Log webhook error:', e.message);
  }
}

async function notifyOwner(payload, { log = true, notificationType = 'owner.notification', notificationStatus = 'info' } = {}) {
  const firstEmbed = payload.embeds?.[0];
  recordNotification(
    notificationType,
    firstEmbed?.title || payload.content || 'Bot notification',
    firstEmbed?.description || payload.content || '',
    notificationStatus
  );
  try {
    const owner = await client.users.fetch(OWNER_ID);
    await owner.send(payload);
  } catch (e) {
    console.error('Owner DM failed:', e.message);
    recordNotification('delivery.owner_dm_failed', 'Owner DM failed', e.message, 'error');
  }
  if (log) await logToServer({ content: payload.content, embeds: payload.embeds });
}

const lastAlert = new Map();
let lastWebhookAt = null;
let lastWebhookType = '';
let lastMonzoAt = null;
let recoveryNote = '';
let recoveryInfo = null;
async function alertOwner(where, err) {
  const msg = `${where}: ${err?.message ?? err}`.slice(0, 500);
  console.error(msg);
  recordNotification('system.error', where, msg, 'error');
  if (Date.now() - (lastAlert.get(msg) ?? 0) < 60 * 1000) return;
  lastAlert.set(msg, Date.now());
  try {
    const owner = await client.users.fetch(OWNER_ID);
    await owner.send(`Bot error - ${msg}`);
  } catch {}
}
process.on('unhandledRejection', (e) => alertOwner('Unhandled error', e));
process.on('uncaughtException', (e) => alertOwner('Crash avoided', e));

// ---------- Helpers ----------

function whereFrom(interaction, customer) {
  return interaction.guild
    ? { guild: interaction.guild.name, channel: `#${interaction.channel?.name ?? 'unknown'}` }
    : { guild: 'Direct Message', channel: `DM with ${customer.username}` };
}

function londonParts(ms) {
  const f = new Intl.DateTimeFormat('en-GB', {
    timeZone: 'Europe/London',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hourCycle: 'h23',
  });
  const p = Object.fromEntries(f.formatToParts(new Date(ms)).map((x) => [x.type, x.value]));
  return { y: +p.year, m: +p.month, d: +p.day, h: +p.hour, mi: +p.minute, s: +p.second };
}
function londonOffset(ms) {
  const p = londonParts(ms);
  return Date.UTC(p.y, p.m - 1, p.d, p.h, p.mi, p.s) - Math.floor(ms / 1000) * 1000;
}
function periodStart(period) {
  const p = londonParts(Date.now());
  let d = p.d;
  if (period === 'week') {
    const dow = new Date(Date.UTC(p.y, p.m - 1, p.d)).getUTCDay();
    d -= (dow + 6) % 7;
  }
  if (period === 'month') d = 1;
  const guess = Date.UTC(p.y, p.m - 1, d);
  return Math.floor((guess - londonOffset(guess)) / 1000);
}

// ---------- File / link delivery & Vault ----------
const FILES_DIR = path.join(DATA_DIR, 'files');
const DELIVERIES_FILE = path.join(DATA_DIR, 'deliveries.json');
const MONZO_FILE = path.join(DATA_DIR, 'monzo_payments.json');

// Write to a temp file then rename, so a crash mid-write can never leave a half-written data file.
function atomicWrite(file, data) {
  const tmp = `${file}.tmp`;
  fs.writeFileSync(tmp, data);
  fs.renameSync(tmp, file);
}

fs.mkdirSync(DATA_DIR, { recursive: true });
const VAULT_DIR = path.join(DATA_DIR, 'vault');
const VAULT_FILE = path.join(DATA_DIR, 'vault.json');
fs.mkdirSync(VAULT_DIR, { recursive: true });

let vault = {};
try {
  vault = JSON.parse(fs.readFileSync(VAULT_FILE, 'utf8'));
} catch {}

const saveVault = () => {
  try {
    atomicWrite(VAULT_FILE, JSON.stringify(vault, null, 2));
  } catch (e) {
    console.error('Could not save vault:', e.message);
  }
  scheduleBackup();
};

const SEND_JOBS_FILE = path.join(DATA_DIR, 'scheduled_sends.json');
let sendJobs = {};
try { sendJobs = JSON.parse(fs.readFileSync(SEND_JOBS_FILE, 'utf8')); } catch {}
function saveSendJobs() {
  try { atomicWrite(SEND_JOBS_FILE, JSON.stringify(sendJobs, null, 2)); }
  catch (e) { console.error('Could not save scheduled sends:', e.message); }
  scheduleBackup();
}

const BASE_URL = PUBLIC_URL.replace(/\/+$/, '');
const MAX_FILE_BYTES = (BASE_URL ? 100 : 10) * 1024 * 1024;
fs.mkdirSync(FILES_DIR, { recursive: true });
const TMP_DIR = path.join(DATA_DIR, 'tmp');
fs.mkdirSync(TMP_DIR, { recursive: true });

let deliveries = {};
try {
  deliveries = JSON.parse(fs.readFileSync(DELIVERIES_FILE, 'utf8'));
} catch {}
const saveDeliveries = () => {
  try {
    atomicWrite(DELIVERIES_FILE, JSON.stringify(deliveries));
  } catch (e) {
    console.error('Could not save deliveries:', e.message);
  }
  scheduleBackup();
};

// Backups are sent a few seconds after any change, but never before the startup restore has finished.
function scheduleBackup() {
  if (!restoreDone) return;
  clearTimeout(backupTimer);
  backupTimer = setTimeout(() => backupData(), 15 * 1000);
}

// Monzo ledger: every Monzo invoice (open / paid / cancelled / expired) so history and totals survive.
let monzoLedger = {};
try {
  monzoLedger = JSON.parse(fs.readFileSync(MONZO_FILE, 'utf8'));
} catch {}
const saveMonzo = () => {
  try {
    atomicWrite(MONZO_FILE, JSON.stringify(monzoLedger));
  } catch (e) {
    console.error('Could not save Monzo ledger:', e.message);
  }
  scheduleBackup();
};
const MONZO_OPEN_MS = 7 * 24 * 60 * 60 * 1000;
const monzoOpen = () =>
  Object.values(monzoLedger).filter(
    (m) => m.status === 'open' && Date.now() < (m.expiresAt ?? m.createdAt + MONZO_OPEN_MS)
  );
const monzoPaid = () => Object.values(monzoLedger).filter((m) => m.status === 'paid');
const tsec = (ms) => Math.floor(ms / 1000);
function markMonzoPaid(key, txId, manual = false) {
  const d = deliveries[key];
  const e =
    monzoLedger[key] ??
    (monzoLedger[key] = {
      key,
      customerId: d?.customerId,
      agentId: d?.agentId,
      amount: d?.amount,
      currency: 'gbp',
      description: d?.description ?? '',
      reference: d?.reference,
      createdAt: d?.savedAt ?? Date.now(),
      channelId: d?.channelId,
    });
  e.status = 'paid';
  e.paidAt = Date.now();
  if (txId) e.txId = txId;
  if (manual) e.manual = true;
  saveMonzo();
  if (d) {
    d.paid = true;
    saveDeliveries();
  }
}

for (const [k, d] of Object.entries(deliveries)) {
  if (Date.now() - d.savedAt > 7 * 24 * 60 * 60 * 1000) {
    if (d.path) {
      try {
        fs.unlinkSync(d.path);
      } catch {}
    }
    if (d.type === 'monzo' && monzoLedger[k]?.status === 'open') {
      monzoLedger[k].status = 'expired';
      try {
        atomicWrite(MONZO_FILE, JSON.stringify(monzoLedger));
      } catch {}
    }
    delete deliveries[k];
  }
}
saveDeliveries();

const liveInteractions = new Map();

// Streams a download straight to disk (no big file held in memory), hashing and size-checking on the way.
async function streamToFile(url, dest, limitBytes) {
  let lastErr;
  for (let attempt = 1; attempt <= 2; attempt++) {
    try {
      const res = await fetch(url, { signal: AbortSignal.timeout(5 * 60 * 1000) });
      if (!res.ok || !res.body) throw new Error(`HTTP ${res.status}`);
      const hash = crypto.createHash('sha256');
      let size = 0;
      const meter = new Transform({
        transform(chunk, _enc, cb) {
          size += chunk.length;
          if (size > limitBytes) return cb(new Error('TOO_BIG'));
          hash.update(chunk);
          cb(null, chunk);
        },
      });
      await pipeline(Readable.fromWeb(res.body), meter, fs.createWriteStream(dest));
      return { size, sha256: hash.digest('hex') };
    } catch (e) {
      lastErr = e;
      try {
        fs.unlinkSync(dest);
      } catch {}
      if (e.message === 'TOO_BIG') break;
    }
  }
  throw lastErr;
}

function sha256File(fp) {
  return new Promise((resolve, reject) => {
    const h = crypto.createHash('sha256');
    fs.createReadStream(fp)
      .on('data', (c) => h.update(c))
      .on('end', () => resolve(h.digest('hex')))
      .on('error', reject);
  });
}

async function downloadAttachment(att, hosted) {
  const limitMb = hosted ? 100 : 10;
  const limit = limitMb * 1024 * 1024;
  if (att.size > limit) throw new Error(`That file is over ${limitMb} MB. ${hosted ? '' : 'Use the hosted link option for bigger files.'}`);
  const tmp = path.join(TMP_DIR, `${crypto.randomBytes(8).toString('hex')}.part`);
  try {
    const { size, sha256 } = await streamToFile(att.url, tmp, limit);
    return { path: tmp, temp: true, name: att.name, size, sha256 };
  } catch (e) {
    if (e.message === 'TOO_BIG') throw new Error(`That file is over ${limitMb} MB.`);
    console.error('Attachment download failed:', e.message);
    throw new Error('Could not download the file you attached.');
  }
}

// Old half-finished downloads are cleared out.
function sweepTmp() {
  try {
    for (const f of fs.readdirSync(TMP_DIR)) {
      const fp = path.join(TMP_DIR, f);
      try {
        if (Date.now() - fs.statSync(fp).mtimeMs > 60 * 60 * 1000) fs.unlinkSync(fp);
      } catch {}
    }
  } catch {}
}
sweepTmp();
setInterval(sweepTmp, 60 * 60 * 1000);

function saveDelivery(key, fileData, interaction, customer) {
  const safeName = fileData.name.replace(/[^\w.\-]/g, '_');
  const filePath = path.join(FILES_DIR, `${key.replace(/[^\w\-]/g, '_')}_${safeName}`);
  if (fileData.temp) {
    try {
      fs.renameSync(fileData.path, filePath);
    } catch {
      fs.copyFileSync(fileData.path, filePath);
      try {
        fs.unlinkSync(fileData.path);
      } catch {}
    }
  } else {
    // Vault file: a hard link is instant and uses no extra space (copy if links are not possible).
    try {
      fs.rmSync(filePath, { force: true });
      fs.linkSync(fileData.path, filePath);
    } catch {
      fs.copyFileSync(fileData.path, filePath);
    }
  }
  deliveries[key] = {
    path: filePath,
    name: fileData.name,
    description: interaction.options.getString('description') ?? '',
    mode: interaction.options.getString('delivery') ?? (BASE_URL ? 'link' : 'direct'),
    channelId: interaction.channelId,
    customerId: customer.id,
    agentId: interaction.user.id,
    savedAt: Date.now(),
  };
  saveDeliveries();
}

function saveLinkDelivery(key, url, interaction, customer) {
  deliveries[key] = {
    link: url,
    name: 'download link',
    channelId: interaction.channelId,
    customerId: customer.id,
    agentId: interaction.user.id,
    savedAt: Date.now(),
  };
  saveDeliveries();
}

const DOWNLOADS_FILE = path.join(DATA_DIR, 'downloads.json');
const DOWNLOAD_HISTORY_FILE = path.join(DATA_DIR, 'download_history.json');
let downloadHistory = [];
try { const h = JSON.parse(fs.readFileSync(DOWNLOAD_HISTORY_FILE, 'utf8')); if (Array.isArray(h)) downloadHistory = h.slice(-500); } catch {}
function recordDownloadEvent(type, details = {}) {
  const entry = { id: crypto.randomUUID(), type, file: short(String(details.file || 'Unknown file'), 140), customerId: details.customerId || null, agentId: details.agentId || null, token: details.token ? String(details.token).slice(0, 12) : null, detail: short(String(details.detail || ''), 300), at: Date.now() };
  downloadHistory.push(entry); if (downloadHistory.length > 500) downloadHistory = downloadHistory.slice(-500);
  try { atomicWrite(DOWNLOAD_HISTORY_FILE, JSON.stringify(downloadHistory, null, 2)); } catch (e) { console.error('Could not save download history:', e.message); }
  scheduleBackup(); return entry;
}
let downloads = {};
try {
  downloads = JSON.parse(fs.readFileSync(DOWNLOADS_FILE, 'utf8'));
} catch {}
const saveDownloads = () => {
  try {
    atomicWrite(DOWNLOADS_FILE, JSON.stringify(downloads));
  } catch (e) {
    console.error('Could not save downloads:', e.message);
  }
  scheduleBackup();
};
const activeDownloadTokens = new Set();

function cleanDownloads() {
  const now = Date.now(); let removed = 0;
  for (const [token, dl] of Object.entries(downloads)) {
    const expired = !dl || !Number.isFinite(Number(dl.expiresAt)) || now > Number(dl.expiresAt);
    const missing = !dl?.path || !fs.existsSync(dl.path);
    if ((expired || missing || dl.used) && !activeDownloadTokens.has(token)) {
      if (dl?.path) { try { fs.unlinkSync(dl.path); } catch {} }
      delete downloads[token]; removed++;
      if (expired) recordDownloadEvent('expired', { file: dl?.name, customerId: dl?.customerId, token, detail: 'Expired link/file removed by cleanup.' });
    }
  }
  try { const activePaths = new Set(Object.values(downloads).map((d) => path.resolve(String(d?.path || ''))));
    for (const name of fs.readdirSync(FILES_DIR)) { const fp = path.join(FILES_DIR,name); try { const st=fs.statSync(fp); if(st.isFile() && now-st.mtimeMs>24*60*60*1000 && /^(download_|backup_download_)/.test(name) && !activePaths.has(path.resolve(fp))) fs.unlinkSync(fp); } catch {} }
  } catch {}
  saveDownloads(); if (removed) console.log(`Download cleanup removed ${removed} expired, used, or missing link record(s).`); return removed;
}
cleanDownloads();
setInterval(cleanDownloads, 60 * 60 * 1000);

function verifyZipIntegrity(filePath) {
  if (!/\.zip$/i.test(String(filePath || ''))) return { isZip: false, entries: 0 };
  if (!filePath || !fs.existsSync(filePath)) throw new Error('ZIP file is missing.');
  const zip = new AdmZip(filePath); const entries = zip.getEntries();
  if (!entries.length) throw new Error('The ZIP archive is empty or invalid.');
  if (typeof zip.test === 'function' && zip.test() === false) throw new Error('ZIP integrity check failed (CRC/checksum error).');
  return { isZip: true, entries: entries.length };
}
function buildDownloadEmbed(name, expiresAt, sizeBytes, label = 'Your download is ready') {
  const isZip = /\.zip$/i.test(String(name || ''));
  return new EmbedBuilder().setColor(GREEN).setTitle(label)
    .setDescription(`Your complete **${short(String(name || 'file'), 180)}** is ready. ${isZip ? 'This is one original ZIP archive — no split parts or joining steps.' : 'Your original file is ready to download.'}`)
    .addFields({ name: 'File type', value: isZip ? 'ZIP archive' : 'File', inline: true }, ...(Number.isFinite(sizeBytes) ? [{ name: 'Size', value: `${(sizeBytes/(1024*1024)).toFixed(2)} MB`, inline: true }] : []), ...(expiresAt ? [{ name: 'Link expires', value: `<t:${Math.floor(expiresAt/1000)}:R>`, inline: true }] : []))
    .setFooter({ text: expiresAt ? 'Bankroll secure delivery • One-time download link' : 'Bankroll secure delivery' }).setTimestamp();
}

function makeDownloadLink(d, paymentIntentId) {
  if (!d?.path || !fs.existsSync(d.path)) throw new Error('The source file is no longer available.');
  verifyZipIntegrity(d.path);
  const token = crypto.randomBytes(24).toString('hex');
  const safeName = path.basename(String(d.name || 'download.zip')).replace(/[^\w.\-]/g, '_');
  const downloadPath = path.join(FILES_DIR, `download_${token}_${safeName}`);
  fs.copyFileSync(d.path, downloadPath);
  downloads[token] = {
    path: downloadPath,
    name: safeName,
    customerId: d.customerId || OWNER_ID,
    agentId: d.agentId || OWNER_ID,
    vaultKey: d.vaultKey ?? null,
    paymentIntentId: paymentIntentId ?? null,
    expiresAt: Date.now() + 24 * 60 * 60 * 1000,
    used: false,
  };
  saveDownloads();
  recordDownloadEvent('link_created', { file: safeName, customerId: d.customerId || OWNER_ID, agentId: d.agentId || OWNER_ID, token, detail: 'One-time link created; expires in 24 hours.' });
  const slug = (d.description || '').toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-+|-+$/g, '').slice(0, 40).replace(/-+$/, '') || 'order';
  return `${BASE_URL}/bankroll-${slug}-download/${token}`;
}

function makeBackupDownloadLink(buffer, name) {
  if (!BASE_URL) return null;
  const token = crypto.randomBytes(24).toString('hex');
  const safeName = path.basename(String(name || 'bankroll-backup.zip')).replace(/[^\w.\-]/g, '_');
  const downloadPath = path.join(FILES_DIR, `backup_download_${token}_${safeName}`);
  fs.writeFileSync(downloadPath, buffer);
  downloads[token] = {
    path: downloadPath,
    name: safeName,
    customerId: OWNER_ID,
    agentId: OWNER_ID,
    vaultKey: null,
    paymentIntentId: null,
    expiresAt: Date.now() + 24 * 60 * 60 * 1000,
    used: false,
    backupDownload: true,
  };
  saveDownloads();
  return `${BASE_URL}/bankroll-backup-download/${token}`;
}

function downloadLinkRow(url, label = 'Download ZIP') {
  return new ActionRowBuilder().addComponents(
    new ButtonBuilder().setLabel(label).setStyle(ButtonStyle.Link).setURL(url)
  );
}

function findBackupChannel() {
  for (const g of client.guilds.cache.values()) {
    const c = g.channels.cache.find((x) => x.name === 'bankroll-backup' && x.isTextBased());
    if (c) return c;
  }
  return null;
}

let lastBackupHash = '';
let lastBackupAt = null;
let lastJsonMsg = null;

// Every stored file (vault, files waiting for payment, hosted downloads), as paths under DATA_DIR.

// Files too big for Discord are hosted here as one whole file (7 days) and linked from the log.
const LOGCOPY_DIR = path.join(DATA_DIR, 'logcopies');
fs.mkdirSync(LOGCOPY_DIR, { recursive: true });
for (const f of fs.readdirSync(LOGCOPY_DIR)) { try { fs.unlinkSync(path.join(LOGCOPY_DIR, f)); } catch {} }
const logCopies = {};
setInterval(() => {
  for (const [t, c] of Object.entries(logCopies)) {
    if (Date.now() > c.expiresAt) {
      try { fs.unlinkSync(c.path); } catch {}
      delete logCopies[t];
    }
  }
}, 60 * 60 * 1000);

// Logs an invoice embed with the whole file attached (same as the old version did).
// Reads the vault file straight from the vault, and if Discord refuses it, says why in the log.
async function logInvoiceWithFile(embed, fileData, filePath) {
  const ch = await getLogChannel();
  const p = [fileData?.path, filePath].find((x) => x && fs.existsSync(x));
  if (!ch || !p) {
    await logToServer({ embeds: [embed] });
    return;
  }
  const buffer = await fs.promises.readFile(p);
  try {
    await ch.send({
      embeds: [embed],
      files: [new AttachmentBuilder(buffer, { name: fileData.name })],
      allowedMentions: { parse: [] },
    });
  } catch (e) {
    console.error('Log post with file failed:', e.message);
    await ch.send({ embeds: [embed], allowedMentions: { parse: [] } }).catch(() => {});
    const mb = (buffer.length / (1024 * 1024)).toFixed(2);
    if (BASE_URL) {
      try {
        const token = crypto.randomBytes(24).toString('hex');
        const dest = path.join(LOGCOPY_DIR, `${token}_${String(fileData.name).replace(/[^\w.\-]/g, '_')}`);
        try { fs.linkSync(p, dest); } catch { fs.copyFileSync(p, dest); }
        logCopies[token] = { path: dest, name: fileData.name, expiresAt: Date.now() + 7 * 24 * 60 * 60 * 1000 };
        await ch.send({
          content: `**${fileData.name}** (${mb} MB) is too big to attach here. Download the whole file (valid 7 days): ${BASE_URL}/bankroll-log-copy/${token}`,
          allowedMentions: { parse: [] },
        });
        return;
      } catch (e2) {
        console.error('Log copy link failed:', e2.message);
      }
    }
    await ch
      .send({
        content: `Could not attach **${fileData.name}** (${mb} MB) to the log. Discord said: ${e.message}`,
        allowedMentions: { parse: [] },
      })
      .catch(() => {});
  }
}
const blobBackedUp = {}; // relative path -> size already in the backup channel
function relData(p) {
  if (!p) return null;
  const rel = path.relative(DATA_DIR, path.resolve(String(p)));
  return rel && !rel.startsWith('..') && !path.isAbsolute(rel) ? rel.split(path.sep).join('/') : null;
}
// A fixed (not secret-guessable) link to one stored file, used when a backup is too big for Discord.
function backupToken(rel) {
  return crypto.createHash('sha256').update(`${DISCORD_TOKEN}|backup|${rel}`).digest('hex').slice(0, 40);
}
function dataFilePaths() {
  const set = new Set();
  for (const v of Object.values(vault)) if (v?.path) set.add(v.path);
  for (const d of [...Object.values(deliveries), ...Object.values(downloads)]) {
    if (!d?.path || d.backupDownload) continue;
    if (d.vaultKey && vault[d.vaultKey]) continue; // a link to a vault file: backed up once, re-linked on restore
    set.add(d.path);
  }
  return [...set].filter((fp) => relData(fp));
}

let backupChain = Promise.resolve();
function backupData() {
  backupChain = backupChain.then(() => backupDataOnce()).catch((e) => console.error('Backup failed:', e.message));
  return backupChain;
}
async function resolveBackupChannel() {
  if (BACKUP_CHANNEL_ID) {
    const byId = await client.channels.fetch(BACKUP_CHANNEL_ID).catch((e) => {
      console.error('Could not fetch BACKUP_CHANNEL_ID:', e.message);
      return null;
    });
    if (byId?.isTextBased?.() && byId.messages?.fetch) return byId;
  }
  for (const guild of client.guilds.cache.values()) {
    let found = guild.channels.cache.find((c) => c.name?.toLowerCase() === 'bankroll-backup' && c.isTextBased?.() && c.messages?.fetch);
    if (!found) {
      const fetched = await guild.channels.fetch().catch(() => null);
      found = fetched?.find((c) => c?.name?.toLowerCase() === 'bankroll-backup' && c.isTextBased?.() && c.messages?.fetch);
    }
    if (found) return found;
  }
  return null;
}

function safeBackupRel(rel) {
  if (typeof rel !== 'string' || !rel.trim()) return null;
  const clean = rel.replace(/\\/g, '/').replace(/^\/+/, '');
  const resolved = path.resolve(DATA_DIR, clean);
  const root = path.resolve(DATA_DIR) + path.sep;
  return resolved.startsWith(root) ? clean : null;
}

async function backupDataOnce() {
  if (!restoreDone) return;
  const ch = await resolveBackupChannel();
  if (!ch) {
    console.error('Backup channel not found. Set BACKUP_CHANNEL_ID or create #bankroll-backup.');
    return;
  }

  try {
    const jsonNames = ['deliveries.json', 'downloads.json', 'download_history.json', 'vault.json', 'monzo_payments.json', 'notifications.json', 'scheduled_sends.json'];
    const files = [];
    for (const name of jsonNames) {
      const fp = path.join(DATA_DIR, name);
      try {
        let buffer = fs.readFileSync(fp);
        // Temporary one-time backup-download tokens are persisted for live links,
        // but excluded from snapshots so every backup does not trigger another backup.
        if (name === 'downloads.json') {
          try {
            const saved = JSON.parse(buffer.toString('utf8'));
            for (const [token, item] of Object.entries(saved)) if (item?.backupDownload) delete saved[token];
            buffer = Buffer.from(JSON.stringify(saved));
          } catch {}
        }
        if (buffer.length) files.push({ name, buffer });
      } catch {}
    }

    if (files.length) {
      const hash = crypto.createHash('sha256').update(Buffer.concat(files.map((f) => f.buffer))).digest('hex');
      if (hash !== lastBackupHash) {
        const manifest = {
          format: 'bankroll-backup-v2',
          createdAt: new Date().toISOString(),
          files: files.map((f) => ({ name: f.name, size: f.buffer.length, sha256: crypto.createHash('sha256').update(f.buffer).digest('hex') })),
        };
        const manifestBuffer = Buffer.from(JSON.stringify(manifest, null, 2));
        const backupZip = new AdmZip();
        for (const f of files) backupZip.addFile(f.name, f.buffer);
        backupZip.addFile('bankroll-backup-manifest.json', manifestBuffer);
        const backupZipBuffer = backupZip.toBuffer();
        const backupLink = makeBackupDownloadLink(backupZipBuffer, 'bankroll-database-backup.zip');
        const attachments = [
          ...files.map((f) => new AttachmentBuilder(f.buffer, { name: f.name })),
          new AttachmentBuilder(manifestBuffer, { name: 'bankroll-backup-manifest.json' }),
          new AttachmentBuilder(backupZipBuffer, { name: 'bankroll-database-backup.zip' }),
        ];
        await ch.send({
          content: `BANKROLL_BACKUP_SNAPSHOT v2 | ${new Date().toISOString()} | ${files.length} database files | complete ZIP archive attached`,
          files: attachments,
          ...(backupLink ? { components: [downloadLinkRow(backupLink, 'Download ZIP')] } : {}),
          allowedMentions: { parse: [] },
        });
        lastBackupHash = hash;
        lastBackupAt = Date.now();
      }
    }

    for (const fp of dataFilePaths()) {
      const rel = safeBackupRel(relData(fp));
      if (!rel) continue;
      let st;
      try { st = fs.statSync(fp); } catch { continue; }
      if (!st.isFile() || !st.size) continue;
      const sha256 = await sha256File(fp);
      const signature = `${st.size}:${sha256}`;
      if (blobBackedUp[rel] === signature) continue;

      let zipBuffer = null;
      let backupLink = null;
      const marker = { rel, size: st.size, sha256, entryName: path.basename(fp), originalName: path.basename(fp) };
      try {
        const zip = new AdmZip();
        zip.addFile(path.basename(fp), fs.readFileSync(fp));
        zipBuffer = zip.toBuffer();
        backupLink = makeBackupDownloadLink(zipBuffer, `${path.basename(fp)}.backup.zip`);
        await ch.send({
          content: `datafilezip-v1:${JSON.stringify(marker)}`,
          files: [new AttachmentBuilder(zipBuffer, { name: `${path.basename(fp)}.backup.zip` })],
          ...(backupLink ? { components: [downloadLinkRow(backupLink, 'Download ZIP')] } : {}),
          allowedMentions: { parse: [] },
        });
        blobBackedUp[rel] = signature;
        lastBackupAt = Date.now();
        continue;
      } catch (zipError) {
        console.error(`ZIP backup upload failed for ${rel}:`, zipError.message);
      }

      if (BASE_URL) {
        try {
          const recoveryUrl = `${BASE_URL}/bankroll-backup-file/${backupToken(rel)}`;
          backupLink = backupLink || (zipBuffer ? makeBackupDownloadLink(zipBuffer, `${path.basename(fp)}.backup.zip`) : null);
          await ch.send({
            content: `datafilelink:${rel}|${st.size}|${recoveryUrl}`,
            ...(backupLink ? { components: [downloadLinkRow(backupLink, 'Download ZIP')] } : {}),
            allowedMentions: { parse: [] },
          });
          blobBackedUp[rel] = signature;
          lastBackupAt = Date.now();
          console.warn(`Stored hosted recovery fallback for ${rel}. The Download ZIP button is a separate one-time convenience link.`);
          continue;
        } catch (linkError) {
          console.error(`Hosted-link backup failed for ${rel}:`, linkError.message);
        }
      }
      scheduleBackup();
      break;
    }
  } catch (e) {
    console.error('Backup failed:', e.message);
  }
}

function discardPending(key) {
  const d = deliveries[key];
  if (!d) return false;
  if (d.path) {
    try {
      fs.unlinkSync(d.path);
    } catch {}
  }
  delete deliveries[key];
  saveDeliveries();
  liveInteractions.delete(key);
  return true;
}

// Finds the bot's invoice message in a channel by the "ID: ..." in its footer, so invoices made
// before message links were saved (or before a restart) can still be linked to and replied under.
const invoiceMsgCache = new Map();
async function findInvoiceMessage(channelId, id) {
  if (!channelId || !id) return null;
  const cached = invoiceMsgCache.get(id);
  try {
    const ch = await client.channels.fetch(channelId).catch(() => null);
    if (!ch || !ch.messages) return null;
    if (cached) {
      const m = await ch.messages.fetch(cached).catch(() => null);
      if (m) return m;
    }
    let before;
    for (let i = 0; i < 5; i++) {
      const batch = await ch.messages.fetch({ limit: 100, ...(before ? { before } : {}) });
      if (!batch.size) break;
      const hit = batch.find(
        (m) => m.author.id === client.user.id && m.embeds.some((e) => e.footer?.text?.includes(`ID: ${id}`))
      );
      if (hit) {
        invoiceMsgCache.set(id, hit.id);
        return hit;
      }
      before = batch.last().id;
      if (batch.size < 100) break;
    }
  } catch (e) {
    console.log('Invoice message lookup failed:', e.message);
  }
  return null;
}

async function getInvoiceMessage(d, key) {
  try {
    if (d.msgId) {
      const ch = await client.channels.fetch(d.msgChannelId || d.channelId).catch(() => null);
      const m = ch ? await ch.messages.fetch(d.msgId).catch(() => null) : null;
      if (m) return m;
    }
  } catch {}
  return findInvoiceMessage(d.channelId, key);
}

async function deliverFile(key, paymentIntentId) {
  const d = deliveries[key];
  if (!d) return;

  let owner = null;
  try { owner = await client.users.fetch(OWNER_ID); } catch {}

  const noFile = !d.link && !d.path;
  if(d.path && fs.existsSync(d.path)){try{verifyZipIntegrity(d.path);}catch(e){recordDownloadEvent('verification_failed',{file:d.name,customerId:d.customerId,agentId:d.agentId,detail:e.message});if(owner)await owner.send(`Download blocked: **${d.name}** failed ZIP integrity verification. The paid file was not sent. ${short(e.message,180)}`).catch(()=>{});return;}}
  let dlink = d.link || null;
  let hosted = false;

  if (d.fallbackLink) {
    const token = d.fallbackLink.split('/').pop();
    const savedLink = downloads[token];
    if (savedLink && !savedLink.used && Date.now() < savedLink.expiresAt && fs.existsSync(savedLink.path)) {
      dlink = d.fallbackLink;
      hosted = true;
    } else {
      delete d.fallbackLink;
      saveDeliveries();
    }
  }

  if (!dlink && d.path && fs.existsSync(d.path) && BASE_URL) {
    const size = fs.statSync(d.path).size;
    if (d.mode === 'link' || size > MAX_FILE_BYTES) {
      dlink = makeDownloadLink(d, paymentIntentId);
      hosted = true;
      d.fallbackLink = dlink;
      saveDeliveries();
    }
  }

  if (!noFile && !dlink && (!d.path || !fs.existsSync(d.path))) {
    if (owner) await owner.send(`Payment received for ${key}, but the file "${d.name}" is gone. Please send it manually to <@${d.customerId}>.`).catch(() => {});
    delete deliveries[key];
    saveDeliveries();
    return;
  }

  const isZip = /\.zip$/i.test(String(d.name || ''));
  const payload = () => {
    if (noFile) return { content: `<@${d.customerId}> payment received. Thank you!` };
    if (dlink) {
      return {
        content: `<@${d.customerId}> payment received. Your complete ${isZip ? 'ZIP' : 'file'} is ready. The secure download link works once and expires in 24 hours.`,
        embeds: [buildDownloadEmbed(d.name, downloads[dlink.split('/').pop()]?.expiresAt, d.path && fs.existsSync(d.path) ? fs.statSync(d.path).size : undefined)],
        components: [downloadLinkRow(dlink, isZip ? 'Download ZIP' : 'Download File')],
        allowedMentions: { parse: [] },
      };
    }
    return {
      content: `<@${d.customerId}> payment received. Here is your complete file.`,
      embeds: [buildDownloadEmbed(d.name, null, d.path && fs.existsSync(d.path) ? fs.statSync(d.path).size : undefined)],
      files: [new AttachmentBuilder(d.path, { name: path.basename(d.name || 'download.zip') })],
      allowedMentions: { parse: [] },
    };
  };

  const sendWithFallback = async (send) => {
    try {
      return await send(payload());
    } catch (attachmentError) {
      if (noFile || dlink || !BASE_URL || !d.path || !fs.existsSync(d.path)) throw attachmentError;
      dlink = makeDownloadLink(d, paymentIntentId);
      hosted = true;
      d.fallbackLink = dlink;
      saveDeliveries();
      console.warn(`Discord attachment failed for ${d.name}; retrying with one hosted ZIP/file link: ${attachmentError.message}`);
      return send(payload());
    }
  };

  let sentTo = null;
  let msgUrl = null;
  let reachedCustomer = false;
  const live = liveInteractions.get(key);
  if (live && Date.now() - live.at < 14 * 60 * 1000) {
    try {
      const original = await live.interaction.fetchReply();
      const msg = await sendWithFallback((p) => live.interaction.followUp(p));
      msgUrl = msg.url;
      reachedCustomer = true;
      sentTo = 'a reply under the invoice message';
      const old = original.embeds[0];
      if (old) {
        const paidEmbed = EmbedBuilder.from(old).setColor(GREY).setTitle(String(old.title || '').startsWith('PAID') ? old.title : `PAID - ${old.title}`);
        if (d.type === 'monzo') paidEmbed.setDescription('Payment received. Thank you!');
        await live.interaction.editReply({ embeds: [paidEmbed], components: [] });
      }
    } catch (e) { console.log('Could not reply under invoice:', e.message); }
  }
  liveInteractions.delete(key);

  if (!sentTo) {
    try {
      const im = await getInvoiceMessage(d, key);
      if (im) {
        const msg = await sendWithFallback((p) => im.reply(p));
        msgUrl = msg.url;
        reachedCustomer = true;
        sentTo = 'a reply under the invoice message';
        const old = im.embeds[0];
        if (old) {
          const paidEmbed = EmbedBuilder.from(old).setColor(GREY).setTitle(String(old.title || '').startsWith('PAID') ? old.title : `PAID - ${old.title}`);
          if (d.type === 'monzo') paidEmbed.setDescription('Payment received. Thank you!');
          await im.edit({ embeds: [paidEmbed], components: [] });
        }
      }
    } catch (e) { console.log('Could not reply to the stored invoice message:', e.message); }
  }

  if (!sentTo) {
    try {
      const ch = await client.channels.fetch(d.channelId);
      const msg = await sendWithFallback((p) => ch.send(p));
      msgUrl = msg.url;
      reachedCustomer = true;
      sentTo = 'the channel where the invoice was created';
    } catch (e) { console.log('Could not post in original channel:', e.message); }
  }

  if (!sentTo && owner) {
    try {
      await sendWithFallback((p) => owner.send(p));
      sentTo = 'your DMs';
    } catch (e) { console.error('Could not deliver file anywhere:', e.message); }
  }

  if (sentTo) {
    if (!reachedCustomer) {
      d.paid = true;
      saveDeliveries();
    } else {
      if (d.path && !d.vaultKey) { try { fs.unlinkSync(d.path); } catch {} }
      delete deliveries[key];
      saveDeliveries();
    }
  }

  if (reachedCustomer) {
    recordDownloadEvent('delivery_sent', { file: d.name, customerId: d.customerId, agentId: d.agentId, detail: `Payment delivery message sent via ${sentTo}.` });
    if (paymentIntentId) {
      await recordInStripe(paymentIntentId, {
        delivered_at: new Date().toISOString(),
        delivered_to: sentTo.slice(0, 200),
        delivered_file: String(d.name || '').slice(0, 100),
        delivery_message: msgUrl || '',
      });
    }
    await notifyOwner({ content: `"${d.name}" was sent to <@${d.customerId}>.` });
  } else if (!sentTo && owner) {
    await owner.send(`I could not deliver "${d.name}" to <@${d.customerId}>. The paid delivery is still saved; they can use the Download ZIP button on their invoice.`).catch(() => {});
  }
}

// ---------- Slash commands ----------
const common = (b) =>
  b
    .setIntegrationTypes([
      ApplicationIntegrationType.GuildInstall,
      ApplicationIntegrationType.UserInstall,
    ])
    .setContexts([
      InteractionContextType.Guild,
      InteractionContextType.BotDM,
      InteractionContextType.PrivateChannel,
    ]);

const bankrollCmd = common(
  new SlashCommandBuilder()
    .setName('bankroll')
    .setDescription('Create a payment link / invoice for a customer')
    .addUserOption((o) => o.setName('customer').setDescription('Who is paying').setRequired(true))
    .addNumberOption((o) =>
      o.setName('amount').setDescription('Amount to charge').setMinValue(1).setRequired(true)
    )
    .addStringOption((o) =>
      o.setName('description').setDescription('What the purchase is for').setRequired(true)
    )
    .addAttachmentOption((o) =>
      o.setName('file').setDescription('File to send the customer once they have paid')
    )
    .addStringOption((o) =>
      o.setName('vault_file').setDescription('Or pick a pre-saved file from your vault').setAutocomplete(true)
    )
    .addStringOption((o) =>
      o
        .setName('delivery')
        .setDescription('How to deliver the file (only used with a file)')
        .addChoices(
          { name: 'Hosted one-time link', value: 'link' },
          { name: 'Direct file upload', value: 'direct' }
        )
    )
    .addStringOption((o) =>
      o
        .setName('currency')
        .setDescription('Currency for this invoice (default: your usual one)')
        .addChoices(
          { name: 'GBP £', value: 'gbp' },
          { name: 'USD $', value: 'usd' },
          { name: 'EUR €', value: 'eur' }
        )
    )
    .addStringOption((o) =>
      o.setName('link').setDescription('Download link to send once they have paid (for big files)')
    )
    .addStringOption((o) =>
      o
        .setName('expires')
        .setDescription('Expiration time, e.g. 30m, 2h, 24h, or 7d (default: no expiry)')
    )
);

const monzoCmd = common(
  new SlashCommandBuilder()
    .setName('bankroll-monzo')
    .setDescription('Create a Monzo (bank transfer) invoice for a customer')
    .addUserOption((o) => o.setName('customer').setDescription('Who is paying').setRequired(true))
    .addNumberOption((o) =>
      o.setName('amount').setDescription('Amount to charge in GBP').setMinValue(1).setRequired(true)
    )
    .addStringOption((o) =>
      o.setName('description').setDescription('What the purchase is for').setRequired(true)
    )
    .addAttachmentOption((o) =>
      o.setName('file').setDescription('File to send the customer once they have paid')
    )
    .addStringOption((o) =>
      o.setName('vault_file').setDescription('Or pick a pre-saved file from your vault').setAutocomplete(true)
    )
    .addStringOption((o) =>
      o
        .setName('delivery')
        .setDescription('How to deliver the file (only used with a file)')
        .addChoices(
          { name: 'Hosted one-time link', value: 'link' },
          { name: 'Direct file upload', value: 'direct' }
        )
    )
    .addStringOption((o) =>
      o.setName('link').setDescription('Download link to send once they have paid (for big files)')
    )
);

const vaultCmd = common(
  new SlashCommandBuilder()
    .setName('vault')
    .setDescription('Manage your persistent file vault')
    .addSubcommand((sub) =>
      sub
        .setName('add')
        .setDescription('Upload a file to the vault')
        .addAttachmentOption((o) => o.setName('file').setDescription('The file to store').setRequired(true))
        .addStringOption((o) => o.setName('name').setDescription('Short key/name for autocomplete (e.g. car-pack-v2)').setRequired(true))
    )
    .addSubcommand((sub) => sub.setName('list').setDescription('List all files currently in your vault'))
    .addSubcommand((sub) =>
      sub.setName('search')
        .setDescription('Search vault keys and filenames')
        .addStringOption((o) => o.setName('query').setDescription('Text to search for').setRequired(true))
    )
    .addSubcommand((sub) =>
      sub.setName('verify')
        .setDescription('Check that a vault file exists and its checksum is valid')
        .addStringOption((o) => o.setName('name').setDescription('The vault key').setAutocomplete(true).setRequired(true))
    )
    .addSubcommand((sub) =>
      sub.setName('rename')
        .setDescription('Rename a vault key without changing the file contents')
        .addStringOption((o) => o.setName('name').setDescription('Current vault key').setAutocomplete(true).setRequired(true))
        .addStringOption((o) => o.setName('new_name').setDescription('New vault key').setRequired(true).setMaxLength(60))
    )
    .addSubcommand((sub) =>
      sub
        .setName('info')
        .setDescription('View detailed info about a vault item')
        .addStringOption((o) => o.setName('name').setDescription('The vault key').setAutocomplete(true).setRequired(true))
    )
    .addSubcommand((sub) =>
      sub
        .setName('remove')
        .setDescription('Remove a file from the vault')
        .addStringOption((o) => o.setName('name').setDescription('The vault key to remove').setAutocomplete(true).setRequired(true))
    )
    .addSubcommand((sub) =>
      sub
        .setName('edit')
        .setDescription('Change the spawn code of a FiveM vehicle zip in the vault (saved as a new vault item)')
        .addStringOption((o) => o.setName('name').setDescription('The vault zip to edit').setAutocomplete(true).setRequired(true))
        .addStringOption((o) =>
          o.setName('spawncode').setDescription('New spawn code (letters, numbers, underscore, max 32)').setRequired(true).setMaxLength(32)
        )
        .addStringOption((o) =>
          o.setName('old_spawncode').setDescription('Current spawn code (only needed if the zip has more than one vehicle)').setMaxLength(32)
        )
        .addStringOption((o) =>
          o.setName('save_as').setDescription('Vault key for the edited copy (default: <name>-<spawncode>)').setMaxLength(60)
        )
    )
);

const sendfileCmd = new SlashCommandBuilder()
  .setName('sendfile')
  .setDescription('Schedule a vault ZIP for delivery in this private DM')
  .setIntegrationTypes([ApplicationIntegrationType.UserInstall])
  .setContexts([InteractionContextType.PrivateChannel])
  .addIntegerOption((o) => o.setName('size').setDescription('How long to wait before sending').setMinValue(1).setMaxValue(720).setRequired(true))
  .addStringOption((o) => o.setName('disk').setDescription('M = minutes, H = hours').setRequired(true)
    .addChoices({ name: 'M', value: 'm' }, { name: 'H', value: 'h' }))
  .addStringOption((o) => o.setName('file').setDescription('Vault ZIP to send (default: newest ZIP in your vault)').setAutocomplete(true));

const cancelCmd = common(
  new SlashCommandBuilder()
    .setName('cancel')
    .setDescription('Cancel and expire an open invoice or checkout session')
    .addStringOption((o) =>
      o
        .setName('order')
        .setDescription('Pick an open invoice/checkout, or paste a Checkout ID (cs_...)')
        .setAutocomplete(true)
        .setRequired(true)
    )
);

const remindCmd = common(
  new SlashCommandBuilder()
    .setName('remind')
    .setDescription("Send a reminder for a customer's unpaid invoice")
    .addUserOption((o) => o.setName('customer').setDescription('Who to remind').setRequired(true))
);

const unpaidCmd = common(
  new SlashCommandBuilder().setName('unpaid').setDescription('List invoices that have not been paid yet')
);

const statusCmd = common(
  new SlashCommandBuilder().setName('status').setDescription('Check the bot is healthy (owner only)')
);

const restoreCmd = common(
  new SlashCommandBuilder()
    .setName('restore')
    .setDescription('Reload a data backup file (owner only)')
    .addAttachmentOption((o) =>
      o
        .setName('file')
        .setDescription('deliveries.json, downloads.json, download_history.json, vault.json, monzo_payments.json or notifications.json')
        .setRequired(true)
    )
);

const notificationsCmd = common(
  new SlashCommandBuilder()
    .setName('notifications')
    .setDescription('View and test saved bot notifications')
    .addSubcommand((sub) =>
      sub.setName('recent')
        .setDescription('Show recent bot notifications')
        .addIntegerOption((o) => o.setName('count').setDescription('How many to show (default 10)').setMinValue(1).setMaxValue(20))
    )
    .addSubcommand((sub) => sub.setName('test').setDescription('Send a test notification to the owner'))
);

const currencyCmd = common(
  new SlashCommandBuilder()
    .setName('currency')
    .setDescription('Convert an amount from one currency to another')
    .addNumberOption((o) => o.setName('amount').setDescription('How much').setMinValue(0.01).setRequired(true))
    .addStringOption((o) =>
      o.setName('from').setDescription('Currency you have, e.g. USD').setRequired(true).setMinLength(3).setMaxLength(3)
    )
    .addStringOption((o) =>
      o.setName('to').setDescription('Currency you want, e.g. GBP').setRequired(true).setMinLength(3).setMaxLength(3)
    )
);

const paymentsCmd = common(
  new SlashCommandBuilder()
    .setName('payments')
    .setDescription('List your most recent card payments')
    .addIntegerOption((o) =>
      o
        .setName('count')
        .setDescription('How many (default 5)')
        .addChoices({ name: '5', value: 5 }, { name: '10', value: 10 })
    )
);

const totalCmd = common(
  new SlashCommandBuilder()
    .setName('total')
    .setDescription('Show your card takings')
    .addStringOption((o) =>
      o
        .setName('period')
        .setDescription('Which period')
        .setRequired(true)
        .addChoices(
          { name: 'Today', value: 'today' },
          { name: 'This week', value: 'week' },
          { name: 'This month', value: 'month' }
        )
    )
);

const customerCmd = common(
  new SlashCommandBuilder()
    .setName('customer')
    .setDescription('Show everything a person has paid you')
    .addUserOption((o) => o.setName('user').setDescription('The customer').setRequired(true))
);

const historyCmd = common(
  new SlashCommandBuilder()
    .setName('history')
    .setDescription('Show all links made for a person, paid or not')
    .addUserOption((o) => o.setName('user').setDescription('The customer').setRequired(true))
);

const refundCmd = common(
  new SlashCommandBuilder()
    .setName('refund')
    .setDescription('Refund a card payment (owner only)')
    .addStringOption((o) =>
      o
        .setName('order')
        .setDescription('Pick a recent payment, or paste a Checkout ID (cs_...) or payment ID (pi_...)')
        .setAutocomplete(true)
        .setRequired(true)
    )
    .addNumberOption((o) =>
      o
        .setName('amount')
        .setDescription('Amount to refund (leave empty to refund everything left)')
        .setMinValue(0.01)
    )
    .addStringOption((o) =>
      o
        .setName('reason')
        .setDescription('Reason (default: requested by customer)')
        .addChoices(
          { name: 'Requested by customer', value: 'r' },
          { name: 'Duplicate', value: 'd' },
          { name: 'Fraudulent', value: 'f' }
        )
    )
);

const setupCmd = new SlashCommandBuilder()
  .setName('setup')
  .setDescription('Create the private #bankroll-log channel in this server (owner only)')
  .setIntegrationTypes([ApplicationIntegrationType.GuildInstall])
  .setContexts([InteractionContextType.Guild]);

const downloadsCmd = common(new SlashCommandBuilder().setName('downloads').setDescription('Manage secure downloads, history and backups (admin only)')
  .addSubcommand((sub) => sub.setName('history').setDescription('View recent download activity').addIntegerOption((o) => o.setName('count').setDescription('Number of events to show').setMinValue(1).setMaxValue(25)))
  .addSubcommand((sub) => sub.setName('cleanup').setDescription('Remove expired and used download files/links'))
  .addSubcommand((sub) => sub.setName('regenerate').setDescription('Create a fresh secure link for a vault file').addStringOption((o) => o.setName('vault_key').setDescription('Exact vault key').setRequired(true)))
  .addSubcommand((sub) => sub.setName('backup').setDescription('Run an immediate data and file backup'))
  .addSubcommand((sub) => sub.setName('verify').setDescription('Verify a vault ZIP archive and checksum').addStringOption((o) => o.setName('vault_key').setDescription('Exact vault key').setRequired(true))));

const commands = [bankrollCmd, monzoCmd, vaultCmd, sendfileCmd, downloadsCmd, cancelCmd, remindCmd, unpaidCmd, statusCmd, restoreCmd, notificationsCmd, currencyCmd, paymentsCmd, totalCmd, customerCmd, historyCmd, refundCmd, setupCmd];

let started = false;
async function autoRestoreOnStartup() {
  let ch = null;
  let msgs = [];
  let exhausted = false;
  try {
    ch = await resolveBackupChannel();
    if (!ch) {
      recoveryNote = ' Backup channel #bankroll-backup was not found. Set BACKUP_CHANNEL_ID or create the channel and grant the bot View Channel + Read Message History.';
      console.error(recoveryNote);
      return;
    }

    const me = ch.guild?.members?.me || (ch.guild ? await ch.guild.members.fetchMe().catch(() => null) : null);
    const perms = me && ch.permissionsFor(me);
    if (perms && (!perms.has(PermissionFlagsBits.ViewChannel) || !perms.has(PermissionFlagsBits.ReadMessageHistory))) {
      recoveryNote = ' Bot cannot read the full #bankroll-backup history. Grant View Channel and Read Message History.';
      console.error(recoveryNote);
      return;
    }

    // Read the ENTIRE channel history, not an arbitrary recent-message window.
    let before;
    let pages = 0;
    while (!exhausted) {
      const batch = await ch.messages.fetch({ limit: 100, ...(before ? { before } : {}) });
      if (!batch.size) { exhausted = true; break; }
      msgs.push(...batch.values());
      before = batch.last().id;
      pages += 1;
      if (pages % 10 === 0) console.log(`Backup restore scan: ${msgs.length} messages checked...`);
      if (batch.size < 100) exhausted = true;
    }
    console.log(`Backup restore scan complete: ${msgs.length} messages in #${ch.name}.`);

    // The bot's own backups and owner-uploaded backup files are trusted sources.
    const trusted = (msg) => msg.author?.id === client.user.id || (OWNER_ID && msg.author?.id === OWNER_ID);
    const targets = {
      'deliveries.json': DELIVERIES_FILE,
      'downloads.json': DOWNLOADS_FILE,
      'download_history.json': DOWNLOAD_HISTORY_FILE,
      'vault.json': VAULT_FILE,
      'monzo_payments.json': MONZO_FILE,
      'notifications.json': NOTIFICATIONS_FILE,
      'scheduled_sends.json': SEND_JOBS_FILE,
    };
    const isValidJsonFor = (name, value) => {
      if (name === 'notifications.json' || name === 'download_history.json') return Array.isArray(value);
      return !!value && typeof value === 'object' && !Array.isArray(value);
    };
    const localNeedsRestore = (name, fp) => {
      try {
        const value = JSON.parse(fs.readFileSync(fp, 'utf8'));
        if (!isValidJsonFor(name, value)) return true;
        return Array.isArray(value) ? value.length === 0 : Object.keys(value).length === 0;
      } catch { return true; }
    };

    // Newest-first scan means the first valid attachment for each database is the newest snapshot.
    const jsonBackups = new Map();
    for (const msg of msgs) {
      if (!trusted(msg)) continue;
      for (const att of msg.attachments.values()) {
        const name = String(att.name || '').toLowerCase();
        if (!targets[name] || jsonBackups.has(name)) continue;
        try {
          if (att.size > 15 * 1024 * 1024) continue;
          const res = await fetch(att.url);
          if (!res.ok) throw new Error(`HTTP ${res.status}`);
          const raw = await res.text();
          const parsed = JSON.parse(raw);
          if (!isValidJsonFor(name, parsed)) throw new Error('unexpected JSON structure');
          jsonBackups.set(name, { raw, createdTimestamp: Number(msg.createdTimestamp || 0) });
        } catch (e) {
          console.error(`Invalid backup attachment ${name}:`, e.message);
        }
      }
    }

    let jsonRestored = 0;
    for (const [name, target] of Object.entries(targets)) {
      if (!localNeedsRestore(name, target)) continue;
      const backup = jsonBackups.get(name);
      if (!backup) continue;
      let shouldImport = localNeedsRestore(name, target);
      if (!shouldImport && backup.createdTimestamp) {
        try {
          // A newer channel snapshot wins over an older on-disk copy; a newer local file is preserved.
          shouldImport = fs.statSync(target).mtimeMs + 1000 < backup.createdTimestamp;
        } catch { shouldImport = true; }
      }
      if (!shouldImport) continue;
      try {
        fs.mkdirSync(path.dirname(target), { recursive: true });
        atomicWrite(target, backup.raw);
        jsonRestored += 1;
        console.log(`Auto-recovered ${name} from #${ch.name}.`);
      } catch (e) {
        console.error(`Failed to write restored ${name}:`, e.message);
      }
    }

    // Reload database objects after importing the newest valid snapshots.
    try { deliveries = JSON.parse(fs.readFileSync(DELIVERIES_FILE, 'utf8')); } catch {}
    try { downloads = JSON.parse(fs.readFileSync(DOWNLOADS_FILE, 'utf8')); } catch {}
    try { const h = JSON.parse(fs.readFileSync(DOWNLOAD_HISTORY_FILE, 'utf8')); if (Array.isArray(h)) downloadHistory = h.slice(-500); } catch {}
    try { vault = JSON.parse(fs.readFileSync(VAULT_FILE, 'utf8')); } catch {}
    try { monzoLedger = JSON.parse(fs.readFileSync(MONZO_FILE, 'utf8')); } catch {}
    try {
      const restoredNotifications = JSON.parse(fs.readFileSync(NOTIFICATIONS_FILE, 'utf8'));
      if (Array.isArray(restoredNotifications)) notificationHistory = restoredNotifications.slice(-100);
    } catch {}
    try { sendJobs = JSON.parse(fs.readFileSync(SEND_JOBS_FILE, 'utf8')); } catch {}

    fs.mkdirSync(VAULT_DIR, { recursive: true });
    fs.mkdirSync(FILES_DIR, { recursive: true });

    // Rebase absolute paths saved by older Render instances into this instance's data directory.
    const vaultRoot = path.resolve(VAULT_DIR) + path.sep;
    const filesRoot = path.resolve(FILES_DIR) + path.sep;
    for (const [key, item] of Object.entries(vault)) {
      if (!item || typeof item !== 'object') continue;
      if (!item.path || !path.resolve(String(item.path)).startsWith(vaultRoot)) {
        item.path = path.join(VAULT_DIR, `${String(key).replace(/[^\w-]/g, '_')}_${String(item.originalName || 'file').replace(/[^\w.\-]/g, '_')}`);
      }
    }
    const rebaseFileRecords = (records) => {
      for (const item of Object.values(records || {})) {
        if (!item?.path) continue;
        if (!path.resolve(String(item.path)).startsWith(filesRoot)) {
          item.path = path.join(FILES_DIR, path.basename(String(item.path)));
        }
      }
    };
    rebaseFileRecords(deliveries);
    rebaseFileRecords(downloads);

    // Index every backed-up file part across the full channel history.
    const groups = new Map();
    const zipBackups = new Map();
    const legacyVaultAttachments = new Map();
    const backupLinks = new Map();
    const addGroupPart = (rel, size, sha, partText, att) => {
      rel = safeBackupRel(rel);
      const [part, total] = String(partText || '').split('/').map(Number);
      if (!rel || !att || !Number.isSafeInteger(size) || size < 1 || size > 150 * 1024 * 1024 || !part || !total || part > total || total > 30) return;
      const key = `${rel}|${size}|${sha || ''}|${total}`;
      if (!groups.has(key)) groups.set(key, { rel, size, sha: sha || null, total, parts: new Map(), created: 0 });
      const group = groups.get(key);
      if (!group.parts.has(part)) group.parts.set(part, att);
    };

    for (const msg of msgs) {
      if (!trusted(msg)) continue;
      const content = String(msg.content || '');
      const att = msg.attachments.first();
      if (content.startsWith('datafilezip-v1:') && att) {
        try {
          const meta = JSON.parse(content.slice('datafilezip-v1:'.length));
          const rel = safeBackupRel(meta.rel);
          const size = Number(meta.size);
          if (rel && Number.isSafeInteger(size) && size > 0 && size <= 150 * 1024 * 1024 && typeof meta.entryName === 'string' && meta.entryName === path.basename(meta.entryName)) {
            if (!zipBackups.has(rel)) zipBackups.set(rel, []);
            zipBackups.get(rel).push({ att, size, sha: String(meta.sha256 || ''), entryName: meta.entryName });
          }
        } catch (e) { console.error('Invalid ZIP backup marker:', e.message); }
      } else if (content.startsWith('datafile-v2:')) {
        const bits = content.slice('datafile-v2:'.length).split('|');
        if (bits.length >= 4) addGroupPart(bits[0], Number(bits[1]), bits[2], bits[3], att);
      } else if (content.startsWith('datafile:')) {
        const bits = content.slice('datafile:'.length).split('|');
        if (bits.length >= 3) addGroupPart(bits.slice(0, -2).join('|'), Number(bits[bits.length - 2]), null, bits[bits.length - 1], att);
      } else if (content.startsWith('datafilelink:')) {
        const bits = content.slice('datafilelink:'.length).split('|');
        if (bits.length >= 3) {
          const rel = safeBackupRel(bits.slice(0, -2).join('|'));
          const size = Number(bits[bits.length - 2]);
          const url = bits[bits.length - 1];
          if (rel && Number.isSafeInteger(size) && size > 0 && /^https?:\/\//i.test(url) && !backupLinks.has(rel)) backupLinks.set(rel, { size, url });
        }
      } else if (content.startsWith('vaultfile:') && att) {
        const key = content.slice('vaultfile:'.length).trim();
        if (key && !legacyVaultAttachments.has(key)) legacyVaultAttachments.set(key, att);
      }
    }

    const allGroups = [...groups.values()];
    const completeGroupsFor = (rel) => allGroups.filter((g) => g.rel === rel && g.parts.size === g.total);
    const downloadGroup = async (g) => {
      const chunks = [];
      for (let i = 1; i <= g.total; i++) {
        const partAtt = g.parts.get(i);
        if (!partAtt) throw new Error(`missing part ${i}/${g.total}`);
        const res = await fetch(partAtt.url);
        if (!res.ok) throw new Error(`HTTP ${res.status} downloading part ${i}`);
        chunks.push(Buffer.from(await res.arrayBuffer()));
      }
      const buf = Buffer.concat(chunks);
      if (buf.length !== g.size) throw new Error(`size mismatch (${buf.length}/${g.size})`);
      if (g.sha && crypto.createHash('sha256').update(buf).digest('hex') !== g.sha) throw new Error('SHA-256 mismatch');
      return buf;
    };
    const downloadZipBackup = async (z) => {
      const res = await fetch(z.att.url);
      if (!res.ok) throw new Error(`HTTP ${res.status} downloading ZIP backup`);
      const zipBuffer = Buffer.from(await res.arrayBuffer());
      const zip = new AdmZip(zipBuffer);
      const entry = zip.getEntry(z.entryName);
      if (!entry || entry.isDirectory) throw new Error('ZIP does not contain the expected file');
      const buf = entry.getData();
      if (buf.length !== z.size) throw new Error(`ZIP file size mismatch (${buf.length}/${z.size})`);
      if (z.sha && crypto.createHash('sha256').update(buf).digest('hex') !== z.sha) throw new Error('ZIP file SHA-256 mismatch');
      return buf;
    };

    // Recreate old vault indexes from legacy vaultfile:key posts if the JSON index is absent.
    for (const [key, att] of legacyVaultAttachments) {
      if (!vault[key]) {
        const safeKey = String(key).replace(/[^\w-]/g, '_');
        vault[key] = {
          path: path.join(VAULT_DIR, `${safeKey}_${String(att.name || 'recovered-file').replace(/[^\w.\-]/g, '_')}`),
          originalName: att.name || 'recovered-file',
          size: att.size,
          addedAt: Date.now(),
        };
      }
    }

    const wanted = dataFilePaths();
    let recovered = 0;
    let alreadyPresent = 0;
    const lost = [];
    for (const fp of wanted) {
      const rel = safeBackupRel(relData(fp));
      if (!rel) continue;
      const candidates = completeGroupsFor(rel);
      let existingOk = false;
      if (fs.existsSync(fp)) {
        try {
          const size = fs.statSync(fp).size;
          const preferred = candidates[0];
          const vaultRecord = Object.values(vault).find((item) => item?.path && path.resolve(String(item.path)) === path.resolve(fp));
          if (preferred) {
            if (size === preferred.size && (!preferred.sha || await sha256File(fp) === preferred.sha)) existingOk = true;
          } else if (vaultRecord) {
            const sizeMatches = !vaultRecord.size || size === Number(vaultRecord.size);
            const hashMatches = !vaultRecord.sha256 || await sha256File(fp) === vaultRecord.sha256;
            existingOk = sizeMatches && hashMatches;
          } else {
            existingOk = true;
          }
        } catch {}
      }
      if (existingOk) {
        alreadyPresent += 1;
        const matching = candidates.find((g) => {
          try { return fs.statSync(fp).size === g.size; } catch { return false; }
        });
        if (matching?.sha) blobBackedUp[rel] = `${matching.size}:${matching.sha}`;
        continue;
      }

      let restored = false;
      let lastError = null;
      for (const z of (zipBackups.get(rel) || [])) {
        try {
          const buf = await downloadZipBackup(z);
          fs.mkdirSync(path.dirname(fp), { recursive: true });
          atomicWrite(fp, buf);
          if (z.sha) blobBackedUp[rel] = `${z.size}:${z.sha}`;
          recovered += 1;
          restored = true;
          break;
        } catch (e) { lastError = e; }
      }
      if (!restored) for (const g of candidates) {
        try {
          const buf = await downloadGroup(g);
          fs.mkdirSync(path.dirname(fp), { recursive: true });
          atomicWrite(fp, buf);
          if (g.sha) blobBackedUp[rel] = `${g.size}:${g.sha}`;
          recovered += 1;
          restored = true;
          break;
        } catch (e) { lastError = e; }
      }

      if (!restored) {
        const vaultKey = Object.keys(vault).find((k) => path.resolve(String(vault[k]?.path || '')) === path.resolve(fp));
        const legacyAtt = vaultKey && legacyVaultAttachments.get(vaultKey);
        if (legacyAtt) {
          try {
            const res = await fetch(legacyAtt.url);
            if (!res.ok) throw new Error(`HTTP ${res.status}`);
            const buf = Buffer.from(await res.arrayBuffer());
            fs.mkdirSync(path.dirname(fp), { recursive: true });
            atomicWrite(fp, buf);
            recovered += 1;
            restored = true;
          } catch (e) { lastError = e; }
        }
      }

      if (!restored && backupLinks.has(rel)) {
        try {
          const link = backupLinks.get(rel);
          const res = await fetch(link.url, { signal: AbortSignal.timeout(120000) });
          if (!res.ok) throw new Error(`backup link HTTP ${res.status}`);
          const buf = Buffer.from(await res.arrayBuffer());
          if (buf.length !== link.size) throw new Error('backup link size mismatch');
          fs.mkdirSync(path.dirname(fp), { recursive: true });
          atomicWrite(fp, buf);
          recovered += 1;
          restored = true;
        } catch (e) { lastError = e; }
      }

      if (!restored) {
        if (!fs.existsSync(fp)) lost.push(`${rel}${lastError ? ` (${lastError.message})` : ''}`);
        else lost.push(`${rel} (backup checksum/size mismatch)`);
      }
    }

    // Recreate delivery/download hard links that point at a restored vault file.
    for (const entry of [...Object.values(deliveries), ...Object.values(downloads)]) {
      if (!entry?.vaultKey || !entry.path || fs.existsSync(entry.path)) continue;
      const item = vault[entry.vaultKey];
      if (!item?.path || !fs.existsSync(item.path)) continue;
      try {
        fs.mkdirSync(path.dirname(entry.path), { recursive: true });
        try { fs.linkSync(item.path, entry.path); } catch { fs.copyFileSync(item.path, entry.path); }
      } catch (e) { console.error('Could not recreate a vault delivery link:', e.message); }
    }

    if (Object.keys(vault).length) saveVault();
    try {
      const dbBuffers = Object.values(targets).map((fp) => fs.readFileSync(fp));
      lastBackupHash = crypto.createHash('sha256').update(Buffer.concat(dbBuffers)).digest('hex');
    } catch {}
    lastBackupAt = msgs.find((m) => trusted(m) && String(m.content || '').startsWith('BANKROLL_BACKUP_SNAPSHOT'))?.createdTimestamp || lastBackupAt;
    recoveryInfo = { ready: wanted.length - lost.length, total: wanted.length, restored: recovered, lost: [...lost], scannedMessages: msgs.length, jsonRestored };
    recoveryNote = ` Scanned ${msgs.length} messages in #${ch.name}; restored ${jsonRestored} database file(s) and ${recovered} stored file(s); ${alreadyPresent} file(s) were already present.` +
      (lost.length ? ` Could not recover: ${lost.join(', ')}.` : ' All indexed files are available.');
    console.log(`Backup restore finished. JSON restored: ${jsonRestored}; files restored: ${recovered}; already present: ${alreadyPresent}; missing: ${lost.length}.`);
  } catch (e) {
    console.error('Auto-restore on startup failed:', e.message);
    recoveryNote = ` Backup restore failed: ${e.message}`;
  } finally {
    restoreDone = true;
  }
}

const onReady = async () => {
  if (started) return;
  started = true;
  console.log(`Logged in as ${client.user.tag}`);
  try {
    // Register EVERYTHING globally so every command works in the server, bot DMs and private DMs.
    await client.application.commands.set(commands.map((c) => c.toJSON()));
    console.log('Global slash commands registered');

    // Clear the old server-only copies so you don't see every command twice in your server.
    if (DISCORD_GUILD_ID) {
      const guild = await client.guilds.fetch(DISCORD_GUILD_ID).catch(() => null);
      if (guild) {
        await guild.commands.set([]);
        console.log(`Cleared old guild commands in ${DISCORD_GUILD_ID}`);
      }
    }
  } catch (e) {
    console.error('Command registration failed:', e);
  }

  await autoRestoreOnStartup();

  backfillMonzoLedger();
  const monzoRebuilt = restoreMonzoReminders();
  restoreReminders().then((n) => {
    const total = n + monzoRebuilt;
    return notifyOwner(
      { content: `Bot is back online with auto-data recovery enabled.${total ? ` Reminders rebuilt: ${total}.` : ''}${recoveryNote}` },
      { log: false }
    );
  });
  setTimeout(backupData, 2 * 60 * 1000);
  setInterval(backupData, 6 * 60 * 60 * 1000);
};
let shuttingDown = false;
for (const sig of ['SIGTERM', 'SIGINT']) {
  process.on(sig, async () => {
    if (shuttingDown) return;
    shuttingDown = true;
    console.log(`${sig} received: saving a last backup before exit`);
    clearTimeout(backupTimer);
    await Promise.race([backupData().catch(() => {}), new Promise((r) => setTimeout(r, 20000))]);
    process.exit(0);
  });
}

client.once('ready', onReady);
client.once('clientReady', onReady);

// ---------- Reminder ----------
function parseExpiresToMinutes(input) {
  if (!input) return null;
  const match = input.toLowerCase().trim().match(/^(\d+)\s*(m|min|mins|minute|minutes|h|hr|hrs|hour|hours|d|day|days)?$/);
  if (!match) return null;
  const val = parseInt(match[1], 10);
  const unit = match[2] || 'm';
  if (unit.startsWith('h')) return val * 60;
  if (unit.startsWith('d')) return val * 1440;
  return val; // minutes default
}

// DMs the seller (the agent who made the invoice, plus the owner) at 25%, 50% and 75% of an invoice's time.
const CHECKIN_PCTS = [25, 50, 75];
const agentReminderTimers = new Map();

function checkinEmbed({ pct, customerId, amount, currency, description, expiresSec, reference, msgUrl, label = 'Invoice' }) {
  const filled = Math.round(pct / 25);
  const bar = '▰'.repeat(filled) + '▱'.repeat(4 - filled);
  const e = new EmbedBuilder()
    .setColor(pct >= 75 ? RED : pct >= 50 ? 0xf1c40f : GREEN)
    .setTitle(`${label} check-in: ${pct}% of the time used`)
    .setDescription(`<@${customerId}> still hasn't paid.`)
    .addFields(
      { name: 'Amount', value: money(amount, currency), inline: true },
      { name: 'Expires', value: `<t:${expiresSec}:R>`, inline: true },
      ...(reference ? [{ name: 'Reference', value: `\`${reference}\``, inline: true }] : []),
      { name: 'Description', value: short(description, 200) || 'No description' },
      ...(msgUrl ? [{ name: 'Invoice message', value: `[Click here to see where the invoice was sent](${msgUrl})` }] : []),
      { name: 'Time used', value: `${bar}  ${pct}%` }
    )
    .setFooter({ text: 'Nudge them with /remind customer:@them' });
  return e;
}

// Buttons under a check-in: "Go to invoice" (jumps to the original invoice message) and the pay link.
function checkinRow(payUrl, msgUrl) {
  const row = new ActionRowBuilder();
  if (msgUrl) {
    row.addComponents(new ButtonBuilder().setLabel('Go to invoice').setStyle(ButtonStyle.Link).setURL(msgUrl));
  }
  if (payUrl) {
    row.addComponents(new ButtonBuilder().setLabel('Pay link').setStyle(ButtonStyle.Link).setURL(payUrl));
  }
  return row.components.length ? [row] : [];
}

async function sendCheckin(agentId, payload, tag) {
  let delivered = false;
  for (const id of new Set([agentId, OWNER_ID].filter(Boolean))) {
    try {
      const u = await client.users.fetch(id);
      await u.send(payload);
      delivered = true;
    } catch (e) {
      console.log(`Could not DM ${id} the ${tag}:`, e.message);
    }
  }
  return delivered;
}

function scheduleAgentReminder({ customerId, agentId, sessionId, amount, description, expiresAt, currency, startMs, done = {} }) {
  for (const pct of CHECKIN_PCTS) {
    const tkey = `${sessionId}:${pct}`;
    if (done[pct] || agentReminderTimers.has(tkey)) continue;
    const at = startMs + (expiresAt * 1000 - startMs) * (pct / 100);
    const timer = setTimeout(async () => {
      agentReminderTimers.delete(tkey);
      try {
        const s = await stripe.checkout.sessions.retrieve(sessionId);
        if (s.status !== 'open' || s.metadata?.[`agent_notified_${pct}`]) return;

        let msgUrl = s.metadata?.invoice_msg_url;
        if (!msgUrl) {
          // Older invoice with no saved link: find the invoice message in its channel.
          const found = await findInvoiceMessage(s.metadata?.channel_id, sessionId);
          if (found) {
            msgUrl = found.url;
            stripe.checkout.sessions.update(sessionId, { metadata: { invoice_msg_url: msgUrl } }).catch(() => {});
          }
        }
        const embed = checkinEmbed({ pct, customerId, amount, currency, description, expiresSec: expiresAt, msgUrl });
        const delivered = await sendCheckin(
          agentId,
          { embeds: [embed], components: checkinRow(s.url, msgUrl) },
          `${pct}% reminder`
        );
        if (delivered) {
          await stripe.checkout.sessions
            .update(sessionId, { metadata: { [`agent_notified_${pct}`]: new Date().toISOString() } })
            .catch(() => {});
        }
      } catch (e) {
        alertOwner(`${pct}% reminder failed`, e);
      }
    }, Math.min(Math.max(at - Date.now(), 1000), 2 ** 31 - 1));
    agentReminderTimers.set(tkey, timer);
  }
}

function scheduleReminders({
  customerId,
  agentId,
  sessionId,
  amount,
  description,
  expiresAt,
  currency = CURRENCY,
  startMs = Date.now(),
  agentDone = {},
}) {
  scheduleAgentReminder({ customerId, agentId, sessionId, amount, description, expiresAt, currency, startMs, done: agentDone });
}

// Same three check-ins for Monzo invoices.
const monzoReminderTimers = new Map();
function scheduleMonzoReminder(m) {
  if (!m || m.status !== 'open') return;
  const end = m.expiresAt ?? m.createdAt + MONZO_OPEN_MS;
  for (const pct of CHECKIN_PCTS) {
    const flag = `notified${pct}`;
    const tkey = `${m.key}:${pct}`;
    if (m[flag] || monzoReminderTimers.has(tkey)) continue;
    const at = m.createdAt + (end - m.createdAt) * (pct / 100);
    const timer = setTimeout(async () => {
      monzoReminderTimers.delete(tkey);
      try {
        const e = monzoLedger[m.key];
        if (!e || e.status !== 'open' || e[flag]) return;
        if (!e.msgUrl) {
          const found = await findInvoiceMessage(e.channelId, e.key);
          if (found) {
            e.msgUrl = found.url;
            saveMonzo();
          }
        }
        const embed = checkinEmbed({
          pct,
          customerId: e.customerId,
          amount: e.amount,
          currency: e.currency,
          description: e.description,
          expiresSec: tsec(end),
          reference: e.reference,
          msgUrl: e.msgUrl,
          label: 'Monzo invoice',
        });
        const payUrl = MONZO_LINK ? `${MONZO_LINK}/${e.amount.toFixed(2)}?d=${encodeURIComponent(e.reference)}` : null;
        const delivered = await sendCheckin(
          e.agentId,
          { embeds: [embed], components: checkinRow(payUrl, e.msgUrl) },
          `Monzo ${pct}% reminder`
        );
        if (delivered) {
          e[flag] = Date.now();
          saveMonzo();
        }
      } catch (err) {
        alertOwner(`Monzo ${pct}% reminder failed`, err);
      }
    }, Math.min(Math.max(at - Date.now(), 1000), 2 ** 31 - 1));
    monzoReminderTimers.set(tkey, timer);
  }
}

// Unpaid Monzo invoices made before the ledger existed are still in deliveries.json:
// give them a ledger entry so they show in /unpaid, /cancel and get their check-in reminders.
function backfillMonzoLedger() {
  let n = 0;
  for (const [key, d] of Object.entries(deliveries)) {
    if (d.type !== 'monzo' || d.paid || monzoLedger[key]) continue;
    monzoLedger[key] = {
      key,
      status: 'open',
      customerId: d.customerId,
      agentId: d.agentId,
      amount: d.amount,
      currency: 'gbp',
      description: d.description ?? '',
      reference: d.reference,
      createdAt: d.savedAt ?? Date.now(),
      expiresAt: (d.savedAt ?? Date.now()) + MONZO_OPEN_MS,
      channelId: d.channelId,
      fileName: d.path ? d.name : d.link ? 'download link' : null,
    };
    n += 1;
  }
  if (n) {
    saveMonzo();
    console.log(`Backfilled ${n} older Monzo invoice(s) into the ledger`);
  }
  return n;
}

function restoreMonzoReminders() {
  let n = 0;
  for (const m of monzoOpen()) {
    if (CHECKIN_PCTS.every((p) => m[`notified${p}`])) continue;
    scheduleMonzoReminder(m);
    n += 1;
  }
  console.log(`Restored ${n} Monzo reminder(s)`);
  return n;
}

async function restoreReminders() {
  try {
    const list = await stripe.checkout.sessions.list({ limit: 100, status: 'open' });
    let n = 0;
    for (const s of list.data) {
      const m = s.metadata ?? {};
      if ((m.kind !== 'invoice' && m.kind !== 'checkout') || !m.agent_id || !m.customer_id || !s.expires_at) continue;
      if (CHECKIN_PCTS.every((p) => m[`agent_notified_${p}`])) continue;
      if (s.expires_at * 1000 - Date.now() < 2 * 60 * 1000) continue;
      scheduleReminders({
        customerId: m.customer_id,
        agentId: m.agent_id,
        sessionId: s.id,
        amount: (s.amount_total ?? 0) / 100,
        description: m.description || 'your order',
        expiresAt: s.expires_at,
        currency: s.currency,
        startMs: s.created * 1000,
        agentDone: Object.fromEntries(CHECKIN_PCTS.map((p) => [p, !!m[`agent_notified_${p}`]])),
      });
      n += 1;
    }
    console.log(`Restored ${n} reminder(s)`);
    return n;
  } catch (e) {
    alertOwner('Could not restore reminders', e);
    return 0;
  }
}

// ---------- Stripe checkout (unified /bankroll command) ----------
async function sendStripeCheckout(interaction) {
  const customer = interaction.options.getUser('customer');
  const amount = interaction.options.getNumber('amount');
  const description = interaction.options.getString('description');
  const agent = interaction.user;
  const cur = (interaction.options.getString('currency') ?? CURRENCY).toLowerCase();

  await interaction.deferReply();

  try {
    const file = interaction.options.getAttachment('file');
    const vaultKey = interaction.options.getString('vault_file');
    const linkRaw = interaction.options.getString('link');
    const expiresInput = interaction.options.getString('expires');

    let expiresMins = null;
    if (expiresInput) {
      expiresMins = parseExpiresToMinutes(expiresInput);
      if (!expiresMins || expiresMins < 1 || expiresMins > 30 * 24 * 60) {
        throw new Error('Invalid expiration time format. Use something like `30m`, `2h`, `24h`, or `7d`.');
      }
    }
    
    if ((file && vaultKey) || (file && linkRaw) || (vaultKey && linkRaw)) {
      throw new Error('Use only one of: attached file, vault file, or download link.');
    }

    let linkUrl = null;
    if (linkRaw) {
      try {
        const u = new URL(linkRaw.trim());
        if (!/^https?:$/.test(u.protocol)) throw new Error();
        linkUrl = u.href;
      } catch {
        throw new Error('That link is not valid. It must start with https://');
      }
    }

    const mode = interaction.options.getString('delivery') ?? (BASE_URL ? 'link' : 'direct');
    if ((file || vaultKey) && mode === 'link' && !BASE_URL) {
      throw new Error('Hosted links are not set up yet (PUBLIC_URL is missing). Pick Direct file upload.');
    }

    let fileData = null;
    if (file) {
      fileData = await downloadAttachment(file, mode === 'link');
    } else if (vaultKey) {
      const item = vault[vaultKey];
      if (!item || !fs.existsSync(item.path)) {
        throw new Error(`Vault item "${vaultKey}" was not found on disk.`);
      }
      fileData = { path: item.path, temp: false, name: item.originalName, size: item.size, sha256: item.sha256 };
    }
    const fileSha = fileData ? fileData.sha256 ?? (await sha256File(fileData.path)) : null;
    if (vaultKey && fileSha && vault[vaultKey] && !vault[vaultKey].sha256) vault[vaultKey].sha256 = fileSha;

    const record = {
      created_at: new Date().toISOString(),
      created_in: (interaction.guild
        ? `${interaction.guild.name} #${interaction.channel?.name ?? 'unknown'}`
        : 'Direct message'
      ).slice(0, 200),
      channel_id: String(interaction.channelId),
      kind: expiresMins ? 'invoice' : 'checkout',
      ...(fileData
        ? {
            file_name: fileData.name.slice(0, 100),
            file_sha256: fileSha,
          }
        : {}),
      ...(linkUrl ? { delivery_link_host: new URL(linkUrl).hostname } : {}),
    };

    const params = {
      mode: 'payment',
      line_items: [
        {
          quantity: 1,
          price_data: {
            currency: cur,
            unit_amount: Math.round(amount * 100),
            product_data: { name: description },
          },
        },
      ],
      success_url: SUCCESS_URL,
      metadata: {
        customer_id: customer.id,
        agent_id: agent.id,
        description: description.slice(0, 400),
        ...record,
      },
      payment_intent_data: {
        metadata: {
          customer_id: customer.id,
          agent_id: agent.id,
          description: description.slice(0, 400),
          ...record,
        },
      },
    };

    let expiresAt = null;
    if (expiresMins) {
      let secs = expiresMins * 60;
      if (expiresMins === 30) secs += 60;
      if (expiresMins === 1440) secs -= 60;
      expiresAt = Math.floor(Date.now() / 1000) + secs;
      params.expires_at = expiresAt;
    }

    const session = await stripe.checkout.sessions.create(params);
    if (fileData) {
      saveDelivery(session.id, fileData, interaction, customer);
      if (vaultKey) {
        deliveries[session.id].vaultKey = vaultKey;
        saveDeliveries();
      }
    }
    if (linkUrl) saveLinkDelivery(session.id, linkUrl, interaction, customer);
    const where = whereFrom(interaction, customer);

    const embed = new EmbedBuilder()
      .setColor(GREEN)
      .setTitle(expiresAt ? 'Bankroll Invoice Created' : 'Bankroll Payment Link Created')
      .setDescription('Please use the link below to complete your purchase.')
      .addFields(
        { name: 'Sales Agent', value: `<@${agent.id}>` },
        { name: 'Customer', value: `<@${customer.id}>` },
        { name: 'Amount', value: money(amount, cur) },
        { name: 'Description', value: description }
      );
    if (expiresAt) {
      embed.addFields({ name: 'Pay by', value: `<t:${expiresAt}:R> (<t:${expiresAt}:f>)` });
    }
    if (fileData) {
      embed.addFields({ name: 'File', value: `${fileData.name} (sent here after payment, or press Get my file)` });
    }
    if (linkUrl) {
      embed.addFields({ name: 'Download', value: 'Link sent here after payment, or press Get my file' });
    }
    embed
      .addFields(
        { name: 'Generated In Guild', value: where.guild },
        { name: 'Generated In Channel', value: where.channel }
      )
      .setFooter({ text: `Created by ${agent.username} | ID: ${session.id}` });

    const row = new ActionRowBuilder().addComponents(
      new ButtonBuilder().setLabel('Pay invoice').setStyle(ButtonStyle.Link).setURL(session.url)
    );
    if (fileData || linkUrl) {
      row.addComponents(
        new ButtonBuilder()
          .setCustomId(`getfile:${session.id}`)
          .setLabel(fileData && /\.zip$/i.test(fileData.name || '') ? 'Download ZIP' : 'Download File')
          .setStyle(ButtonStyle.Secondary)
      );
    }

    const sentInvoice = await interaction.editReply({ content: `<@${customer.id}>`, embeds: [embed], components: [row] });
    if (sentInvoice?.url) {
      // Saved so the 25/50/75% check-ins can link back to this invoice message.
      await stripe.checkout.sessions
        .update(session.id, { metadata: { invoice_msg_url: sentInvoice.url } })
        .catch((e) => console.error('Could not save invoice message link:', e.message));
    }
    if (sentInvoice?.id && deliveries[session.id]) {
      // Saved so the paid reply can go under the invoice even after a restart.
      deliveries[session.id].msgId = sentInvoice.id;
      deliveries[session.id].msgChannelId = sentInvoice.channelId;
      saveDeliveries();
    }
    
    // GUARANTEED LOGGING (invoice + a copy of the file, any size)
    logInvoiceWithFile(embed, fileData, deliveries[session.id]?.path, session.id).catch((e) =>
      console.error('Logging failed:', e.message)
    );

    if (fileData || linkUrl) {
      liveInteractions.set(session.id, { interaction, at: Date.now() });
      setTimeout(() => liveInteractions.delete(session.id), 14 * 60 * 1000);
    }

    const remindEnd = expiresAt ?? session.expires_at;
    if (remindEnd) {
      scheduleReminders({
        customerId: customer.id,
        agentId: agent.id,
        sessionId: session.id,
        amount,
        description,
        expiresAt: remindEnd,
        currency: cur,
      });
    }
  } catch (err) {
    console.error(err);
    await interaction.editReply({ content: `Failed to create invoice: ${err.message}` });
  }
}

// ---------- Monzo invoice (/monzo) ----------
async function sendMonzoInvoice(interaction) {
  const customer = interaction.options.getUser('customer');
  const amount = Math.round(interaction.options.getNumber('amount') * 100) / 100;
  const description = interaction.options.getString('description');
  const agent = interaction.user;

  await interaction.deferReply();

  try {
    const file = interaction.options.getAttachment('file');
    const vaultKey = interaction.options.getString('vault_file');
    const linkRaw = interaction.options.getString('link');

    if ((file && vaultKey) || (file && linkRaw) || (vaultKey && linkRaw)) {
      throw new Error('Use only one of: attached file, vault file, or download link.');
    }
    if (!MONZO_LINK && !MONZO_PAYMENT_DETAILS) {
      throw new Error('Monzo is not set up yet. Add MONZO_USERNAME (or MONZO_ME_URL / MONZO_PAYMENT_DETAILS) in the bot environment variables.');
    }

    let linkUrl = null;
    if (linkRaw) {
      try {
        const u = new URL(linkRaw.trim());
        if (!/^https?:$/.test(u.protocol)) throw new Error();
        linkUrl = u.href;
      } catch {
        throw new Error('That link is not valid. It must start with https://');
      }
    }

    const mode = interaction.options.getString('delivery') ?? (BASE_URL ? 'link' : 'direct');
    if ((file || vaultKey) && mode === 'link' && !BASE_URL) {
      throw new Error('Hosted links are not set up yet (PUBLIC_URL is missing). Pick Direct file upload.');
    }

    let fileData = null;
    if (file) {
      fileData = await downloadAttachment(file, mode === 'link');
    } else if (vaultKey) {
      const item = vault[vaultKey];
      if (!item || !fs.existsSync(item.path)) {
        throw new Error(`Vault item "${vaultKey}" was not found on disk.`);
      }
      fileData = { path: item.path, temp: false, name: item.originalName, size: item.size, sha256: item.sha256 };
    }

    const reference = 'BR' + crypto.randomBytes(4).toString('hex').toUpperCase().slice(0, 6);
    const key = `monzo_${crypto.randomBytes(8).toString('hex')}`;

    if (fileData) {
      saveDelivery(key, fileData, interaction, customer);
      if (vaultKey) {
        deliveries[key].vaultKey = vaultKey;
        saveDeliveries();
      }
    } else if (linkUrl) saveLinkDelivery(key, linkUrl, interaction, customer);
    else {
      deliveries[key] = {
        name: 'no file',
        channelId: interaction.channelId,
        customerId: customer.id,
        agentId: agent.id,
        savedAt: Date.now(),
      };
    }
    Object.assign(deliveries[key], {
      type: 'monzo',
      paid: false,
      amount,
      reference,
      description,
    });
    saveDeliveries();
    monzoLedger[key] = {
      key,
      status: 'open',
      customerId: customer.id,
      agentId: agent.id,
      amount,
      currency: 'gbp',
      description,
      reference,
      createdAt: Date.now(),
      expiresAt: Date.now() + MONZO_OPEN_MS,
      channelId: interaction.channelId,
      fileName: fileData ? fileData.name : linkUrl ? 'download link' : null,
    };
    saveMonzo();
    scheduleMonzoReminder(monzoLedger[key]);

    const where = whereFrom(interaction, customer);
    const embed = new EmbedBuilder()
      .setColor(GREEN)
      .setTitle('Bankroll Monzo Invoice Created')
      .setDescription('Awaiting payment. Pay the exact amount below and include the reference.')
      .addFields(
        { name: 'Sales Agent', value: `<@${agent.id}>` },
        { name: 'Customer', value: `<@${customer.id}>` },
        { name: 'Amount', value: money(amount, 'gbp'), inline: true },
        { name: 'Payment reference', value: `\`${reference}\``, inline: true },
        { name: 'Description', value: description }
      );
    embed.addFields({ name: 'Pay by', value: `<t:${tsec(monzoLedger[key].expiresAt)}:R>` });
    if (MONZO_PAYMENT_DETAILS) embed.addFields({ name: 'Pay to', value: MONZO_PAYMENT_DETAILS.replace(/\\n/g, '\n') });
    if (fileData) embed.addFields({ name: 'File', value: `${fileData.name} (sent here as soon as payment lands, or press Get my file)` });
    if (linkUrl) embed.addFields({ name: 'Download', value: 'Link sent here as soon as payment lands, or press Get my file' });
    embed
      .addFields(
        { name: 'Generated In Guild', value: where.guild },
        { name: 'Generated In Channel', value: where.channel }
      )
      .setFooter({ text: `Created by ${agent.username} | ID: ${key}` });

    const row = new ActionRowBuilder();
    if (MONZO_LINK) {
      const payUrl = `${MONZO_LINK}/${amount.toFixed(2)}?d=${encodeURIComponent(reference)}`;
      row.addComponents(new ButtonBuilder().setLabel('Pay with Monzo').setStyle(ButtonStyle.Link).setURL(payUrl));
    }
    if (fileData || linkUrl) {
      row.addComponents(
        new ButtonBuilder().setCustomId(`getfile:${key}`).setLabel(fileData && /\.zip$/i.test(fileData.name || '') ? 'Download ZIP' : 'Download File').setStyle(ButtonStyle.Secondary)
      );
    }

    const sentInvoice = await interaction.editReply({
      content: `<@${customer.id}>`,
      embeds: [embed],
      components: row.components.length ? [row] : [],
    });
    if (sentInvoice?.url) {
      monzoLedger[key].msgUrl = sentInvoice.url;
      saveMonzo();
    }
    if (sentInvoice?.id && deliveries[key]) {
      deliveries[key].msgId = sentInvoice.id;
      deliveries[key].msgChannelId = sentInvoice.channelId;
      saveDeliveries();
    }
    logInvoiceWithFile(embed, fileData, deliveries[key]?.path, key).catch((e) =>
      console.error('Logging failed:', e.message)
    );

    liveInteractions.set(key, { interaction, at: Date.now() });
    setTimeout(() => liveInteractions.delete(key), 14 * 60 * 1000);
  } catch (err) {
    console.error(err);
    await interaction.editReply({ content: `Failed to create Monzo invoice: ${err.message}` });
  }
}

// ---------- FiveM vehicle zip: spawn code tools ----------
// SPAWNCODE-TOOLS-START
const escRe = (s) => s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
const BAD_PATH = (p) => p.split('/').includes('..');

// Spawn codes found in a vehicle resource: <modelName> in vehicles*.meta, else the .yft file names.
function zipSpawnCodes(zip) {
  const entries = zip.getEntries().filter((e) => !e.isDirectory);
  const found = new Set();
  for (const e of entries) {
    const base = path.posix.basename(e.entryName.replace(/\\/g, '/')).toLowerCase();
    if (!/^vehicles.*\.meta$/.test(base)) continue;
    const text = e.getData().toString('utf8');
    for (const m of text.matchAll(/<modelName>\s*([^<\s]+)\s*<\/modelName>/gi)) found.add(m[1].toLowerCase());
  }
  if (!found.size) {
    for (const e of entries) {
      const m = path.posix.basename(e.entryName.replace(/\\/g, '/')).toLowerCase().match(/^([a-z0-9_]+?)(?:_hi)?\.yft$/);
      if (m) found.add(m[1]);
    }
  }
  return [...found];
}

// Rewrites a vehicle resource zip so spawn code `oldCode` becomes `newCode`:
//  - stream files / folders named after it (adder.yft, adder_hi.yft, adder.ytd, adder+hi.ytd ...) are renamed
//  - <modelName>, <txdName>, <handlingId>, <handlingName>, <parent>, <child> in .meta/.xml files are updated
//    (txd / handling names are only touched if the zip defines them itself, so vanilla references stay intact)
//  - fxmanifest.lua / __resource.lua paths that pointed at renamed files are updated
function replaceSpawnCode(zipBuf, oldCode, newCode) {
  const src = new AdmZip(zipBuf);
  const prefix = new RegExp(`^${escRe(oldCode)}(?![a-z0-9])`, 'i');
  const word = new RegExp(`(?<![A-Za-z0-9])${escRe(oldCode)}(?![A-Za-z0-9])`, 'i');
  const swap = (s) => newCode + s.slice(oldCode.length);

  const entries = src.getEntries().filter((e) => !e.isDirectory);
  const files = entries.map((e) => ({ e, name: e.entryName.replace(/\\/g, '/').replace(/^\/+/, '') }));
  for (const f of files) if (BAD_PATH(f.name)) throw new Error('The zip contains an unsafe file path.');

  const isText = (n) => /\.(meta|xml|lua|json|cfg|txt|md|ini|js)$/i.test(n);
  const isMeta = (n) => /\.(meta|xml)$/i.test(n);

  // What the zip defines itself
  const ownTxd = new Set();
  const ownHandling = new Set();
  for (const f of files) {
    const base = path.posix.basename(f.name);
    if (/\.ytd$/i.test(base)) ownTxd.add(base.replace(/\.ytd$/i, '').toLowerCase());
    if (isMeta(f.name)) {
      const t = f.e.getData().toString('utf8');
      for (const m of t.matchAll(/<handlingName>\s*([^<\s]+)\s*<\/handlingName>/gi)) ownHandling.add(m[1].toLowerCase());
    }
  }

  const renameSeg = (seg, isFile) => {
    const dot = isFile ? seg.lastIndexOf('.') : -1;
    const stem = dot > 0 ? seg.slice(0, dot) : seg;
    const ext = dot > 0 ? seg.slice(dot) : '';
    return prefix.test(stem) ? swap(stem) + ext : seg;
  };

  const renamedPaths = new Map();
  const used = new Set();
  for (const f of files) {
    const parts = f.name.split('/');
    f.newName = parts.map((seg, i) => renameSeg(seg, i === parts.length - 1)).join('/');
    if (f.newName !== f.name) renamedPaths.set(f.name, f.newName);
    if (used.has(f.newName.toLowerCase())) throw new Error(`Renaming would create two files called ${f.newName}.`);
    used.add(f.newName.toLowerCase());
  }

  const tagRe = /(<(modelName|txdName|handlingId|handlingName|parent|child)>\s*)([^<]*?)(\s*<\/\2>)/gi;
  const stats = { renamedFiles: renamedPaths.size, metaEdits: 0, manifestEdits: 0, mentions: [], skipped: [] };
  const out = new AdmZip();

  for (const f of files) {
    let data = f.e.getData();
    const lower = f.name.toLowerCase();
    if (isMeta(f.name)) {
      if (data[0] === 0xff && data[1] === 0xfe) {
        stats.skipped.push(`${f.name} (UTF-16, edit by hand)`);
      } else {
        const text = data.toString('utf8');
        const next = text.replace(tagRe, (m, open, tag, val, close) => {
          if (!prefix.test(val)) return m;
          const v = val.toLowerCase();
          const t = tag.toLowerCase();
          const ok =
            t === 'modelname' || t === 'handlingname'
              ? true
              : t === 'handlingid'
              ? ownHandling.has(v)
              : ownTxd.has(v) || v === oldCode;
          if (!ok) return m;
          stats.metaEdits += 1;
          return open + swap(val) + close;
        });
        if (next !== text) data = Buffer.from(next, 'utf8');
      }
    } else if (/(^|\/)(fxmanifest\.lua|__resource\.lua)$/.test(lower)) {
      let text = data.toString('utf8');
      let changed = false;
      for (const [from, to] of renamedPaths) {
        const re = new RegExp(escRe(from), 'gi');
        if (re.test(text)) {
          text = text.replace(new RegExp(escRe(from), 'gi'), to);
          stats.manifestEdits += 1;
          changed = true;
        }
      }
      if (changed) data = Buffer.from(text, 'utf8');
    } else if (isText(f.name) && data.length < 5 * 1024 * 1024) {
      // Scripts / configs are left alone, but flagged so you can check them.
      if (word.test(data.toString('utf8'))) stats.mentions.push(f.name);
    }
    out.addFile(f.newName, data);
  }

  if (!stats.renamedFiles && !stats.metaEdits) {
    throw new Error(`Nothing in the zip uses the spawn code \`${oldCode}\`.`);
  }
  return { buffer: out.toBuffer(), ...stats };
}
// SPAWNCODE-TOOLS-END

// ---------- Vault Command Handler ----------
async function cmdDownloads(interaction) {
  if (!allowed.has(interaction.user.id)) return interaction.editReply('Only authorized admins can manage downloads.');
  const sub = interaction.options.getSubcommand();
  if (sub === 'history') {
    const count = interaction.options.getInteger('count') || 15; const rows = downloadHistory.slice(-count).reverse();
    if (!rows.length) return interaction.editReply('No download activity has been recorded yet.');
    const lines = rows.map((e) => `${e.type.includes('failed') || e.type.includes('denied') || e.type.includes('expired') ? '⚠️' : e.type === 'download_completed' ? '✅' : '📦'} <t:${Math.floor(e.at/1000)}:R> **${short(e.type.replace(/_/g,' '),60)}** — ${short(e.file,90)}${e.customerId ? ` — <@${e.customerId}>` : ''}${e.detail ? `\n${short(e.detail,140)}` : ''}`);
    return interaction.editReply({ embeds: [new EmbedBuilder().setColor(GREY).setTitle(`Download activity (${downloadHistory.length} stored)`).setDescription(lines.join('\n\n').slice(0,3900)).setFooter({ text: 'Audit history is retained across restarts and included in backups.' })] });
  }
  if (sub === 'cleanup') { const removed = cleanDownloads(); recordNotification('downloads.cleanup','Download cleanup completed',`${removed} expired, used or missing download record(s) removed.`,'success'); return interaction.editReply(`Cleanup complete. Removed **${removed}** expired, used or missing download record(s).`); }
  if (sub === 'backup') { await interaction.editReply('Starting an immediate Bankroll backup…'); await backupData(); recordNotification('downloads.backup','Manual backup completed','An immediate backup run was requested from /downloads backup.','success'); return interaction.editReply('Backup run finished. Check #bankroll-backup and the bot logs for any upload errors.'); }
  const key = interaction.options.getString('vault_key').trim(); const item = vault[key];
  if (!item?.path || !fs.existsSync(item.path)) return interaction.editReply(`Vault file \`${key}\` is missing.`);
  if (sub === 'verify') {
    try { const actualHash=await sha256File(item.path); const size=fs.statSync(item.path).size; const zipInfo=verifyZipIntegrity(item.path); const hashMatches=!item.sha256||item.sha256===actualHash; if(!item.sha256){item.sha256=actualHash;saveVault();}
      return interaction.editReply({ embeds:[new EmbedBuilder().setColor(hashMatches?GREEN:RED).setTitle(`Download integrity: ${key}`).setDescription(hashMatches?'File exists and the SHA-256 checksum matches.':'Checksum mismatch: do not send this file until checked.').addFields({name:'File',value:short(item.originalName||key,100),inline:true},{name:'Size',value:`${(size/(1024*1024)).toFixed(2)} MB`,inline:true},{name:'ZIP entries',value:zipInfo.isZip?String(zipInfo.entries):'Not a ZIP',inline:true},{name:'SHA-256',value:actualHash}).setTimestamp()] });
    } catch(e) { recordDownloadEvent('verification_failed',{file:item.originalName||key,detail:e.message}); return interaction.editReply(`ZIP/file verification failed: ${e.message}`); }
  }
  if (sub === 'regenerate') {
    if(!BASE_URL) return interaction.editReply('PUBLIC_URL is not configured, so Bankroll cannot create hosted download links.');
    try { const zipInfo=verifyZipIntegrity(item.path); const size=fs.statSync(item.path).size; const url=makeDownloadLink({path:item.path,name:item.originalName||path.basename(item.path),customerId:interaction.user.id,agentId:interaction.user.id,vaultKey:key,description:'admin-regenerated-download'},null); const token=url.split('/').pop(); const expiresAt=downloads[token]?.expiresAt;
      recordDownloadEvent('link_regenerated',{file:item.originalName||key,customerId:interaction.user.id,agentId:interaction.user.id,token,detail:`Admin regenerated a link for vault key ${key}; ZIP entries ${zipInfo.entries||0}.`});
      return interaction.editReply({embeds:[buildDownloadEmbed(item.originalName||key,expiresAt,size,'Fresh download link generated')],components:[downloadLinkRow(url,/\.zip$/i.test(item.originalName||'')?'Download ZIP':'Download File')]});
    } catch(e) { recordDownloadEvent('link_regeneration_failed',{file:item.originalName||key,customerId:interaction.user.id,detail:e.message}); return interaction.editReply(`Could not regenerate the link: ${e.message}`); }
  }
}

async function cmdVault(interaction) {
  if (!allowed.has(interaction.user.id)) {
    return interaction.reply({ content: 'Only authorized users can manage the vault.', ephemeral: true });
  }

  await interaction.deferReply({ ephemeral: true });
  const sub = interaction.options.getSubcommand();

  if (sub === 'add') {
    const att = interaction.options.getAttachment('file');
    const key = interaction.options.getString('name').toLowerCase().trim().replace(/[^a-z0-9\-_]/g, '-');
    
    if (vault[key] && fs.existsSync(vault[key].path)) {
      return interaction.editReply(`A vault item with the name **${key}** already exists. Remove it first or pick a different name.`);
    }

    if (att.size > 100 * 1024 * 1024) {
      return interaction.editReply('That file is over 100 MB.');
    }

    const filePath = path.join(VAULT_DIR, `${key}_${att.name.replace(/[^\w.\-]/g, '_')}`);

    const tmpPath = path.join(TMP_DIR, `${crypto.randomBytes(8).toString('hex')}.part`);
    let info;
    try {
      info = await streamToFile(att.url, tmpPath, 100 * 1024 * 1024);
      fs.renameSync(tmpPath, filePath);
    } catch (err) {
      try { fs.unlinkSync(tmpPath); } catch {}
      return interaction.editReply(
        `Failed to download and save file: ${err.message === 'TOO_BIG' ? 'that file is over 100 MB' : err.message}`
      );
    }

    if (/\.zip$/i.test(att.name)) { try { verifyZipIntegrity(filePath); } catch (zipError) { try { fs.unlinkSync(filePath); } catch {} return interaction.editReply(`That ZIP failed its integrity check and was not added: ${short(zipError.message, 220)}`); } }
    vault[key] = {
      path: filePath,
      originalName: att.name,
      size: info.size,
      sha256: info.sha256,
      addedAt: Date.now(),
    };
    delete blobBackedUp[relData(filePath)];
    saveVault();
    recordNotification('vault.add', 'Vault file added', `${key}: ${att.name}`, 'success');

    let codeNote = '';
    if (/\.zip$/i.test(att.name)) {
      try {
        const codes = zipSpawnCodes(new AdmZip(filePath));
        if (codes.length) {
          codeNote = ` Spawn code${codes.length > 1 ? 's' : ''} found: ${codes.slice(0, 10).map((c) => `\`${c}\``).join(', ')}. Change it with /vault edit.`;
        }
      } catch {}
    }
    await interaction.editReply(`Successfully added **${att.name}** to the vault as \`${key}\`!${codeNote} Saving a backup copy now...`);
    clearTimeout(backupTimer);
    backupData()
      .then(() =>
        interaction.followUp({
          content: blobBackedUp[relData(filePath)]
            ? `Backup copy of \`${key}\` is saved in the backup channel.`
            : `Backup copy of \`${key}\` is not saved yet. The bot will keep retrying, check /vault list.`,
          ephemeral: true,
        })
      )
      .catch(() => {});
    return;
  }

  if (sub === 'search') {
    const query = interaction.options.getString('query').trim().toLowerCase();
    const matches = Object.entries(vault).filter(([key, item]) =>
      key.toLowerCase().includes(query) || String(item.originalName || '').toLowerCase().includes(query)
    ).slice(0, 25);
    if (!matches.length) return interaction.editReply(`No vault files matched \`${query}\`.`);
    return interaction.editReply(matches.slice(0, 12).map(([key, item]) =>
      `• \`${key}\` — ${item.originalName || 'unnamed'} (${fs.existsSync(item.path) ? 'available' : 'missing'})`
    ).join('\n').slice(0, 1900));
  }

  if (sub === 'verify') {
    const key = interaction.options.getString('name');
    const item = vault[key];
    if (!item) return interaction.editReply(`Vault item \`${key}\` not found.`);
    if (!item.path || !fs.existsSync(item.path)) return interaction.editReply(`\`${key}\` is missing from disk. Re-add the file with /vault add.`);
    try {
      const actualHash = await sha256File(item.path);
      const actualSize = fs.statSync(item.path).size;
      let zipCheck = 'Not a ZIP file';
      if (/\.zip$/i.test(item.originalName || item.path)) {
        try {
          const zip = new AdmZip(item.path);
          zip.getEntries();
          zipCheck = 'ZIP opened successfully';
        } catch (zipError) {
          zipCheck = `ZIP could not be opened: ${short(zipError.message, 180)}`;
        }
      }
      const hashMatches = !item.sha256 || item.sha256 === actualHash;
      if (!item.sha256) { item.sha256 = actualHash; saveVault(); }
      if (!item.size || item.size !== actualSize) { item.size = actualSize; saveVault(); }
      return interaction.editReply({ embeds: [new EmbedBuilder().setColor(hashMatches ? GREEN : RED)
        .setTitle(`Vault verification: ${key}`)
        .setDescription(hashMatches ? 'File exists and checksum is valid.' : 'Checksum mismatch: the file may have changed or become corrupted.')
        .addFields(
          { name: 'Filename', value: String(item.originalName || 'Unknown'), inline: true },
          { name: 'Size', value: `${(actualSize / (1024 * 1024)).toFixed(2)} MB`, inline: true },
          { name: 'SHA-256', value: actualHash },
          { name: 'ZIP check', value: zipCheck }
        )] });
    } catch (e) {
      return interaction.editReply(`Could not verify \`${key}\`: ${e.message}`);
    }
  }

  if (sub === 'rename') {
    const oldKey = interaction.options.getString('name');
    const newKey = interaction.options.getString('new_name').toLowerCase().trim().replace(/[^a-z0-9_-]/g, '-').slice(0, 60);
    if (!/^[a-z0-9][a-z0-9_-]{0,59}$/.test(newKey)) return interaction.editReply('Choose a key using letters, numbers, underscores or hyphens.');
    if (!vault[oldKey]) return interaction.editReply(`Vault item \`${oldKey}\` not found.`);
    if (vault[newKey]) return interaction.editReply(`Vault key \`${newKey}\` already exists.`);
    vault[newKey] = { ...vault[oldKey] };
    delete vault[oldKey];
    for (const d of [...Object.values(deliveries), ...Object.values(downloads)]) {
      if (d?.vaultKey === oldKey) d.vaultKey = newKey;
    }
    saveVault(); saveDeliveries(); saveDownloads();
    await interaction.editReply(`Renamed vault key \`${oldKey}\` to \`${newKey}\`. The file itself was not changed.`);
    recordNotification('vault.rename', 'Vault item renamed', `${oldKey} -> ${newKey}`, 'success');
    return;
  }

  if (sub === 'list') {
    const keys = Object.keys(vault);
    if (!keys.length) return interaction.editReply('Your vault is currently empty.');

    const lines = keys.map(
      (k) =>
        `• \`${k}\` — **${vault[k].originalName}** (${(vault[k].size / (1024 * 1024)).toFixed(1)} MB)` +
        (fs.existsSync(vault[k].path) ? (blobBackedUp[relData(vault[k].path)] ? ' - backed up' : ' - backup pending') : ' - file missing, re-add it')
    );
    const embed = new EmbedBuilder()
      .setColor(GREEN)
      .setTitle(`Vault Inventory (${keys.length} items)`)
      .setDescription(lines.join('\n'));
    return interaction.editReply({ embeds: [embed] });
  }

  if (sub === 'info') {
    const key = interaction.options.getString('name');
    const item = vault[key];
    if (!item) return interaction.editReply(`Vault item \`${key}\` not found.`);

    const embed = new EmbedBuilder()
      .setColor(GREEN)
      .setTitle(`Vault Item: ${key}`)
      .addFields(
        { name: 'Original Filename', value: item.originalName, inline: true },
        { name: 'File Size', value: `${(item.size / (1024 * 1024)).toFixed(2)} MB`, inline: true },
        { name: 'Added At', value: `<t:${Math.floor(item.addedAt / 1000)}:f>`, inline: true }
      );
    return interaction.editReply({ embeds: [embed] });
  }

  if (sub === 'edit') {
    const key = interaction.options.getString('name');
    const item = vault[key];
    if (!item || !fs.existsSync(item.path)) return interaction.editReply(`Vault item \`${key}\` was not found.`);
    if (!/\.zip$/i.test(item.originalName)) return interaction.editReply('Only .zip vault files can be edited.');

    const newCode = interaction.options.getString('spawncode').trim().toLowerCase();
    if (!/^[a-z0-9_]{1,32}$/.test(newCode)) {
      return interaction.editReply('The spawn code can only use letters, numbers and underscores (max 32).');
    }
    const newKey = (interaction.options.getString('save_as') || `${key}-${newCode}`)
      .toLowerCase()
      .trim()
      .replace(/[^a-z0-9\-_]/g, '-');
    if (vault[newKey] && fs.existsSync(vault[newKey].path)) {
      return interaction.editReply(`A vault item called \`${newKey}\` already exists. Use save_as to pick another name or remove it first.`);
    }

    let buf;
    let result;
    let oldCode = (interaction.options.getString('old_spawncode') || '').trim().toLowerCase();
    try {
      buf = await fs.promises.readFile(item.path);
      if (!oldCode) {
        const codes = zipSpawnCodes(new AdmZip(buf));
        if (!codes.length) return interaction.editReply('I could not find a spawn code in that zip. Pass old_spawncode yourself.');
        if (codes.length > 1) {
          return interaction.editReply(`This zip has more than one vehicle: ${codes.map((c) => `\`${c}\``).join(', ')}. Pass old_spawncode to say which one to change.`);
        }
        oldCode = codes[0];
      }
      if (oldCode === newCode) return interaction.editReply('That is already the spawn code.');
      result = replaceSpawnCode(buf, oldCode, newCode);
    } catch (e) {
      console.error('Vault edit failed:', e.message);
      return interaction.editReply(`Could not edit that zip: ${e.message}`);
    }

    const outName = `${newCode}.zip`;
    const outPath = path.join(VAULT_DIR, `${newKey}_${outName}`);
    try {
      atomicWrite(outPath, result.buffer);
    } catch (e) {
      return interaction.editReply(`Could not save the edited zip: ${e.message}`);
    }
    vault[newKey] = {
      path: outPath,
      originalName: outName,
      size: result.buffer.length,
      sha256: crypto.createHash('sha256').update(result.buffer).digest('hex'),
      addedAt: Date.now(),
      editedFrom: key,
      spawncode: newCode,
    };
    delete blobBackedUp[relData(outPath)];
    saveVault();
    recordNotification('vault.edit', 'Vault file edited', `${key} -> ${newKey}`, 'success');

    const lines = [
      `Saved as vault item \`${newKey}\` (**${outName}**). Spawn code \`${oldCode}\` is now \`${newCode}\`.`,
      `Renamed files: ${result.renamedFiles}. Meta tags updated: ${result.metaEdits}.` +
        (result.manifestEdits ? ` Manifest paths updated: ${result.manifestEdits}.` : ''),
    ];
    if (result.mentions.length) {
      lines.push(`Still mentions \`${oldCode}\` (not changed, check them): ${result.mentions.slice(0, 8).join(', ')}`);
    }
    if (result.skipped.length) lines.push(`Skipped: ${result.skipped.join(', ')}`);
    lines.push(`Use it on an invoice with vault_file: \`${newKey}\`. The original \`${key}\` is untouched.`);
    await interaction.editReply(lines.join('\n'));
    clearTimeout(backupTimer);
    backupData().catch(() => {});
    return;
  }

  if (sub === 'remove') {
    const key = interaction.options.getString('name');
    if (!vault[key]) return interaction.editReply(`Vault item \`${key}\` not found.`);

    try {
      fs.unlinkSync(vault[key].path);
    } catch {}
    delete blobBackedUp[relData(vault[key].path)];
    delete vault[key];
    saveVault();
    recordNotification('vault.remove', 'Vault file removed', key, 'success');

    return interaction.editReply(`Removed \`${key}\` from the vault.`);
  }
}

// ---------- /cancel ----------
async function cmdCancel(interaction) {
  if (!allowed.has(interaction.user.id)) {
    return interaction.reply({ content: 'Only authorized users can cancel invoices.', ephemeral: true });
  }
  await interaction.deferReply({ ephemeral: true });

  const input = interaction.options.getString('order').trim();

  if (input.startsWith('monzo_')) {
    const e = monzoLedger[input];
    if (!e) return interaction.editReply('I could not find that Monzo invoice.');
    if (e.status !== 'open') return interaction.editReply(`That Monzo invoice is already ${e.status} (cannot be cancelled).`);
    e.status = 'cancelled';
    e.cancelledAt = Date.now();
    e.cancelledBy = interaction.user.id;
    saveMonzo();
    const hadPending = discardPending(input);
    const embed = new EmbedBuilder()
      .setColor(GREY)
      .setTitle('Monzo Invoice Cancelled / Removed')
      .addFields(
        { name: 'Reference', value: e.reference, inline: true },
        { name: 'Description', value: short(e.description, 100), inline: true },
        { name: 'Amount', value: money(e.amount, e.currency), inline: true },
        { name: 'Pending File/Link Cleaned', value: hadPending ? 'Yes' : 'None attached', inline: true },
        { name: 'Cancelled By', value: `<@${interaction.user.id}>`, inline: true }
      )
      .setTimestamp();
    await interaction.editReply({ embeds: [embed] });
    await logToServer({ embeds: [embed] });
    return;
  }

  if (!input.startsWith('cs_')) {
    return interaction.editReply('Please pick an invoice from the list, or paste a Checkout ID starting with `cs_...`.');
  }

  try {
    const session = await stripe.checkout.sessions.retrieve(input);
    if (session.status !== 'open') {
      return interaction.editReply(`That checkout session is already ${session.status} (cannot be cancelled).`);
    }

    await stripe.checkout.sessions.expire(input);
    const hadPending = discardPending(input);

    const embed = new EmbedBuilder()
      .setColor(GREY)
      .setTitle('Invoice Cancelled / Removed')
      .addFields(
        { name: 'Checkout ID', value: input, inline: true },
        { name: 'Description', value: short(session.metadata?.description, 100), inline: true },
        { name: 'Amount', value: money((session.amount_total ?? 0) / 100, session.currency), inline: true },
        { name: 'Pending File/Link Cleaned', value: hadPending ? 'Yes' : 'None attached', inline: true },
        { name: 'Cancelled By', value: `<@${interaction.user.id}>`, inline: true }
      )
      .setTimestamp();

    await interaction.editReply({ embeds: [embed] });
    await logToServer({ embeds: [embed] });
  } catch (err) {
    console.error('Cancel failed:', err);
    await interaction.editReply(`Failed to cancel invoice: ${err.message}`);
  }
}

// ---------- /remind ----------
async function cmdRemind(interaction) {
  const user = interaction.options.getUser('customer');
  await interaction.deferReply();
  const sessions = await stripe.checkout.sessions.list({ limit: 100, status: 'open' });
  const s = sessions.data.find((x) => x.metadata?.customer_id === user.id);
  if (!s) {
    const m = monzoOpen().find((x) => x.customerId === user.id);
    if (!m) return interaction.editReply(`<@${user.id}> has no unpaid invoice.`);
    const embed = new EmbedBuilder()
      .setColor(GREEN)
      .setTitle('Payment reminder')
      .setDescription(
        `You have an unpaid Monzo invoice for **${money(m.amount, m.currency)}** (${short(m.description, 100)}).\n` +
          `Please pay the exact amount and include the reference \`${m.reference}\`.` +
          (MONZO_PAYMENT_DETAILS ? `\n${MONZO_PAYMENT_DETAILS.replace(/\\n/g, '\n')}` : '')
      );
    const row = new ActionRowBuilder();
    if (MONZO_LINK) {
      row.addComponents(
        new ButtonBuilder()
          .setLabel('Pay with Monzo')
          .setStyle(ButtonStyle.Link)
          .setURL(`${MONZO_LINK}/${m.amount.toFixed(2)}?d=${encodeURIComponent(m.reference)}`)
      );
    }
    const mUrl = m.msgUrl || (await findInvoiceMessage(m.channelId, m.key))?.url;
    if (mUrl) row.addComponents(new ButtonBuilder().setLabel('Go to invoice').setStyle(ButtonStyle.Link).setURL(mUrl));
    return interaction.editReply({ content: `<@${user.id}>`, embeds: [embed], components: row.components.length ? [row] : [] });
  }

  const embed = new EmbedBuilder()
    .setColor(GREEN)
    .setTitle('Payment reminder')
    .setDescription(
      `You have an unpaid invoice for **${money((s.amount_total ?? 0) / 100, s.currency)}** (${short(s.metadata?.description, 100)}).\n` +
        `The link expires <t:${s.expires_at}:R>.`
    );
  const row = new ActionRowBuilder().addComponents(
    new ButtonBuilder().setLabel('Pay invoice').setStyle(ButtonStyle.Link).setURL(s.url)
  );
  const sUrl = s.metadata?.invoice_msg_url || (await findInvoiceMessage(s.metadata?.channel_id, s.id))?.url;
  if (sUrl) row.addComponents(new ButtonBuilder().setLabel('Go to invoice').setStyle(ButtonStyle.Link).setURL(sUrl));
  return interaction.editReply({ content: `<@${user.id}>`, embeds: [embed], components: [row] });
}

// ---------- /unpaid and /status ----------
async function cmdUnpaid(interaction) {
  let mine = [];
  try {
    const res = await stripe.checkout.sessions.list({ limit: 100, status: 'open' });
    mine = res.data.filter((s) => s.metadata?.agent_id);
  } catch (e) {
    console.error('Stripe unpaid lookup failed:', e.message);
  }
  const monzo = monzoOpen();
  const total = mine.length + monzo.length;
  if (!total) return interaction.editReply('No unpaid invoices right now.');

  const lines = [
    ...mine.map(
      (s) =>
        `${money((s.amount_total ?? 0) / 100, s.currency)} - <@${s.metadata.customer_id}> - ` +
        `${short(s.metadata.description, 40)} - expires <t:${s.expires_at}:R> (Card)`
    ),
    ...monzo.map(
      (m) =>
        `${money(m.amount, m.currency)} - <@${m.customerId}> - ${short(m.description, 40)} - ` +
        `ref ${m.reference} - made <t:${tsec(m.createdAt)}:R> (Monzo)`
    ),
  ];
  const embed = new EmbedBuilder()
    .setColor(GREEN)
    .setTitle(`Unpaid invoices (${total})`)
    .setDescription(lines.slice(0, 20).join('\n'))
    .setFooter({ text: total > 20 ? `Showing 20 of ${total}.` : 'Card links still open and Monzo invoices waiting for payment.' });
  return interaction.editReply({ embeds: [embed] });
}

async function cmdNotifications(interaction) {
  const sub = interaction.options.getSubcommand();
  if (sub === 'test') {
    const embed = new EmbedBuilder().setColor(GREEN).setTitle('Bankroll notification test')
      .setDescription('Notifications are working. This test has been saved to notification history.')
      .setTimestamp();
    await notifyOwner({ embeds: [embed] }, { notificationType: 'notification.test', notificationStatus: 'success' });
    return interaction.editReply('Test notification sent to the bot owner (or check the logs if DMs are disabled).');
  }
  const count = interaction.options.getInteger('count') || 10;
  const recent = notificationHistory.slice(-count).reverse();
  if (!recent.length) return interaction.editReply('No notifications have been recorded yet.');
  const lines = recent.map((n) => {
    const stamp = Math.floor((n.at || Date.now()) / 1000);
    const mark = n.status === 'error' ? '❌' : n.status === 'success' ? '✅' : 'ℹ️';
    return `${mark} <t:${stamp}:R> **${short(n.title, 90)}**\n${short(n.detail, 180)}`;
  });
  const embed = new EmbedBuilder().setColor(GREY).setTitle('Recent notifications')
    .setDescription(lines.join('\n\n').slice(0, 4000));
  return interaction.editReply({ embeds: [embed] });
}

async function cmdStatus(interaction) {
  if (interaction.user.id !== OWNER_ID) return interaction.editReply('Only the owner can run this.');
  let open = 'unknown';
  try {
    const res = await stripe.checkout.sessions.list({ limit: 100, status: 'open' });
    open = String(res.data.filter((s) => s.metadata?.agent_id).length);
  } catch {}
  const up = Math.floor(process.uptime());
  const when = (ms) => (ms ? `<t:${Math.floor(ms / 1000)}:R>` : 'Not yet');
  const embed = new EmbedBuilder()
    .setColor(GREEN)
    .setTitle('Bot status')
    .addFields(
      { name: 'Uptime', value: `${Math.floor(up / 3600)}h ${Math.floor((up % 3600) / 60)}m`, inline: true },
      { name: 'Discord ping', value: `${client.ws.ping} ms`, inline: true },
      { name: 'Open invoices', value: open, inline: true },
      { name: 'Reminders queued', value: String(agentReminderTimers.size + monzoReminderTimers.size), inline: true },
      { name: 'Files waiting', value: String(Object.keys(deliveries).length), inline: true },
      { name: 'Vault items', value: String(Object.keys(vault).length), inline: true },
      { name: 'Notifications saved', value: String(notificationHistory.length), inline: true },
      { name: 'Hosted links', value: BASE_URL ? 'On' : 'Off (PUBLIC_URL not set)', inline: true },
      {
        name: 'File storage',
        value: short(DATA_DIR_STATUS, 100),
        inline: true,
      },
      { name: 'Last Stripe notice', value: lastWebhookAt ? `${when(lastWebhookAt)} (${lastWebhookType})` : 'None since start', inline: true },
      { name: 'Open Monzo invoices', value: String(monzoOpen().length), inline: true },
      { name: 'Last Monzo notice', value: lastMonzoAt ? when(lastMonzoAt) : 'None since start', inline: true },
      { name: 'Last data backup', value: when(lastBackupAt), inline: true }
    );
  return interaction.editReply({ embeds: [embed] });
}

// ---------- /restore ----------
async function cmdRestore(interaction) {
  if (interaction.user.id !== OWNER_ID) return interaction.editReply('Only the owner can run this.');
  const att = interaction.options.getAttachment('file');
  if (att.size > 5 * 1024 * 1024) return interaction.editReply('That file is too big to be a backup.');

  const fname = att.name.toLowerCase();
  const target = fname.startsWith('deliveries') ? 'deliveries' : fname.startsWith('downloads') ? 'downloads' : fname.startsWith('download_history') ? 'download_history' : fname.startsWith('vault') ? 'vault' : fname.startsWith('monzo') ? 'monzo' : fname.startsWith('notifications') ? 'notifications' : null;
  if (!target) return interaction.editReply('Pick deliveries.json, downloads.json, download_history.json, vault.json, monzo_payments.json or notifications.json from the backup channel.');

  let data;
  try {
    const res = await fetch(att.url);
    data = JSON.parse(await res.text());
  } catch {
    return interaction.editReply('That file could not be read as a backup.');
  }
  if (!data || typeof data !== 'object' || (Array.isArray(data) && target !== 'notifications' && target !== 'download_history')) {
    return interaction.editReply('That file does not look like a backup.');
  }
  if (target === 'notifications') {
    notificationHistory = Array.isArray(data) ? data.filter((n) => n && typeof n === 'object').slice(-100) : [];
    saveNotifications();
    return interaction.editReply(`Restored ${notificationHistory.length} notification record(s).`);
  }
  if (target === 'download_history') {
    downloadHistory = Array.isArray(data) ? data.filter((n) => n && typeof n === 'object').slice(-500) : [];
    try { atomicWrite(DOWNLOAD_HISTORY_FILE, JSON.stringify(downloadHistory, null, 2)); } catch (e) { return interaction.editReply(`Could not save download history: ${e.message}`); }
    return interaction.editReply(`Restored ${downloadHistory.length} download history record(s).`);
  }

  const store = target === 'deliveries' ? deliveries : target === 'downloads' ? downloads : target === 'monzo' ? monzoLedger : vault;
  const filesRoot = path.resolve(target === 'vault' ? VAULT_DIR : FILES_DIR) + path.sep;
  let added = 0;
  let already = 0;
  let missing = 0;
  let skipped = 0;
  for (const [k, v] of Object.entries(data)) {
    if (!v || typeof v !== 'object') {
      skipped += 1;
      continue;
    }
    if (store[k]) {
      already += 1;
      continue;
    }
    if (target === 'downloads' && (v.used || Date.now() > v.expiresAt)) {
      skipped += 1;
      continue;
    }
    if (v.path) {
      if (!path.resolve(String(v.path)).startsWith(filesRoot)) {
        skipped += 1;
        continue;
      }
      if (!fs.existsSync(v.path)) {
        missing += 1;
        continue;
      }
    } else if (target === 'deliveries' && v.link) {
      try {
        if (!/^https?:$/.test(new URL(v.link).protocol)) throw new Error();
      } catch {
        skipped += 1;
        continue;
      }
    } else if (target !== 'vault' && target !== 'monzo' && target !== 'notifications') {
      skipped += 1;
      continue;
    }
    store[k] = v;
    added += 1;
  }
  if (target === 'deliveries') saveDeliveries();
  else if (target === 'downloads') saveDownloads();
  else if (target === 'monzo') saveMonzo();
  else saveVault();

  return interaction.editReply(
    `Restored **${added}** entr${added === 1 ? 'y' : 'ies'} into ${target}.\n` +
      `Already there: ${already}. File missing on the server: ${missing}. Skipped: ${skipped}.`
  );
}

// ---------- /currency ----------
async function cmdCurrency(interaction) {
  const amount = interaction.options.getNumber('amount');
  const from = interaction.options.getString('from').trim().toUpperCase();
  const to = interaction.options.getString('to').trim().toUpperCase();
  if (from === to) return interaction.editReply(`**${amount} ${from}** = **${amount} ${to}**`);

  const res = await fetch(
    `https://api.frankfurter.dev/v1/latest?amount=${amount}&base=${from}&symbols=${to}`
  ).catch(() => null);
  if (!res || !res.ok) {
    return interaction.editReply('Could not convert that. Use 3-letter codes like USD, GBP, EUR.');
  }
  const data = await res.json();
  const value = data.rates?.[to];
  if (value == null) return interaction.editReply('That currency was not found.');

  const embed = new EmbedBuilder()
    .setColor(GREEN)
    .setTitle('Currency conversion')
    .setDescription(`**${amount} ${from}** = **${value.toFixed(2)} ${to}**`)
    .setFooter({ text: `1 ${from} = ${(value / amount).toFixed(4)} ${to} | Rates from ${data.date} (ECB)` });
  return interaction.editReply({ embeds: [embed] });
}

// ---------- Stats commands ----------
async function succeededPayments(sinceUnix) {
  const params = { limit: 100, expand: ['data.latest_charge.balance_transaction'] };
  if (sinceUnix) params.created = { gte: sinceUnix };
  const all = await stripe.paymentIntents.list(params).autoPagingToArray({ limit: 2000 });
  return all.filter((pi) => pi.status === 'succeeded' && pi.metadata?.agent_id);
}

async function cmdPayments(interaction) {
  const count = interaction.options.getInteger('count') ?? 5;
  let card = [];
  try {
    card = await succeededPayments();
  } catch (e) {
    console.error('Stripe payments lookup failed:', e.message);
  }
  const all = [
    ...card.map((pi) => ({
      at: pi.created,
      text:
        `${money(pi.amount_received / 100, pi.currency)} - <@${pi.metadata.customer_id}> - ` +
        `${short(pi.metadata.description, 40)} - <t:${pi.created}:d> (Card)`,
    })),
    ...monzoPaid().map((m) => ({
      at: tsec(m.paidAt),
      text:
        `${money(m.amount, m.currency)} - <@${m.customerId}> - ` +
        `${short(m.description, 40)} - <t:${tsec(m.paidAt)}:d> (Monzo)`,
    })),
  ]
    .sort((a, b) => b.at - a.at)
    .slice(0, count);
  if (!all.length) return interaction.editReply('No payments found yet.');

  const embed = new EmbedBuilder()
    .setColor(GREEN)
    .setTitle(`Last ${all.length} payments`)
    .setDescription(all.map((x, i) => `**${i + 1}.** ${x.text}`).join('\n'));
  return interaction.editReply({ embeds: [embed] });
}

async function cmdTotal(interaction) {
  const period = interaction.options.getString('period');
  const since = periodStart(period);
  const everything = await succeededPayments(since);
  const list = everything.filter((pi) => pi.currency === CURRENCY.toLowerCase());
  const otherCurrency = everything.length - list.length;

  const monzoInPeriod = monzoPaid().filter((m) => tsec(m.paidAt) >= since);
  const monzoList = monzoInPeriod.filter((m) => m.currency === CURRENCY.toLowerCase());
  const monzoOther = monzoInPeriod.length - monzoList.length;
  const monzoSum = monzoList.reduce((a, m) => a + m.amount, 0);

  const cardSum = list.reduce((a, pi) => a + pi.amount_received, 0) / 100;
  const sum = cardSum + monzoSum;

  let feePence = 0;
  let refundedPence = 0;
  let missingFees = 0;
  for (const pi of list) {
    const ch = pi.latest_charge && typeof pi.latest_charge === 'object' ? pi.latest_charge : null;
    refundedPence += ch?.amount_refunded ?? 0;
    const bt = ch && ch.balance_transaction && typeof ch.balance_transaction === 'object' ? ch.balance_transaction : null;
    if (bt) feePence += bt.fee;
    else missingFees += 1;
  }
  const fees = feePence / 100;
  const refunded = refundedPence / 100;
  const keep = sum - refunded - fees;

  const perAgent = {};
  for (const pi of list) {
    const id = pi.metadata.agent_id;
    perAgent[id] = (perAgent[id] ?? 0) + pi.amount_received / 100;
  }
  for (const m of monzoList) {
    if (m.agentId) perAgent[m.agentId] = (perAgent[m.agentId] ?? 0) + m.amount;
  }
  const agentLines = Object.entries(perAgent)
    .sort((a, b) => b[1] - a[1])
    .map(([id, v]) => `<@${id}>: ${money(v, CURRENCY)}`);

  const label = { today: 'Today', week: 'This week', month: 'This month' }[period];
  const embed = new EmbedBuilder()
    .setColor(GREEN)
    .setTitle(`Takings - ${label}`)
    .addFields(
      { name: 'Total taken', value: money(sum, CURRENCY), inline: true },
      { name: 'Refunded', value: money(refunded, CURRENCY), inline: true },
      { name: 'Stripe fees', value: money(fees, CURRENCY), inline: true },
      { name: 'You keep', value: money(keep, CURRENCY), inline: true },
      { name: 'Card', value: money(cardSum, CURRENCY), inline: true },
      { name: 'Monzo', value: money(monzoSum, CURRENCY), inline: true },
      { name: 'Payments', value: String(list.length + monzoList.length), inline: true },
      { name: 'By sales agent', value: agentLines.join('\n') || '—' }
    )
    .setFooter({
      text:
        'Card + Monzo payments. Stripe does not return its fee on refunds; Monzo payments have no fees or refunds tracked.' +
        (otherCurrency + monzoOther ? ` ${otherCurrency + monzoOther} payment(s) in other currencies not counted.` : '') +
        (missingFees ? ` Fee unavailable for ${missingFees} payment(s).` : ''),
    });
  return interaction.editReply({ embeds: [embed] });
}

async function cmdCustomer(interaction) {
  const user = interaction.options.getUser('user');
  let card = [];
  try {
    const res = await stripe.paymentIntents.search({
      query: `status:'succeeded' AND metadata['customer_id']:'${user.id}'`,
      limit: 100,
    });
    card = res.data;
  } catch (e) {
    console.error('Stripe customer lookup failed:', e.message);
  }
  const monzo = monzoPaid().filter((m) => m.customerId === user.id);
  const all = [
    ...card.map((pi) => ({
      at: pi.created,
      amount: pi.amount_received / 100,
      text: `${money(pi.amount_received / 100, pi.currency)} - ${short(pi.metadata.description, 40)} - <t:${pi.created}:d> (Card)`,
    })),
    ...monzo.map((m) => ({
      at: tsec(m.paidAt),
      amount: m.amount,
      text: `${money(m.amount, m.currency)} - ${short(m.description, 40)} - <t:${tsec(m.paidAt)}:d> (Monzo)`,
    })),
  ].sort((a, b) => b.at - a.at);
  if (!all.length) return interaction.editReply(`<@${user.id}> hasn't paid you anything yet.`);

  const sum = all.reduce((a, x) => a + x.amount, 0);
  const embed = new EmbedBuilder()
    .setColor(GREEN)
    .setTitle(`Payments from ${user.username}`)
    .setDescription(all.slice(0, 15).map((x) => x.text).join('\n'))
    .addFields(
      { name: 'Total paid', value: money(sum, CURRENCY), inline: true },
      { name: 'Payments', value: String(all.length), inline: true }
    );
  return interaction.editReply({ embeds: [embed] });
}

async function cmdHistory(interaction) {
  const user = interaction.options.getUser('user');
  let card = [];
  try {
    const sessions = await stripe.checkout.sessions.list({ limit: 100 }).autoPagingToArray({ limit: 500 });
    card = sessions.filter((s) => s.metadata?.customer_id === user.id);
  } catch (e) {
    console.error('Stripe history lookup failed:', e.message);
  }
  const monzo = Object.values(monzoLedger).filter((m) => m.customerId === user.id);
  const status = (s) => (s.payment_status === 'paid' ? 'Paid' : s.status === 'expired' ? 'Expired' : 'Unpaid');
  const mStatus = (m) => ({ paid: 'Paid', cancelled: 'Cancelled', expired: 'Expired' }[m.status] ?? 'Unpaid');
  const all = [
    ...card.map((s) => ({
      at: s.created,
      text: `**${status(s)}** - ${money((s.amount_total ?? 0) / 100, s.currency)} - ${short(s.metadata?.description, 40)} - <t:${s.created}:d> (Card)`,
    })),
    ...monzo.map((m) => ({
      at: tsec(m.createdAt),
      text: `**${mStatus(m)}** - ${money(m.amount, m.currency)} - ${short(m.description, 40)} - <t:${tsec(m.createdAt)}:d> (Monzo)`,
    })),
  ].sort((a, b) => b.at - a.at);
  if (!all.length) return interaction.editReply(`No links found for <@${user.id}>.`);

  const embed = new EmbedBuilder()
    .setColor(GREEN)
    .setTitle(`Links made for ${user.username}`)
    .setDescription(all.slice(0, 20).map((x) => x.text).join('\n'))
    .setFooter({ text: `Showing ${Math.min(all.length, 20)} of ${all.length}. Card and Monzo.` });
  return interaction.editReply({ embeds: [embed] });
}

// ---------- /setup ----------
async function cmdSetup(interaction) {
  if (interaction.user.id !== OWNER_ID) {
    return interaction.reply({ content: 'Only the owner can run this.', ephemeral: true });
  }
  if (!interaction.guild) {
    return interaction.reply({ content: 'Run this inside your server, not in a DM.', ephemeral: true });
  }
  await interaction.deferReply({ ephemeral: true });

  const guild = interaction.guild;
  const view = [PermissionFlagsBits.ViewChannel, PermissionFlagsBits.ReadMessageHistory];
  const overwrites = [
    { id: guild.roles.everyone.id, deny: [PermissionFlagsBits.ViewChannel] },
    {
      id: client.user.id,
      allow: [
        ...view,
        PermissionFlagsBits.SendMessages,
        PermissionFlagsBits.EmbedLinks,
        PermissionFlagsBits.AttachFiles,
      ],
    },
  ];
  const seen = new Set();
  for (const id of allowed) {
    if (seen.has(id)) continue;
    seen.add(id);
    const member = await guild.members.fetch(id).catch(() => null);
    if (member) overwrites.push({ id, allow: view });
  }

  const wanted = [
    ['bankroll-log', 'Private log of Bankroll invoices, payments, refunds and disputes', 'Log channel ready.'],
    ['bankroll-backup', 'Private data backups from the Bankroll bot', 'Backup channel ready.'],
  ];
  const results = [];
  for (const [name, topic, hello] of wanted) {
    const existing = guild.channels.cache.find((c) => c.name === name && c.isTextBased());
    if (existing) {
      results.push(`<#${existing.id}> already exists.`);
      continue;
    }
    try {
      const ch = await guild.channels.create({
        name,
        type: ChannelType.GuildText,
        topic,
        permissionOverwrites: overwrites,
      });
      await ch.send({ content: hello });
      results.push(`Created <#${ch.id}>.`);
    } catch (e) {
      results.push(`Couldn't create #${name}: ${e.message}.`);
    }
  }
  results.push('Only you and your allowed users can see them.');
  return interaction.editReply(results.join('\n'));
}

// ---------- Refunds ----------
const REFUND_REASONS = {
  r: { api: 'requested_by_customer', label: 'Requested by customer' },
  d: { api: 'duplicate', label: 'Duplicate' },
  f: { api: 'fraudulent', label: 'Fraudulent' },
};

async function resolvePaymentIntent(input) {
  const id = input.trim();
  const expand = { expand: ['latest_charge'] };
  if (id.startsWith('cs_')) {
    const sess = await stripe.checkout.sessions.retrieve(id);
    const piId = typeof sess.payment_intent === 'string' ? sess.payment_intent : sess.payment_intent?.id;
    if (!piId) throw new Error('That checkout has not been paid.');
    return stripe.paymentIntents.retrieve(piId, expand);
  }
  if (id.startsWith('pi_')) return stripe.paymentIntents.retrieve(id, expand);
  throw new Error('Pick a payment from the list, or paste a Checkout ID (cs_...) or payment ID (pi_...).');
}

async function onAutocomplete(interaction) {
  try {
    if (!allowed.has(interaction.user.id)) return await interaction.respond([]);
    
    // Vault remove/info autocomplete
    if (
      (interaction.commandName === 'vault' && ['remove', 'info', 'edit', 'verify', 'rename'].includes(interaction.options.getSubcommand())) ||
      interaction.commandName === 'bankroll' ||
      interaction.commandName === 'bankroll-monzo' ||
      interaction.commandName === 'sendfile'
    ) {
      const typed = interaction.options.getFocused().toLowerCase();
      const entries = Object.entries(vault);
      const filtered = entries.filter(([k, v]) => !typed || k.toLowerCase().includes(typed) || v.originalName.toLowerCase().includes(typed));
      
      filtered.sort((a, b) => {
        const aStarts = a[0].toLowerCase().startsWith(typed);
        const bStarts = b[0].toLowerCase().startsWith(typed);
        if (aStarts && !bStarts) return -1;
        if (!aStarts && bStarts) return 1;
        return 0;
      });

      const choices = filtered.slice(0, 25).map(([k, v]) => ({
        name: interaction.commandName === 'vault' ? `${k} (${v.originalName})` : `[Vault] ${k} - ${v.originalName}`,
        value: k,
      }));
      return await interaction.respond(choices);
    }

    // Cancel command autocomplete for open checkout sessions / invoices
    if (interaction.commandName === 'cancel') {
      const typed = interaction.options.getFocused().toLowerCase();
      let sessions = { data: [] };
      try {
        sessions = await stripe.checkout.sessions.list({ limit: 50, status: 'open' });
      } catch (e) {
        console.error('Stripe cancel autocomplete failed:', e.message);
      }
      const card = sessions.data
        .filter((s) => s.metadata?.agent_id)
        .filter((s) => !typed || `${s.metadata.description} ${s.id}`.toLowerCase().includes(typed))
        .map((s) => ({
          name: `[Card] ${money((s.amount_total ?? 0) / 100, s.currency)} - ${short(s.metadata.description, 50)} - ID: ${s.id}`.slice(0, 100),
          value: s.id,
        }));
      const monzo = monzoOpen()
        .filter((m) => !typed || `${m.description} ${m.reference} ${m.key}`.toLowerCase().includes(typed))
        .map((m) => ({
          name: `[Monzo] ${money(m.amount, m.currency)} - ${short(m.description, 50)} - ${m.reference}`.slice(0, 100),
          value: m.key,
        }));
      return await interaction.respond([...monzo, ...card].slice(0, 25));
    }

    if (interaction.commandName !== 'refund' || interaction.user.id !== OWNER_ID) {
      return await interaction.respond([]);
    }
    const typed = interaction.options.getFocused().toLowerCase();
    const list = await stripe.paymentIntents.list({ limit: 50 });
    const choices = list.data
      .filter((pi) => pi.status === 'succeeded' && pi.metadata?.agent_id)
      .filter((pi) => !typed || `${pi.metadata.description} ${pi.id}`.toLowerCase().includes(typed))
      .slice(0, 25)
      .map((pi) => ({
        name: `${money(pi.amount_received / 100, pi.currency)} - ${short(pi.metadata.description, 50)} - ${new Date(pi.created * 1000).toLocaleDateString('en-GB')}`.slice(0, 100),
        value: pi.id,
      }));
    await interaction.respond(choices);
  } catch (e) {
    console.error('Autocomplete failed:', e.message);
    try {
      await interaction.respond([]);
    } catch {}
  }
}

async function cmdRefund(interaction) {
  if (interaction.user.id !== OWNER_ID) {
    return interaction.reply({ content: 'Only the owner can refund payments.', ephemeral: true });
  }
  await interaction.deferReply({ ephemeral: true });

  const pi = await resolvePaymentIntent(interaction.options.getString('order'));
  const charge = pi.latest_charge && typeof pi.latest_charge === 'object' ? pi.latest_charge : null;
  if (pi.status !== 'succeeded' || !charge) {
    return interaction.editReply('That payment was not completed.');
  }
  if (charge.disputed) {
    return interaction.editReply('That payment is under dispute.');
  }

  const refundable = charge.amount - charge.amount_refunded;
  if (refundable <= 0) return interaction.editReply('That payment has already been fully refunded.');

  const wanted = interaction.options.getNumber('amount');
  const pence = wanted ? Math.round(wanted * 100) : refundable;
  if (pence > refundable) {
    return interaction.editReply(`You can refund at most ${money(refundable / 100, pi.currency)}.`);
  }

  const code = interaction.options.getString('reason') ?? 'r';
  const m = pi.metadata ?? {};
  const embed = new EmbedBuilder()
    .setColor(RED)
    .setTitle('Confirm refund')
    .addFields(
      { name: 'Refund amount', value: money(pence / 100, pi.currency), inline: true },
      { name: 'Originally paid', value: money(pi.amount_received / 100, pi.currency), inline: true },
      { name: 'Reason', value: REFUND_REASONS[code].label, inline: true },
      { name: 'Customer', value: m.customer_id ? `<@${m.customer_id}>` : 'Unknown', inline: true },
      { name: 'Description', value: short(m.description, 200) },
      { name: 'Payment ID', value: pi.id }
    );

  const row = new ActionRowBuilder().addComponents(
    new ButtonBuilder().setCustomId(`refund_yes:${pi.id}:${pence}:${code}`).setLabel('Confirm refund').setStyle(ButtonStyle.Danger),
    new ButtonBuilder().setCustomId('refund_no').setLabel('Cancel').setStyle(ButtonStyle.Secondary)
  );
  return interaction.editReply({ embeds: [embed], components: [row] });
}

async function onRefundButton(interaction) {
  if (interaction.user.id !== OWNER_ID) {
    return interaction.reply({ content: 'Only the owner can refund payments.', ephemeral: true });
  }
  if (interaction.customId === 'refund_no') {
    return interaction.update({ content: 'Refund cancelled.', embeds: [], components: [] });
  }

  const [, piId, penceStr, code] = interaction.customId.split(':');
  const pence = parseInt(penceStr, 10);
  const reason = REFUND_REASONS[code] ?? REFUND_REASONS.r;
  await interaction.deferUpdate();

  try {
    const refund = await stripe.refunds.create(
      {
        payment_intent: piId,
        amount: pence,
        reason: reason.api,
        metadata: { refunded_by: interaction.user.id },
      },
      { idempotencyKey: `refund-${interaction.message.id}` }
    );

    await recordInStripe(piId, {
      last_refund_at: new Date().toISOString(),
      last_refund_amount: (pence / 100).toFixed(2),
      last_refund_reason: reason.api,
    });

    const pi = await stripe.paymentIntents.retrieve(piId, { expand: ['latest_charge'] });
    const m = pi.metadata ?? {};
    const fullyRefunded = !!(pi.latest_charge && typeof pi.latest_charge === 'object' && pi.latest_charge.refunded);

    if (fullyRefunded) {
      const sess = await stripe.checkout.sessions.list({ payment_intent: piId, limit: 1 });
      const sid = sess.data[0]?.id;
      if (sid) discardPending(sid);
    }

    if (m.customer_id) {
      try {
        const u = await client.users.fetch(m.customer_id);
        await u.send({
          embeds: [
            new EmbedBuilder()
              .setColor(GREEN)
              .setTitle('Refund issued')
              .setDescription(`You've been refunded **${money(refund.amount / 100, refund.currency)}** for **${short(m.description, 100)}**.`),
          ],
        });
      } catch {}
    }

    const done = new EmbedBuilder()
      .setColor(GREEN)
      .setTitle('Refund sent')
      .addFields(
        { name: 'Amount', value: money(refund.amount / 100, refund.currency), inline: true },
        { name: 'Status', value: refund.status, inline: true },
        { name: 'Reason', value: reason.label, inline: true },
        { name: 'Refund ID', value: refund.id }
      );
    await interaction.editReply({ content: '', embeds: [done], components: [] });
    await logToServer({ embeds: [done] });
  } catch (err) {
    console.error('Refund failed:', err);
    await interaction.editReply({ content: `Refund failed: ${err.message}`, embeds: [], components: [] });
  }
}

async function recordInStripe(paymentIntentId, fields) {
  if (!paymentIntentId) return;
  try {
    await stripe.paymentIntents.update(paymentIntentId, { metadata: fields });
  } catch (e) {
    console.error('Could not save record to Stripe:', e.message);
  }
}

function withoutGetFile(message) {
  const rows = [];
  for (const r of message.components) {
    const row = ActionRowBuilder.from(r);
    row.setComponents(row.components.filter((b) => !(b.data.custom_id ?? '').startsWith('getfile:')));
    if (row.components.length) rows.push(row);
  }
  return rows;
}

async function onGetFile(interaction) {
  const key = interaction.customId.slice('getfile:'.length);
  const d = deliveries[key];
  if (!d) {
    await interaction.update({ components: withoutGetFile(interaction.message) });
    return interaction.followUp({ content: 'The file was already sent or is no longer available.', ephemeral: true });
  }
  if (interaction.user.id !== d.customerId && !allowed.has(interaction.user.id)) {
    return interaction.reply({ content: 'Only the customer can get this file.', ephemeral: true });
  }

  let checkPi = null;
  if (d.type === 'monzo') {
    if (!d.paid) return interaction.reply({ content: 'Payment has not been received yet.', ephemeral: true });
  } else {
    const session = await stripe.checkout.sessions.retrieve(key);
    if (session.payment_status !== 'paid') return interaction.reply({ content: 'Payment has not been received yet.', ephemeral: true });
    checkPi = typeof session.payment_intent === 'string' ? session.payment_intent : session.payment_intent?.id;
    if (checkPi) {
      const pi = await stripe.paymentIntents.retrieve(checkPi, { expand: ['latest_charge'] });
      if (pi.latest_charge && typeof pi.latest_charge === 'object' && pi.latest_charge.refunded) {
        discardPending(key);
        return interaction.reply({ content: 'This payment was refunded.', ephemeral: true });
      }
    }
  }

  if (d.path && fs.existsSync(d.path)) {
    try { verifyZipIntegrity(d.path); }
    catch (e) {
      recordDownloadEvent('verification_failed', { file: d.name, customerId: d.customerId, agentId: d.agentId, detail: e.message });
      return interaction.reply({ content: `I stopped this delivery because the file failed its integrity check: ${short(e.message, 180)}. Please contact the seller.`, ephemeral: true });
    }
  }
  let dlink = d.link || null;
  let hosted = false;
  if (d.fallbackLink) {
    const token = d.fallbackLink.split('/').pop();
    const savedLink = downloads[token];
    if (savedLink && !savedLink.used && Date.now() < savedLink.expiresAt && fs.existsSync(savedLink.path)) {
      dlink = d.fallbackLink;
      hosted = true;
    } else {
      delete d.fallbackLink;
      saveDeliveries();
    }
  }
  if (!dlink && d.path && fs.existsSync(d.path) && BASE_URL) {
    const size = fs.statSync(d.path).size;
    if (d.mode === 'link' || size > MAX_FILE_BYTES) {
      dlink = makeDownloadLink(d, checkPi);
      hosted = true;
      d.fallbackLink = dlink;
      saveDeliveries();
    }
  }
  if (!dlink && (!d.path || !fs.existsSync(d.path))) {
    delete deliveries[key];
    saveDeliveries();
    return interaction.reply({ content: 'Sorry, the file is no longer available.', ephemeral: true });
  }

  await interaction.deferUpdate();
  const isZip = /\.zip$/i.test(String(d.name || ''));
  const getPayload = () => dlink
    ? {
        content: `<@${d.customerId}> here is your secure download. The link works once and expires in 24 hours.`,
        embeds: [buildDownloadEmbed(d.name, downloads[dlink.split('/').pop()]?.expiresAt, d.path && fs.existsSync(d.path) ? fs.statSync(d.path).size : undefined)],
        components: [downloadLinkRow(dlink, isZip ? 'Download ZIP' : 'Download File')],
        allowedMentions: { parse: [] },
      }
    : {
        content: `<@${d.customerId}> here is your complete file.`,
        embeds: [buildDownloadEmbed(d.name, null, d.path && fs.existsSync(d.path) ? fs.statSync(d.path).size : undefined)],
        files: [new AttachmentBuilder(d.path, { name: path.basename(d.name || 'download.zip') })],
        allowedMentions: { parse: [] },
      };

  let msg;
  try {
    msg = await interaction.followUp(getPayload());
  } catch (attachmentError) {
    console.error('Single-file attachment failed:', attachmentError.message);
    if (dlink || !BASE_URL || !d.path || !fs.existsSync(d.path)) {
      return interaction.followUp({ content: `Discord could not send the file (${short(attachmentError.message, 180)}). Your paid file is still saved; please contact the seller.`, ephemeral: true });
    }
    try {
      dlink = makeDownloadLink(d, checkPi);
      hosted = true;
      d.fallbackLink = dlink;
      saveDeliveries();
      msg = await interaction.followUp(getPayload());
    } catch (linkError) {
      console.error('One-time download link fallback failed:', linkError.message);
      return interaction.followUp({ content: 'Discord rejected the ZIP and the one-time download link could not be created. Your paid file is still saved; please contact the seller.', ephemeral: true });
    }
  }

  const old = interaction.message.embeds[0];
  await interaction.message.edit({
    embeds: old ? [EmbedBuilder.from(old).setColor(GREY).setTitle(String(old.title || '').startsWith('PAID') ? old.title : `PAID - ${old.title}`)] : [],
    components: [],
  }).catch(() => {});

  if (d.path && !d.vaultKey) { try { fs.unlinkSync(d.path); } catch {} }
  delete deliveries[key];
  saveDeliveries();
  liveInteractions.delete(key);
  await recordInStripe(checkPi, {
    delivered_at: new Date().toISOString(),
    delivered_to: 'Download ZIP button pressed',
    delivered_file: String(d.name || '').slice(0, 100),
    delivery_message: msg.url ?? '',
  });
  recordDownloadEvent('delivery_sent', { file: d.name, customerId: d.customerId, agentId: d.agentId, detail: 'Customer requested the paid file; delivery message was sent.' });
  await notifyOwner({ content: `"${d.name}" was delivered to <@${d.customerId}>. Hosted downloads are tracked separately when the file is actually downloaded.` });
}

function randomSearchGb(durationMinutes) {
  const center = Math.max(2, 2 + ((durationMinutes - 2) * 198) / 118);
  const jitter = 0.82 + Math.random() * 0.36;
  return Math.max(2, Math.round(center * jitter));
}
function sendfileProgress(job, now = Date.now()) {
  const total = Math.max(1, job.dueAt - job.createdAt);
  const pct = Math.max(0, Math.min(100, Math.floor(((now - job.createdAt) / total) * 100)));
  const filled = Math.round(pct / 10);
  const bar = '█'.repeat(filled) + '░'.repeat(10 - filled);
  const eta = Math.floor(job.dueAt / 1000);
  const totalGb = job.searchGb || randomSearchGb(total / 60000);
  const scannedGb = Math.min(totalGb, Math.round((totalGb * pct) / 100));
  const done = now >= job.dueAt;
  const state = done
    ? `Search complete - **${totalGb} GB** scanned, file located`
    : `Searching through **${scannedGb} GB** of **${totalGb} GB** of disk space`;
  return `**Scheduled file: ${job.originalName}**\n${bar} **${pct}%**\n${state}\nEstimated completion: <t:${eta}:F> (<t:${eta}:R>)\nPress **Check progress / Get File** to refresh. At 100% the same button sends your file.`;
}
function sendfileButton(jobId) {
  return new ActionRowBuilder().addComponents(
    new ButtonBuilder().setCustomId(`sendfile:${jobId}`).setLabel('Check progress / Get File').setStyle(ButtonStyle.Primary)
  );
}
async function startScheduledSend(interaction) {
  if (interaction.inGuild() || interaction.channel?.type !== ChannelType.DM) {
    return interaction.reply({ content: 'Run /sendfile inside a one-to-one private DM, not in a server or group DM.', ephemeral: true });
  }
  let key = interaction.options.getString('file');
  if (!key) {
    // No file picked: use the newest ZIP in the vault.
    const newest = Object.entries(vault)
      .filter(([, v]) => v?.path && /\.zip$/i.test(String(v.originalName || v.path)) && fs.existsSync(v.path))
      .sort((a, b) => (b[1].addedAt || 0) - (a[1].addedAt || 0))[0];
    if (!newest) {
      return interaction.reply({ content: 'There is no ZIP in your vault yet. Add one with /vault add first.', ephemeral: true });
    }
    key = newest[0];
  }
  const item = vault[key];
  if (!item || !item.path || !fs.existsSync(item.path)) {
    return interaction.reply({ content: `Vault file \`${key}\` is unavailable. Check it with /vault verify.`, ephemeral: true });
  }
  if (!/\.zip$/i.test(String(item.originalName || item.path))) {
    return interaction.reply({ content: ' /sendfile currently accepts ZIP files only. Add a .zip file to the vault first.', ephemeral: true });
  }
  try { new AdmZip(item.path).getEntries(); }
  catch { return interaction.reply({ content: 'That vault item is not a readable ZIP file. Check it with /vault verify.', ephemeral: true }); }
  const amount = interaction.options.getInteger('size', true);
  const unit = interaction.options.getString('disk', true);
  const multiplier = unit === 'h' ? 60 * 60 * 1000 : 60 * 1000;
  const durationMs = amount * multiplier;
  if (durationMs > 30 * 24 * 60 * 60 * 1000) {
    return interaction.reply({ content: 'The maximum delay is 30 days.', ephemeral: true });
  }
  const jobId = crypto.randomBytes(9).toString('hex');
  const now = Date.now();
  const job = {
    id: jobId,
    vaultKey: key,
    originalName: String(item.originalName || path.basename(item.path)),
    ownerId: interaction.user.id,
    channelId: interaction.channelId,
    createdAt: now,
    dueAt: now + durationMs,
    searchGb: randomSearchGb(durationMs / 60000),
    status: 'waiting',
  };
  sendJobs[jobId] = job;
  saveSendJobs();
  await interaction.reply({ content: sendfileProgress(job, now), components: [sendfileButton(jobId)], allowedMentions: { parse: [] } });
  try {
    const msg = await interaction.fetchReply();
    job.messageId = msg.id;
    saveSendJobs();
  } catch (e) { console.error('Could not save /sendfile message ID:', e.message); }
}
// Large archives are never split. If a complete ZIP exceeds Discord's upload limit,
// the bot serves the original ZIP through a secure one-time download link.

async function onScheduledSendButton(interaction) {
  const jobId = interaction.customId.slice('sendfile:'.length);
  const job = sendJobs[jobId];
  if (!job || job.channelId !== interaction.channelId) {
    return interaction.reply({ content: 'This scheduled file request could not be found in this DM.', ephemeral: true });
  }
  if (job.status === 'sent' || job.status === 'link_sent') {
    return interaction.reply({ content: job.status === 'sent' ? 'This ZIP has already been sent.' : 'The one-time download link has already been sent.', ephemeral: true });
  }
  if (job.status === 'sending') return interaction.reply({ content: 'Your file is being prepared right now, one moment.', ephemeral: true });
  if (Date.now() < job.dueAt) {
    return interaction.update({ content: sendfileProgress(job), components: [sendfileButton(jobId)], allowedMentions: { parse: [] } });
  }

  const item = vault[job.vaultKey];
  if (!item?.path || !fs.existsSync(item.path)) {
    return interaction.update({ content: `The scheduled time has arrived, but vault file \`${job.vaultKey}\` is missing. Re-add it, then contact the bot owner.`, components: [sendfileButton(jobId)] });
  }
  try{verifyZipIntegrity(item.path);}catch(e){recordDownloadEvent('verification_failed',{file:job.originalName,customerId:interaction.user.id,detail:e.message});return interaction.update({content:`I stopped this delivery because the ZIP failed its integrity check: ${short(e.message,180)}. Please contact the bot owner.`,components:[]});}

  job.status = 'sending';
  await interaction.update({ content: `${sendfileProgress(job)}\n\nPreparing your complete ZIP now...`, components: [], allowedMentions: { parse: [] } });

  const finish = async (status, note) => {
    job.status = status;
    job.completedAt = Date.now();
    saveSendJobs();
    await interaction.editReply({ content: `${sendfileProgress(job)}\n\n${note}`, components: [], allowedMentions: { parse: [] } }).catch(() => {});
  };

  const sendHostedLink = async () => {
    const link = makeDownloadLink({
      path: item.path,
      name: path.basename(job.originalName || item.originalName || item.path),
      customerId: interaction.user.id,
      agentId: OWNER_ID,
      vaultKey: job.vaultKey,
      description: 'scheduled-vault-file',
    }, null);
    await interaction.followUp({
      content: `**${job.originalName}** is ready. This is the original complete ZIP; the secure link works once and expires in 24 hours.`,
      embeds: [new EmbedBuilder().setColor(GREEN).setTitle('Your ZIP is ready').setDescription('Use the button below to download the complete original archive.')],
      components: [downloadLinkRow(link, 'Download ZIP')],
      allowedMentions: { parse: [] },
    });
    recordDownloadEvent('delivery_sent', { file: job.originalName, customerId: interaction.user.id, agentId: OWNER_ID, detail: 'Scheduled ZIP delivery link sent.' });
    await finish('link_sent', 'One-time Download ZIP button sent below.');
  };

  try {
    const fileSize = fs.statSync(item.path).size;
    if (fileSize > MAX_FILE_BYTES) {
      if (!BASE_URL) throw new Error('The ZIP exceeds Discord upload limits and PUBLIC_URL is not configured.');
      return await sendHostedLink();
    }
    try {
      await interaction.followUp({
        content: `**${job.originalName}** - scheduled delivery complete. The original ZIP is attached below.`,
        files: [new AttachmentBuilder(item.path, { name: path.basename(job.originalName) })],
        allowedMentions: { parse: [] },
      });
      recordDownloadEvent('delivery_sent', { file: job.originalName, customerId: interaction.user.id, agentId: OWNER_ID, detail: 'Complete ZIP attachment sent directly.' });
      return await finish('sent', 'Complete ZIP sent below.');
    } catch (uploadError) {
      console.error('Scheduled ZIP attachment failed:', uploadError.message);
      if (!BASE_URL) throw uploadError;
      return await sendHostedLink();
    }
  } catch (error) {
    console.error('Scheduled ZIP delivery failed:', error.message);
    job.status = 'waiting';
    saveSendJobs();
    const failure = `I couldn't deliver **${job.originalName}**. ${error.message} Press **Check progress / Get File** to retry.`;
    await interaction.editReply({ content: `${sendfileProgress(job)}\n\n${failure}`, components: [sendfileButton(jobId)], allowedMentions: { parse: [] } }).catch(() => {});
  }
}

async function onButton(interaction) {
  if (interaction.customId.startsWith('sendfile:')) return onScheduledSendButton(interaction);
  if (interaction.customId.startsWith('getfile:')) return onGetFile(interaction);
  if (interaction.customId.startsWith('refund_')) return onRefundButton(interaction);
  if (!interaction.customId.startsWith('delivered:')) return;
  if (!allowed.has(interaction.user.id)) {
    return interaction.reply({ content: 'Only the seller can do this.', ephemeral: true });
  }
  const piId = interaction.customId.slice('delivered:'.length);
  const now = new Date();
  await recordInStripe(piId, {
    delivered_at: now.toISOString(),
    delivered_to: 'Marked as delivered manually',
    delivered_by: interaction.user.id,
  });
  const updated = EmbedBuilder.from(interaction.message.embeds[0]).addFields({
    name: 'Delivered',
    value: `<t:${Math.floor(now.getTime() / 1000)}:f>`,
  });
  await interaction.update({ embeds: [updated], components: [] });
}

async function onDisputeCreated(d) {
  const piId = typeof d.payment_intent === 'string' ? d.payment_intent : d.payment_intent?.id;
  const pi = piId ? await stripe.paymentIntents.retrieve(piId) : null;
  const dash = `https://dashboard.stripe.com${d.livemode ? '' : '/test'}/disputes/${d.id}`;

  const embed = new EmbedBuilder()
    .setColor(RED)
    .setTitle('DISPUTE OPENED')
    .addFields(
      { name: 'Amount', value: money(d.amount / 100, d.currency), inline: true },
      { name: 'Reason', value: String(d.reason ?? 'unknown'), inline: true },
      { name: 'Open in Stripe', value: dash }
    );
  await notifyOwner({ embeds: [embed] });
}

async function onDisputeClosed(d) {
  const won = d.status === 'won';
  const embed = new EmbedBuilder()
    .setColor(won ? GREEN : RED)
    .setTitle(`Dispute ${d.status.replace(/_/g, ' ')}`)
    .addFields(
      { name: 'Amount', value: money(d.amount / 100, d.currency), inline: true }
    );
  await notifyOwner({ embeds: [embed] });
}

// ---------- Interaction router ----------
client.on('interactionCreate', async (interaction) => {
  try {
    if (interaction.isAutocomplete()) return await onAutocomplete(interaction);
    if (interaction.isButton()) return await onButton(interaction);
    if (!interaction.isChatInputCommand()) return;
    if (!commands.some((c) => c.name === interaction.commandName)) return;

    if (!allowed.has(interaction.user.id)) {
      return interaction.reply({
        content: "You aren't allowed to use this command.",
        ephemeral: true,
      });
    }

    switch (interaction.commandName) {
      case 'bankroll':
        return await sendStripeCheckout(interaction);
      case 'bankroll-monzo':
        return await sendMonzoInvoice(interaction);
      case 'vault':
        return await cmdVault(interaction);
      case 'sendfile':
        return await startScheduledSend(interaction);
      case 'downloads':
        await interaction.deferReply({ ephemeral: true });
        return await cmdDownloads(interaction);
      case 'cancel':
        return await cmdCancel(interaction);
      case 'remind':
        return await cmdRemind(interaction);
      case 'restore':
        await interaction.deferReply({ ephemeral: true });
        return await cmdRestore(interaction);
      case 'unpaid':
        await interaction.deferReply({ ephemeral: true });
        return await cmdUnpaid(interaction);
      case 'status':
        await interaction.deferReply({ ephemeral: true });
        return await cmdStatus(interaction);
      case 'notifications':
        await interaction.deferReply({ ephemeral: true });
        return await cmdNotifications(interaction);
      case 'currency':
        await interaction.deferReply({ ephemeral: true });
        return await cmdCurrency(interaction);
      case 'refund':
        return await cmdRefund(interaction);
      case 'setup':
        return await cmdSetup(interaction);
      default:
        await interaction.deferReply({ ephemeral: true });
        if (interaction.commandName === 'payments') return await cmdPayments(interaction);
        if (interaction.commandName === 'total') return await cmdTotal(interaction);
        if (interaction.commandName === 'customer') return await cmdCustomer(interaction);
        if (interaction.commandName === 'history') return await cmdHistory(interaction);
    }
  } catch (err) {
    alertOwner(`/${interaction.commandName ?? interaction.customId?.split(':')[0]} failed`, err);
    try {
      if (interaction.deferred || interaction.replied) {
        await interaction.editReply({ content: `Something went wrong: ${err.message}` });
      } else {
        await interaction.reply({ content: `Something went wrong: ${err.message}`, ephemeral: true });
      }
    } catch {}
  }
});

// ---------- Monzo Webhook Listener ----------
const app = express();

const processedMonzoTx = new Set();

app.post('/monzo-webhook', express.json(), async (req, res) => {
  if (MONZO_WEBHOOK_TOKEN && req.query.token !== MONZO_WEBHOOK_TOKEN) {
    console.error('Monzo webhook rejected: bad or missing token');
    return res.status(403).send('Forbidden');
  }
  console.log('Monzo webhook hit! Type:', req.body?.type);
  res.status(200).send('OK');

  try {
    const data = req.body?.data;
    if (req.body?.type !== 'transaction.created' || !data) return;
    if (MONZO_ACCOUNT_ID && data.account_id && data.account_id !== MONZO_ACCOUNT_ID) return;
    if (data.id) {
      if (processedMonzoTx.has(data.id)) return;
      processedMonzoTx.add(data.id);
    }

    // If an access token is set, double-check the transaction with Monzo's API (best effort:
    // Monzo tokens expire, so on any failure we fall back to the webhook data).
    let tx = data;
    if (MONZO_ACCESS_TOKEN && data.id) {
      try {
        const r = await fetch(`https://api.monzo.com/transactions/${encodeURIComponent(data.id)}`, {
          headers: { Authorization: `Bearer ${MONZO_ACCESS_TOKEN}` },
        });
        if (r.ok) tx = (await r.json()).transaction ?? data;
        else console.log(`Monzo API check skipped (${r.status}); using webhook data. Token may have expired.`);
      } catch (e) {
        console.log('Monzo API check failed:', e.message);
      }
    }
    if (tx.decline_reason) return;

    const amountPaid = Math.round(Number(tx.amount)) / 100;
    if (!(amountPaid > 0)) return;
    console.log(`Monzo incoming payment: ${amountPaid}`);
    lastMonzoAt = Date.now();

    const text = [tx.description, tx.notes, tx.counterparty?.name, tx.metadata?.notes, data.description, data.notes]
      .filter(Boolean)
      .join(' ')
      .toUpperCase();

    const open = Object.entries(deliveries).filter(
      ([, d]) => d.type === 'monzo' && !d.paid && Math.abs(amountPaid - d.amount) < 0.01
    );
    let matches = open.filter(([, d]) => d.reference && text.includes(d.reference));
    if (!matches.length) matches = open;

    if (matches.length === 1) {
      const [key] = matches[0];
      const d = deliveries[key];
      markMonzoPaid(key, data.id);

      const paidEmbed = new EmbedBuilder()
        .setColor(GREEN)
        .setTitle('Bankroll Monzo Payment Received')
        .setDescription(d.description || 'No description')
        .addFields(
          { name: 'Customer', value: d.customerId ? `<@${d.customerId}>` : 'Unknown', inline: true },
          { name: 'Sales Agent', value: d.agentId ? `<@${d.agentId}>` : 'Unknown', inline: true },
          { name: 'Amount', value: money(amountPaid, 'gbp'), inline: true },
          { name: 'Payment reference', value: d.reference ? `\`${d.reference}\`` : 'None', inline: true },
          { name: 'Transaction ID', value: String(data.id ?? 'Unknown') }
        );
      await notifyOwner({ embeds: [paidEmbed] });

      await deliverFile(key, null).catch((e) => alertOwner('Monzo file delivery failed', e));
      console.log(`Monzo payment delivered for invoice ${key} (${amountPaid})`);
    } else if (matches.length > 1) {
      await notifyOwner({
        content: `Monzo payment of ${money(amountPaid, 'gbp')} arrived but ${matches.length} open invoices match that amount, so nothing was sent automatically. Check which customer paid before sending anything.`,
      });
    } else {
      // Nothing matched: still tell the owner, so a payment is never silent.
      const byRef = Object.entries(deliveries).find(
        ([, d]) => d.type === 'monzo' && !d.paid && d.reference && text.includes(d.reference)
      );
      await notifyOwner({
        content: byRef
          ? `Monzo payment of ${money(amountPaid, 'gbp')} arrived with reference \`${byRef[1].reference}\` for <@${byRef[1].customerId}>, but that invoice is for ${money(byRef[1].amount, 'gbp')}, so nothing was sent automatically. Check it and use /cancel or send the file yourself.`
          : `Monzo payment of ${money(amountPaid, 'gbp')} arrived${tx.counterparty?.name ? ` from ${tx.counterparty.name}` : ''}, but no open Monzo invoice matches it, so nothing was sent.`,
      });
    }
  } catch (e) {
    alertOwner('Monzo webhook error', e);
  }
});

// ---------- Stripe webhook ----------

app.post('/webhook', express.raw({ type: 'application/json' }), async (req, res) => {
  let event;
  try {
    event = stripe.webhooks.constructEvent(
      req.body,
      req.headers['stripe-signature'],
      STRIPE_WEBHOOK_SECRET
    );
  } catch (err) {
    console.error('Webhook signature failed:', err.message);
    return res.status(400).send(`Webhook Error: ${err.message}`);
  }

  lastWebhookAt = Date.now();
  lastWebhookType = event.type;

  if (event.type === 'checkout.session.completed') {
    const s = event.data.object;
    if (s.payment_status === 'paid') {
      await recordInStripe(s.payment_intent, { paid_at: new Date(event.created * 1000).toISOString() });
      try {
        const { customer_id, agent_id, description } = s.metadata ?? {};
        const total = (s.amount_total ?? 0) / 100;

        const embed = new EmbedBuilder()
          .setColor(GREEN)
          .setTitle('Bankroll Checkout')
          .setDescription(description || 'No description')
          .addFields(
            { name: 'Customer', value: customer_id ? `<@${customer_id}>` : 'Unknown', inline: true },
            { name: 'Sales Agent', value: agent_id ? `<@${agent_id}>` : 'Unknown', inline: true },
            { name: 'Amount', value: total.toFixed(2), inline: true },
            { name: 'Transaction ID', value: s.id }
          );

        const components = [];
        if (!deliveries[s.id] && s.payment_intent) {
          components.push(
            new ActionRowBuilder().addComponents(
              new ButtonBuilder()
                .setCustomId(`delivered:${s.payment_intent}`)
                .setLabel('Mark as delivered')
                .setStyle(ButtonStyle.Primary)
            )
          );
        }

        await notifyOwner({ embeds: [embed], components });
      } catch (err) {
        console.error('Failed to DM owner:', err);
      }
      await deliverFile(s.id, s.payment_intent).catch((e) => alertOwner('File delivery failed', e));
    }
  }

  if (event.type === 'checkout.session.expired') {
    const s = event.data.object;
    if (s.metadata?.agent_id) {
      discardPending(s.id);
      const embed = new EmbedBuilder()
        .setColor(GREY)
        .setTitle('Invoice expired unpaid')
        .addFields(
          { name: 'Customer', value: s.metadata.customer_id ? `<@${s.metadata.customer_id}>` : 'Unknown', inline: true },
          { name: 'Sales Agent', value: s.metadata.agent_id ? `<@${s.metadata.agent_id}>` : 'Unknown', inline: true }
        );
      await notifyOwner({ embeds: [embed] });
    }
  }

  if (event.type === 'charge.dispute.created') {
    await onDisputeCreated(event.data.object).catch((e) => alertOwner('Dispute alert failed', e));
  }
  if (event.type === 'charge.dispute.closed') {
    await onDisputeClosed(event.data.object).catch((e) => alertOwner('Dispute close alert failed', e));
  }

  res.json({ received: true });
});

app.get('/bankroll-log-copy/:token', (req, res) => {
  const c = logCopies[req.params.token];
  if (!c || Date.now() > c.expiresAt || !fs.existsSync(c.path)) return res.status(410).send('This link has expired.');
  const ua = req.headers['user-agent'] || '';
  if (req.method === 'HEAD' || /bot|crawler|spider|preview|embed/i.test(ua)) return res.status(200).send('Log copy');
  res.setHeader('Content-Type', 'application/octet-stream');
  res.setHeader('Content-Disposition', `attachment; filename*=UTF-8''${encodeURIComponent(c.name)}`);
  fs.createReadStream(c.path).on('error', () => res.destroy()).pipe(res);
});

app.get('/bankroll-backup-file/:token', (req, res) => {
  const fp = dataFilePaths().find((f) => backupToken(relData(f)) === req.params.token);
  if (!fp || !fs.existsSync(fp)) return res.status(404).send('Not found');
  const ua = req.headers['user-agent'] || '';
  if (req.method === 'HEAD' || /bot|crawler|spider|preview|embed/i.test(ua)) return res.status(200).send('Backup file');
  res.setHeader('Content-Type', 'application/octet-stream');
  res.setHeader('Content-Disposition', `attachment; filename*=UTF-8''${encodeURIComponent(path.basename(fp))}`);
  fs.createReadStream(fp).on('error', () => res.destroy()).pipe(res);
});

const downloadAttempts = new Map();
app.get('/:name/:token', (req, res) => {
  if (!req.params.name.startsWith('bankroll-')) return res.status(404).send('Not found');
  const token = req.params.token; const remote=String(req.ip||req.socket?.remoteAddress||'unknown');
  const attempt=downloadAttempts.get(remote)||{count:0,resetAt:Date.now()+60000}; if(Date.now()>attempt.resetAt){attempt.count=0;attempt.resetAt=Date.now()+60000;} attempt.count++;downloadAttempts.set(remote,attempt);
  if(attempt.count>40)return res.status(429).send('Too many download attempts. Try again in a minute.');
  if(downloadAttempts.size>5000){for(const [ip,value] of downloadAttempts)if(Date.now()>value.resetAt)downloadAttempts.delete(ip);}
  if(!/^[a-f0-9]{48,64}$/i.test(token)){recordDownloadEvent('access_denied',{detail:'Malformed download token.'});return res.status(404).send('Not found');}
  const t=downloads[token]; const filesRoot=path.resolve(FILES_DIR)+path.sep; const safePath=t?.path&&path.resolve(String(t.path)).startsWith(filesRoot);
  if(!t||t.used||activeDownloadTokens.has(token)||Date.now()>t.expiresAt||!safePath||!fs.existsSync(t.path)){recordDownloadEvent('access_denied',{file:t?.name,customerId:t?.customerId,token,detail:'Expired, used, missing or invalid download token/path.'});return res.status(410).send('This download link has already been used or has expired.');}

  const ua = req.headers['user-agent'] || '';
  if (req.method === 'HEAD' || /bot|crawler|spider|preview|embed/i.test(ua)) {
    return res.status(200).send('Download link');
  }
  t.used = true; activeDownloadTokens.add(token); saveDownloads();
  res.setHeader('Content-Type', 'application/octet-stream');
  res.setHeader('Content-Disposition', `attachment; filename*=UTF-8''${encodeURIComponent(t.name)}`);
  let finished = false;
  res.on('finish', () => {
    finished = true;
    try {
      fs.unlinkSync(t.path);
    } catch {}
    delete downloads[token]; activeDownloadTokens.delete(token); saveDownloads();
    recordDownloadEvent('download_completed',{file:t.name,customerId:t.customerId,agentId:t.agentId,token,detail:'The complete file was successfully streamed.'});
    notifyOwner({ content: `<@${t.customerId}> downloaded "${t.name}".` });
    recordInStripe(t.paymentIntentId, { downloaded_at: new Date().toISOString() });
  });
  res.on('close', () => { if(!finished){t.used=false;activeDownloadTokens.delete(token);saveDownloads();recordDownloadEvent('download_interrupted',{file:t.name,customerId:t.customerId,token,detail:'Connection closed before the file finished; link re-enabled.'});} });
  const stream = fs.createReadStream(t.path);
  stream.on('error', (e) => { t.used=false;activeDownloadTokens.delete(token);saveDownloads();recordDownloadEvent('download_failed',{file:t.name,customerId:t.customerId,token,detail:short(e.message,180)});res.destroy(); });
  stream.pipe(res);
});

app.get('/', (_, res) => res.send('Bankroll bot running'));
app.listen(PORT, () => console.log(`Webhook server listening on :${PORT}`));

client.on('error', (e) => console.error('Client error:', e));
client.on('shardError', (e) => console.error('Shard error:', e));
client.on('shardDisconnect', (ev, id) => console.error('Shard disconnected', id, ev.code));
client.on('warn', (m) => console.warn('Warn:', m));
client.on('shardReady', (id) => console.log('Shard ready', id));
client.on('shardReconnecting', (id) => console.log('Shard reconnecting', id));

client
  .login(DISCORD_TOKEN)
  .then(() => console.log('login() resolved'))
  .catch((e) => console.error('Login failed:', e));
