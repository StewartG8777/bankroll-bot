require('dotenv').config();
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const express = require('express');
const Stripe = require('stripe');
const AdmZip = require('adm-zip'); // Used for archive editing and small archive inspection.
const yauzl = require('yauzl'); // Streaming ZIP reader for large archives.
const zlib = require('zlib');
const { pipeline } = require('stream/promises');
const { Readable, Transform } = require('stream');
const { monitorEventLoopDelay } = require('perf_hooks');
const {
  Client,
  GatewayIntentBits,
  Partials,
  EmbedBuilder,
  ActionRowBuilder,
  ButtonBuilder,
  ButtonStyle,
  StringSelectMenuBuilder,
  ModalBuilder,
  TextInputBuilder,
  TextInputStyle,
  SlashCommandBuilder,
  ContextMenuCommandBuilder,
  ApplicationCommandType,
  InteractionContextType,
  ApplicationIntegrationType,
  AttachmentBuilder,
  ChannelType,
  PermissionFlagsBits,
  Options,
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
  RENDER_API_KEY = '',
  RENDER_SERVICE_ID = '',
} = process.env;

const MONZO_LINK = (MONZO_ME_URL || (MONZO_USERNAME ? `https://monzo.me/${MONZO_USERNAME.replace(/^\/+|\/+$/g, '')}` : '')).replace(/\/+$/, '');

// Fail with a clear Render log line instead of an obscure crash when a required variable is missing.
for (const [name, value] of Object.entries({ DISCORD_TOKEN, STRIPE_SECRET_KEY })) {
  if (!String(value || '').trim()) {
    console.error(`STARTUP ERROR: required environment variable ${name} is missing or empty. Set it in the Render Environment tab and redeploy.`);
    process.exit(1);
  }
}
if (!OWNER_ID) console.warn('STARTUP WARNING: OWNER_ID is not set; owner-only commands and owner DMs will not work.');
if (!/^whsec_/.test(String(STRIPE_WEBHOOK_SECRET || ''))) console.warn('STARTUP WARNING: STRIPE_WEBHOOK_SECRET is missing or does not start with whsec_. Stripe webhooks will be rejected. A LIVE key needs the signing secret of the LIVE webhook endpoint.');
console.log(`Stripe mode: ${/^(sk|rk)_live_/.test(STRIPE_SECRET_KEY) ? 'LIVE' : 'TEST'}`);
const stripe = new Stripe(STRIPE_SECRET_KEY);
const client = new Client({
  intents: [GatewayIntentBits.Guilds, GatewayIntentBits.DirectMessages],
  partials: [Partials.Channel],
  // The backup restore pages through thousands of messages. Without limits discord.js keeps every
  // fetched message (and its attachments) in memory forever, which is risky on a 512 MB free instance.
  makeCache: Options.cacheWithLimits({
    ...Options.DefaultMakeCacheSettings,
    MessageManager: 25,
    ReactionManager: 0,
    GuildMemberManager: 50,
    PresenceManager: 0,
    VoiceStateManager: 0,
    GuildScheduledEventManager: 0,
    StageInstanceManager: 0,
    ThreadManager: 0,
  }),
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
const PRESENCE_FILE = path.join(DATA_DIR, 'presence.json');
let savedPresence = { status: 'online', updatedAt: 0 };
try {
  const parsedPresence = JSON.parse(fs.readFileSync(PRESENCE_FILE, 'utf8'));
  if (['online', 'idle', 'dnd', 'invisible'].includes(parsedPresence?.status)) savedPresence = { status: parsedPresence.status, updatedAt: Number(parsedPresence.updatedAt || 0) };
} catch {}
function savePresence(status) {
  if (!['online', 'idle', 'dnd', 'invisible'].includes(status)) throw new Error('Invalid Discord presence status.');
  savedPresence = { status, updatedAt: Date.now() };
  atomicWrite(PRESENCE_FILE, JSON.stringify(savedPresence, null, 2));
  scheduleBackup();
}
let notificationHistory = [];
try {
  const savedNotifications = JSON.parse(fs.readFileSync(NOTIFICATIONS_FILE, 'utf8'));
  notificationHistory = Array.isArray(savedNotifications) ? savedNotifications.slice(-100) : [];
} catch {}
let restoreDone = false;
let backupTimer = null;
function saveNotifications() {
  debouncedSave('notifications', () => {
    try { atomicWrite(NOTIFICATIONS_FILE, JSON.stringify(notificationHistory.slice(-100))); }
    catch (e) { console.error('Could not save notification history:', e.message); }
  });
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
let logChannelCache = { channel: null, at: 0 };
async function findLogChannel() {
  if (LOG_CHANNEL_ID) return client.channels.fetch(LOG_CHANNEL_ID).catch(() => null);
  for (const g of client.guilds.cache.values()) {
    const c = g.channels.cache.find((x) => x.name === 'bankroll-log' && x.isTextBased());
    if (c) return c;
  }
  return null;
}
// The channel is looked up once and reused for 10 minutes; a failed post clears it so it is found again.
async function getLogChannel() {
  if (logChannelCache.channel && Date.now() - logChannelCache.at < 10 * 60 * 1000) return logChannelCache.channel;
  const channel = await findLogChannel();
  logChannelCache = { channel: channel || null, at: Date.now() };
  return channel;
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
    logChannelCache = { channel: null, at: 0 };
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
process.on('unhandledRejection', (e) => {
  console.error('UNHANDLED REJECTION STACK:', e?.stack || e);
  void alertOwner('Unhandled error', e).catch((alertError) => console.error('Could not report unhandled rejection:', alertError?.stack || alertError));
});
process.on('uncaughtException', (e) => {
  // Keep the complete stack in Render logs. The short owner DM alone hides the failing line.
  console.error('UNCAUGHT EXCEPTION STACK:', e?.stack || e);
  void alertOwner('Crash avoided', e).catch((alertError) => console.error('Could not report uncaught exception:', alertError?.stack || alertError));
});

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
function londonMidnightUnix(year, monthIndex, day) {
  const guess = Date.UTC(year, monthIndex, day);
  return Math.floor((guess - londonOffset(guess)) / 1000);
}
function periodBounds(period) {
  if (period === 'all') return { since: null, until: null };
  const p = londonParts(Date.now());
  const today = Date.UTC(p.y, p.m - 1, p.d);
  const dow = new Date(today).getUTCDay();
  const monday = today - ((dow + 6) % 7) * 86400000;
  let start;
  let end;
  if (period === 'today') {
    start = today;
    end = today + 86400000;
  } else if (period === 'week') {
    start = monday;
    end = monday + 7 * 86400000;
  } else if (period === 'last_week') {
    start = monday - 7 * 86400000;
    end = monday;
  } else if (period === 'month') {
    start = Date.UTC(p.y, p.m - 1, 1);
    end = Date.UTC(p.y, p.m, 1);
  } else if (period === 'last_month') {
    start = Date.UTC(p.y, p.m - 2, 1);
    end = Date.UTC(p.y, p.m - 1, 1);
  } else {
    throw new Error('Unknown totals period.');
  }
  const startDate = new Date(start);
  const endDate = new Date(end);
  return {
    since: londonMidnightUnix(startDate.getUTCFullYear(), startDate.getUTCMonth(), startDate.getUTCDate()),
    until: londonMidnightUnix(endDate.getUTCFullYear(), endDate.getUTCMonth(), endDate.getUTCDate()),
  };
}

// ---------- File / link delivery & Vault ----------
const FILES_DIR = path.join(DATA_DIR, 'files');
const DELIVERIES_FILE = path.join(DATA_DIR, 'deliveries.json');
const MONZO_FILE = path.join(DATA_DIR, 'monzo_payments.json');
const MONZO_REVIEW_FILE = path.join(DATA_DIR, 'monzo_review.json');
const WEBHOOK_EVENTS_FILE = path.join(DATA_DIR, 'webhook_events.json');
const WIPED_CUSTOMERS_FILE = path.join(DATA_DIR, 'wiped_customers.json');
let processedWebhookEvents = {};
try { processedWebhookEvents = JSON.parse(fs.readFileSync(WEBHOOK_EVENTS_FILE, 'utf8')); } catch {}
function hasProcessedWebhook(id) { return !!(id && processedWebhookEvents[id]); }
function markWebhookProcessed(id, type) {
  if (!id) return;
  processedWebhookEvents[id] = { type: String(type || 'unknown'), at: Date.now() };
  processedWebhookEvents = Object.fromEntries(Object.entries(processedWebhookEvents).sort((a,b) => Number(b[1]?.at || 0)-Number(a[1]?.at || 0)).slice(0,2000));
  debouncedSave('webhook_events', () => {
    try { atomicWrite(WEBHOOK_EVENTS_FILE, JSON.stringify(processedWebhookEvents)); } catch (e) { console.error('Could not persist webhook event:', e.message); }
  });
  scheduleBackup();
}
const deliveryRetryTimers = new Map();
const deliveryWatchTimers = new Map();
function startDeliveryWatch(key) {
  const d = deliveries[key];
  if (!d || d.deliveryComplete || deliveryWatchTimers.has(key)) return;
  d.deliveryWatchStartedAt = Number(d.deliveryWatchStartedAt || Date.now());
  const remaining = Math.max(0, 60000 - (Date.now() - d.deliveryWatchStartedAt));
  const timer = setTimeout(async () => {
    deliveryWatchTimers.delete(key);
    const current = deliveries[key];
    if (!current || current.deliveryComplete || current.deliverySlowAlertSentAt) return;
    current.deliverySlowAlertSentAt = Date.now();
    saveDeliveries();
    const tag = current.testMode ? '[TEST] ' : '';
    const message = [
      '⚠️ ' + tag + 'DELIVERY DELAYED — OVER 1 MINUTE',
      '',
      '**File:** ' + String(current.name || 'file'),
      '**Customer:** <@' + String(current.customerId || '') + '>',
      '**Order:** ' + key,
      '**Attempts:** ' + Number(current.deliveryAttempts || 0),
      '**Status:** Payment is confirmed, but the file has not been posted to the invoice yet. Automatic retries are continuing.',
      current.deliveryError ? '**Latest error:** ' + String(current.deliveryError).slice(0, 500) : '',
    ].filter(Boolean).join('\n');
    try { const owner = await client.users.fetch(OWNER_ID); await owner.send(message); }
    catch (e) { console.error('Could not DM owner about delayed delivery:', e.message); }
    recordNotification('delivery.delayed', 'Paid delivery delayed over one minute', 'Order ' + key + ' for ' + String(current.name || 'file') + ' is still pending.', 'warning');
  }, remaining);
  timer.unref?.();
  deliveryWatchTimers.set(key, timer);
}
function clearDeliveryWatch(key) {
  const timer = deliveryWatchTimers.get(key);
  if (timer) clearTimeout(timer);
  deliveryWatchTimers.delete(key);
}
function scheduleDeliveryRetry(key, paymentIntentId, delayMs) {
  const d = deliveries[key];
  if (!d || d.deliveryComplete || deliveryRetryTimers.has(key)) return false;
  const attempts = Number(d.deliveryAttempts || 0);
  // Never abandon a confirmed payment. Back off to a maximum of six hours instead
  // of stopping after eight attempts; alert the owner at most once every six hours.
  const delays = [15000, 60000, 300000, 900000, 1800000, 3600000, 7200000, 14400000, 21600000];
  const retryDelay = delays[Math.min(attempts, delays.length - 1)];
  const delay = Math.max(1000, Number(delayMs || retryDelay));
  if (attempts >= 8 && Date.now() - Number(d.lastDeliveryRetryAlertAt || 0) >= 6 * 60 * 60 * 1000) {
    d.lastDeliveryRetryAlertAt = Date.now();
    alertOwner('Paid delivery still pending', new Error(`Order ${key} has not delivered after ${attempts} attempts. Bankroll will keep retrying every six hours. If the file is present, use /delivery resend with order ${key}; if it is missing, restore it to the vault first.`));
  }
  d.nextDeliveryRetryAt = Date.now() + delay;
  if (paymentIntentId) d.paymentIntentId = paymentIntentId;
  try { atomicWrite(DELIVERIES_FILE, JSON.stringify(deliveries)); } catch {}
  const timer = setTimeout(async () => {
    deliveryRetryTimers.delete(key);
    const current = deliveries[key];
    if (!current || current.deliveryComplete) return;
    current.deliveryAttempts = Number(current.deliveryAttempts || 0) + 1;
    current.lastDeliveryAttemptAt = Date.now(); delete current.nextDeliveryRetryAt; saveDeliveries();
    try { await deliverFile(key, paymentIntentId || current.paymentIntentId || null); }
    catch (e) {
      console.error(`Automatic delivery retry failed for ${key}:`, e);
      scheduleDeliveryRetry(key, paymentIntentId || current.paymentIntentId || null);
    }
  }, delay);
  timer.unref?.(); deliveryRetryTimers.set(key, timer); return true;
}
function restorePendingDeliveryRetries() {
  let count = 0;
  for (const [key,d] of Object.entries(deliveries)) {
    if (!d || d.deliveryComplete || !(d.paymentConfirmed || d.paid)) continue;
    startDeliveryWatch(key);
    if (scheduleDeliveryRetry(key, d.paymentIntentId || null, Math.max(1000, Number(d.nextDeliveryRetryAt || Date.now()) - Date.now()))) count++;
  }
  if (count) console.log(`Restored ${count} pending paid-delivery retry(s).`);
  return count;
}

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
// Largest file sent as a Discord attachment; anything bigger gets the one-time Download ZIP button instead.
const ATTACH_MAX_BYTES = 100 * 1024 * 1024;
fs.mkdirSync(FILES_DIR, { recursive: true });
const TMP_DIR = path.join(DATA_DIR, 'tmp');
fs.mkdirSync(TMP_DIR, { recursive: true });

let deliveries = {};
try {
  deliveries = JSON.parse(fs.readFileSync(DELIVERIES_FILE, 'utf8'));
} catch {}
const saveDeliveries = () => {
  debouncedSave('deliveries', () => {
    try { atomicWrite(DELIVERIES_FILE, JSON.stringify(deliveries)); }
    catch (e) { console.error('Could not save deliveries:', e.message); }
  });
  scheduleBackup();
};

// Backups are posted at most once every 10 minutes (a final one still runs on shutdown), and never before the
// startup restore has finished. `urgent` (a payment was just confirmed) goes out within 15 seconds instead.
const BACKUP_MIN_INTERVAL_MS = 10 * 60 * 1000;
let lastBackupRunAt = 0;
let backupUrgentPending = false;
function scheduleBackup(urgent = false) {
  if (!restoreDone) return;
  if (urgent) backupUrgentPending = true;
  else if (backupUrgentPending) return; // an urgent backup is already queued; don't push it back
  clearTimeout(backupTimer);
  const wait = backupUrgentPending ? 15 * 1000 : Math.max(15 * 1000, BACKUP_MIN_INTERVAL_MS - (Date.now() - lastBackupRunAt));
  backupTimer = setTimeout(() => { backupUrgentPending = false; backupData(); }, wait);
}

// Several save functions fire many times in a burst (a single delivery can call saveDeliveries() a dozen
// times). Writes for the same file are coalesced into one write ~1.5s later; flushSaves() writes anything
// pending immediately (used before every backup, restore and on shutdown).
const pendingSaves = new Map();
function debouncedSave(label, writeFn, delayMs = 1500) {
  const existing = pendingSaves.get(label);
  if (existing) { existing.writeFn = writeFn; return; }
  pendingSaves.set(label, { writeFn, timer: setTimeout(() => runPendingSave(label), delayMs) });
}
function runPendingSave(label) {
  const entry = pendingSaves.get(label);
  if (!entry) return;
  clearTimeout(entry.timer);
  pendingSaves.delete(label);
  try { entry.writeFn(); } catch (e) { console.error(`Could not save ${label}:`, e.message); }
}
function flushSaves() { for (const label of [...pendingSaves.keys()]) runPendingSave(label); }

// SHA-256 of a stored file, recomputed only when its size or modified time changed.
const fileHashCache = new Map();
async function sha256FileCached(fp, st) {
  const hit = fileHashCache.get(fp);
  if (hit && hit.size === st.size && hit.mtimeMs === st.mtimeMs) return hit.sha;
  const sha = await sha256File(fp);
  fileHashCache.set(fp, { size: st.size, mtimeMs: st.mtimeMs, sha });
  return sha;
}

// Monzo ledger: every Monzo invoice (open / paid / cancelled / expired) so history and totals survive.
let monzoLedger = {};
try { monzoLedger = JSON.parse(fs.readFileSync(MONZO_FILE, 'utf8')); } catch {}
let monzoReviewState = { transactions: {}, reviewed: {} };
try {
  const savedReview = JSON.parse(fs.readFileSync(MONZO_REVIEW_FILE, 'utf8'));
  if (savedReview && typeof savedReview === 'object') monzoReviewState = {
    transactions: savedReview.transactions && typeof savedReview.transactions === 'object' ? savedReview.transactions : {},
    reviewed: savedReview.reviewed && typeof savedReview.reviewed === 'object' ? savedReview.reviewed : {},
  };
} catch {}
function mergeMonzoReviewStates(current, incoming) {
  const a = current && typeof current === 'object' ? current : {};
  const b = incoming && typeof incoming === 'object' ? incoming : {};
  const transactions = { ...(b.transactions || {}), ...(a.transactions || {}) };
  const reviewed = { ...(b.reviewed || {}) };
  for (const [txId, record] of Object.entries(a.reviewed || {})) {
    const diskRecord = reviewed[txId];
    if (!diskRecord || Number(record?.reviewedAt || 0) >= Number(diskRecord?.reviewedAt || 0)) reviewed[txId] = record;
  }
  return { transactions, reviewed };
}
function saveMonzoReview() {
  try {
    fs.mkdirSync(path.dirname(MONZO_REVIEW_FILE), { recursive: true });
    // Merge the on-disk copy before writing so a stale in-memory object can
    // never erase reviews that were saved by an earlier interaction.
    try {
      const diskState = JSON.parse(fs.readFileSync(MONZO_REVIEW_FILE, 'utf8'));
      if (diskState && typeof diskState === 'object') monzoReviewState = mergeMonzoReviewStates(monzoReviewState, diskState);
    } catch (readError) { if (readError.code !== 'ENOENT') throw readError; }
    atomicWrite(MONZO_REVIEW_FILE, JSON.stringify(monzoReviewState, null, 2));
    const persisted = JSON.parse(fs.readFileSync(MONZO_REVIEW_FILE, 'utf8'));
    if (!persisted || typeof persisted !== 'object' || !persisted.reviewed || typeof persisted.reviewed !== 'object') {
      throw new Error('Saved review file has an invalid structure.');
    }
    for (const [txId, record] of Object.entries(monzoReviewState.reviewed || {})) {
      if (!persisted.reviewed[txId] || Number(persisted.reviewed[txId].reviewedAt || 0) !== Number(record?.reviewedAt || 0)) {
        throw new Error('Review missing or changed after disk read-back: ' + txId);
      }
    }
    scheduleBackup();
    return true;
  } catch (e) {
    console.error('Could not verify saved Monzo review records:', e.message);
    return false;
  }
}

// Owner-supplied customer mapping for the 15 Monzo sales, newest transaction to oldest.
// Each number refers to the corresponding customer in MONZO_REVIEW_CUSTOMER_IDS.
const MONZO_REVIEW_CUSTOMER_IDS = [
  '474754953722724372',
  '1316063283526111295',
  '311970125190397962',
  '1293026761340354712',
  '857815213353336892',
  '803348583701020702',
  '1394737341716893790',
  '651001341904420897',
  '558205144156733460',
];
const MONZO_REVIEW_CUSTOMER_ORDER_NEWEST_FIRST = [7, 8, 7, 2, 2, 9, 2, 2, 3, 4, 5, 1, 1, 6, 6];
const MONZO_CUSTOMER_ASSIGNMENT_VERSION = 'review-customer-ids-2026-11-01-v2';
const PREVIOUS_MONZO_CUSTOMER_ASSIGNMENT_VERSION = 'review-customer-ids-2026-11-01';
function applyMonzoReviewCustomerIds() {
  const customerIds = MONZO_REVIEW_CUSTOMER_IDS;
  const order = MONZO_REVIEW_CUSTOMER_ORDER_NEWEST_FIRST;
  if (customerIds.length !== 9 || order.length !== 15 ||
      customerIds.some((id) => !/^\d{17,20}$/.test(id)) ||
      order.some((number) => !Number.isInteger(number) || number < 1 || number > customerIds.length)) {
    console.error('Monzo customer ID import skipped: the supplied customer list or transaction mapping failed validation.');
    return 0;
  }

  const sales = Object.entries(monzoReviewState.reviewed || {})
    .filter(([txId, record]) => record?.category === 'sale' && monzoReviewState.transactions?.[txId]?.id)
    // Match the user's supplied sequence to review order: last reviewed first.
    .sort(([aId, aRecord], [bId, bRecord]) =>
      Number(bRecord?.reviewedAt || 0) - Number(aRecord?.reviewedAt || 0) ||
      String(bId).localeCompare(String(aId))
    );

  if (sales.length !== order.length) {
    console.warn('Monzo customer ID import not applied: expected exactly ' + order.length + ' reviewed sales for the supplied numbered mapping, found ' + sales.length + '. No records were changed.');
    return 0;
  }

  const assignments = sales.map(([txId, record], index) => ({
    txId,
    record,
    customerId: customerIds[order[index] - 1],
    ledgerRecord: monzoLedger['monzo_import_' + txId],
  }));
  const alreadyApplied = assignments.every(({ record, customerId, ledgerRecord }) =>
    record.customerId === customerId &&
    record.bulkCustomerAssignmentVersion === MONZO_CUSTOMER_ASSIGNMENT_VERSION &&
    ledgerRecord?.customerId === customerId
  );
  if (alreadyApplied) return 0;

  const canApply = assignments.every(({ record, customerId, ledgerRecord }) => {
    if (!ledgerRecord) return false;
    const oldAutoAssignment = record.bulkCustomerAssignmentVersion === PREVIOUS_MONZO_CUSTOMER_ASSIGNMENT_VERSION &&
      ledgerRecord.customerId === record.customerId;
    return (!record.customerId || record.customerId === customerId || oldAutoAssignment) &&
      (!ledgerRecord.customerId || ledgerRecord.customerId === customerId || oldAutoAssignment);
  });
  if (!canApply) {
    console.warn('Monzo customer ID import not applied: at least one sale has a conflicting manually assigned customer ID or is missing its ledger entry. No records were changed.');
    return 0;
  }

  assignments.forEach(({ record, customerId, ledgerRecord }) => {
    record.customerId = customerId;
    record.bulkCustomerAssignmentVersion = MONZO_CUSTOMER_ASSIGNMENT_VERSION;
    ledgerRecord.customerId = customerId;
  });
  saveMonzo();
  saveMonzoReview();
  console.log('Imported the supplied numbered customer mapping for ' + assignments.length + ' Monzo sales (newest to oldest).');
  return assignments.length;
}

let wipedCustomers = new Set();
try { const ids = JSON.parse(fs.readFileSync(WIPED_CUSTOMERS_FILE, 'utf8')); if (Array.isArray(ids)) wipedCustomers = new Set(ids.map(String)); } catch {}
function saveWipedCustomers() { atomicWrite(WIPED_CUSTOMERS_FILE, JSON.stringify([...wipedCustomers], null, 2)); }
const saveMonzo = () => {
  debouncedSave('monzo_payments', () => {
    try { atomicWrite(MONZO_FILE, JSON.stringify(monzoLedger)); }
    catch (e) { console.error('Could not save Monzo ledger:', e.message); }
  });
  scheduleBackup();
};
const MONZO_OPEN_MS = 7 * 24 * 60 * 60 * 1000;
const monzoOpen = () =>
  Object.values(monzoLedger).filter(
    (m) => m.status === 'open' && Date.now() < (m.expiresAt ?? m.createdAt + MONZO_OPEN_MS)
  );
const monzoPaid = () => {
  // One Monzo transaction must count once, even if it is linked to both an
  // invoice record and a historical-review import record.
  const unique = new Map();
  for (const [key, payment] of Object.entries(monzoLedger)) {
    if (payment?.status !== 'paid') continue;
    const identity = payment.txId ? 'tx:' + String(payment.txId) : 'key:' + key;
    const current = unique.get(identity);
    const isReviewImport = key === 'monzo_import_' + String(payment.txId || '') ||
      String(payment.source || '').includes('historical transaction');
    const currentIsReviewImport = current && (
      current._bankrollKey === 'monzo_import_' + String(current.txId || '') ||
      String(current.source || '').includes('historical transaction')
    );
    if (!current || (isReviewImport && !currentIsReviewImport)) {
      unique.set(identity, { ...payment, _bankrollKey: key });
    }
  }
  return [...unique.values()].map(({ _bankrollKey, ...payment }) => payment);
};

// Ensure every historical sale approved in /monzo-review is present in the same
// paid ledger used by /payments, /total, /customer and financial reports.
function syncMonzoReviewSalesToLedger(reloadFromDisk = false) {
  // Startup backup recovery can replace these JSON files after the module-level variables were first
  // loaded, so startup (and /restore) pass reloadFromDisk=true to reconcile against the recovered records.
  // The report commands call it without that and no longer re-read two JSON files on every run.
  if (reloadFromDisk) {
    flushSaves();
    try {
      const restoredLedger = JSON.parse(fs.readFileSync(MONZO_FILE, 'utf8'));
      if (restoredLedger && typeof restoredLedger === 'object' && !Array.isArray(restoredLedger)) {
        monzoLedger = { ...monzoLedger, ...restoredLedger };
      }
    } catch (error) {
      if (error.code !== 'ENOENT') console.error('Could not reload recovered Monzo ledger:', error.message);
    }
    try {
      const restoredReview = JSON.parse(fs.readFileSync(MONZO_REVIEW_FILE, 'utf8'));
      if (restoredReview && typeof restoredReview === 'object') {
        monzoReviewState = mergeMonzoReviewStates(monzoReviewState, restoredReview);
      }
    } catch (error) {
      if (error.code !== 'ENOENT') console.error('Could not reload recovered Monzo review records:', error.message);
    }
  }

  let added = 0;
  let repaired = 0;
  for (const [txId, review] of Object.entries(monzoReviewState.reviewed || {})) {
    if (review?.category !== 'sale') continue;
    const key = 'monzo_import_' + txId;
    const dateMs = Number(review.settledAt || review.createdAt || review.reviewedAt || Date.now());
    const existing = monzoLedger[key];
    const entry = {
      ...(existing || {}),
      key,
      status: 'paid',
      imported: true,
      recordType: 'sale',
      source: 'Monzo historical transaction import',
      customerId: String(review.customerId || existing?.customerId || ''),
      agentId: String(review.agentId || OWNER_ID),
      amount: Number(review.amount || existing?.amount || 0),
      currency: String(review.currency || existing?.currency || 'gbp').toLowerCase(),
      description: String(review.sold || review.description || existing?.description || review.transactionDescription || 'Historical Monzo sale'),
      originalDescription: String(review.transactionDescription || existing?.originalDescription || ''),
      counterparty: String(review.counterparty || existing?.counterparty || ''),
      reference: String(review.reference || existing?.reference || ''),
      txId,
      createdAt: Number(review.createdAt || existing?.createdAt || dateMs),
      paidAt: dateMs,
      importedAt: Number(review.reviewedAt || existing?.importedAt || Date.now()),
      reviewRecord: review,
    };
    if (!Number.isFinite(entry.amount) || entry.amount <= 0) {
      console.warn('Skipped invalid historical Monzo sale ' + txId + ': amount was missing or invalid.');
      continue;
    }
    if (!existing) added++;
    else if (existing.status !== 'paid' || !existing.customerId || !existing.agentId || !existing.paidAt) repaired++;
    monzoLedger[key] = entry;
  }
  if (added || repaired) {
    saveMonzo();
    console.log('Synced historical Monzo sales into the financial ledger: ' + added + ' added, ' + repaired + ' repaired.');
  } else {
    console.log('Historical Monzo sales are already present in the financial ledger.');
  }
  return { added, repaired };
}
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
  scheduleBackup(true);
  if (d) {
    d.paid = true;
    d.paymentConfirmed = true;
    d.paymentConfirmedAt = e.paidAt;
    if (txId) d.txId = txId;
    saveDeliveries();
  }
}

// Removes delivery records (and their files) older than 7 days. Runs at startup and then hourly.
// A paid order that has not been delivered yet is never purged: its file is still owed to the customer.
function purgeOldDeliveries() {
  let removed = 0;
  for (const [k, d] of Object.entries(deliveries)) {
    if (!d || !(Date.now() - d.savedAt > 7 * 24 * 60 * 60 * 1000)) continue;
    if ((d.paymentConfirmed || d.paid) && !d.deliveryComplete) continue;
    if (d.path) {
      try {
        fs.unlinkSync(d.path);
      } catch {}
    }
    if (d.type === 'monzo' && monzoLedger[k]?.status === 'open') {
      monzoLedger[k].status = 'expired';
      saveMonzo();
    }
    delete deliveries[k];
    removed++;
  }
  if (removed) console.log(`Purged ${removed} delivery record(s) older than 7 days.`);
  saveDeliveries();
}
purgeOldDeliveries();
setInterval(purgeOldDeliveries, 60 * 60 * 1000).unref?.();

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
    mode: interaction.options.getString('delivery') ?? 'direct',
    channelId: interaction.channelId,
    customerId: customer.id,
    agentId: interaction.user.id,
    savedAt: Date.now(),
    ...(fileData.sha256 ? { sha256: fileData.sha256 } : {}),
    ...(fileData.size ? { size: fileData.size } : {}),
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
  debouncedSave('download_history', () => {
    try { atomicWrite(DOWNLOAD_HISTORY_FILE, JSON.stringify(downloadHistory)); } catch (e) { console.error('Could not save download history:', e.message); }
  });
  scheduleBackup(); return entry;
}
let downloads = {};
try {
  downloads = JSON.parse(fs.readFileSync(DOWNLOADS_FILE, 'utf8'));
} catch {}
const saveDownloads = () => {
  debouncedSave('downloads', () => {
    try { atomicWrite(DOWNLOADS_FILE, JSON.stringify(downloads)); }
    catch (e) { console.error('Could not save downloads:', e.message); }
  });
  scheduleBackup();
};
const activeDownloadTokens = new Set();

function cleanDownloads() {
  const now = Date.now(); let removed = 0;
  for (const [token, dl] of Object.entries(downloads)) {
    const expired = !dl || (dl.expiresAt != null && Number.isFinite(Number(dl.expiresAt)) && now > Number(dl.expiresAt));
    const missing = !dl?.path || !fs.existsSync(dl.path);
    if ((expired || missing || (dl.used && !dl.multiUse)) && !activeDownloadTokens.has(token)) {
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

// CRC-32 table used while reading ZIP entries sequentially from disk.
const ZIP_CRC_TABLE = (() => {
  const table = new Uint32Array(256);
  for (let n = 0; n < 256; n++) {
    let c = n;
    for (let k = 0; k < 8; k++) c = (c & 1) ? (0xedb88320 ^ (c >>> 1)) : (c >>> 1);
    table[n] = c >>> 0;
  }
  return table;
})();

// Validate ZIP structure, entry sizes and CRCs using streams. Large archives are
// processed one entry at a time without loading the archive or entry into RAM.
async function verifyZipIntegrity(filePath) {
  if (!/\.zip$/i.test(String(filePath || ''))) return { isZip: false, entries: 0 };
  if (!filePath || !fs.existsSync(filePath)) throw new Error('ZIP file is missing.');
  return new Promise((resolve, reject) => {
    yauzl.open(filePath, { lazyEntries: true, autoClose: true, decodeStrings: true }, (openError, zip) => {
      if (openError) return reject(openError);
      let entries = 0;
      let settled = false;
      const fail = (error) => {
        if (settled) return;
        settled = true;
        try { zip.close(); } catch {}
        reject(error);
      };
      zip.on('error', fail);
      zip.on('end', () => {
        if (settled) return;
        settled = true;
        if (!entries) return reject(new Error('The ZIP archive is empty or invalid.'));
        resolve({ isZip: true, entries, streamed: true });
      });
      zip.on('entry', (entry) => {
        const entryName = String(entry.fileName || '').replace(/\\/g, '/');
        if (entryName.startsWith('/') || entryName.split('/').includes('..')) return fail(new Error('The ZIP contains an unsafe file path.'));
        if (entryName.endsWith('/')) return zip.readEntry();
        entries++;
        zip.openReadStream(entry, (streamError, stream) => {
          if (streamError) return fail(streamError);
          // zlib.crc32 (Node 20.15+/22.2+) runs natively; the JS table loop is only a fallback.
          const nativeCrc = typeof zlib.crc32 === 'function';
          let crc = nativeCrc ? 0 : 0xffffffff;
          let size = 0;
          stream.on('data', (chunk) => {
            size += chunk.length;
            if (nativeCrc) { crc = zlib.crc32(chunk, crc); return; }
            for (let i = 0; i < chunk.length; i++) crc = ZIP_CRC_TABLE[(crc ^ chunk[i]) & 0xff] ^ (crc >>> 8);
          });
          stream.on('error', fail);
          stream.on('end', () => {
            const actualCrc = nativeCrc ? crc >>> 0 : (crc ^ 0xffffffff) >>> 0;
            if (size !== Number(entry.uncompressedSize)) return fail(new Error(`ZIP entry size mismatch: ${entry.fileName}`));
            if (actualCrc !== (Number(entry.crc32) >>> 0)) return fail(new Error(`ZIP CRC/checksum error: ${entry.fileName}`));
            zip.readEntry();
          });
        });
      });
      zip.readEntry();
    });
  });
}
function buildDownloadEmbed(name, expiresAt, sizeBytes, label = 'Your download is ready', oneTime = false) {
  const isZip = /\.zip$/i.test(String(name || ''));
  return new EmbedBuilder().setColor(GREEN).setTitle(label)
    .setDescription(`Your complete **${short(String(name || 'file'), 180)}** is ready.`)
    .addFields(
      { name: 'File type', value: isZip ? 'ZIP archive' : 'File', inline: true },
      ...(Number.isFinite(sizeBytes) ? [{ name: 'Size', value: `${(sizeBytes / (1024 * 1024)).toFixed(2)} MB`, inline: true }] : []),
      ...(expiresAt ? [{ name: 'Link expires', value: `<t:${Math.floor(expiresAt / 1000)}:R>`, inline: true }] : [])
    )
    .setFooter({ text: expiresAt || oneTime ? 'Bankroll secure delivery • One-time download link' : 'Bankroll secure delivery' })
    .setTimestamp();
}

function buildCustomerDeliveryEmbed(name, sizeBytes, downloadUrl = null) {
  const isZip = /\.zip$/i.test(String(name || ''));
  const safeName = short(String(name || 'file'), 180);
  const embed = new EmbedBuilder()
    .setColor(GREEN)
    .setAuthor({ name: 'BANKROLL | FILE DELIVERY' })
    .setTitle(isZip ? 'Your ZIP is ready!' : 'Your file is ready!')
    .setDescription(
      'Your purchase has been successfully processed. Your complete file is ready to download.' +
      (downloadUrl ? `\n\n[Download ${isZip ? 'ZIP' : 'file'}](${downloadUrl}) — secure one-time link.` : '')
    )
    .addFields(
      { name: 'File', value: safeName, inline: false },
      { name: 'Status', value: downloadUrl ? 'Ready · Secure download' : 'Ready · Attached below', inline: true },
      ...(Number.isFinite(sizeBytes) ? [{ name: 'File size', value: `${(sizeBytes / (1024 * 1024)).toFixed(2)} MB`, inline: true }] : [])
    )
    .setFooter({ text: downloadUrl ? 'Bankroll secure delivery • One-time link' : 'Bankroll secure delivery' })
    .setTimestamp();
  return embed;
}

async function makeDownloadLink(d, paymentIntentId) {
  if (!d?.path || !fs.existsSync(d.path)) throw new Error('The source file is no longer available.');
  const token = crypto.randomBytes(24).toString('hex');
  const safeName = path.basename(String(d.name || 'download.zip')).replace(/[^\w.\-]/g, '_');
  const downloadPath = path.join(FILES_DIR, `download_${token}_${safeName}`);
  // A hard link is instant and uses no extra disk; fall back to a copy if links are not possible.
  try { await fs.promises.link(d.path, downloadPath); }
  catch { await fs.promises.copyFile(d.path, downloadPath); }
  downloads[token] = {
    path: downloadPath,
    name: safeName,
    customerId: d.customerId || OWNER_ID,
    agentId: d.agentId || OWNER_ID,
    vaultKey: d.vaultKey ?? null,
    paymentIntentId: paymentIntentId ?? null,
    expiresAt: null,
    used: false,
    multiUse: !!d.multiUse,
    testMode: !!d.testMode,
  };
  saveDownloads();
  recordDownloadEvent('link_created', { file: safeName, customerId: d.customerId || OWNER_ID, agentId: d.agentId || OWNER_ID, token, detail: d.multiUse ? 'Public multi-use vault export link; no time expiry.' : 'One-time download link; no time expiry. Link is invalidated after a successful download.' });
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
  const sizeBytes = fs.statSync(p).size;
  try {
    // Pass the path to Discord.js so the attachment is read as a stream instead of buffering the entire file.
    await ch.send({ embeds: [embed], files: [new AttachmentBuilder(p, { name: fileData.name })], allowedMentions: { parse: [] } });
  } catch (e) {
    console.error('Log post with file failed:', e.message);
    await ch.send({ embeds: [embed], allowedMentions: { parse: [] } }).catch(() => {});
    const mb = (sizeBytes / (1024 * 1024)).toFixed(2);
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
const blobBackupMsgIds = {}; // relative path -> message ids of the parts this process uploaded last
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
    // Download-link copies are disposable derivatives; the original vault/order file is backed up instead.
    if (path.basename(String(d.path)).startsWith('download_')) continue;
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
  flushSaves(); // make sure the files on disk are current before they are read below
  lastBackupRunAt = Date.now();
  const ch = await resolveBackupChannel();
  if (!ch) {
    console.error('Backup channel not found. Set BACKUP_CHANNEL_ID or create #bankroll-backup.');
    return;
  }

  try {
    const jsonNames = ['deliveries.json', 'downloads.json', 'download_history.json', 'vault.json', 'monzo_payments.json', 'monzo_review.json', 'notifications.json', 'scheduled_sends.json', 'webhook_events.json', 'presence.json', 'wiped_customers.json'];
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
      // These three change constantly (every notification, download event and webhook). They are still
      // INCLUDED in a snapshot whenever one posts, but on their own they no longer trigger one.
      const NOISY_BACKUP_FILES = new Set(['notifications.json', 'download_history.json', 'webhook_events.json']);
      const hash = crypto.createHash('sha256').update(Buffer.concat(files.filter((f) => !NOISY_BACKUP_FILES.has(f.name)).map((f) => f.buffer))).digest('hex');
      if (hash !== lastBackupHash) {
        const manifest = {
          format: 'bankroll-backup-v2',
          createdAt: new Date().toISOString(),
          files: files.map((f) => ({ name: f.name, size: f.buffer.length, sha256: crypto.createHash('sha256').update(f.buffer).digest('hex') })),
        };
        const manifestBuffer = Buffer.from(JSON.stringify(manifest, null, 2));
        // Keep each database file and its manifest as separate Discord attachments.
        // Building a combined ZIP with AdmZip is synchronous and can stall the
        // Discord gateway/event loop when the backup grows.
        const attachments = [
          ...files.map((f) => new AttachmentBuilder(f.buffer, { name: f.name })),
          new AttachmentBuilder(manifestBuffer, { name: 'bankroll-backup-manifest.json' }),
        ];
        await ch.send({
          content: `BANKROLL_BACKUP_SNAPSHOT v2 | ${new Date().toISOString()} | ${files.length} database files | individual files and manifest attached`,
          files: attachments,
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
      const sha256 = await sha256FileCached(fp, st);
      const signature = `${st.size}:${sha256}`;
      if (blobBackedUp[rel] === signature) continue;

      // Back up the actual file bytes in Discord, not a URL to the running web
      // instance. Hosted URLs can become stale after a restart/redeploy and return 404.
      // 8 MiB parts stay below Discord's common attachment limits; the restore
      // parser verifies the original size and SHA-256 before writing the file.
      const MAX_DISCORD_BACKUP_FILE = 150 * 1024 * 1024;
      const PART_BYTES = 8 * 1024 * 1024;
      if (st.size > MAX_DISCORD_BACKUP_FILE) {
        console.error(`Vault file exceeds the 150 MiB Discord backup limit and was not marked backed up: ${rel} (${st.size} bytes).`);
        continue;
      }
      try {
        // Read one 8 MiB part at a time (peak memory ~8 MiB instead of the whole file).
        const total = Math.ceil(st.size / PART_BYTES);
        const sentIds = [];
        const handle = await fs.promises.open(fp, 'r');
        try {
          for (let part = 1; part <= total; part++) {
            const start = (part - 1) * PART_BYTES;
            const piece = Buffer.alloc(Math.min(PART_BYTES, st.size - start));
            const { bytesRead } = await handle.read(piece, 0, piece.length, start);
            if (bytesRead !== piece.length) throw new Error(`short read on ${rel} part ${part}/${total}`);
            const sent = await ch.send({
              content: `datafile-v2:${rel}|${st.size}|${sha256}|${part}/${total}`,
              files: [new AttachmentBuilder(piece, { name: `bankroll-backup-part-${part}-of-${total}.bin` })],
              allowedMentions: { parse: [] },
            });
            if (sent?.id) sentIds.push(sent.id);
          }
        } finally { await handle.close().catch(() => {}); }
        // The new copy is complete: delete the parts THIS process uploaded earlier for the same file,
        // so the backup channel (and every startup scan) stops growing with each change.
        const superseded = blobBackupMsgIds[rel] || [];
        blobBackupMsgIds[rel] = sentIds;
        for (const oldId of superseded) await ch.messages.delete(oldId).catch(() => {});
        blobBackedUp[rel] = signature;
        lastBackupAt = Date.now();
        console.log(`Backed up ${rel} to Discord as ${total} verified part(s).`);
      } catch (attachmentError) {
        console.error(`Discord attachment backup failed for ${rel}:`, attachmentError.message);
        scheduleBackup();
        break;
      }
    }
  } catch (e) {
    console.error('Backup failed:', e.message);
  }
}


/**
 * Monzo Review's durable store is Discord, not Render's local filesystem.
 * Every completed review writes a paired, checksummed snapshot of both files
 * to #bankroll-backup. Startup recovery already scans these attachments newest
 * first and restores the newest valid copy of each database.
 */
async function backupMonzoReviewToDiscord() {
  const ch = await resolveBackupChannel();
  if (!ch) throw new Error('The #bankroll-backup channel could not be found.');
  const reviewRaw = fs.readFileSync(MONZO_REVIEW_FILE, 'utf8');
  const ledgerRaw = fs.readFileSync(MONZO_FILE, 'utf8');
  const review = JSON.parse(reviewRaw);
  const ledger = JSON.parse(ledgerRaw);
  if (!review || typeof review !== 'object' || !review.transactions || !review.reviewed ||
      !ledger || typeof ledger !== 'object' || Array.isArray(ledger)) {
    throw new Error('Monzo review or payment data failed validation; Discord backup was not sent.');
  }
  const files = [
    { name: 'monzo_review.json', buffer: Buffer.from(reviewRaw) },
    { name: 'monzo_payments.json', buffer: Buffer.from(ledgerRaw) },
  ];
  const manifest = {
    format: 'bankroll-monzo-review-v1',
    createdAt: new Date().toISOString(),
    files: files.map((file) => ({
      name: file.name,
      size: file.buffer.length,
      sha256: crypto.createHash('sha256').update(file.buffer).digest('hex'),
    })),
  };
  const attachments = [
    ...files.map((file) => new AttachmentBuilder(file.buffer, { name: file.name })),
    new AttachmentBuilder(Buffer.from(JSON.stringify(manifest, null, 2)), { name: 'monzo-review-backup-manifest.json' }),
  ];
  const sent = await ch.send({
    content: 'BANKROLL_MONZO_REVIEW_SNAPSHOT v1 | ' + manifest.createdAt +
      ' | ' + Object.keys(review.transactions).length + ' transactions | ' +
      Object.keys(review.reviewed).length + ' reviewed | paired Monzo review + payment ledger',
    files: attachments,
    allowedMentions: { parse: [] },
  });
  if (!sent?.id) throw new Error('Discord did not confirm the Monzo review backup message.');
  lastBackupAt = Date.now();
  console.log('Monzo Review stored in Discord. Snapshot ' + sent.id +
    ' contains ' + Object.keys(review.reviewed).length + ' reviewed sale(s).');
  return sent.id;
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


async function updateInvoicePaidMessage(d, key, paymentIntentId = null) {
  let invoice = null;
  const live = liveInteractions.get(key);
  if (live?.interaction) { try { invoice = await live.interaction.fetchReply(); } catch {} }
  if (!invoice) invoice = await getInvoiceMessage(d, key);
  if (!invoice?.embeds?.[0]) return false;
  const previous = invoice.embeds[0];
  const oldTitle = String(previous.title || 'Bankroll Invoice').replace(/^PAID\s*[|:-]\s*/i, '');
  const paidAt = d.paidAt || d.paymentConfirmedAt || Date.now();
  const fields = (previous.fields || []).filter((x) => !['payment status','paid at','amount paid','transaction reference'].includes(String(x.name || '').toLowerCase()));
  const paidEmbed = EmbedBuilder.from(previous).setColor(GREEN).setTitle(`PAID | ${oldTitle}`)
    .setDescription(d.type === 'monzo' ? 'Payment received successfully. Thank you! Your Monzo payment has been confirmed.' : 'Payment received successfully. Your Stripe payment has been confirmed.')
    .setFields(fields).addFields(
      { name: 'Payment Status', value: 'PAID', inline: true },
      { name: 'Amount Paid', value: money(Number(d.amount || 0), d.type === 'monzo' ? 'gbp' : (d.currency || CURRENCY)), inline: true },
      { name: 'Paid At', value: `<t:${Math.floor(paidAt / 1000)}:F>`, inline: true },
      { name: 'Transaction Reference', value: String(d.txId || paymentIntentId || d.paymentIntentId || key).slice(0,100) }
    );
  const rows = [];
  // Once payment is confirmed, remove the old Download ZIP interaction button.
  // The paid status is shown on the invoice; the actual download is posted as a reply to that invoice.
  for (const row of invoice.components || []) {
    const kept = (row.components || []).filter((component) =>
      component.customId && !String(component.customId).startsWith('getfile:') && !String(component.customId).startsWith('testpaid:')
    );
    if (kept.length) rows.push(new ActionRowBuilder().addComponents(kept.map((component) => ButtonBuilder.from(component))));
  }
  try {
    const payload = { embeds: [paidEmbed], components: rows };
    if (live?.interaction && Date.now() - live.at < 14*60*1000) {
      try { await live.interaction.editReply(payload); return true; } catch {}
    }
    await invoice.edit(payload);
    return true;
  } catch (e) { console.log('Could not update paid invoice embed:', e.message); return false; }
}

const activeDeliveryJobs = new Set();
async function deliverFile(key, paymentIntentId, options = {}) {
  const current = deliveries[key];
  if (!current || (current.deliveryComplete && !options.force)) return false;
  if (activeDeliveryJobs.has(key)) return false;
  activeDeliveryJobs.add(key);
  try {
    return await deliverFileUnlocked(key, paymentIntentId, !!options.force);
  } catch (e) {
    current.deliveryError = String(e?.message || e).slice(0, 500);
    current.paymentConfirmed = true;
    current.paid = true;
    saveDeliveries();
    const tag = current.testMode ? '[TEST] ' : '';
    if (Date.now() - Number(current.lastDeliveryFailureAlertAt || 0) >= 5 * 60 * 1000) {
      current.lastDeliveryFailureAlertAt = Date.now();
      saveDeliveries();
      try {
        const owner = await client.users.fetch(OWNER_ID);
        await owner.send(['❌ ' + tag + 'DELIVERY FAILED — AUTOMATIC RETRY SCHEDULED', '',
        '**File:** ' + String(current.name || 'file'),
        '**Customer:** <@' + String(current.customerId || '') + '>',
        '**Order:** ' + key,
        '**Error:** ' + current.deliveryError, '',
          'The paid order is saved. Bankroll will keep retrying automatically.'].join('\n'));
      } catch (dmError) { console.error('Could not DM owner about failed delivery:', dmError.message); }
    }
    scheduleDeliveryRetry(key, paymentIntentId || current.paymentIntentId || null);
    return false;
  } finally { activeDeliveryJobs.delete(key); }
}
async function deliverFileUnlocked(key, paymentIntentId, force = false) {
  const d = deliveries[key];
  if (!d || (d.deliveryComplete && !force)) return false;

  let owner = null;
  try { owner = await client.users.fetch(OWNER_ID); } catch {}
  const wasPaymentConfirmed = !!d.paymentConfirmed;
  d.paymentConfirmed = true; d.paid = true;
  d.paymentConfirmedAt = d.paymentConfirmedAt || Date.now();
  if (paymentIntentId) d.paymentIntentId = paymentIntentId;
  saveDeliveries();
  if (!wasPaymentConfirmed) scheduleBackup(true);
  startDeliveryWatch(key);
  // The original invoice message is left untouched; the delivery is posted as a reply under it.
  if (!wasPaymentConfirmed) {
    await notifyOwner({
      embeds: [new EmbedBuilder().setColor(GREEN).setTitle(d.testMode ? '[TEST] Payment confirmed' : 'Payment confirmed')
        .setDescription(`Payment confirmed for **${short(String(d.name || 'purchase'), 120)}**.`)
        .addFields(
          { name: 'Customer', value: `<@${d.customerId}>`, inline: true },
          { name: 'Amount', value: money(Number(d.amount || 0), d.currency || CURRENCY), inline: true },
          { name: 'Order', value: String(key).slice(0, 100) }
        ).setTimestamp()]
    }, { notificationType: 'payment.confirmed', notificationStatus: 'success' });
  }

  const noFile = !d.link && !d.path;
  if (d.path && fs.existsSync(d.path) && d.sha256 && await sha256File(d.path) !== d.sha256) {
    recordDownloadEvent('verification_failed',{file:d.name,customerId:d.customerId,agentId:d.agentId,detail:'SHA-256 checksum does not match the saved file record.'});
    d.deliveryError = 'File SHA-256 checksum does not match the saved file record.'; saveDeliveries();
    if(owner) await owner.send(`Download warning: **${d.name}** has changed since it was saved. The paid order has been retained for manual recovery.`).catch(()=>{});
    return;
  }
  let dlink = d.link || null;
  let hosted = false;

  if (d.fallbackLink) {
    const token = d.fallbackLink.split('/').pop();
    const savedLink = downloads[token];
    if (savedLink && !savedLink.used && (savedLink.expiresAt == null || Date.now() < Number(savedLink.expiresAt)) && fs.existsSync(savedLink.path)) {
      dlink = d.fallbackLink;
      hosted = true;
    } else {
      delete d.fallbackLink;
      saveDeliveries();
    }
  }

  if (!dlink && d.path && fs.existsSync(d.path) && BASE_URL) {
    const size = fs.statSync(d.path).size;
    if (d.mode === 'link' || size > ATTACH_MAX_BYTES) {
      dlink = await makeDownloadLink(d, paymentIntentId);
      hosted = true;
      d.fallbackLink = dlink;
      saveDeliveries();
    }
  }

  if (!noFile && !dlink && (!d.path || !fs.existsSync(d.path))) {
    d.deliveryError = 'The paid file is missing from storage.'; d.paymentConfirmed = true; d.paid = true; saveDeliveries();
    if (owner) await owner.send(`Payment received for ${key}, but the file "${d.name}" is missing. The paid order is retained and automatic checks will continue. Restore the original file to the vault, then use /delivery resend with order ${key}.`).catch(() => {});
    scheduleDeliveryRetry(key, paymentIntentId, 6 * 60 * 60 * 1000);
    return;
  }

  const isZip = /\.zip$/i.test(String(d.name || ''));
  const deliverySize = d.path && fs.existsSync(d.path) ? fs.statSync(d.path).size : undefined;
  // When the file is attached directly, also include a one-time download link next to it.
  let attachLink = null;
  if (!noFile && !dlink && d.path && fs.existsSync(d.path) && BASE_URL) {
    try { attachLink = await makeDownloadLink(d, paymentIntentId); d.fallbackLink = attachLink; saveDeliveries(); }
    catch (linkErr) { console.error('Could not make one-time link to send beside the attachment:', linkErr.message); }
  }
  const payload = () => {
    if (noFile) return { content: `<@${d.customerId}> payment received. Thank you!`, allowedMentions: { parse: [] } };
    if (dlink) {
      return {
        content: `<@${d.customerId}> payment received. Your complete ${isZip ? 'ZIP' : 'file'} is ready below.`,
        embeds: [buildDownloadEmbed(d.name, null, deliverySize, `${d.testMode ? '[TEST] ' : ''}Your download is ready`, true)],
        components: [downloadLinkRow(dlink, isZip ? 'Download ZIP' : 'Download File')],
        allowedMentions: { parse: [] },
      };
    }
    return {
      content: `<@${d.customerId}> payment received. Your complete file is attached below.`,
      embeds: [buildCustomerDeliveryEmbed(d.name, deliverySize)],
      files: [new AttachmentBuilder(d.path, { name: path.basename(d.name || 'download.zip') })],
      ...(attachLink ? { components: [downloadLinkRow(attachLink, isZip ? 'Download ZIP' : 'Download File')] } : {}),
      allowedMentions: { parse: [] },
    };
  };

  const sendWithFallback = async (send) => {
    try {
      return await send(payload());
    } catch (attachmentError) {
      if (noFile || dlink || !BASE_URL || !d.path || !fs.existsSync(d.path)) throw attachmentError;
      dlink = attachLink || await makeDownloadLink(d, paymentIntentId);
      hosted = true;
      d.fallbackLink = dlink;
      saveDeliveries();
      console.warn(`Discord attachment failed for ${d.name}; retrying with one hosted ZIP/file link: ${attachmentError.message}`);
      return send(payload());
    }
  };

  let sentTo = null;
  let msgUrl = null;
  let invoiceReply = null;

  // Post the delivery under the original invoice. Inside the 15 minutes after the invoice command, the saved
  // interaction can always post (it works in a private DM the bot is not part of). After that the bot may be
  // blocked (Missing Access) in that DM, so the customer's Get file button on the invoice takes over (the owner also gets a one-time link).
  let blocked = false;
  try {
    const replyText = noFile
      ? 'Payment received — thank you!'
      : dlink
        ? `Payment received — your complete ${isZip ? 'ZIP' : 'file'} is ready to download below.`
        : 'Payment received — your complete file is attached below.';
    const live = liveInteractions.get(key);
    let lastError = null;
    if (live?.interaction && Date.now() - live.at < 14 * 60 * 1000) {
      try {
        invoiceReply = await sendWithFallback((p) => live.interaction.followUp({ ...p, content: replyText, allowedMentions: { parse: [] } }));
        sentTo = 'a reply to the original invoice';
      } catch (e) { lastError = e; console.error(`Live invoice reply failed for ${key}:`, e.message); }
    }
    if (!sentTo) {
      try {
        const invoice = await getInvoiceMessage(d, key);
        if (!invoice) throw new Error('The original invoice message could not be found.');
        invoiceReply = await sendWithFallback((p) => invoice.reply({ ...p, content: replyText, allowedMentions: { parse: [] }, failIfNotExists: true }));
        sentTo = 'a reply to the original invoice';
      } catch (e) { lastError = lastError || e; if (!sentTo) throw e; }
    }
    msgUrl = invoiceReply?.url || null;
  } catch (e) {
    const invoiceError = String(e?.message || e).slice(0, 300);
    console.error(`Could not reply to invoice for paid order ${key}:`, e);
    blocked = /missing access|missing permissions|unknown channel|cannot send messages/i.test(invoiceError) || Number(e?.code) === 50001 || Number(e?.code) === 50013;

    d.deliveryError = `Invoice reply failed: ${invoiceError}`;
    d.paid = true;
    d.paymentConfirmed = true;
    if (blocked) d.awaitingOwnerHandoff = true;
    saveDeliveries();
    let ownerLink = null;
    if (blocked && d.path && fs.existsSync(d.path) && BASE_URL) {
      try { ownerLink = d.fallbackLink || (d.fallbackLink = await makeDownloadLink(d, paymentIntentId)); saveDeliveries(); } catch (linkErr) { console.error('Could not make hand-off link:', linkErr.message); }
    }

    if (owner && Date.now() - Number(d.lastDeliveryFailureAlertAt || 0) >= 5 * 60 * 1000) {
      d.lastDeliveryFailureAlertAt = Date.now();
      saveDeliveries();
      await owner.send([
        (blocked ? '⚠️ ' : '❌ ') + (d.testMode ? '[TEST] ' : '') + (blocked ? 'PAID — CUSTOMER NEEDS TO PRESS GET FILE' : 'DELIVERY FAILED — AUTOMATIC RETRY SCHEDULED'),
        '',
        '**File:** ' + String(d.name || 'file'),
        '**Customer:** <@' + String(d.customerId || '') + '>',
        '**Order:** ' + key,
        '**Invoice error:** ' + invoiceError,
        '',
        blocked
          ? 'The bot cannot post in that DM any more (Discord only lets it post there for 15 minutes after the command). The payment is saved. The customer can press **Get file** on the invoice, or you can send them this one-time link:\n' + (ownerLink || '(no link could be made: set PUBLIC_URL and use /delivery resend)')
          : 'The paid order is saved. Bankroll will retry by replying to the original invoice only. No customer DM will be sent.',
      ].join('\n')).catch(() => {});
    }
  }

  if (!sentTo && blocked) {
    // Retrying cannot help: the owner has been given the link, so stop retrying.
    clearDeliveryWatch(key);
    const retryTimer = deliveryRetryTimers.get(key); if (retryTimer) clearTimeout(retryTimer); deliveryRetryTimers.delete(key);
    return false;
  }

  if (sentTo) {
    d.deliveryComplete = true;
    d.deliveredAt = Date.now();
    d.deliveryMessageUrl = msgUrl || null;
    d.deliveryMethod = sentTo;
    delete d.deliveryError;
    saveDeliveries();
  } else {
    if (d.deliveryAttempts == null) d.deliveryAttempts = 0;
    scheduleDeliveryRetry(key, paymentIntentId);
    return false;
  }

  if (sentTo) {
    // Delivered: make sure the invoice no longer offers a Get file button (prevents duplicate deliveries).
    try {
      const live = liveInteractions.get(key);
      const inv = (live?.interaction && Date.now() - live.at < 14 * 60 * 1000 ? await live.interaction.fetchReply().catch(() => null) : null) || await getInvoiceMessage(d, key);
      if (inv?.components?.length) {
        const rows = withoutGetFile(inv);
        if (live?.interaction && Date.now() - live.at < 14 * 60 * 1000) {
          await live.interaction.editReply({ components: rows }).catch(() => inv.edit({ components: rows }));
        } else {
          await inv.edit({ components: rows });
        }
      }
    } catch (e) { console.log('Could not remove Get file button after delivery:', e.message); }
    clearDeliveryWatch(key);
    delete d.deliveryWatchStartedAt;
    delete d.deliverySlowAlertSentAt;
    const retryTimer = deliveryRetryTimers.get(key); if (retryTimer) clearTimeout(retryTimer); deliveryRetryTimers.delete(key);
    recordDownloadEvent('delivery_sent', { file: d.name, customerId: d.customerId, agentId: d.agentId, detail: `Payment delivery posted as ${sentTo}.` });
    if (paymentIntentId) {
      await recordInStripe(paymentIntentId, {
        delivered_at: new Date().toISOString(),
        delivered_to: sentTo.slice(0, 200),
        delivered_file: String(d.name || '').slice(0, 100),
        delivery_message: msgUrl || '',
      });
    }
    await notifyOwner({
      embeds: [new EmbedBuilder().setColor(GREEN).setTitle(d.testMode ? '[TEST] File sent' : 'File sent')
        .setDescription(`**File**\n${short(String(d.name || 'file'), 150)}`)
        .addFields(
          { name: 'Customer', value: `<@${d.customerId}>` },
          { name: 'Delivery Method', value: 'Automatic file delivery' },
        ).setTimestamp()]
    }, { notificationType: 'delivery.sent', notificationStatus: 'success' });
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
    .setDescription('Bankroll invoices, Stripe balance and payouts')
    .addSubcommand((sub) => sub
      .setName('checkout')
      .setDescription('Create a payment link / invoice for a customer')
      .addUserOption((o) => o.setName('customer').setDescription('Who is paying').setRequired(true))
      .addNumberOption((o) => o.setName('amount').setDescription('Amount to charge').setMinValue(1).setRequired(true))
      .addStringOption((o) => o.setName('description').setDescription('What the purchase is for').setRequired(true))
      .addAttachmentOption((o) => o.setName('file').setDescription('File to send the customer once they have paid'))
      .addStringOption((o) => o.setName('vault_file').setDescription('Or pick a pre-saved file from your vault').setAutocomplete(true))
      .addStringOption((o) => o.setName('delivery').setDescription('How to deliver the file (only used with a file)')
        .addChoices({ name: 'Hosted one-time link', value: 'link' }, { name: 'Direct file upload', value: 'direct' }))
      .addStringOption((o) => o.setName('currency').setDescription('Currency for this invoice (default: your usual one)')
        .addChoices({ name: 'GBP £', value: 'gbp' }, { name: 'USD dollars', value: 'usd' }, { name: 'EUR €', value: 'eur' }))
      .addStringOption((o) => o.setName('link').setDescription('Download link to send once they have paid (for big files)'))
      .addStringOption((o) => o.setName('expires').setDescription('Expiration time, e.g. 30m, 2h, 24h, or 7d (default: no expiry)')))
    .addSubcommand((sub) => sub.setName('balance').setDescription('Show available and pending Stripe balances'))
    .addSubcommand((sub) => sub.setName('payout').setDescription('Withdraw all available funds in a currency to your linked bank account')
      .addStringOption((o) => o.setName('currency').setDescription('Currency to withdraw (default: GBP)')
        .addChoices({ name: 'GBP £', value: 'gbp' }, { name: 'USD dollars', value: 'usd' }, { name: 'EUR €', value: 'eur' })))
    .addSubcommand((sub) => sub.setName('cashlogs').setDescription('Show your recent Stripe payout history'))
);

const addTestPurchaseOptions = (sub, isCard) => {
  sub
    .addUserOption((o) => o.setName('customer').setDescription('Who receives the test invoice').setRequired(true))
    .addNumberOption((o) => o.setName('amount').setDescription('Test invoice amount').setMinValue(1).setRequired(true))
    .addStringOption((o) => o.setName('description').setDescription('What the test purchase is for').setRequired(true))
    .addAttachmentOption((o) => o.setName('file').setDescription('File to deliver after simulated payment'))
    .addStringOption((o) => o.setName('vault_file').setDescription('Or choose a saved vault file').setAutocomplete(true))
    .addStringOption((o) => o.setName('delivery').setDescription('Direct file first, or hosted download link')
      .addChoices({ name: 'Direct file upload with link fallback', value: 'direct' }, { name: 'Hosted one-time link', value: 'link' }));
  if (isCard) {
    sub.addStringOption((o) => o.setName('currency').setDescription('Invoice currency')
      .addChoices({ name: 'GBP', value: 'gbp' }, { name: 'US dollars', value: 'usd' }, { name: 'Euros', value: 'eur' }));
  }
  sub.addStringOption((o) => o.setName('link').setDescription('External download link instead of a file'));
  if (isCard) sub.addStringOption((o) => o.setName('expires').setDescription('Invoice expiry, e.g. 30m, 2h, 24h, or 7d'));
  return sub;
};

const testCmd = common(
  new SlashCommandBuilder()
    .setName('test')
    .setDescription('Create a real-layout test invoice with simulated payment (owner only)')
    .addSubcommand((sub) => addTestPurchaseOptions(sub.setName('bankroll').setDescription('Test a Stripe/card invoice'), true))
    .addSubcommand((sub) => addTestPurchaseOptions(sub.setName('monzo').setDescription('Test a Monzo invoice'), false))
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

const monzoReviewCmd = common(
  new SlashCommandBuilder()
    .setName('monzo-review')
    .setDescription('Review Monzo sales and add them to Bankroll history (owner only)')
    .addStringOption((o) =>
      o.setName('period').setDescription('Which transactions to review').setRequired(true).addChoices(
        { name: 'This month', value: 'month' },
        { name: 'Last month', value: 'last_month' },
        { name: 'This week', value: 'week' },
        { name: 'Last week', value: 'last_week' }
      )
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
        .setName('export')
        .setDescription('Export a vault file to this Discord chat')
        .addStringOption((o) => o.setName('name').setDescription('The vault file to export').setAutocomplete(true).setRequired(true))
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
  .setDescription('Schedule any vault file for delivery in this DM or group DM')
  .setIntegrationTypes([ApplicationIntegrationType.UserInstall])
  .setContexts([InteractionContextType.PrivateChannel])
  .addIntegerOption((o) => o.setName('size').setDescription('How long to wait before sending').setMinValue(1).setMaxValue(720).setRequired(true))
  .addStringOption((o) => o.setName('disk').setDescription('M = minutes, H = hours').setRequired(true)
    .addChoices({ name: 'M', value: 'm' }, { name: 'H', value: 'h' }))
  .addStringOption((o) => o.setName('file').setDescription('Vault file to send (default: newest file in your vault)').setAutocomplete(true));

const cancelCmd = common(
  new SlashCommandBuilder()
    .setName('cancel')
    .setDescription('Cancel an unpaid invoice or inspect a recent purchase')
    .addStringOption((o) =>
      o
        .setName('order')
        .setDescription('Choose from the 50 most recent purchases or paste an order/reference ID')
        .setAutocomplete(true)
        .setRequired(true)
    )
    .addIntegerOption((o) =>
      o
        .setName('page')
        .setDescription('Browse the newest 25 (page 1) or previous 25 (page 2)')
        .addChoices({ name: '1 - Newest 25', value: 1 }, { name: '2 - Previous 25', value: 2 })
    )
);

const sendCmd = common(
  new SlashCommandBuilder().setName('send').setDescription('Send a message as Bankroll without showing the command publicly')
    .addStringOption((o) => o.setName('message').setDescription('The message Bankroll should post').setMaxLength(2000).setRequired(true))
);

const spamCmd = common(
  new SlashCommandBuilder().setName('spam').setDescription('Post a timed repeated-message embed in this chat or channel')
    .addStringOption((o) => o.setName('message').setDescription('Text or emoji to repeat').setMaxLength(1800).setRequired(true))
    .addIntegerOption((o) => o.setName('duration').setDescription('Duration in minutes (1–15)').setMinValue(1).setMaxValue(15).setRequired(true))
    .addBooleanOption((o) => o.setName('dm').setDescription('Use slower sending with automatic rate-limit retries (messages stay here)'))
);
const stopSpamCmd = common(
  new SlashCommandBuilder().setName('stop').setDescription('Owner: stop every active spam session')
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
        .setDescription('JSON backup file to restore (database or notification data)')
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
    .setDescription('List your most recent card and Monzo payments')
    .addIntegerOption((o) =>
      o
        .setName('count')
        .setDescription('How many payments to show (1–25, default 5)')
        .setMinValue(1)
        .setMaxValue(25)
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
          { name: 'Last week', value: 'last_week' },
          { name: 'This month', value: 'month' },
          { name: 'Last month', value: 'last_month' },
          { name: 'All time', value: 'all' }
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

const previewCmd = common(
  new SlashCommandBuilder().setName('preview').setDescription('Select a customer and preview their Bankroll experience')
);

const exportCmd = common(
  new SlashCommandBuilder()
    .setName('export')
    .setDescription('Export Bankroll source files')
    .addSubcommand((sub) => sub.setName('index').setDescription('Upload the raw index.js file to the private index logs channel'))
);

const botAdminCmd = common(
  new SlashCommandBuilder()
    .setName('bot')
    .setDescription('Owner controls for the Bankroll bot')
    .addSubcommand((sub) => sub.setName('refresh-commands').setDescription('Refresh all Discord slash commands immediately'))
    .addSubcommand((sub) => sub.setName('redeploy').setDescription('Trigger a new deployment from the latest GitHub commit on Render'))
    .addSubcommand((sub) => sub.setName('restart').setDescription('Restart the running Render service'))
    .addSubcommand((sub) => sub.setName('avatar').setDescription('Change the bot profile picture').addAttachmentOption((o) => o.setName('image').setDescription('PNG, JPG or GIF image').setRequired(true)))
    .addSubcommand((sub) => sub.setName('status').setDescription('Change the bot online presence').addStringOption((o) => o.setName('presence').setDescription('Discord presence colour/status').setRequired(true).addChoices(
      { name: 'Online (green)', value: 'online' },
      { name: 'Idle (yellow)', value: 'idle' },
      { name: 'Do Not Disturb (red)', value: 'dnd' },
      { name: 'Invisible', value: 'invisible' }
    )))
    .addSubcommand((sub) => sub.setName('clear-dm').setDescription('Delete the bot\'s recent messages in this DM').addIntegerOption((o) => o.setName('count').setDescription('Maximum bot messages to delete (default 500)').setMinValue(1).setMaxValue(1000)))
);

const deleteBotMessageCmd = new ContextMenuCommandBuilder()
  .setName('Delete Bankroll Message')
  .setType(ApplicationCommandType.Message)
  .setIntegrationTypes([ApplicationIntegrationType.UserInstall])
  .setContexts([InteractionContextType.BotDM, InteractionContextType.PrivateChannel]);

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

const deliveryCmd = common(new SlashCommandBuilder().setName('delivery').setDescription('Track and resend customer deliveries')
  .addSubcommand((sub) => sub.setName('status').setDescription('Check a payment order delivery status').addStringOption((o) => o.setName('order').setDescription('Checkout session ID or Monzo invoice ID').setRequired(true)))
  .addSubcommand((sub) => sub.setName('resend').setDescription('Resend a paid customer file without charging again').addStringOption((o) => o.setName('order').setDescription('Checkout session ID or Monzo invoice ID').setRequired(true))));

const wipeCmd = common(new SlashCommandBuilder().setName('wipe').setDescription('Remove a customer from Bankroll transaction records and logs (owner only)')
  .addStringOption((o) => o.setName('confirm').setDescription('WIPE for one customer, WIPE ALL when no customer is chosen').setRequired(true).addChoices({ name: 'WIPE', value: 'WIPE' }, { name: 'WIPE ALL', value: 'WIPE ALL' }))
  .addUserOption((o) => o.setName('user').setDescription('Customer to wipe. Leave empty to wipe ALL customers, payments and payment records')));

const commands = [sendCmd, spamCmd, stopSpamCmd, deleteBotMessageCmd, bankrollCmd, monzoCmd, monzoReviewCmd, testCmd, vaultCmd, sendfileCmd, downloadsCmd, deliveryCmd, cancelCmd, remindCmd, unpaidCmd, statusCmd, restoreCmd, notificationsCmd, currencyCmd, paymentsCmd, totalCmd, customerCmd, historyCmd, refundCmd, setupCmd, previewCmd, exportCmd, botAdminCmd, wipeCmd];

// Re-uses the result of an async lookup for a short time (and shares one in-flight request). Failed lookups
// are never cached.
const asyncCache = new Map();
function cachedAsync(key, ttlMs, fn) {
  const hit = asyncCache.get(key);
  if (hit && Date.now() - hit.at < ttlMs) return hit.promise;
  const promise = Promise.resolve().then(fn);
  asyncCache.set(key, { at: Date.now(), promise });
  promise.catch(() => { if (asyncCache.get(key)?.promise === promise) asyncCache.delete(key); });
  if (asyncCache.size > 200) for (const [k, v] of asyncCache) if (Date.now() - v.at > 5 * 60 * 1000) asyncCache.delete(k);
  return promise;
}
const recentCheckoutSessions = () => cachedAsync('sessions:recent500', 30 * 1000, () => stripe.checkout.sessions.list({ limit: 100 }).autoPagingToArray({ limit: 500 }));

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
      'monzo_review.json': MONZO_REVIEW_FILE,
      'notifications.json': NOTIFICATIONS_FILE,
      'scheduled_sends.json': SEND_JOBS_FILE,
      'webhook_events.json': WEBHOOK_EVENTS_FILE,
      'presence.json': PRESENCE_FILE,
      'wiped_customers.json': WIPED_CUSTOMERS_FILE,
    };
    const isValidJsonFor = (name, value) => {
      if (name === 'notifications.json' || name === 'download_history.json' || name === 'wiped_customers.json') return Array.isArray(value);
      return !!value && typeof value === 'object' && !Array.isArray(value);
    };
    const localNeedsRestore = (name, fp) => {
      try {
        const value = JSON.parse(fs.readFileSync(fp, 'utf8'));
        if (!isValidJsonFor(name, value)) return true;
        if (name === 'monzo_review.json') {
          // The valid empty review shape has two top-level keys, so a generic
          // Object.keys check incorrectly treats it as populated and blocks recovery.
          return !Object.keys(value.transactions || {}).length && !Object.keys(value.reviewed || {}).length;
        }
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
      const restoredReview = JSON.parse(fs.readFileSync(MONZO_REVIEW_FILE, 'utf8'));
      if (restoredReview && typeof restoredReview === 'object') {
        monzoReviewState = {
          transactions: restoredReview.transactions && typeof restoredReview.transactions === 'object' ? restoredReview.transactions : {},
          reviewed: restoredReview.reviewed && typeof restoredReview.reviewed === 'object' ? restoredReview.reviewed : {},
        };
      }
      console.log(`Restored Monzo review data: ${Object.keys(monzoReviewState.transactions).length} transactions, ${Object.keys(monzoReviewState.reviewed).length} reviewed.`);
    } catch (error) {
      if (error.code !== 'ENOENT') console.error('Could not reload restored Monzo review data:', error.message);
    }
    try {
      const restoredNotifications = JSON.parse(fs.readFileSync(NOTIFICATIONS_FILE, 'utf8'));
      if (Array.isArray(restoredNotifications)) notificationHistory = restoredNotifications.slice(-100);
    } catch {}
    try { sendJobs = JSON.parse(fs.readFileSync(SEND_JOBS_FILE, 'utf8')); } catch {}
    try { processedWebhookEvents = JSON.parse(fs.readFileSync(WEBHOOK_EVENTS_FILE, 'utf8')); } catch {}

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
    // Streams every part straight into a temp file, hashing as it goes, then verifies size + SHA-256
    // and renames it into place. Only one 8 MiB part is ever held in memory.
    const downloadGroupToFile = async (g, destPath) => {
      fs.mkdirSync(path.dirname(destPath), { recursive: true });
      const tmpPath = `${destPath}.restore.tmp`;
      const hash = crypto.createHash('sha256');
      let written = 0;
      const out = fs.createWriteStream(tmpPath);
      const finished = new Promise((resolve, reject) => { out.on('finish', resolve); out.on('error', reject); });
      try {
        for (let i = 1; i <= g.total; i++) {
          const partAtt = g.parts.get(i);
          if (!partAtt) throw new Error(`missing part ${i}/${g.total}`);
          const res = await fetch(partAtt.url, { signal: AbortSignal.timeout(5 * 60 * 1000) });
          if (!res.ok) throw new Error(`HTTP ${res.status} downloading part ${i}`);
          const piece = Buffer.from(await res.arrayBuffer());
          hash.update(piece);
          written += piece.length;
          if (!out.write(piece)) await new Promise((resolve) => out.once('drain', resolve));
        }
        out.end();
        await finished;
        if (written !== g.size) throw new Error(`size mismatch (${written}/${g.size})`);
        if (g.sha && hash.digest('hex') !== g.sha) throw new Error('SHA-256 mismatch');
        fs.renameSync(tmpPath, destPath);
      } catch (e) {
        out.destroy();
        try { fs.unlinkSync(tmpPath); } catch {}
        throw e;
      }
    };
    const downloadZipBackup = async (z) => {
      const res = await fetch(z.att.url, { signal: AbortSignal.timeout(120000) });
      if (!res.ok) throw new Error(`HTTP ${res.status} downloading ZIP backup`);
      const zipBuffer = Buffer.from(await res.arrayBuffer());

      // Read legacy backup ZIPs with yauzl's streaming inflater. AdmZip.getData()
      // inflates the whole entry synchronously and was causing long gateway stalls.
      const buf = await new Promise((resolve, reject) => {
        yauzl.fromBuffer(zipBuffer, { lazyEntries: true, autoClose: true, decodeStrings: true }, (openError, zip) => {
          if (openError) return reject(openError);
          let settled = false;
          const fail = (error) => {
            if (settled) return;
            settled = true;
            try { zip.close(); } catch {}
            reject(error);
          };
          zip.on('error', fail);
          zip.on('end', () => {
            if (!settled) fail(new Error('ZIP does not contain the expected file'));
          });
          zip.on('entry', (entry) => {
            if (entry.fileName !== z.entryName || entry.fileName.endsWith('/')) {
              zip.readEntry();
              return;
            }
            zip.openReadStream(entry, (streamError, stream) => {
              if (streamError) return fail(streamError);
              const chunks = [];
              stream.on('data', (chunk) => chunks.push(Buffer.from(chunk)));
              stream.on('error', fail);
              stream.on('end', () => {
                if (settled) return;
                settled = true;
                resolve(Buffer.concat(chunks));
                try { zip.close(); } catch {}
              });
            });
          });
          zip.readEntry();
        });
      });
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
          await downloadGroupToFile(g, fp);
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
        if (!fs.existsSync(fp)) {
          const staleLink = lastError && /backup link HTTP (404|410)/.test(String(lastError.message));
          lost.push(`${rel}${lastError ? ` (${lastError.message}${staleLink ? ' - old hosted-link backup is dead; re-add the file with /vault add so its bytes are stored in Discord' : ''})` : ''}`);
        }
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
    // Do not mark the current local files as already backed up here. If the
    // backup channel is empty (or has no valid snapshot), doing so suppresses
    // the first real snapshot and leaves this deployment with no recovery point.
    // backupDataOnce sets lastBackupHash only after Discord confirms a snapshot.
    lastBackupHash = '';
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

async function reconcilePaidStripeDeliveries() {
  const pending = Object.entries(deliveries).filter(([key,d]) => key.startsWith('cs_') && d && !d.paymentConfirmed && !d.paid);
  let count = 0; let cursor = 0;
  const workers = Array.from({ length: Math.min(3, pending.length) }, async () => {
    while (cursor < pending.length) {
      const [key,d] = pending[cursor++];
      try {
        const session = await stripe.checkout.sessions.retrieve(key);
        if (session.payment_status !== 'paid') continue;
        d.paymentConfirmed = true; d.paid = true; d.paymentConfirmedAt = session.created ? session.created*1000 : Date.now();
        d.amount = (session.amount_total || 0)/100; d.currency = String(session.currency || CURRENCY).toLowerCase(); d.paymentIntentId = session.payment_intent || null;
        saveDeliveries(); await deliverFile(key, session.payment_intent || null); count++;
      } catch (e) { console.error(`Could not reconcile Stripe order ${key}:`,e.message); }
    }
  });
  await Promise.all(workers);
  if (count) console.log(`Recovered ${count} paid Stripe order(s) during startup reconciliation.`);
  return count;
}

const onReady = async () => {
  if (started) return;
  started = true;
  console.log(`Logged in as ${client.user.tag}`);
  // Register the slash commands from this deployed version on every startup.
  // This ensures new/changed commands are published after a Render deployment.
  try {
    const commandJson = commands.map((c) => c.toJSON());
    const commandHash = crypto.createHash('sha256').update(JSON.stringify(commandJson)).digest('hex');
    const commandHashFile = path.join(DATA_DIR, 'commands.hash');
    let previousHash = '';
    try { previousHash = fs.readFileSync(commandHashFile, 'utf8').trim(); } catch {}
    if (previousHash === commandHash) {
      console.log('Slash commands unchanged since the last start; skipping re-registration (use /bot refresh-commands to force).');
    } else {
      await client.application.commands.set(commandJson);
      console.log(`Registered ${commands.length} global slash/context command(s) from the running version.`);
      if (DISCORD_GUILD_ID) {
        const guild = await client.guilds.fetch(DISCORD_GUILD_ID).catch(() => null);
        if (guild) await guild.commands.set([]);
      }
      try { atomicWrite(commandHashFile, commandHash); } catch {}
    }
  } catch (error) {
    console.error('Automatic slash-command refresh failed:', error);
  }
  // Restart-loop warning: DM the owner if the bot has started 3+ times in 10 minutes.
  try {
    const startsFile = path.join(DATA_DIR, 'startups.json');
    let starts = [];
    try { starts = JSON.parse(fs.readFileSync(startsFile, 'utf8')); } catch {}
    const nowMs = Date.now();
    starts = (Array.isArray(starts) ? starts : []).filter((t) => nowMs - Number(t) < 10 * 60 * 1000);
    starts.push(nowMs);
    atomicWrite(startsFile, JSON.stringify(starts));
    if (starts.length >= 3) {
      await alertOwner('Restart loop warning', new Error('The bot has started ' + starts.length + ' times in the last 10 minutes. Check the Render logs for a crash or a failing deploy.'));
    }
  } catch (e) { console.error('Restart-loop check failed:', e.message); }
  await autoRestoreOnStartup();
  try {
    const status = ['online', 'idle', 'dnd', 'invisible'].includes(savedPresence?.status) ? savedPresence.status : 'online';
    await client.user.setPresence({ status, activities: [] });
    console.log(`Restored saved bot presence: ${status}`);
  } catch (error) {
    console.error('Could not restore saved bot presence:', error);
  }
  void syncStripeLedger().then(() => { if (!stripeLedgerReady) setTimeout(() => void syncStripeLedger(), 2 * 60 * 1000); });
  setInterval(() => void syncStripeLedger(), 6 * 60 * 60 * 1000);
  await reconcilePaidStripeDeliveries();
  restorePendingDeliveryRetries();

  backfillMonzoLedger();
  syncMonzoReviewSalesToLedger(true);
  applyMonzoReviewCustomerIds();
  const monzoRebuilt = restoreMonzoReminders();
  restoreReminders().then(async (n) => {
    const total = n + monzoRebuilt;
    console.log(`Startup recovery complete. Reminders rebuilt: ${total}.${recoveryNote}`);
    const deployedCommit = process.env.RENDER_GIT_COMMIT || process.env.GITHUB_SHA || process.env.SOURCE_VERSION || 'Not supplied by hosting platform';
    const hasRecoveryIssue = !!recoveryInfo?.lost?.length || /Backup restore failed/i.test(recoveryNote);
    const startupEmbed = new EmbedBuilder()
      .setColor(hasRecoveryIssue ? RED : GREEN)
      .setTitle(hasRecoveryIssue ? 'Bankroll online — recovery needs attention' : 'Bankroll is back online')
      .setDescription('The bot has connected to Discord and completed its startup recovery.')
      .addFields(
        { name: 'Deployed commit', value: String(deployedCommit).slice(0, 100), inline: false },
        { name: 'Reminders rebuilt', value: String(total), inline: true },
        { name: 'Backup recovery', value: short(recoveryNote.trim() || 'No recovery details reported.', 900), inline: false }
      )
      .setTimestamp();
    await notifyOwner({ embeds: [startupEmbed] }, {
      log: false,
      notificationType: 'system.startup',
      notificationStatus: hasRecoveryIssue ? 'error' : 'success'
    });
  }).catch(async (error) => {
    console.error('Could not restore reminders:', error.message);
    await alertOwner('Startup reminder recovery failed', error);
  });
  setTimeout(backupData, 2 * 60 * 1000);
  setInterval(backupData, 6 * 60 * 60 * 1000);
  setInterval(async () => {
    try {
      fs.accessSync(DATA_DIR, fs.constants.W_OK);
      if (restoreDone && lastBackupAt && Date.now() - lastBackupAt > 18*60*60*1000) await alertOwner('Backup health warning', new Error('No successful backup has been recorded in over 18 hours.'));
      for (const [key,d] of Object.entries(deliveries)) {
        if ((d?.paymentConfirmed || d?.paid) && !deliveryRetryTimers.has(key) && !d.deliveryComplete) {
          if (d.deliveryError && d.deliveryError.includes('missing from storage') && d.path && fs.existsSync(d.path)) delete d.deliveryError;
          if (!d.deliveryError || (d.deliveryError.includes('missing from storage') && (!d.path || !fs.existsSync(d.path)))) {
            scheduleDeliveryRetry(key, d.paymentIntentId || null, Math.max(1000, Number(d.nextDeliveryRetryAt || Date.now()) - Date.now()));
          }
        }
      }
    } catch (e) { await alertOwner('Bankroll storage health check failed',e); }
  },5*60*1000);
};
let shuttingDown = false;
for (const sig of ['SIGTERM', 'SIGINT']) {
  process.on(sig, async () => {
    if (shuttingDown) return;
    shuttingDown = true;
    console.log(`${sig} received: saving a last backup before exit`);
    clearTimeout(backupTimer);
    flushSaves();
    await Promise.race([backupData().catch(() => {}), new Promise((r) => setTimeout(r, 20000))]);
    try { await client.destroy(); } catch {}
    process.exit(0);
  });
}

// clientReady is the supported ready event in current discord.js versions.
// Register only once so startup recovery and scheduled jobs cannot run twice.
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

    const mode = interaction.options.getString('delivery') ?? 'direct';
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
    if (fileData && fileSha) fileData.sha256 = fileSha;
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
    if (deliveries[session.id]) {
      Object.assign(deliveries[session.id], { type: 'stripe', amount, currency: cur, description });
      saveDeliveries();
    }
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
      embed.addFields({ name: 'File', value: `${fileData.name} (posted as a reply to this invoice after payment)` });
    }
    if (linkUrl) {
      embed.addFields({ name: 'Download', value: 'Download link posted as a reply to this invoice after payment' });
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
      // Backup for when the bot can no longer post in the DM (paid after 15 minutes, or a very big file):
      // the customer's click lets the bot reply under the invoice with the file or the download embed.
      row.addComponents(
        new ButtonBuilder().setCustomId(`getfile:${session.id}`).setLabel('Get file').setStyle(ButtonStyle.Secondary)
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

    const mode = interaction.options.getString('delivery') ?? 'direct';
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

    if (fileData) {
      fileData.sha256 = fileData.sha256 || await sha256File(fileData.path);
      fileData.size = fileData.size || fs.statSync(fileData.path).size;
    }

    const reference = 'BR' + crypto.randomBytes(4).toString('hex').toUpperCase();
    const paymentBase = MONZO_LINK ? MONZO_LINK.replace(/\/+$/, '') : '';
    const paymentUrl = paymentBase ? paymentBase + '/' + amount.toFixed(2) + '?d=' + encodeURIComponent(reference) : null;
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
      currency: 'gbp',
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
      paymentUrl,
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
    if (fileData) embed.addFields({ name: 'File', value: `${fileData.name} (posted as a reply to this invoice as soon as payment lands)` });
    if (linkUrl) embed.addFields({ name: 'Download', value: 'Download link posted as a reply to this invoice as soon as payment lands' });
    embed
      .addFields(
        { name: 'Generated In Guild', value: where.guild },
        { name: 'Generated In Channel', value: where.channel }
      )
      .setFooter({ text: `Created by ${agent.username} | ID: ${key}` });

    const row = new ActionRowBuilder();
    if (MONZO_LINK) {
      row.addComponents(new ButtonBuilder().setLabel('Pay with Monzo').setStyle(ButtonStyle.Link).setURL(paymentUrl));
    }
    if (fileData || linkUrl) {
      row.addComponents(
        new ButtonBuilder().setCustomId(`getfile:${key}`).setLabel('Get file').setStyle(ButtonStyle.Secondary)
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

// Same result as zipSpawnCodes(), but reads the archive from disk with the streaming ZIP reader: only the small
// vehicles*.meta files are inflated and the archive is never loaded into memory or processed synchronously.
function zipSpawnCodesFromFile(filePath) {
  return new Promise((resolve, reject) => {
    yauzl.open(filePath, { lazyEntries: true, autoClose: true, decodeStrings: true }, (openError, zip) => {
      if (openError) return reject(openError);
      const found = new Set();
      const yftNames = new Set();
      let settled = false;
      const finish = (error) => {
        if (settled) return;
        settled = true;
        try { zip.close(); } catch {}
        if (error) return reject(error);
        resolve([...(found.size ? found : yftNames)]);
      };
      zip.on('error', finish);
      zip.on('end', () => finish());
      zip.on('entry', (entry) => {
        const name = String(entry.fileName || '').replace(/\\/g, '/');
        if (name.endsWith('/')) return zip.readEntry();
        const base = path.posix.basename(name).toLowerCase();
        const yft = base.match(/^([a-z0-9_]+?)(?:_hi)?\.yft$/);
        if (yft) yftNames.add(yft[1]);
        if (/^vehicles.*\.meta$/.test(base) && Number(entry.uncompressedSize) <= 5 * 1024 * 1024) {
          zip.openReadStream(entry, (streamError, stream) => {
            if (streamError) return finish(streamError);
            const chunks = [];
            stream.on('data', (chunk) => chunks.push(chunk));
            stream.on('error', finish);
            stream.on('end', () => {
              const text = Buffer.concat(chunks).toString('utf8');
              for (const m of text.matchAll(/<modelName>\s*([^<\s]+)\s*<\/modelName>/gi)) found.add(m[1].toLowerCase());
              zip.readEntry();
            });
          });
        } else {
          zip.readEntry();
        }
      });
      zip.readEntry();
    });
  });
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
    try { const actualHash=await sha256File(item.path); const size=fs.statSync(item.path).size; const zipInfo=await verifyZipIntegrity(item.path); const hashMatches=!item.sha256||item.sha256===actualHash; if(!item.sha256){item.sha256=actualHash;saveVault();}
      return interaction.editReply({ embeds:[new EmbedBuilder().setColor(hashMatches?GREEN:RED).setTitle(`Download integrity: ${key}`).setDescription(hashMatches?'File exists and the SHA-256 checksum matches.':'Checksum mismatch: do not send this file until checked.').addFields({name:'File',value:short(item.originalName||key,100),inline:true},{name:'Size',value:`${(size/(1024*1024)).toFixed(2)} MB`,inline:true},{name:'ZIP entries',value:zipInfo.isZip?String(zipInfo.entries):'Not a ZIP',inline:true},{name:'SHA-256',value:actualHash}).setTimestamp()] });
    } catch(e) { recordDownloadEvent('verification_failed',{file:item.originalName||key,detail:e.message}); return interaction.editReply(`ZIP/file verification failed: ${e.message}`); }
  }
  if (sub === 'regenerate') {
    if(!BASE_URL) return interaction.editReply('PUBLIC_URL is not configured, so Bankroll cannot create hosted download links.');
    try { const zipInfo=await verifyZipIntegrity(item.path); const size=fs.statSync(item.path).size; const url=await makeDownloadLink({path:item.path,name:item.originalName||path.basename(item.path),customerId:interaction.user.id,agentId:interaction.user.id,vaultKey:key,description:'admin-regenerated-download'},null); const token=url.split('/').pop(); const expiresAt=downloads[token]?.expiresAt;
      recordDownloadEvent('link_regenerated',{file:item.originalName||key,customerId:interaction.user.id,agentId:interaction.user.id,token,detail:`Admin regenerated a link for vault key ${key}; ZIP entries ${zipInfo.entries||0}.`});
      return interaction.editReply({embeds:[buildDownloadEmbed(item.originalName||key,expiresAt,size,'Fresh download link generated')],components:[downloadLinkRow(url,/\.zip$/i.test(item.originalName||'')?'Download ZIP':'Download File')]});
    } catch(e) { recordDownloadEvent('link_regeneration_failed',{file:item.originalName||key,customerId:interaction.user.id,detail:e.message}); return interaction.editReply(`Could not regenerate the link: ${e.message}`); }
  }
}

async function cmdVault(interaction) {
  if (!allowed.has(interaction.user.id)) {
    return interaction.reply({ content: 'Only authorized users can manage the vault.', ephemeral: true });
  }

  const sub = interaction.options.getSubcommand();
  await interaction.deferReply({ ephemeral: sub !== 'export' });

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

    if (/\.zip$/i.test(att.name)) { try { await verifyZipIntegrity(filePath); } catch (zipError) { try { fs.unlinkSync(filePath); } catch {} return interaction.editReply(`That ZIP failed its integrity check and was not added: ${short(zipError.message, 220)}`); } }
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
        const codes = await zipSpawnCodesFromFile(filePath);
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
          const zipInfo = await verifyZipIntegrity(item.path);
          zipCheck = `ZIP verified: ${zipInfo.entries} file(s), all checksums OK`;
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

  if (sub === 'export') {
    const key = interaction.options.getString('name');
    const item = vault[key];
    if (!item) return interaction.editReply('Vault item `' + key + '` not found.');
    if (!item.path || !fs.existsSync(item.path)) return interaction.editReply('Vault file `' + key + '` is missing from storage. Re-add it before exporting.');

    const fileName = path.basename(item.originalName || path.basename(item.path));
    const size = fs.statSync(item.path).size;
    const readyEmbed = new EmbedBuilder()
      .setColor(GREEN)
      .setTitle('Vault export ready')
      .setDescription('**' + short(fileName, 180) + '** is ready to download.')
      .addFields(
        { name: 'Vault key', value: '`' + short(key, 80) + '`', inline: true },
        { name: 'File size', value: (size / (1024 * 1024)).toFixed(2) + ' MB', inline: true }
      )
      .setTimestamp();

    // Try the complete file as a normal Discord attachment first.
    try {
      await interaction.editReply({ embeds: [readyEmbed], files: [new AttachmentBuilder(item.path, { name: fileName })], components: [] });
      recordDownloadEvent('vault_export_attached', { file: fileName, customerId: interaction.user.id, agentId: interaction.user.id, detail: 'Vault key ' + key + '; delivered as a direct Discord attachment.' });
      recordNotification('vault.export', 'Vault file exported', key + ' exported as a direct Discord attachment.', 'success');
      return;
    } catch (attachmentError) {
      console.warn('Direct vault export attachment failed for ' + key + '; falling back to hosted download: ' + attachmentError.message);
    }

    if (!BASE_URL) return interaction.editReply({ content: 'Discord could not accept the full file as an attachment, and PUBLIC_URL is not configured for a hosted download link. Set PUBLIC_URL and try again.', embeds: [], files: [], components: [] });
    try {
      const url = await makeDownloadLink({ path: item.path, name: fileName, customerId: interaction.user.id, agentId: interaction.user.id, vaultKey: key, description: 'vault-export', multiUse: true }, null);
      const token = url.split('/').pop();
      const fallbackEmbed = EmbedBuilder.from(readyEmbed)
        .setDescription('Discord could not accept the full file as an attachment, so a secure download button is ready instead.\n\n**' + short(fileName, 180) + '** — ' + (size / (1024 * 1024)).toFixed(2) + ' MB.')
        .setFooter({ text: 'The download link is one-time-use and does not expire by time.' });
      recordDownloadEvent('vault_export_link', { file: fileName, customerId: interaction.user.id, agentId: interaction.user.id, token, detail: 'Vault key ' + key + '; direct attachment failed, hosted link supplied.' });
      recordNotification('vault.export', 'Vault export link created', key + ' exported using a hosted one-time download link.', 'success');
      return interaction.editReply({ embeds: [fallbackEmbed], components: [downloadLinkRow(url, /\.zip$/i.test(fileName) ? 'Download ZIP' : 'Download File')], files: [] });
    } catch (error) {
      console.error('Vault export fallback failed:', error);
      return interaction.editReply({ content: 'Could not export `' + key + '`: ' + short(error.message, 250), embeds: [], components: [], files: [] });
    }
  }

  if (sub === 'edit') {
    const key = interaction.options.getString('name');
    const item = vault[key];
    if (!item || !fs.existsSync(item.path)) return interaction.editReply(`Vault item \`${key}\` was not found.`);
    if (!/\.zip$/i.test(item.originalName)) return interaction.editReply('Only .zip vault files can be edited.');

    const archiveSize = fs.statSync(item.path).size;
    if (archiveSize > 50 * 1024 * 1024) return interaction.editReply('This ZIP is larger than 50 MB. Bankroll can stream-verify and deliver it safely, but the current spawn-code editor rewrites archives in memory. Edit this archive locally to avoid a high-memory operation.');

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
        const codes = await zipSpawnCodesFromFile(item.path);
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
  if (!allowed.has(interaction.user.id)) return interaction.reply({ content: 'Only authorized users can cancel invoices.', flags: 64 });
  if (!interaction.deferred && !interaction.replied) await interaction.deferReply({ flags: 64 });
  const input = String(interaction.options.getString('order') || '').trim();

  // Accept internal Monzo IDs and the human-readable payment reference (e.g. MONZO-6825).
  const monzoMatch = Object.entries(monzoLedger).find(([key, entry]) =>
    String(key).toLowerCase() === input.toLowerCase() ||
    String(entry?.key || '').toLowerCase() === input.toLowerCase() ||
    String(entry?.reference || '').toLowerCase() === input.toLowerCase()
  );

  if (monzoMatch) {
    const [ledgerKey, e] = monzoMatch;
    const delivery = deliveries[ledgerKey];
    const currentStatus = String(e.status || (delivery?.paid ? 'paid' : 'open')).toLowerCase();
    if (currentStatus === 'paid' || delivery?.paid || delivery?.paymentConfirmed) {
      return interaction.editReply('This Monzo purchase is already PAID. It cannot be cancelled; Monzo payments must be refunded through Monzo.');
    }
    if (currentStatus !== 'open') {
      return interaction.editReply(`This Monzo invoice is already ${currentStatus.toUpperCase()} and cannot be cancelled.`);
    }

    e.status = 'cancelled';
    e.cancelledAt = Date.now();
    e.cancelledBy = interaction.user.id;
    saveMonzo();

    // Update the original invoice and remove its old payment/download buttons.
    try {
      const invoice = delivery
        ? await getInvoiceMessage(delivery, ledgerKey)
        : await findInvoiceMessage(e.channelId, ledgerKey);
      if (invoice?.embeds?.[0]) {
        const previous = invoice.embeds[0];
        const fields = (previous.fields || []).filter((field) =>
          !['payment status', 'cancelled at', 'cancelled by'].includes(String(field.name || '').toLowerCase())
        );
        const oldTitle = String(previous.title || 'Monzo Invoice').replace(/^(?:PAID|CANCELLED|EXPIRED)\s*[|:-]\s*/i, '');
        const cancelledEmbed = EmbedBuilder.from(previous)
          .setColor(GREY)
          .setTitle(`CANCELLED | ${oldTitle}`)
          .setDescription('This invoice has been cancelled. It is no longer payable through this Bankroll invoice.')
          .setFields(fields)
          .addFields(
            { name: 'Payment Status', value: 'CANCELLED', inline: true },
            { name: 'Cancelled At', value: `<t:${tsec(e.cancelledAt)}:F>`, inline: true },
            { name: 'Cancelled By', value: `<@${interaction.user.id}>`, inline: true }
          );
        await invoice.edit({ embeds: [cancelledEmbed], components: [] });
      }
    } catch (updateError) {
      console.error('Could not update cancelled Monzo invoice message:', updateError);
    }

    const hadPending = discardPending(ledgerKey);
    const embed = new EmbedBuilder()
      .setColor(GREY)
      .setTitle('Monzo Invoice Cancelled')
      .addFields(
        { name: 'Reference', value: String(e.reference || ledgerKey || 'Not recorded'), inline: true },
        { name: 'Description', value: short(String(e.description || 'Monzo invoice'), 100), inline: true },
        { name: 'Amount', value: money(Number(e.amount || 0), e.currency || 'gbp'), inline: true },
        { name: 'Pending File/Link Cleaned', value: hadPending ? 'Yes' : 'None attached', inline: true },
        { name: 'Cancelled By', value: `<@${interaction.user.id}>`, inline: true }
      )
      .setTimestamp();
    await interaction.editReply({ embeds: [embed] });
    await logToServer({ embeds: [embed] });
    return;
  }

  if (!input.startsWith('cs_')) {
    return interaction.editReply('Choose a purchase from autocomplete, or paste a Monzo reference (e.g. MONZO-6825) or Stripe Checkout ID (cs_...).');
  }

  try {
    const session = await stripe.checkout.sessions.retrieve(input);
    if (session.status === 'complete' || session.payment_status === 'paid') {
      return interaction.editReply('This Stripe purchase is already PAID. It cannot be cancelled. Use /refund to issue an eligible refund.');
    }
    if (session.status === 'expired') {
      return interaction.editReply('This Stripe checkout has already EXPIRED; there is nothing left to cancel.');
    }
    if (session.status !== 'open') {
      return interaction.editReply(`This checkout is ${String(session.status || 'not open').toUpperCase()} and cannot be cancelled.`);
    }

    await stripe.checkout.sessions.expire(input);
    const hadPending = discardPending(input);
    const embed = new EmbedBuilder()
      .setColor(GREY)
      .setTitle('Invoice Cancelled')
      .addFields(
        { name: 'Checkout ID', value: input, inline: true },
        { name: 'Description', value: short(String(session.metadata?.description || 'Stripe checkout'), 100), inline: true },
        { name: 'Amount', value: money((session.amount_total ?? 0) / 100, session.currency || CURRENCY), inline: true },
        { name: 'Pending File/Link Cleaned', value: hadPending ? 'Yes' : 'None attached', inline: true },
        { name: 'Cancelled By', value: `<@${interaction.user.id}>`, inline: true }
      )
      .setTimestamp();
    await interaction.editReply({ embeds: [embed] });
    await logToServer({ embeds: [embed] });
  } catch (err) {
    console.error('Cancel failed:', err);
    if (interaction.deferred || interaction.replied) await interaction.editReply({ content: `Failed to cancel invoice: ${err.message}`, embeds: [] }).catch(() => {});
    else await interaction.reply({ content: `Failed to cancel invoice: ${err.message}`, flags: 64 }).catch(() => {});
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

async function cmdPaymentTest(interaction) {
  if (interaction.user.id !== OWNER_ID) {
    return interaction.reply({ content: 'Only the bot owner can run payment simulations.', ephemeral: true });
  }

  const method = interaction.options.getSubcommand();
  const customer = interaction.options.getUser('customer', true);
  const agent = interaction.user;
  await interaction.deferReply();

  let testKey = null;
  let createdPath = null;
  try {
    const amount = Math.round(interaction.options.getNumber('amount', true) * 100) / 100;
    const description = interaction.options.getString('description', true);
    const currency = method === 'monzo' ? 'gbp' : (interaction.options.getString('currency') || CURRENCY).toLowerCase();
    const file = interaction.options.getAttachment('file');
    const vaultKey = interaction.options.getString('vault_file');
    const linkRaw = interaction.options.getString('link');
    const mode = interaction.options.getString('delivery') || 'direct';
    const expiresInput = method === 'bankroll' ? interaction.options.getString('expires') : null;

    if ((file && vaultKey) || (file && linkRaw) || (vaultKey && linkRaw)) {
      throw new Error('Use only one of: attached file, vault file, or download link.');
    }
    if (mode === 'link' && !BASE_URL && (file || vaultKey)) {
      throw new Error('Hosted links are not configured. Add PUBLIC_URL to Render or choose Direct file upload.');
    }

    let expiresMins = null;
    if (expiresInput) {
      expiresMins = parseExpiresToMinutes(expiresInput);
      if (!expiresMins || expiresMins < 1 || expiresMins > 30 * 24 * 60) {
        throw new Error('Invalid expiry. Use something like 30m, 2h, 24h, or 7d.');
      }
    }

    let externalLink = null;
    if (linkRaw) {
      try {
        const u = new URL(linkRaw.trim());
        if (!/^https?:$/.test(u.protocol)) throw new Error();
        externalLink = u.href;
      } catch {
        throw new Error('That link is invalid. It must start with http:// or https://.');
      }
    }

    let fileData = null;
    if (file) {
      fileData = await downloadAttachment(file, mode === 'link');
    } else if (vaultKey) {
      const item = vault[vaultKey];
      if (!item || !item.path || !fs.existsSync(item.path)) throw new Error(`Vault item "${vaultKey}" was not found on disk.`);
      fileData = { path: item.path, temp: false, name: item.originalName, size: item.size, sha256: item.sha256 };
    }

    if (fileData && !BASE_URL && mode !== 'link' && fs.statSync(fileData.path).size > MAX_FILE_BYTES) {
      throw new Error('This file is too large for a direct Discord attachment, and PUBLIC_URL is not configured for the download-link fallback.');
    }

    const testId = crypto.randomBytes(5).toString('hex').toUpperCase();
    testKey = (method === 'monzo' ? 'monzo_test_' : 'stripe_test_') + testId.toLowerCase();
    const reference = 'TEST-' + method.toUpperCase() + '-' + testId;
    let expiresAt = null;
    if (method === 'monzo') expiresAt = Date.now() + MONZO_OPEN_MS;
    else if (expiresMins) {
      let secs = expiresMins * 60;
      if (expiresMins === 30) secs += 60;
      if (expiresMins === 1440) secs -= 60;
      expiresAt = Math.floor(Date.now() / 1000) + secs;
    }

    if (fileData) {
      saveDelivery(testKey, fileData, interaction, customer);
      createdPath = deliveries[testKey]?.path || null;
      if (vaultKey) deliveries[testKey].vaultKey = vaultKey;
    } else if (externalLink) {
      saveLinkDelivery(testKey, externalLink, interaction, customer);
    } else {
      deliveries[testKey] = {
        name: description,
        description,
        channelId: interaction.channelId,
        customerId: customer.id,
        agentId: agent.id,
        savedAt: Date.now(),
      };
    }

    Object.assign(deliveries[testKey], {
      type: method === 'monzo' ? 'monzo' : 'stripe',
      paid: false,
      paymentConfirmed: false,
      amount,
      currency,
      reference,
      txId: reference,
      testMode: true,
      testMethod: method,
    });
    saveDeliveries();

    const where = whereFrom(interaction, customer);
    const fileLabel = fileData?.name || (externalLink ? 'External download link' : 'No file attached');
    const embed = new EmbedBuilder()
      .setColor(GREEN)
      .setTitle(method === 'monzo' ? 'Bankroll Monzo Test Invoice' : 'Bankroll Card Test Invoice')
      .setDescription('TEST MODE — no real payment will be taken. The owner can simulate payment using the button below.')
      .addFields(
        { name: 'Sales Agent', value: `<@${agent.id}>`, inline: true },
        { name: 'Customer', value: `<@${customer.id}>`, inline: true },
        { name: 'Amount', value: money(amount, currency), inline: true },
        { name: 'Description', value: description }
      );
    if (method === 'monzo') embed.addFields({ name: 'Payment reference', value: `\`${reference}\``, inline: true });
    if (expiresAt) embed.addFields({ name: 'Pay by', value: method === 'monzo' ? `<t:${tsec(expiresAt)}:R>` : `<t:${expiresAt}:R> (<t:${expiresAt}:f>)` });
    if (fileData || externalLink) embed.addFields({ name: 'File', value: `${fileLabel}\n${mode === 'link' ? 'Download Button' : 'Direct attachment / Download Button'}` });
    embed.addFields(
      { name: 'Generated In Guild', value: where.guild },
      { name: 'Generated In Channel', value: where.channel }
    ).setFooter({ text: `Created by ${agent.username} | ID: ${testKey} | TEST MODE` });

    const row = new ActionRowBuilder().addComponents(
      new ButtonBuilder().setCustomId('testpaid:' + testKey).setLabel('Mark as Paid (TEST)').setStyle(ButtonStyle.Success)
    );
    const invoiceMessage = await interaction.editReply({
      content: `<@${customer.id}>`,
      embeds: [embed],
      components: [row],
      allowedMentions: { parse: [] },
    });
    deliveries[testKey].msgId = invoiceMessage.id;
    deliveries[testKey].msgChannelId = invoiceMessage.channelId;
    saveDeliveries();

    if (method === 'monzo') {
      monzoLedger[testKey] = {
        key: testKey, status: 'open', customerId: customer.id, agentId: agent.id,
        amount, currency: 'gbp', description, reference, createdAt: Date.now(),
        expiresAt, channelId: interaction.channelId, fileName: fileLabel,
        msgUrl: invoiceMessage.url, testMode: true,
      };
      saveMonzo();
    }

    liveInteractions.set(testKey, { interaction, at: Date.now() });
    setTimeout(() => liveInteractions.delete(testKey), 14 * 60 * 1000);
    recordDownloadEvent('test_invoice_created', {
      file: fileLabel, customerId: customer.id, agentId: agent.id,
      detail: 'Test invoice created using the real invoice file, vault, link, amount, description and delivery options.',
    });
    return interaction.followUp({
      content: 'Test invoice posted. Click **Mark as Paid (TEST)** to simulate payment. The selected vault file or attachment will be delivered as a reply to this invoice; if Discord rejects the attachment, Bankroll will use a hosted Download button. No real payment is taken.',
      ephemeral: true,
    });
  } catch (err) {
    if (testKey && deliveries[testKey]) {
      deliveries[testKey].deliveryError = String(err.message || err).slice(0, 300);
      saveDeliveries();
    } else if (createdPath) {
      try { fs.unlinkSync(createdPath); } catch {}
    }
    console.error('Payment test failed:', err);
    return interaction.followUp({ content: 'Payment test failed: ' + err.message, ephemeral: true }).catch(() => {});
  }
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
      { name: 'Last data backup', value: when(lastBackupAt), inline: true },
      { name: 'Memory (RSS)', value: `${(process.memoryUsage().rss / 1024 / 1024).toFixed(0)} MB`, inline: true },
      { name: 'Heap used', value: `${(process.memoryUsage().heapUsed / 1024 / 1024).toFixed(0)} MB`, inline: true },
      { name: 'Event loop delay', value: `${(eventLoopMonitor.mean / 1e6 || 0).toFixed(1)} ms avg`, inline: true }
    );
  return interaction.editReply({ embeds: [embed] });
}

// ---------- /restore ----------
async function cmdRestore(interaction) {
  if (interaction.user.id !== OWNER_ID) return interaction.editReply('Only the owner can run this.');
  flushSaves();
  const att = interaction.options.getAttachment('file');
  if (att.size > 5 * 1024 * 1024) return interaction.editReply('That file is too big to be a backup.');

  const fname = att.name.toLowerCase();
  const target = fname.startsWith('deliveries') ? 'deliveries' : fname.startsWith('downloads') ? 'downloads' : fname.startsWith('download_history') ? 'download_history' : fname.startsWith('vault') ? 'vault' : fname.startsWith('monzo_review') ? 'monzo_review' : fname.startsWith('monzo') ? 'monzo' : fname.startsWith('notifications') ? 'notifications' : null;
  if (!target) return interaction.editReply('Pick deliveries.json, downloads.json, download_history.json, vault.json, monzo_payments.json, monzo_review.json or notifications.json from the backup channel.');

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
    try { atomicWrite(DOWNLOAD_HISTORY_FILE, JSON.stringify(downloadHistory)); } catch (e) { return interaction.editReply(`Could not save download history: ${e.message}`); }
    return interaction.editReply(`Restored ${downloadHistory.length} download history record(s).`);
  }

  if (target === 'monzo_review') {
    if (!data.transactions || typeof data.transactions !== 'object' || Array.isArray(data.transactions) ||
        !data.reviewed || typeof data.reviewed !== 'object' || Array.isArray(data.reviewed)) {
      return interaction.editReply('That monzo_review.json backup must contain transactions and reviewed objects.');
    }
    const before = Object.keys(monzoReviewState.reviewed || {}).length;
    monzoReviewState = mergeMonzoReviewStates(monzoReviewState, data);
    if (!saveMonzoReview()) return interaction.editReply('❌ The review backup was merged in memory but could not be verified on disk. Check Render logs.');
    const after = Object.keys(monzoReviewState.reviewed || {}).length;
    syncMonzoReviewSalesToLedger(true);
    return interaction.editReply('Restored/merged Monzo review history. ' + Math.max(0, after - before) + ' new reviewed sale(s); ' + after + ' total reviewed sale(s) now saved.');
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
    if (target === 'downloads' && (v.used || (v.expiresAt != null && Date.now() > Number(v.expiresAt)))) {
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
  else if (target === 'monzo_review') saveMonzoReview();
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

// ---------- Local card-payment ledger ----------
// /total, /payments and /customer used to list up to 2000 payment intents from Stripe on every run. Now
// every succeeded card payment is kept in a small local ledger that is (1) fully re-synced from Stripe at
// startup and every 6 hours, and (2) updated live from webhooks. Until the first full sync of this run has
// succeeded (or if it fails) the commands fall back to asking Stripe directly, exactly as before.
// The ledger is derived data that Stripe can always rebuild, so it is NOT part of the Discord backups.
const STRIPE_LEDGER_FILE = path.join(DATA_DIR, 'stripe_ledger.json');
let stripeLedger = {};
let stripeLedgerReady = false;
try {
  const savedLedger = JSON.parse(fs.readFileSync(STRIPE_LEDGER_FILE, 'utf8'));
  if (savedLedger && typeof savedLedger.payments === 'object' && !Array.isArray(savedLedger.payments)) stripeLedger = savedLedger.payments;
} catch {}
function saveStripeLedger() {
  debouncedSave('stripe_ledger', () => {
    try { atomicWrite(STRIPE_LEDGER_FILE, JSON.stringify({ savedAt: Date.now(), payments: stripeLedger })); }
    catch (e) { console.error('Could not save Stripe ledger:', e.message); }
  });
}
// Turns a Stripe PaymentIntent (ideally expanded with latest_charge.balance_transaction) into a ledger record.
function paymentRecordFromPI(pi, { requireAgent = true } = {}) {
  if (!pi || pi.status !== 'succeeded') return null;
  if (requireAgent && !pi.metadata?.agent_id) return null;
  const ch = pi.latest_charge && typeof pi.latest_charge === 'object' ? pi.latest_charge : null;
  const bt = ch?.balance_transaction && typeof ch.balance_transaction === 'object' ? ch.balance_transaction : null;
  return {
    id: pi.id,
    created: Number(pi.created || 0),
    amount: Number(pi.amount_received ?? pi.amount ?? 0),
    currency: String(pi.currency || '').toLowerCase(),
    customerId: String(pi.metadata?.customer_id || ''),
    agentId: String(pi.metadata?.agent_id || ''),
    description: String(pi.metadata?.description || ''),
    fee: bt ? Number(bt.fee || 0) : null,
    refunded: ch ? Number(ch.amount_refunded || 0) : 0,
  };
}
let stripeLedgerSyncing = null;
function syncStripeLedger() {
  if (stripeLedgerSyncing) return stripeLedgerSyncing;
  stripeLedgerSyncing = (async () => {
    try {
      const all = await stripe.paymentIntents.list({ limit: 100, expand: ['data.latest_charge.balance_transaction'] }).autoPagingToArray({ limit: 2000 });
      for (const pi of all) {
        const record = paymentRecordFromPI(pi);
        if (record) stripeLedger[record.id] = record;
      }
      stripeLedgerReady = true;
      saveStripeLedger();
      console.log(`Stripe ledger synced: ${Object.keys(stripeLedger).length} card payment(s) on record.`);
    } catch (e) {
      console.error('Stripe ledger sync failed (commands keep asking Stripe directly):', e.message);
    } finally { stripeLedgerSyncing = null; }
  })();
  return stripeLedgerSyncing;
}
async function updateStripeLedgerFromPI(piId) {
  if (!piId) return;
  try {
    const pi = await stripe.paymentIntents.retrieve(String(piId), { expand: ['latest_charge.balance_transaction'] });
    const record = paymentRecordFromPI(pi);
    if (record) { stripeLedger[record.id] = record; saveStripeLedger(); }
  } catch (e) { console.error('Could not update the local payment ledger:', e.message); }
}

// ---------- Stats commands ----------
async function succeededPayments(sinceUnix) {
  let records;
  if (stripeLedgerReady) {
    records = Object.values(stripeLedger).filter((r) => !sinceUnix || r.created >= sinceUnix);
  } else {
    const params = { limit: 100, expand: ['data.latest_charge.balance_transaction'] };
    if (sinceUnix) params.created = { gte: sinceUnix };
    const all = await stripe.paymentIntents.list(params).autoPagingToArray({ limit: 2000 });
    records = all.map((pi) => paymentRecordFromPI(pi)).filter(Boolean);
  }
  return records.filter((r) => !wipedCustomers.has(r.customerId));
}

async function cmdPayments(interaction) {
  // Reconcile every saved /monzo-review sale into the shared paid ledger before reporting.
  syncMonzoReviewSalesToLedger();
  const count = interaction.options.getInteger('count') ?? 5;
  let card = [];
  try {
    card = await succeededPayments();
  } catch (e) {
    console.error('Stripe payments lookup failed:', e.message);
  }
  const all = [
    ...card.map((r) => ({
      at: r.created,
      text:
        `${money(r.amount / 100, r.currency)} - ${r.customerId ? `<@${r.customerId}>` : 'Unknown customer'} - ` +
        `${short(r.description, 40)} - <t:${r.created}:d> (Card)`,
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
  // Reconcile every saved /monzo-review sale into the shared paid ledger before reporting.
  syncMonzoReviewSalesToLedger();
  const period = interaction.options.getString('period');
  const { since, until } = periodBounds(period);
  const everything = (await succeededPayments(since)).filter((r) => until == null || r.created < until);
  const list = everything.filter((r) => r.currency === CURRENCY.toLowerCase());
  const otherCurrency = everything.length - list.length;

  const monzoInPeriod = monzoPaid().filter((m) => (since == null || tsec(m.paidAt) >= since) && (until == null || tsec(m.paidAt) < until));
  const monzoList = monzoInPeriod.filter((m) => m.currency === CURRENCY.toLowerCase());
  const monzoOther = monzoInPeriod.length - monzoList.length;
  const monzoSum = monzoList.reduce((a, m) => a + m.amount, 0);

  const cardSum = list.reduce((a, r) => a + r.amount, 0) / 100;
  const sum = cardSum + monzoSum;

  let feePence = 0;
  let refundedPence = 0;
  let missingFees = 0;
  for (const r of list) {
    refundedPence += r.refunded || 0;
    if (r.fee != null) feePence += r.fee;
    else missingFees += 1;
  }
  const fees = feePence / 100;
  const refunded = refundedPence / 100;
  const keep = sum - refunded - fees;

  const perAgent = {};
  for (const r of list) {
    const id = r.agentId;
    perAgent[id] = (perAgent[id] ?? 0) + r.amount / 100;
  }
  for (const m of monzoList) {
    if (m.agentId) perAgent[m.agentId] = (perAgent[m.agentId] ?? 0) + m.amount;
  }
  const agentLines = Object.entries(perAgent)
    .sort((a, b) => b[1] - a[1])
    .map(([id, v]) => `<@${id}>: ${money(v, CURRENCY)}`);

  const label = { today: 'Today', week: 'This week', last_week: 'Last week', month: 'This month', last_month: 'Last month', all: 'All time' }[period];
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

async function cmdExport(interaction) {
  if (interaction.user.id !== OWNER_ID) return interaction.editReply('Only the bot owner can export the source file.');
  if (interaction.options.getSubcommand() !== 'index') return interaction.editReply('Unknown export option.');
  if (!interaction.guild) return interaction.editReply('Run /export index in a server where Bankroll can manage channels.');

  const channelName = 'bankroll-index-logs';
  let logChannel = interaction.guild.channels.cache.find(
    (channel) => channel.name === channelName && channel.type === ChannelType.GuildText
  );

  if (!logChannel) {
    const me = interaction.guild.members.me || await interaction.guild.members.fetchMe();
    if (!me.permissions.has(PermissionFlagsBits.ManageChannels)) {
      return interaction.editReply('Bankroll needs Manage Channels permission to create the private index logs channel.');
    }
    const overwrites = [
      { id: interaction.guild.roles.everyone.id, deny: [PermissionFlagsBits.ViewChannel] },
      { id: me.id, allow: [PermissionFlagsBits.ViewChannel, PermissionFlagsBits.SendMessages, PermissionFlagsBits.AttachFiles, PermissionFlagsBits.ReadMessageHistory] },
    ];
    if (interaction.guild.members.cache.has(OWNER_ID) || await interaction.guild.members.fetch(OWNER_ID).catch(() => null)) {
      overwrites.push({ id: OWNER_ID, allow: [PermissionFlagsBits.ViewChannel, PermissionFlagsBits.SendMessages, PermissionFlagsBits.AttachFiles, PermissionFlagsBits.ReadMessageHistory] });
    }
    logChannel = await interaction.guild.channels.create({
      name: channelName,
      type: ChannelType.GuildText,
      permissionOverwrites: overwrites,
      reason: 'Private Bankroll index.js export and audit log',
    });
  }

  const sourcePath = path.join(__dirname, 'index.js');
  if (!fs.existsSync(sourcePath)) return interaction.editReply('Could not find the running index.js file.');
  const timestamp = new Date().toLocaleString('en-GB', { timeZone: 'Europe/London', dateStyle: 'medium', timeStyle: 'medium' });
  await logChannel.send({
    content: '**Bankroll index.js export**\\nRequested by: <@' + interaction.user.id + '>\\nTime: ' + timestamp,
    files: [new AttachmentBuilder(sourcePath, { name: 'index.js' })],
    allowedMentions: { users: [interaction.user.id] },
  });
  return interaction.editReply('Raw `index.js` uploaded to <#' + logChannel.id + '>.');
}

// Shown by /bot refresh-commands so the owner can see what this deployed version changed. Add a line per update.
const RECENT_UPDATES = [
  '/bk balance, /bk payout and /bk payouts: live Stripe balance, confirmed manual payouts to the linked bank account, and payout history.',
  '/monzo-review: customer Discord ID can be left blank and assigned after all transactions are reviewed.',
  '/monzo-review: supports this month, last month, this week and last week, with editable transaction dates.',
  '/bot refresh-commands: compares the full command definition, reports added/removed/changed commands and verifies Discord registration.',
  '/sendfile: accepts any file type stored in the vault, not only ZIP archives.',
  '/export index: uploads the raw index.js file to the private #bankroll-index-logs channel and records who ran it and when.',
  'Historical Monzo customer IDs are assigned to the nine reviewed sales in newest-to-oldest order.',
];

async function cmdBotAdmin(interaction) {
  if (interaction.user.id !== OWNER_ID) return interaction.editReply('Only the bot owner can use these controls.');
  const sub = interaction.options.getSubcommand();

  if (sub === 'refresh-commands') {
    const desired = commands.map((c) => c.toJSON());
    const currentCollection = await client.application.commands.fetch().catch((e) => {
      console.error('Could not fetch existing global slash commands:', e.message);
      return null;
    });
    const current = currentCollection ? [...currentCollection.values()].map((c) => c.toJSON()) : [];
    // Compare the complete command schema, not just a small subset of option fields.
    // Discord adds IDs/version metadata to fetched commands; those are deliberately ignored.
    const simplify = (value) => {
      if (Array.isArray(value)) return value.map(simplify);
      if (!value || typeof value !== 'object') return value;
      const ignored = new Set(['id', 'application_id', 'guild_id', 'version']);
      return Object.fromEntries(Object.keys(value).sort()
        .filter((key) => !ignored.has(key) && value[key] !== undefined && value[key] !== null)
        .map((key) => [key, simplify(value[key])]));
    };
    const compareOptions = (before = [], after = [], prefix = '') => {
      const a = new Map(before.map((o) => [o.name, o]));
      const b = new Map(after.map((o) => [o.name, o]));
      const changes = [];
      for (const [name, next] of b) {
        const path = prefix ? prefix + ' / ' + name : name;
        if (!a.has(name)) changes.push('added option ' + path);
        else if (JSON.stringify(simplify(a.get(name))) !== JSON.stringify(simplify(next))) {
          const nested = compareOptions(a.get(name).options || [], next.options || [], path);
          changes.push(...(nested.length ? nested : ['updated option ' + path]));
        }
      }
      for (const name of a.keys()) if (!b.has(name)) changes.push('removed option ' + (prefix ? prefix + ' / ' : '') + name);
      return changes;
    };
    const oldByName = new Map(current.map((c) => [c.name, c]));
    const newByName = new Map(desired.map((c) => [c.name, c]));
    const added = [...newByName.keys()].filter((n) => !oldByName.has(n));
    const removed = [...oldByName.keys()].filter((n) => !newByName.has(n));
    const changed = [];
    for (const [name, next] of newByName) {
      const prev = oldByName.get(name);
      if (!prev) continue;
      if (JSON.stringify(simplify(prev)) !== JSON.stringify(simplify(next))) {
        const details = compareOptions(prev.options || [], next.options || []);
        changed.push('/' + name + (details.length ? ': ' + details.join(', ') : ': description or command settings updated'));
      }
    }

    try {
      await client.application.commands.set(desired);
    } catch (e) {
      console.error('Slash command refresh failed:', e);
      return interaction.editReply(('**Slash command refresh failed.** Discord rejected the command definitions:\n' + String(e?.message || e)).slice(0, 1900));
    }
    const verifiedCollection = await client.application.commands.fetch().catch((e) => {
      console.error('Could not verify refreshed global commands:', e.message);
      return null;
    });
    const verifiedCount = verifiedCollection?.size ?? null;
    let guildNote = '';
    if (DISCORD_GUILD_ID) {
      const guild = await client.guilds.fetch(DISCORD_GUILD_ID).catch(() => null);
      if (guild) {
        const oldGuild = await guild.commands.fetch().catch(() => null);
        const oldGuildCount = oldGuild?.size || 0;
        await guild.commands.set([]);
        guildNote = '\nRemoved ' + oldGuildCount + ' old server-only command copy/copies.';
      }
    }
    const changeLines = [
      added.length ? 'Added: ' + added.map((n) => '/' + n).join(', ') : 'Added: none',
      removed.length ? 'Removed: ' + removed.map((n) => '/' + n).join(', ') : 'Removed: none',
      changed.length ? 'Changed: ' + changed.join(' | ') : 'Changed: none',
    ];
    const lines = [
      '**Slash commands refreshed and sent to Discord.**',
      'Desired: **' + desired.length + '** global commands' +
        (verifiedCount === null ? ' • Verification fetch unavailable' : ' • Discord now reports **' + verifiedCount + '**'),
      '',
      '**Changes since the previous Discord command list:**',
      ...changeLines,
      '',
      '**Latest code updates (running Render version):**',
      ...RECENT_UPDATES.map((u) => '• ' + u),
      '',
      'Global command changes can take a little while to appear in every Discord client. This refresh uses the code currently running on Render; deploy first if GitHub has newer code.',
      guildNote
    ].filter(Boolean);
    let response = lines.join('\n');
    if (response.length > 1900) response = response.slice(0, 1880) + '\n…Use the Render logs for the complete registration result.';
    return interaction.editReply(response);
  }

  if (sub === 'avatar') {
    const image = interaction.options.getAttachment('image');
    const type = String(image.contentType || '').toLowerCase();
    if (!/^image\/(png|jpe?g|gif|webp)$/.test(type)) return interaction.editReply('Please upload a PNG, JPG, GIF or WEBP image.');
    if (image.size > 8 * 1024 * 1024) return interaction.editReply('That image is over 8 MB. Please upload a smaller image.');
    const response = await fetch(image.url);
    if (!response.ok) throw new Error(`Could not download the avatar image (HTTP ${response.status}).`);
    const buffer = Buffer.from(await response.arrayBuffer());
    if (!buffer.length || buffer.length > 8 * 1024 * 1024) return interaction.editReply('The image is empty or exceeds the 8 MB limit.');
    await client.user.setAvatar(buffer);
    return interaction.editReply('Bot profile picture updated successfully.');
  }

  if (sub === 'clear-dm') {
    if (interaction.guild || !interaction.channel?.isDMBased?.()) {
      return interaction.editReply('Run this command inside your direct message conversation with Bankroll.');
    }
    const limit = interaction.options.getInteger('count') || 500;
    let deleted = 0;
    let scanned = 0;
    let before;
    while (deleted < limit && scanned < 2000) {
      const batch = await interaction.channel.messages.fetch({ limit: 100, ...(before ? { before } : {}) });
      if (!batch.size) break;
      const ordered = [...batch.values()].sort((a, b) => b.createdTimestamp - a.createdTimestamp);
      before = ordered[ordered.length - 1]?.id;
      scanned += ordered.length;
      for (const message of ordered) {
        if (deleted >= limit) break;
        if (message.author?.id !== client.user.id || message.id === interaction.id) continue;
        try {
          await message.delete();
          deleted++;
        } catch (error) {
          console.warn('Could not delete a Bankroll DM message:', error.message);
        }
      }
      if (batch.size < 100) break;
    }
    return interaction.editReply(`Deleted **${deleted}** Bankroll message(s) from this DM. Your messages were left untouched.`);
  }

  if (sub === 'status') {
    const presence = interaction.options.getString('presence');
    await client.user.setPresence({ status: presence, activities: [] });
    try {
      savePresence(presence);
    } catch (error) {
      console.error('Could not persist bot presence:', error);
      return interaction.editReply(`Presence changed to **${presence}**, but could not be saved for the next restart: ${short(error.message, 180)}`);
    }
    const labels = { online: 'Online (green)', idle: 'Idle (yellow)', dnd: 'Do Not Disturb (red)', invisible: 'Invisible' };
    return interaction.editReply(`Bot presence changed to **${labels[presence] || presence}** and saved. It will be restored after restarts.`);
  }

  if (sub === 'restart' || sub === 'redeploy') {
    if (!RENDER_API_KEY || !RENDER_SERVICE_ID) {
      return interaction.editReply('Render control is not configured. Add RENDER_API_KEY and RENDER_SERVICE_ID to the Render environment variables, then try again.');
    }
    const endpoint = sub === 'restart'
      ? `https://api.render.com/v1/services/${encodeURIComponent(RENDER_SERVICE_ID)}/restart`
      : `https://api.render.com/v1/services/${encodeURIComponent(RENDER_SERVICE_ID)}/deploys`;
    const response = await fetch(endpoint, {
      method: 'POST',
      headers: { Authorization: `Bearer ${RENDER_API_KEY}`, 'Content-Type': 'application/json' },
      ...(sub === 'redeploy' ? { body: JSON.stringify({ clearCache: 'do_not_clear' }) } : {}),
    });
    const body = await response.text();
    if (!response.ok) throw new Error(`Render ${sub} request failed (HTTP ${response.status}): ${short(body, 350)}`);
    return interaction.editReply(sub === 'redeploy'
      ? 'Render accepted the deployment request. It will build and deploy the latest commit from the connected branch.'
      : 'Render accepted the restart request. The bot will briefly go offline while the service restarts.');
  }
}

async function cmdCustomerPreview(interaction) {
  const ids = new Set();
  for (const d of Object.values(deliveries)) if (d?.customerId) ids.add(String(d.customerId));
  for (const m of Object.values(monzoLedger)) if (m?.customerId) ids.add(String(m.customerId));
  for (const d of Object.values(downloads)) if (d?.customerId && !d.backupDownload) ids.add(String(d.customerId));
  let sessions = [];
  try { sessions = await recentCheckoutSessions(); }
  catch (e) { console.error('Customer preview Stripe lookup failed:', e.message); }
  for (const x of sessions) if (x.metadata?.customer_id) ids.add(String(x.metadata.customer_id));
  const choices = [];
  const previewIds = [...ids].slice(0, 25);
  const previewLabels = await Promise.all(previewIds.map((id) => client.users.fetch(id).then((u) => u.username).catch(() => 'Customer')));
  for (const [index, id] of previewIds.entries()) {
    const label = previewLabels[index];
    const count = Object.values(deliveries).filter((d) => String(d?.customerId) === id).length +
      Object.values(monzoLedger).filter((m) => String(m?.customerId) === id).length +
      sessions.filter((x) => String(x.metadata?.customer_id) === id).length;
    choices.push({ label: short(label, 65), description: `${count} invoice/order record(s) • ID ${id.slice(-8)}`.slice(0, 100), value: id });
  }
  if (!choices.length) return interaction.editReply('No customers found yet. Customers appear here after an invoice or order has been created.');
  const menu = new StringSelectMenuBuilder().setCustomId('customer_preview_select').setPlaceholder('Choose a customer to preview').addOptions(choices);
  const row = new ActionRowBuilder().addComponents(menu);
  return interaction.editReply({ content: `**Customer preview**\nSelect someone below to see their invoice history, payment state, delivery message and the buttons they would see. This is a private, read-only preview.`, components: [row] });
}

async function onCustomerPreviewSelect(interaction) {
  if (!allowed.has(interaction.user.id)) return interaction.reply({ content: 'You are not allowed to use this preview.', ephemeral: true });
  const customerId = interaction.values?.[0];
  if (!/^\d{17,20}$/.test(String(customerId || ''))) return interaction.update({ content: 'Invalid customer selection.', embeds: [], components: [] });
  let customer;
  try { customer = await client.users.fetch(customerId); } catch { customer = { username: 'Unknown customer', id: customerId }; }
  let sessions = [];
  try {
    sessions = await recentCheckoutSessions();
    sessions = sessions.filter((x) => String(x.metadata?.customer_id || '') === customerId && !wipedCustomers.has(String(x.metadata?.customer_id || '')));
  } catch (e) { console.error('Customer preview session lookup failed:', e.message); }
  const localOrders = Object.entries(deliveries).filter(([, d]) => String(d?.customerId || '') === customerId);
  const monzo = Object.entries(monzoLedger).filter(([, m]) => String(m?.customerId || '') === customerId);
  const links = Object.values(downloads).filter((d) => String(d?.customerId || '') === customerId && !d.backupDownload);
  const status = (x) => x.payment_status === 'paid' ? 'PAID' : x.status === 'expired' ? 'EXPIRED' : 'AWAITING PAYMENT';
  const rows = [
    ...sessions.map((x) => ({ at: x.created, text: `**${status(x)}** • ${money((x.amount_total || 0) / 100, x.currency)} • ${short(x.metadata?.description || 'Bankroll order', 65)} • Card` })),
    ...monzo.map(([key, x]) => ({ at: tsec(x.createdAt || x.paidAt), text: `**${String(x.status || 'open').toUpperCase()}** • ${money(x.amount, x.currency || 'gbp')} • ${short(x.description || 'Monzo invoice', 65)} • Monzo` })),
    ...localOrders.filter(([key, d]) => d.type !== 'monzo' && !sessions.some((x) => x.id === key)).map(([key, d]) => ({ at: tsec(d.savedAt || Date.now()), text: `**${d.paid || d.paymentConfirmed ? 'PAID' : 'AWAITING PAYMENT'}** • ${money(d.amount || 0, d.currency || CURRENCY)} • ${short(d.description || d.name || 'Order', 65)} • Card` })),
  ].sort((a, b) => b.at - a.at);
  const activeLinks = links.filter((d) => !d.used && (d.expiresAt == null || Date.now() < Number(d.expiresAt))).length;
  const completed = downloadHistory.filter((e) => e.type === 'download_completed' && String(e.customerId || '') === customerId).length;
  const latestDelivery = localOrders.map(([, d]) => d).sort((a, b) => Number(b.deliveredAt || b.paymentConfirmedAt || b.savedAt || 0) - Number(a.deliveredAt || a.paymentConfirmedAt || a.savedAt || 0))[0];
  const embed = new EmbedBuilder().setColor(GREEN).setTitle(`Customer view • ${short(customer.username, 180)}`)
    .setDescription(rows.length ? rows.slice(0, 15).map((x) => x.text).join('\n') : 'No invoice records found for this customer.')
    .addFields(
      { name: 'Customer', value: `<@${customerId}>\n\`${customerId}\``, inline: true },
      { name: 'Orders / invoices', value: String(rows.length), inline: true },
      { name: 'Active download links', value: String(activeLinks), inline: true },
      { name: 'Completed downloads', value: String(completed), inline: true },
      { name: 'Customer-facing experience', value: latestDelivery?.paid || latestDelivery?.paymentConfirmed
        ? (latestDelivery?.fallbackLink || links.length ? 'After payment: **Your download is ready** embed with a one-time **Download ZIP / Download File** button.\n\nIf sent as a Discord attachment, they receive the file directly instead of a hosted-link button.' : 'After payment: a **Your download is ready** embed with the ZIP/file attached directly in the DM.')
        : 'Before payment: invoice details and payment instructions/buttons (depending on card or Monzo method).', inline: false },
      { name: 'Download status', value: activeLinks ? 'A hosted one-time link is available.' : completed ? 'A download has completed; no active hosted link remains.' : 'No active hosted download link recorded.', inline: false }
    ).setFooter({ text: 'Private read-only preview • No customer messages sent • No payment or download actions performed' }).setTimestamp();
  return interaction.update({ content: '', embeds: [embed], components: [] });
}

async function cmdDelivery(interaction) {
  if (interaction.user.id !== OWNER_ID) return interaction.editReply('Only the bot owner can manage paid deliveries.');
  const sub = interaction.options.getSubcommand();
  const key = interaction.options.getString('order').trim();
  const d = deliveries[key];
  if (!d) return interaction.editReply('Order not found in saved delivery records. Use the full Checkout Session ID (cs_...) or Monzo invoice ID.');
  const paid = !!(d.paymentConfirmed || d.paid || d.status === 'paid');
  if (sub === 'status') {
    const completed = downloadHistory.find((e) => e.type === 'download_completed' && e.customerId === d.customerId && e.file === d.name && Number(e.at || 0) >= Number(d.deliveredAt || 0));
    const state = !paid ? 'AWAITING PAYMENT' : d.deliveryComplete ? (completed ? 'DOWNLOADED' : 'DELIVERED — AWAITING DOWNLOAD') : d.deliveryError ? 'DELIVERY FAILED / NEEDS ATTENTION' : 'PAID — DELIVERY PENDING';
    const lines = [
      '**Order:** ' + key,
      '**Customer:** ' + (d.customerId ? '<@' + d.customerId + '>' : 'Unknown'),
      '**Payment:** ' + (paid ? 'Paid' : 'Not confirmed'),
      '**Delivery:** ' + state,
      '**Amount:** ' + money(Number(d.amount || 0), d.currency || CURRENCY),
      '**Paid at:** ' + (d.paymentConfirmedAt ? '<t:' + Math.floor(d.paymentConfirmedAt / 1000) + ':F>' : '—'),
      '**Delivered at:** ' + (d.deliveredAt ? '<t:' + Math.floor(d.deliveredAt / 1000) + ':F>' : '—'),
      '**Download completed:** ' + (completed ? '<t:' + Math.floor(completed.at / 1000) + ':F>' : 'Not recorded'),
      '**Last error:** ' + short(d.deliveryError || 'None', 500),
      d.deliveryMessageUrl ? '**Delivery message:** ' + d.deliveryMessageUrl : ''
    ].filter(Boolean);
    return interaction.editReply({ embeds: [new EmbedBuilder().setColor(!paid ? GREY : d.deliveryComplete ? GREEN : RED).setTitle('Delivery status • ' + short(d.name || key, 150)).setDescription(lines.join('\n')).setTimestamp()] });
  }
  if (!paid) return interaction.editReply('This order is not marked as paid. Bankroll will not resend unpaid orders.');
  // If this delivery originally used a vault item, reconnect it to the current
  // vault path before refusing a resend. This helps after a vault file is re-added.
  if ((!d.path || !fs.existsSync(d.path)) && d.vaultKey && vault[d.vaultKey]?.path && fs.existsSync(vault[d.vaultKey].path)) {
    try {
      fs.mkdirSync(path.dirname(d.path || path.join(FILES_DIR, path.basename(vault[d.vaultKey].path))), { recursive: true });
      const restoredPath = d.path || path.join(FILES_DIR, path.basename(vault[d.vaultKey].path));
      try { fs.linkSync(vault[d.vaultKey].path, restoredPath); } catch { fs.copyFileSync(vault[d.vaultKey].path, restoredPath); }
      d.path = restoredPath;
      delete d.deliveryError;
      saveDeliveries();
    } catch (restoreError) {
      console.error('Could not relink vault file for paid delivery:', restoreError);
    }
  }
  if (!d.path || !fs.existsSync(d.path)) return interaction.editReply('The original file is still missing. Re-add it to the vault with /vault add, then retry /delivery resend.');
  if (activeDeliveryJobs.has(key)) return interaction.editReply('This order is already being delivered. Wait for the current attempt to finish.');
  d.deliveryComplete = false;
  d.deliveryAttempts = 0;
  delete d.nextDeliveryRetryAt;
  delete d.deliveryError;
  delete d.fallbackLink;
  saveDeliveries();
  await interaction.editReply('Resending the paid file now. No new payment will be created.');
  await deliverFile(key, d.paymentIntentId || null, { force: true });
  return interaction.editReply(deliveries[key] && deliveries[key].deliveryComplete
    ? 'Resend completed for <@' + d.customerId + '>. The original payment record has been kept.'
    : 'The resend attempt did not complete. The paid order remains saved for recovery.');
}

async function cmdWipe(interaction) {
  if (interaction.user.id !== OWNER_ID) return interaction.editReply('Only the bot owner can wipe customer records.');
  const user = interaction.options.getUser('user');
  if (user?.bot) return interaction.editReply('Choose a real customer account, not a bot.');
  const wipeAll = !user;
  const confirmValue = interaction.options.getString('confirm');
  if (confirmValue !== (wipeAll ? 'WIPE ALL' : 'WIPE')) {
    return interaction.editReply(wipeAll ? 'Wipe cancelled. To wipe ALL customers and payments, choose WIPE ALL.' : 'Wipe cancelled. Choose WIPE to confirm.');
  }

  // Which customers are being wiped: one chosen customer, or every customer Bankroll has a record of.
  const ids = new Set();
  if (user) ids.add(String(user.id));
  else {
    for (const d of Object.values(deliveries)) if (d?.customerId) ids.add(String(d.customerId));
    for (const m of Object.values(monzoLedger)) if (m?.customerId) ids.add(String(m.customerId));
    for (const d of Object.values(downloads)) if (!d?.backupDownload && d?.customerId) ids.add(String(d.customerId));
    for (const h of downloadHistory) if (h?.customerId) ids.add(String(h.customerId));
    for (const h of Object.values(monzoReviewState.reviewed || {})) if (h?.customerId) ids.add(String(h.customerId));
    try {
      const all = await stripe.paymentIntents.list({ limit: 100 }).autoPagingToArray({ limit: 5000 });
      for (const pi of all) if (pi.metadata?.agent_id && pi.metadata?.customer_id) ids.add(String(pi.metadata.customer_id));
    } catch (e) { console.error('Could not list Stripe payments for wipe:', e.message); }
  }
  const id = user ? String(user.id) : 'ALL';
  let monzoCount = 0, deliveryCount = 0, downloadCount = 0, historyCount = 0, logCount = 0, reviewCount = 0;
  const removedDeliveryKeys = Object.entries(deliveries).filter(([, d]) => ids.has(String(d?.customerId || ''))).map(([key]) => key);
  const removedDownloadTokens = Object.entries(downloads).filter(([, d]) => !d?.backupDownload && ids.has(String(d?.customerId || ''))).map(([token]) => token);
  const removedDownloadPaths = removedDownloadTokens.map((token) => downloads[token]?.path).filter(Boolean);

  for (const key of removedDeliveryKeys) {
    const timer = deliveryRetryTimers.get(key);
    if (timer) clearTimeout(timer);
    deliveryRetryTimers.delete(key);
    liveInteractions.delete(key);
    delete deliveries[key];
    deliveryCount++;
  }
  for (const key of Object.keys(monzoLedger)) {
    if (!ids.has(String(monzoLedger[key]?.customerId || ''))) continue;
    for (const pct of CHECKIN_PCTS) {
      const timerKey = key + ':' + pct;
      const timer = monzoReminderTimers.get(timerKey);
      if (timer) clearTimeout(timer);
      monzoReminderTimers.delete(timerKey);
    }
    delete monzoLedger[key];
    monzoCount++;
  }
  for (const token of removedDownloadTokens) { delete downloads[token]; downloadCount++; }
  const oldHistoryLength = downloadHistory.length;
  downloadHistory = downloadHistory.filter((entry) => !ids.has(String(entry?.customerId || '')));
  historyCount = oldHistoryLength - downloadHistory.length;
  for (const [txId, record] of Object.entries(monzoReviewState.reviewed || {})) {
    if (!ids.has(String(record?.customerId || ''))) continue;
    delete monzoReviewState.reviewed[txId];
    reviewCount++;
  }
  if (reviewCount) saveMonzoReview();

  const referencedPaths = new Set([
    ...Object.values(vault).map((v) => path.resolve(String(v?.path || ''))),
    ...Object.values(deliveries).map((d) => path.resolve(String(d?.path || ''))),
    ...Object.values(downloads).map((d) => path.resolve(String(d?.path || ''))),
  ]);
  for (const oldPath of removedDownloadPaths) {
    const resolved = path.resolve(String(oldPath));
    if (resolved.startsWith(path.resolve(FILES_DIR) + path.sep) && !referencedPaths.has(resolved)) {
      try { await fs.promises.unlink(resolved); } catch {}
    }
  }

  for (const wipedId of ids) wipedCustomers.add(wipedId);
  saveWipedCustomers();
  saveMonzo();
  saveDeliveries();
  saveDownloads();
  atomicWrite(DOWNLOAD_HISTORY_FILE, JSON.stringify(downloadHistory, null, 2));

  // Remove only Bankroll-authored log messages that contain the selected customer's ID.
  try {
    const channel = await getLogChannel();
    if (channel?.messages?.fetch) {
      let before;
      for (let page = 0; page < 10; page++) {
        const batch = await channel.messages.fetch({ limit: 100, ...(before ? { before } : {}) });
        if (!batch.size) break;
        for (const message of batch.values()) {
          if (message.author?.id !== client.user.id) continue;
          const parts = [message.content || '', ...message.embeds.flatMap((e) => [
            e.title || '', e.description || '', e.footer?.text || '',
            ...(e.fields || []).flatMap((field) => [field.name || '', field.value || '']),
          ])];
          const body = parts.join(' ');
          if ([...ids].some((wipedId) => body.includes(wipedId))) {
            if (await message.delete().then(() => true).catch(() => false)) logCount++;
          }
        }
        before = batch.last()?.id;
        if (batch.size < 100) break;
      }
    }
  } catch (error) { console.error('Customer log cleanup failed:', error.message); }

  recordNotification('customer.wipe', 'Customer records wiped',
    'Removed Bankroll-local Monzo, delivery, download and audit records for ' + (user ? 'customer ' + id : ids.size + ' customers (wipe all)') + '. Provider-side payment records are not deleted.', 'warning');
  await backupData();
  return interaction.editReply({
    content: [
      '**Customer records wiped**',
      user ? 'Customer: <@' + id + '> (' + id + ')' : 'Customers wiped: **' + ids.size + '** (everyone)',
      'Monzo records removed: **' + monzoCount + '**',
      'Delivery/order records removed: **' + deliveryCount + '**',
      'Download links removed: **' + downloadCount + '**',
      'Download history entries removed: **' + historyCount + '**',
      'Historical Monzo review records removed: **' + reviewCount + '**',
      'Bot-authored log messages deleted: **' + logCount + '**',
      '',
      'Their card payments are now hidden from Bankroll reports. Actual Stripe payments and Monzo bank statements remain unchanged.',
    ].join('\n'),
    allowedMentions: { users: user ? [id] : [] },
  });
}

function monzoReviewTransactionsSorted(period = 'last_month') {
  const bounds = periodBounds(period);
  return Object.values(monzoReviewState.transactions || {})
    .filter((tx) => {
      const at = Math.floor(Date.parse(tx.created || tx.settled || 0) / 1000);
      return tx && tx.id && Number(tx.amount) > 0 && at >= bounds.since && at < bounds.until && !monzoReviewState.reviewed?.[tx.id];
    })
    .sort((a, b) => Date.parse(b.created || b.settled || 0) - Date.parse(a.created || a.settled || 0));
}

function monzoReviewPayload(page = 0, period = 'last_month') {
  const pending = monzoReviewTransactionsSorted(period);
  const pageSize = 25;
  const pageCount = Math.max(1, Math.ceil(pending.length / pageSize));
  const current = Math.max(0, Math.min(Number(page) || 0, pageCount - 1));
  const items = pending.slice(current * pageSize, (current + 1) * pageSize);
  const bounds = periodBounds(period);
  const all = Object.values(monzoReviewState.transactions || {}).filter((tx) => {
    const at = Math.floor(Date.parse(tx.created || tx.settled || 0) / 1000);
    return Number(tx.amount) > 0 && at >= bounds.since && at < bounds.until;
  });
  const reviewed = all.filter((tx) => !!monzoReviewState.reviewed?.[tx.id]).length;
  const income = all.reduce((sum, tx) => sum + Number(tx.amount || 0), 0) / 100;
  const embed = new EmbedBuilder()
    .setColor(GREEN)
    .setTitle('Monzo historical transaction review')
    .setDescription(
      '**' + ({ month: 'This month', last_month: 'Last month', week: 'This week', last_week: 'Last week' }[period] || period) + '**\n' +
      'Incoming transactions: **' + all.length + '** • Reviewed: **' + reviewed + '** • Awaiting review: **' + pending.length + '**\n' +
      'Money received: **' + money(income, 'gbp') + '**\n\n' +
      'Choose a transaction. Add the customer Discord ID, what was sold, and correct the transaction date if needed. Sales agent is automatically set to you.'
    )
    .setFooter({ text: pending.length ? 'Page ' + (current + 1) + ' of ' + pageCount + ' • ' + pending.length + ' awaiting review' : 'All imported transactions have been reviewed.' });
  const components = [];
  if (items.length) {
    const menu = new StringSelectMenuBuilder()
      .setCustomId('monzo_review_select')
      .setPlaceholder('Select a transaction to review')
      .addOptions(items.map((tx) => {
        const currency = String(tx.currency || 'GBP').toLowerCase();
        const amount = money(Number(tx.amount || 0) / 100, currency);
        const who = tx.counterparty?.name || tx.merchant?.name || tx.description || 'Monzo transaction';
        const when = new Date(tx.created || tx.settled || Date.now()).toLocaleDateString('en-GB');
        return {
          label: (amount + ' • ' + short(String(who), 55)).slice(0, 100),
          description: (when + ' • ' + String(tx.description || tx.notes || 'No description')).slice(0, 100),
          value: String(tx.id),
        };
      }));
    components.push(new ActionRowBuilder().addComponents(menu));
    // Stable, distinct IDs prevent Discord's duplicate-component-ID rejection
    // when the pending list shrinks to a single page after reviews are saved.
    components.push(new ActionRowBuilder().addComponents(
      new ButtonBuilder().setCustomId('monzo_review_page_prev').setLabel('Previous').setStyle(ButtonStyle.Secondary).setDisabled(current === 0),
      new ButtonBuilder().setCustomId('monzo_review_page_next').setLabel('Next').setStyle(ButtonStyle.Secondary).setDisabled(current >= pageCount - 1)
    ));
  }
  return { embeds: [embed], components, ephemeral: true };
}

async function fetchMonzoReviewTransactions(period = 'last_month') {
  if (!MONZO_ACCESS_TOKEN || !MONZO_ACCOUNT_ID) {
    throw new Error('Monzo history access is not configured. Set MONZO_ACCESS_TOKEN and MONZO_ACCOUNT_ID in the bot environment.');
  }
  const bounds = periodBounds(period);
  const since = bounds.since;
  const until = bounds.until;
  const found = new Map();
  // Monzo caps each request at 100. Consecutive 24-hour windows avoid silently
  // truncating a busy month while preserving the exact London month boundaries.
  for (let cursor = since; cursor < until; cursor += 24 * 60 * 60) {
    const before = Math.min(until, cursor + 24 * 60 * 60);
    const params = new URLSearchParams({
      account_id: MONZO_ACCOUNT_ID,
      since: new Date(cursor * 1000).toISOString(),
      before: new Date(before * 1000).toISOString(),
      limit: '100',
    });
    const response = await fetch('https://api.monzo.com/transactions?' + params.toString(), {
      headers: { Authorization: 'Bearer ' + MONZO_ACCESS_TOKEN },
    });
    if (!response.ok) {
      const body = await response.text().catch(() => '');
      if (response.status === 401) throw new Error('Monzo rejected the access token (401). It may have expired; refresh MONZO_ACCESS_TOKEN in Render.');
      if (response.status === 403) throw new Error('Monzo denied transaction-history access (403). Check that the token has read-transactions permission.');
      throw new Error('Monzo transaction request failed (' + response.status + '): ' + short(body, 250));
    }
    const data = await response.json();
    const batch = Array.isArray(data.transactions) ? data.transactions : [];
    if (batch.length >= 100) {
      throw new Error('Monzo returned 100 transactions in one 24-hour window. Import stopped rather than silently truncating that day.');
    }
    for (const tx of batch) if (tx?.id) found.set(tx.id, tx);
  }
  return [...found.values()].sort((a, b) => Date.parse(a.created || a.settled || 0) - Date.parse(b.created || b.settled || 0));
}

async function cmdMonzoReview(interaction) {
  if (interaction.user.id !== OWNER_ID) return interaction.editReply('Only the bot owner can review historical Monzo transactions.');
  const period = interaction.options.getString('period') || 'last_month';
  const transactions = (await fetchMonzoReviewTransactions(period)).filter((tx) => Number(tx.amount) > 0);
  // Refresh from disk without discarding newer in-memory entries.
  try {
    const diskLedger = JSON.parse(fs.readFileSync(MONZO_FILE, 'utf8'));
    if (diskLedger && typeof diskLedger === 'object' && !Array.isArray(diskLedger)) monzoLedger = { ...monzoLedger, ...diskLedger };
  } catch (error) { if (error.code !== 'ENOENT') console.error('Could not refresh Monzo ledger before review:', error.message); }
  try {
    const diskReview = JSON.parse(fs.readFileSync(MONZO_REVIEW_FILE, 'utf8'));
    if (diskReview && typeof diskReview === 'object') monzoReviewState = mergeMonzoReviewStates(monzoReviewState, diskReview);
  } catch (error) { if (error.code !== 'ENOENT') console.error('Could not refresh Monzo reviews before review:', error.message); }
  let recoveredFromLedger = 0;
  for (const tx of transactions) {
    if (!monzoReviewState.transactions[tx.id]) monzoReviewState.transactions[tx.id] = tx;

    // A completed review also creates a paid monzo_import_<transaction id>
    // entry in monzo_payments.json. Rebuild missing review metadata from that
    // durable financial record so the review screen cannot show 0 reviewed
    // when the corresponding sale is already saved in the ledger.
    if (!monzoReviewState.reviewed[tx.id]) {
      const ledger = monzoLedger['monzo_import_' + tx.id] ||
        Object.values(monzoLedger).find((entry) => entry?.txId === tx.id && entry?.imported && entry?.status === 'paid');
      if (ledger && ledger.status === 'paid' && ledger.imported) {
        const dateMs = Number(ledger.paidAt || ledger.createdAt || Date.parse(tx.created || tx.settled || '') || Date.now());
        const savedReview = ledger.reviewRecord && typeof ledger.reviewRecord === 'object' ? ledger.reviewRecord : {};
        monzoReviewState.reviewed[tx.id] = {
          ...savedReview,
          transactionId: tx.id,
          amount: Number(ledger.amount || Number(tx.amount || 0) / 100),
          currency: String(ledger.currency || tx.currency || 'gbp').toLowerCase(),
          transactionDescription: String(ledger.originalDescription || tx.description || ''),
          counterparty: String(ledger.counterparty || tx.counterparty?.name || tx.merchant?.name || ''),
          sold: String(ledger.description || tx.description || 'Historical Monzo sale'),
          category: 'sale',
          customerId: String(ledger.customerId || ''),
          agentId: String(ledger.agentId || OWNER_ID),
          createdAt: Number(ledger.createdAt || dateMs),
          settledAt: Number(ledger.paidAt || dateMs),
          transactionDate: new Date(dateMs).toISOString().slice(0, 10),
          reviewedAt: Number(ledger.importedAt || ledger.paidAt || dateMs),
          reviewedBy: OWNER_ID,
          recoveredFromLedger: true,
        };
        recoveredFromLedger++;
      }
    }
  }
  saveMonzoReview();
  if (recoveredFromLedger) {
    console.log('Recovered ' + recoveredFromLedger + ' Monzo review record(s) from existing paid financial ledger entries.');
  }
  const reviewedCount = transactions.filter((tx) => !!monzoReviewState.reviewed[tx.id]).length;
  const payload = monzoReviewPayload(0, period);
  payload.embeds[0].addFields(
    { name: 'Transactions fetched this run', value: String(transactions.length), inline: true },
    { name: 'Already reviewed', value: String(reviewedCount), inline: true },
    { name: 'Recovered from financial records', value: String(recoveredFromLedger), inline: true }
  );
  return interaction.editReply(payload);
}

async function showMonzoReviewModal(interaction) {
  if (interaction.user.id !== OWNER_ID) return interaction.reply({ content: 'Only the bot owner can review these transactions.', ephemeral: true });
  const txId = interaction.values[0];
  const tx = monzoReviewState.transactions[txId];
  if (!tx) return interaction.reply({ content: 'That transaction is no longer in the review list. Run /monzo-review again.', ephemeral: true });
  if (monzoReviewState.reviewed[txId]) return interaction.reply({ content: 'That transaction has already been reviewed.', ephemeral: true });
  const modal = new ModalBuilder()
    .setCustomId('monzo_review_modal:' + txId)
    .setTitle('Review Monzo sale');
  const customer = new TextInputBuilder()
    .setCustomId('customer_id')
    .setLabel('Customer Discord ID (optional)')
    .setStyle(TextInputStyle.Short)
    .setRequired(false)
    .setMaxLength(25);
  const sold = new TextInputBuilder()
    .setCustomId('sold')
    .setLabel('What was sold / what was this for?')
    .setStyle(TextInputStyle.Paragraph)
    .setRequired(true)
    .setMaxLength(500)
    .setPlaceholder('Product, vehicle, service, expense details, etc.');
  const txDate = new Date(tx.created || tx.settled || Date.now()).toISOString().slice(0, 10);
  const date = new TextInputBuilder()
    .setCustomId('transaction_date')
    .setLabel('Transaction date (YYYY-MM-DD)')
    .setStyle(TextInputStyle.Short)
    .setRequired(true)
    .setMaxLength(10)
    .setValue(txDate);
  modal.addComponents(
    new ActionRowBuilder().addComponents(customer),
    new ActionRowBuilder().addComponents(sold),
    new ActionRowBuilder().addComponents(date)
  );
  await interaction.showModal(modal);
}

async function saveMonzoReviewModal(interaction) {
  if (interaction.user.id !== OWNER_ID) return interaction.reply({ content: 'Only the bot owner can save historical Monzo records.', ephemeral: true });
  const txId = interaction.customId.slice('monzo_review_modal:'.length);
  const tx = monzoReviewState.transactions[txId];
  if (!tx) return interaction.reply({ content: 'Transaction not found. Run /monzo-review again.', ephemeral: true });
  if (monzoReviewState.reviewed[txId]) return interaction.reply({ content: 'This transaction has already been saved.', ephemeral: true });

  const cleanDiscordId = (value) => String(value || '').trim().replace(/^<@!?(\d+)>$/, '$1');
  const customerId = cleanDiscordId(interaction.fields.getTextInputValue('customer_id'));
  const sold = interaction.fields.getTextInputValue('sold').trim();
  const dateText = interaction.fields.getTextInputValue('transaction_date').trim();
  const dateMatch = /^(\d{4})-(\d{2})-(\d{2})$/.exec(dateText);
  const dateMs = dateMatch ? Date.UTC(Number(dateMatch[1]), Number(dateMatch[2]) - 1, Number(dateMatch[3]), 12) : NaN;
  if (!Number.isFinite(dateMs) || new Date(dateMs).toISOString().slice(0, 10) !== dateText) {
    return interaction.reply({ content: 'Enter a valid transaction date in YYYY-MM-DD format.', ephemeral: true });
  }
  if (customerId && !/^\d{17,20}$/.test(customerId)) {
    return interaction.reply({ content: 'That customer Discord ID is not valid. Use the numeric ID or leave it blank to assign later.', ephemeral: true });
  }
  if (Number(tx.amount) <= 0) {
    return interaction.reply({ content: 'Only incoming Monzo transactions are included as sales.', ephemeral: true });
  }

  const record = {
    transactionId: txId,
    amount: Number(tx.amount || 0) / 100,
    currency: String(tx.currency || 'GBP').toLowerCase(),
    transactionDescription: String(tx.description || ''),
    counterparty: String(tx.counterparty?.name || tx.merchant?.name || ''),
    sold: sold,
    category: 'sale',
    customerId: customerId,
    agentId: OWNER_ID,
    createdAt: dateMs,
    settledAt: dateMs,
    transactionDate: dateText,
    reviewedAt: Date.now(),
    reviewedBy: interaction.user.id,
  };
  monzoReviewState.reviewed[txId] = record;
  if (!saveMonzoReview()) {
    delete monzoReviewState.reviewed[txId];
    return interaction.reply({ content: '❌ Bankroll could not verify this review was saved to disk. It has NOT been marked as saved. Check the Render logs for the Monzo review save error.', ephemeral: true });
  }
  {
    const key = 'monzo_import_' + txId;
    monzoLedger[key] = {
      key: key,
      status: 'paid',
      reviewRecord: record,
      imported: true,
      recordType: 'sale',
      source: 'Monzo historical transaction import',
      customerId: customerId,
      agentId: OWNER_ID,
      amount: record.amount,
      currency: record.currency,
      description: sold,
      originalDescription: record.transactionDescription,
      counterparty: record.counterparty,
      reference: String(tx.notes || ''),
      txId: txId,
      createdAt: record.createdAt,
      paidAt: record.settledAt,
      importedAt: record.reviewedAt,
    };
    saveMonzo();
  }
  // Do not report a completed review until both JSON databases are safely stored
  // as attachments in Discord. Render's local /tmp filesystem is disposable.
  try {
    await backupMonzoReviewToDiscord();
  } catch (backupError) {
    console.error('Monzo Review Discord storage failed:', backupError.message);
    return interaction.reply({
      content: '⚠️ The review is saved in this running instance, but Discord storage failed: ' +
        backupError.message + '\\nDo not restart or redeploy Bankroll yet. Do not review this transaction again; retry the backup with /downloads backup and check #bankroll-backup.',
      ephemeral: true,
      allowedMentions: { parse: [] },
    });
  }
  const outcome = 'Recorded **' + money(record.amount, record.currency) + '** as a historical sale' + (customerId ? ' for <@' + customerId + '>' : ' with customer ID left blank for later assignment') + ', assigned to you (<@' + OWNER_ID + '>). It is included in Bankroll payment history and totals.';
  return interaction.reply({ content: '✅ ' + outcome + '\n**Transaction:** ' + txId + '\n**Date:** ' + dateText + '\n**Details:** ' + short(sold, 300), ephemeral: true, allowedMentions: { parse: [] } });
}

async function cmdCustomer(interaction) {
  // Reconcile every saved /monzo-review sale into the shared paid ledger before reporting.
  syncMonzoReviewSalesToLedger();
  const user = interaction.options.getUser('user');
  let card = [];
  if (stripeLedgerReady) {
    card = Object.values(stripeLedger).filter((r) => r.customerId === String(user.id) && !wipedCustomers.has(r.customerId));
  } else {
    try {
      const res = await stripe.paymentIntents.search({
        query: `status:'succeeded' AND metadata['customer_id']:'${user.id}'`,
        limit: 100,
      });
      card = res.data.map((pi) => paymentRecordFromPI(pi, { requireAgent: false })).filter((r) => r && !wipedCustomers.has(r.customerId));
    } catch (e) {
      console.error('Stripe customer lookup failed:', e.message);
    }
  }
  const monzo = monzoPaid().filter((m) => m.customerId === user.id && !wipedCustomers.has(String(m.customerId || '')));
  const deliveryState = (d) => !d ? 'delivery record unavailable' : d.deliveryComplete ? 'delivered' : d.deliveryError ? 'delivery needs attention' : 'delivery pending';
  const all = [
    ...card.map((r) => {
      const d = Object.entries(deliveries).find(([key, x]) => (key === r.id || x?.paymentIntentId === r.id) && String(x?.customerId) === user.id)?.[1];
      return { at: r.created, amount: r.amount / 100, text: `${money(r.amount / 100, r.currency)} - ${short(r.description, 40)} - <t:${r.created}:d> (Card) • ${deliveryState(d)}` };
    }),
    ...monzo.map((m) => {
      const d = Object.values(deliveries).find((x) => x?.type === 'monzo' && x.customerId === user.id && (x.txId === m.txId || x.reference === m.reference || x.description === m.description));
      return { at: tsec(m.paidAt), amount: m.amount, text: `${money(m.amount, m.currency)} - ${short(m.description, 40)} - <t:${tsec(m.paidAt)}:d> (Monzo) • ${deliveryState(d)}` };
    }),
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
  // Reconcile every saved /monzo-review sale into the shared paid ledger before reporting.
  syncMonzoReviewSalesToLedger();
  const user = interaction.options.getUser('user');
  let card = [];
  try {
    const sessions = await recentCheckoutSessions();
    card = sessions.filter((s) => s.metadata?.customer_id === user.id && !wipedCustomers.has(String(s.metadata?.customer_id || '')));
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
      (interaction.commandName === 'vault' && ['remove', 'info', 'edit', 'verify', 'rename', 'export'].includes(interaction.options.getSubcommand())) ||
      interaction.commandName === 'bankroll' ||
      interaction.commandName === 'bankroll-monzo' ||
      interaction.commandName === 'test' ||
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

    // /cancel: browse the 50 most recent purchases (25 per page), including paid and unpaid.
    if (interaction.commandName === 'cancel') {
      const typed = String(interaction.options.getFocused() || '').toLowerCase();
      const page = interaction.options.getInteger('page') || 1;
      // Typing fires an autocomplete request per keystroke; reuse the Stripe lists for 30 seconds.
      const [openResult, completeResult] = await Promise.allSettled([
        cachedAsync('ac:sessions:open', 30 * 1000, () => stripe.checkout.sessions.list({ limit: 100, status: 'open' })),
        cachedAsync('ac:sessions:complete', 30 * 1000, () => stripe.checkout.sessions.list({ limit: 100, status: 'complete' })),
      ]);
      const sessions = [];
      for (const [label, result] of [['open', openResult], ['complete', completeResult]]) {
        if (result.status === 'fulfilled') {
          sessions.push(...result.value.data.map((session) => ({ ...session, _listStatus: label })));
        } else {
          console.error(`Stripe /cancel ${label} purchase lookup failed:`, result.reason?.message || result.reason);
        }
      }

      const card = sessions
        .filter((session) => session.metadata?.agent_id)
        .map((session) => {
          const paid = session._listStatus === 'complete' &&
            (session.payment_status === 'paid' || session.payment_status === 'no_payment_required');
          const status = paid ? 'PAID' : 'UNPAID';
          return {
            at: Number(session.created || 0) * 1000,
            value: session.id,
            search: `${session.metadata?.description || ''} ${session.id} ${session.metadata?.customer_id || ''}`.toLowerCase(),
            name: `[Card] [${status}] ${money((session.amount_total ?? 0) / 100, session.currency || CURRENCY)} - ${short(String(session.metadata?.description || 'Purchase'), 32)} - ${session.id}`.slice(0, 100),
          };
        })
        .filter((purchase) => purchase.name.includes('[PAID]') || purchase.name.includes('[UNPAID]'));

      const monzo = Object.entries(monzoLedger).map(([key, entry]) => {
        if (entry.imported) return null;
        const delivery = deliveries[key];
        const rawStatus = String(entry.status || (delivery?.paid ? 'paid' : 'open')).toLowerCase();
        const expired = rawStatus === 'open' &&
          Date.now() >= (entry.expiresAt ?? (entry.createdAt || Date.now()) + MONZO_OPEN_MS);
        const paid = rawStatus === 'paid' || !!delivery?.paid || !!delivery?.paymentConfirmed;
        if (!paid && (expired || rawStatus !== 'open')) return null;
        const status = paid ? 'PAID' : 'UNPAID';
        return {
          at: Number(entry.paidAt || entry.createdAt || delivery?.savedAt || 0),
          value: key,
          search: `${entry.description || ''} ${entry.reference || ''} ${key} ${entry.customerId || ''}`.toLowerCase(),
          name: `[Monzo] [${status}] ${money(Number(entry.amount || 0), entry.currency || 'gbp')} - ${short(String(entry.description || 'Purchase'), 24)} - ${entry.reference || key}`.slice(0, 100),
        };
      }).filter(Boolean);

      const purchases = [...monzo, ...card]
        .filter((purchase) => !typed || purchase.search.includes(typed) || purchase.name.toLowerCase().includes(typed))
        .sort((a, b) => b.at - a.at)
        .slice(0, 50);
      const pageItems = purchases.slice((page - 1) * 25, page * 25);
      return await interaction.respond(pageItems.map(({ name, value }) => ({ name, value })));
    }

    if (interaction.commandName !== 'refund' || interaction.user.id !== OWNER_ID) {
      return await interaction.respond([]);
    }
    const typed = interaction.options.getFocused().toLowerCase();
    const list = await cachedAsync('ac:refund:payments', 30 * 1000, () => stripe.paymentIntents.list({ limit: 50 }));
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
    if (!interaction.responded) {
      console.error('Autocomplete failed:', e.message);
      try { await interaction.respond([]); } catch (responseError) {
        if (!/already been acknowledged|already responded/i.test(responseError.message)) console.error('Autocomplete fallback response failed:', responseError.message);
      }
    }
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
  if (d.deliveryComplete) {
    // The file was already delivered automatically after payment; never send a second copy.
    await interaction.update({ components: withoutGetFile(interaction.message) }).catch(() => {});
    return interaction.followUp({ content: 'Your file was already sent as a reply to this invoice.', ephemeral: true }).catch(() => {});
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

  // Payment has been confirmed above. Update the original invoice to PAID
  // when the customer clicks the download button, before sending the file.
  d.paid = true;
  d.paymentConfirmed = true;
  d.paidAt = d.paidAt || Date.now();
  d.paymentConfirmedAt = d.paymentConfirmedAt || d.paidAt;
  if (checkPi) d.paymentIntentId = checkPi;
  saveDeliveries();
  // The original invoice message is left untouched.

  let dlink = d.link || null;
  let hosted = false;
  if (d.fallbackLink) {
    const token = d.fallbackLink.split('/').pop();
    const savedLink = downloads[token];
    if (savedLink && !savedLink.used && (savedLink.expiresAt == null || Date.now() < Number(savedLink.expiresAt)) && fs.existsSync(savedLink.path)) {
      dlink = d.fallbackLink;
      hosted = true;
    } else {
      delete d.fallbackLink;
      saveDeliveries();
    }
  }
  if (!dlink && d.path && fs.existsSync(d.path) && BASE_URL) {
    const size = fs.statSync(d.path).size;
    if (d.mode === 'link' || size > ATTACH_MAX_BYTES) {
      dlink = await makeDownloadLink(d, checkPi);
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
  // Attached file: also include a one-time download link next to it.
  let attachLink = null;
  if (!dlink && d.path && fs.existsSync(d.path) && BASE_URL) {
    try { attachLink = await makeDownloadLink(d, checkPi); d.fallbackLink = attachLink; saveDeliveries(); }
    catch (linkErr) { console.error('Could not make one-time link to send beside the attachment:', linkErr.message); }
  }
  const getPayload = () => dlink
    ? {
        content: `<@${d.customerId}> here is your secure download. The link works once and has no time expiry.`,
        embeds: [buildDownloadEmbed(d.name, downloads[dlink.split('/').pop()]?.expiresAt, d.path && fs.existsSync(d.path) ? fs.statSync(d.path).size : undefined, `${d.testMode ? '[TEST] ' : ''}Your download is ready`, true)],
        components: [downloadLinkRow(dlink, isZip ? 'Download ZIP' : 'Download File')],
        allowedMentions: { parse: [] },
      }
    : {
        content: `<@${d.customerId}> here is your complete file.`,
        embeds: [buildDownloadEmbed(d.name, null, d.path && fs.existsSync(d.path) ? fs.statSync(d.path).size : undefined)],
        files: [new AttachmentBuilder(d.path, { name: path.basename(d.name || 'download.zip') })],
        ...(attachLink ? { components: [downloadLinkRow(attachLink, isZip ? 'Download ZIP' : 'Download File')] } : {}),
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
      dlink = attachLink || await makeDownloadLink(d, checkPi);
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
  await interaction.message.edit({ components: [] }).catch(() => {});

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
  await notifyOwner({
    embeds: [new EmbedBuilder().setColor(GREEN).setTitle(d.testMode ? '[TEST] File sent' : 'File sent')
      .setDescription(`**File**\n${short(String(d.name || 'file'), 150)}`)
      .addFields(
        { name: 'Customer', value: `<@${d.customerId}>` },
        { name: 'Delivery Method', value: 'Get file button on the invoice' },
      ).setTimestamp()]
  }, { notificationType: 'delivery.sent', notificationStatus: 'success' });
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
  // User-installed application commands can run in group DMs as well as one-to-one DMs.
  // Accept any DM-based channel, but never allow this private file-scheduling command in a server.
  if (interaction.inGuild() || !interaction.channel?.isDMBased?.()) {
    return interaction.reply({ content: 'Run /sendfile inside a private DM or group DM with Bankroll, not in a server.', ephemeral: true });
  }
  let key = interaction.options.getString('file');
  if (!key) {
    // No file picked: use the newest available file in the vault.
    const newest = Object.entries(vault)
      .filter(([, v]) => v?.path && fs.existsSync(v.path))
      .sort((a, b) => (b[1].addedAt || 0) - (a[1].addedAt || 0))[0];
    if (!newest) {
      return interaction.reply({ content: 'There are no files in your vault yet. Add one with /vault add first.', ephemeral: true });
    }
    key = newest[0];
  }
  const item = vault[key];
  if (!item || !item.path || !fs.existsSync(item.path)) {
    return interaction.reply({ content: `Vault file \`${key}\` is unavailable. Check it with /vault verify.`, ephemeral: true });
  }
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
// Files are never modified. If an item exceeds Discord's upload limit,
// the bot serves the original file through a secure one-time download link.

async function onScheduledSendButton(interaction) {
  const jobId = interaction.customId.slice('sendfile:'.length);
  const job = sendJobs[jobId];
  if (!job || job.channelId !== interaction.channelId) {
    return interaction.reply({ content: 'This scheduled file request could not be found in this DM.', ephemeral: true });
  }
  if (job.status === 'sent' || job.status === 'link_sent') {
    return interaction.reply({ content: job.status === 'sent' ? 'This file has already been sent.' : 'The one-time download link has already been sent.', ephemeral: true });
  }
  if (job.status === 'sending') return interaction.reply({ content: 'Your file is being prepared right now, one moment.', ephemeral: true });
  if (Date.now() < job.dueAt) {
    return interaction.update({ content: sendfileProgress(job), components: [sendfileButton(jobId)], allowedMentions: { parse: [] } });
  }

  const item = vault[job.vaultKey];
  if (!item?.path || !fs.existsSync(item.path)) {
    return interaction.update({ content: `The scheduled time has arrived, but vault file \`${job.vaultKey}\` is missing. Re-add it, then contact the bot owner.`, components: [sendfileButton(jobId)] });
  }
  job.status = 'sending';
  await interaction.update({ content: `${sendfileProgress(job)}\n\nPreparing your complete file now...`, components: [], allowedMentions: { parse: [] } });

  const finish = async (status, note) => {
    job.status = status;
    job.completedAt = Date.now();
    saveSendJobs();
    await interaction.editReply({ content: `${sendfileProgress(job)}\n\n${note}`, components: [], allowedMentions: { parse: [] } }).catch(() => {});
  };

  const sendHostedLink = async () => {
    const link = await makeDownloadLink({
      path: item.path,
      name: path.basename(job.originalName || item.originalName || item.path),
      customerId: interaction.user.id,
      agentId: OWNER_ID,
      vaultKey: job.vaultKey,
      description: 'scheduled-vault-file',
    }, null);
    await interaction.followUp({
      content: `**${job.originalName}** is ready. This is the original file; the secure link works once and has no time expiry.`,
      embeds: [new EmbedBuilder().setColor(GREEN).setTitle('Your file is ready').setDescription('Use the button below to download the original file.')],
      components: [downloadLinkRow(link, 'Download File')],
      allowedMentions: { parse: [] },
    });
    recordDownloadEvent('delivery_sent', { file: job.originalName, customerId: interaction.user.id, agentId: OWNER_ID, detail: 'Scheduled file download link sent.' });
    await finish('link_sent', 'One-time Download File button sent below.');
  };

  try {
    const fileSize = fs.statSync(item.path).size;
    if (fileSize > MAX_FILE_BYTES) {
      if (!BASE_URL) throw new Error('The file exceeds Discord upload limits and PUBLIC_URL is not configured.');
      return await sendHostedLink();
    }
    try {
      await interaction.followUp({
        content: `**${job.originalName}** - scheduled delivery complete. The original file is attached below.`,
        files: [new AttachmentBuilder(item.path, { name: path.basename(job.originalName) })],
        allowedMentions: { parse: [] },
      });
      recordDownloadEvent('delivery_sent', { file: job.originalName, customerId: interaction.user.id, agentId: OWNER_ID, detail: 'Original file attachment sent directly.' });
      return await finish('sent', 'Complete file sent below.');
    } catch (uploadError) {
      console.error('Scheduled file attachment failed:', uploadError.message);
      if (!BASE_URL) throw uploadError;
      return await sendHostedLink();
    }
  } catch (error) {
    console.error('Scheduled file delivery failed:', error.message);
    job.status = 'waiting';
    saveSendJobs();
    const failure = `I couldn't deliver **${job.originalName}**. ${error.message} Press **Check progress / Get File** to retry.`;
    await interaction.editReply({ content: `${sendfileProgress(job)}\n\n${failure}`, components: [sendfileButton(jobId)], allowedMentions: { parse: [] } }).catch(() => {});
  }
}

async function onButton(interaction) {
  if (interaction.customId === 'monzo_review_page_prev' || interaction.customId === 'monzo_review_page_next') {
    if (interaction.user.id !== OWNER_ID) return interaction.reply({ content: 'Only the bot owner can review these transactions.', ephemeral: true });
    const embed = interaction.message?.embeds?.[0];
    const description = embed?.description || '';
    const period = description.includes('This month') ? 'month' : description.includes('This week') ? 'week' : description.includes('Last week') ? 'last_week' : 'last_month';
    const pageMatch = /Page (\\d+) of \\d+/.exec(embed?.footer?.text || '');
    const currentPage = pageMatch ? Math.max(0, Number(pageMatch[1]) - 1) : 0;
    const targetPage = interaction.customId === 'monzo_review_page_next' ? currentPage + 1 : Math.max(0, currentPage - 1);
    return interaction.update(monzoReviewPayload(targetPage, period));
  }
  if (interaction.customId.startsWith('testpaid:')) {
    if (interaction.user.id !== OWNER_ID) {
      return interaction.reply({ content: 'Only the bot owner can mark a test invoice as paid.', ephemeral: true });
    }
    const key = interaction.customId.slice('testpaid:'.length);
    const d = deliveries[key];
    if (!d?.testMode) {
      return interaction.reply({ content: 'This test invoice is invalid or has expired.', ephemeral: true });
    }
    if (d.deliveryComplete || d.paid) {
      return interaction.reply({ content: 'This test invoice has already been marked as paid.', ephemeral: true });
    }
    await interaction.deferUpdate();
    await deliverFile(key, null);
    if (deliveries[key]?.deliveryComplete) {
      return interaction.followUp({ content: 'Test payment marked as paid. The invoice has been updated, the download has been posted as a reply, and the owner notification has been triggered.', ephemeral: true });
    }
    return interaction.followUp({ content: 'The test invoice was marked as paid, but delivery has not completed. Check /delivery status for order ' + key + '.', ephemeral: true });
  }
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

// ---------- Opt-in timed message sessions ----------
const spamSessions = new Map();
const spamRequests = new Map();

async function cmdSend(interaction) {
  if (interaction.user.id !== OWNER_ID) {
    return interaction.reply({ content: 'Only the bot owner can use /send.', ephemeral: true });
  }
  const message = interaction.options.getString('message', true);
  await interaction.reply({ content: 'Sending message…', ephemeral: true });
  try {
    await interaction.followUp({
      content: message,
      ephemeral: false,
      allowedMentions: { parse: [] }
    });
    return interaction.editReply({ content: 'Message sent publicly.' });
  } catch (error) {
    console.error('[/send] Public message failed:', error);
    return interaction.editReply({ content: 'Could not send the public message. Check that Bankroll has permission to send messages in this chat.' });
  }
}

async function cmdSpam(interaction) {
  const message = interaction.options.getString('message', true);
  const durationMinutes = interaction.options.getInteger('duration', true);
  const dmMode = interaction.options.getBoolean('dm') ?? false;
  const requestId = crypto.randomBytes(8).toString('hex');
  spamRequests.set(requestId, {
    ownerId: interaction.user.id,
    message,
    durationMinutes,
    dmMode,
    expiresAt: Date.now() + 10 * 60 * 1000
  });
  const expiry = setTimeout(() => spamRequests.delete(requestId), 10 * 60 * 1000);
  expiry.unref?.();

  const embed = new EmbedBuilder()
    .setColor(GREY)
    .setTitle('Timed message ready')
    .setDescription(dmMode
      ? 'Press **Start** to post the repeated message here using a slower interval and automatic rate-limit retries.'
      : 'Press **Start** to begin posting the message below as a reply to this embed. Bankroll will automatically wait and retry if Discord rate-limits it.')
    .addFields(
      { name: 'Message to repeat', value: message.slice(0, 1024) },
      { name: 'Duration', value: durationMinutes + ' minute(s)', inline: true },
      { name: 'Sending location', value: 'This channel / chat', inline: true },
      { name: 'Interval', value: dmMode ? 'Every 5 seconds (plus any rate-limit wait)' : 'Every 1 second (plus any rate-limit wait)', inline: true }
    )
    .setFooter({ text: 'Only the person who ran /spam can start or stop this session.' });
  const row = new ActionRowBuilder().addComponents(
    new ButtonBuilder().setCustomId('spam:start:' + requestId).setLabel('Start').setStyle(ButtonStyle.Success)
  );
  return interaction.reply({ embeds: [embed], components: [row], allowedMentions: { parse: [] } });
}

async function handleSpamConsent(interaction) {
  const parts = interaction.customId.split(':');
  const action = parts[1];
  const requestId = parts[2];
  const request = spamRequests.get(requestId);
  if (!request || request.expiresAt < Date.now()) {
    return interaction.reply({ content: 'This spam setup has expired. Run /spam again.', ephemeral: true });
  }
  if (interaction.user.id !== request.ownerId) {
    return interaction.reply({ content: 'Only the person who ran /spam can start this session.', ephemeral: true });
  }
  spamRequests.delete(requestId);
  if (action !== 'start') return interaction.reply({ content: 'Unknown spam action.', ephemeral: true });

  const sessionId = crypto.randomBytes(8).toString('hex');
  const session = {
    ownerId: request.ownerId,
    sourceMessage: interaction.message,
    interactionWebhook: interaction.webhook,
    message: request.message,
    endAt: Date.now() + request.durationMinutes * 60000,
    slowMode: request.dmMode,
    intervalMs: request.dmMode ? 5000 : 1000,
    sent: 0,
    timeout: null
  };
  spamSessions.set(sessionId, session);
  await interaction.update({
    embeds: [new EmbedBuilder().setColor(GREEN).setTitle('Spam running')
      .setDescription(request.dmMode
        ? 'The message is being posted here using slower sending and automatic rate-limit retries.'
        : 'The message is being posted here. Bankroll will automatically wait and retry if Discord rate-limits it.')
      .addFields(
        { name: 'Repeated message', value: request.message.slice(0, 1024) },
        { name: 'Duration', value: request.durationMinutes + ' minute(s)', inline: true },
        { name: 'Sending location', value: 'This channel / chat', inline: true },
        { name: 'Interval', value: request.dmMode ? 'Every 5 seconds (plus any rate-limit wait)' : 'Every 1 second (plus any rate-limit wait)', inline: true }
      )
      .setFooter({ text: 'Press Stop to end the session early.' })],
    components: [new ActionRowBuilder().addComponents(
      new ButtonBuilder().setCustomId('spam:stop:' + sessionId).setLabel('Stop').setStyle(ButtonStyle.Danger)
    )]
  });

  const sendNext = async () => {
    if (!spamSessions.has(sessionId)) return;
    if (Date.now() >= session.endAt) {
      spamSessions.delete(sessionId);
      await session.interactionWebhook.send({ content: 'Message session finished.', allowedMentions: { parse: [] } }).catch(() => {});
      return;
    }

    try {
      // Send through the interaction webhook. This works in Guild, Bot DM and
      // User Install contexts where the bot may not be a member of the server.
      // Use exactly one endpoint per attempt so a timeout cannot create duplicates.
      await session.interactionWebhook.send({
        content: session.message,
        allowedMentions: { parse: [] }
      });
      session.sent += 1;
    } catch (error) {
      const isRateLimited = Number(error?.status) === 429
        || Number(error?.rawError?.code) === 20028
        || Number(error?.rawError?.code) === 20029
        || Number(error?.code) === 20028
        || Number(error?.code) === 20029;
      if (isRateLimited) {
        const rawRetryAfter = Number(error?.rawError?.retry_after ?? error?.data?.retry_after ?? 0);
        const retryAfterMs = rawRetryAfter > 0
          ? rawRetryAfter * 1000
          : Math.max(0, Number(error?.retryAfter || 0));
        // Keep retrying until the requested session ends. Normal mode still
        // respects Discord's server-provided wait; slow mode waits at least 5s.
        const waitMs = Math.max(session.slowMode ? session.intervalMs : 1000, Math.ceil(retryAfterMs) + 1000);
        console.warn(`Spam rate limit hit; waiting ${waitMs}ms before retrying (${session.sent} sent).`);
        if (Date.now() + waitMs < session.endAt) {
          session.timeout = setTimeout(sendNext, waitMs);
          session.timeout.unref?.();
          return;
        }
        spamSessions.delete(sessionId);
        await session.interactionWebhook.send({
          content: `Message session finished. Sent ${session.sent} message(s); Discord rate-limited the final attempt until the session ended.`,
          allowedMentions: { parse: [] }
        }).catch(() => {});
        return;
      }

      spamSessions.delete(sessionId);
      console.error('Timed message send failed:', error);
      await session.sourceMessage.edit({
        embeds: [new EmbedBuilder().setColor(RED).setTitle('Spam failed')
          .setDescription('Bankroll could not send the repeated message. The session has stopped.')
          .addFields(
            { name: 'Messages sent', value: String(session.sent), inline: true },
            { name: 'Discord error', value: String(error.message || 'Unknown error').slice(0, 1000) }
          )],
        components: []
      }).catch(() => {});
      return;
    }

    if (Date.now() >= session.endAt) {
      spamSessions.delete(sessionId);
      return;
    }
    session.timeout = setTimeout(sendNext, session.intervalMs);
    session.timeout.unref?.();
  };
  await sendNext();
}

async function handleSpamStopButton(interaction) {
  const id = interaction.customId.split(':')[2];
  const session = spamSessions.get(id);
  if (!session) return interaction.reply({ content: 'This message session has already ended.', ephemeral: true });
  if (interaction.user.id !== session.ownerId) return interaction.reply({ content: 'Only the person who started this session can stop it.', ephemeral: true });
  clearTimeout(session.timeout);
  spamSessions.delete(id);
  return interaction.update({ embeds: [new EmbedBuilder().setColor(RED).setTitle('Spam stopped').setDescription('The repeated messages have stopped.')], components: [] });
}
// ---------- Stripe balance and payout commands ----------
const pendingStripePayouts = new Map();

function stripeBalanceLines(items = []) {
  return items.filter((x) => Number(x.amount || 0) !== 0)
    .map((x) => '**' + String(x.currency || '').toUpperCase() + '** — ' + money(Number(x.amount || 0) / 100, String(x.currency || CURRENCY).toLowerCase()));
}

async function cmdBankrollFinance(interaction) {
  if (interaction.user.id !== OWNER_ID) {
    return interaction.editReply('Only the Bankroll owner can view balances or initiate payouts.');
  }
  const sub = interaction.options.getSubcommand();

  if (sub === 'balance') {
    const balance = await stripe.balance.retrieve();
    const available = stripeBalanceLines(balance.available);
    const pending = stripeBalanceLines(balance.pending);
    const embed = new EmbedBuilder().setColor(GREEN).setTitle('BANKROLL | STRIPE BALANCE')
      .setDescription('Live balance returned directly by Stripe.')
      .addFields(
        { name: 'Available to withdraw', value: available.join('\n') || '£0.00', inline: true },
        { name: 'Pending', value: pending.join('\n') || '£0.00', inline: true }
      ).setTimestamp();
    return interaction.editReply({ embeds: [embed] });
  }

  if (sub === 'cashlogs') {
    const result = await stripe.payouts.list({ limit: 10 });
    if (!result.data.length) return interaction.editReply('No Stripe payouts were found.');
    const embed = new EmbedBuilder().setColor(GREEN).setTitle('BANKROLL | RECENT STRIPE PAYOUTS')
      .setDescription(result.data.map((p) => {
        const date = p.arrival_date ? '<t:' + p.arrival_date + ':D>' : 'Arrival date unavailable';
        return '**' + money(Number(p.amount || 0) / 100, p.currency) + '** · ' + String(p.status || 'unknown').toUpperCase() + '\n' + date + ' · ' + String(p.id);
      }).join('\n\n').slice(0, 4000)).setTimestamp();
    return interaction.editReply({ embeds: [embed] });
  }

  if (sub === 'payout') {
    const currency = String(interaction.options.getString('currency') || 'gbp').toLowerCase();
    const balance = await stripe.balance.retrieve();
    const available = (balance.available || []).find((x) => String(x.currency).toLowerCase() === currency);
    const amount = Number(available?.amount || 0);
    if (amount <= 0) {
      return interaction.editReply('There are no available ' + currency.toUpperCase() + ' funds to withdraw. Pending funds cannot be paid out yet.');
    }

    const requestId = crypto.randomUUID();
    pendingStripePayouts.set(requestId, { ownerId: interaction.user.id, currency, amount, createdAt: Date.now() });
    const embed = new EmbedBuilder().setColor(0xf1c40f).setTitle('CONFIRM STRIPE PAYOUT')
      .setDescription('Bankroll is ready to request a payout to your Stripe account’s linked default bank account.')
      .addFields(
        { name: 'Payout amount', value: money(amount / 100, currency), inline: true },
        { name: 'Currency', value: currency.toUpperCase(), inline: true },
        { name: 'Funds', value: 'Available balance only', inline: true },
        { name: 'Destination', value: 'Your linked Stripe bank account', inline: false }
      )
      .setFooter({ text: 'Stripe will process the payout; bank arrival is not instant.' });
    const row = new ActionRowBuilder().addComponents(
      new ButtonBuilder().setCustomId('bkpay:confirm:' + requestId).setLabel('Confirm payout').setStyle(ButtonStyle.Success),
      new ButtonBuilder().setCustomId('bkpay:cancel:' + requestId).setLabel('Cancel').setStyle(ButtonStyle.Secondary)
    );
    return interaction.editReply({ embeds: [embed], components: [row] });
  }
}

async function handleBankrollPayoutButton(interaction) {
  const parts = interaction.customId.split(':');
  const action = parts[1];
  const requestId = parts[2];
  const request = pendingStripePayouts.get(requestId);
  if (!request) return interaction.reply({ content: 'This payout confirmation has expired. Run /bk payout again to get a fresh balance.', ephemeral: true });
  if (interaction.user.id !== OWNER_ID || interaction.user.id !== request.ownerId) {
    return interaction.reply({ content: 'Only the Bankroll owner can confirm this payout.', ephemeral: true });
  }
  if (action === 'cancel') {
    pendingStripePayouts.delete(requestId);
    return interaction.update({ content: 'Payout cancelled. No money was moved.', embeds: [], components: [] });
  }
  if (action !== 'confirm') return;

  pendingStripePayouts.delete(requestId);
  await interaction.update({ content: 'Checking the live Stripe balance and submitting the payout…', embeds: [], components: [] });
  try {
    const balance = await stripe.balance.retrieve();
    const current = (balance.available || []).find((x) => String(x.currency).toLowerCase() === request.currency);
    const amount = Number(current?.amount || 0);
    if (amount <= 0) {
      return interaction.followUp({ content: 'No available ' + request.currency.toUpperCase() + ' funds remain. No payout was created.', ephemeral: true });
    }

    const payout = await stripe.payouts.create(
      { amount, currency: request.currency, method: 'standard' },
      { idempotencyKey: 'bankroll-payout-' + requestId }
    );
    const embed = new EmbedBuilder().setColor(GREEN).setTitle('BANKROLL | PAYOUT REQUESTED')
      .setDescription('Stripe accepted the payout request to your linked default bank account.')
      .addFields(
        { name: 'Amount', value: money(Number(payout.amount || amount) / 100, payout.currency || request.currency), inline: true },
        { name: 'Status', value: String(payout.status || 'pending').toUpperCase(), inline: true },
        { name: 'Arrival date', value: payout.arrival_date ? '<t:' + payout.arrival_date + ':D>' : 'Stripe has not provided a date yet', inline: true },
        { name: 'Payout ID', value: String(payout.id || 'Not supplied') }
      ).setTimestamp();
    await interaction.followUp({ embeds: [embed], ephemeral: true });
    await logToServer({ embeds: [embed] });
    recordNotification('stripe.payout', 'Stripe payout requested', money(Number(payout.amount || amount) / 100, payout.currency || request.currency) + ' · ' + payout.id, 'success');
  } catch (error) {
    console.error('Stripe payout failed:', error);
    await interaction.followUp({ content: 'Stripe rejected the payout. No successful payout was confirmed.\n\n' + String(error.message || error).slice(0, 1200), ephemeral: true });
    await logToServer({ embeds: [new EmbedBuilder().setColor(RED).setTitle('BANKROLL | PAYOUT FAILED').setDescription(String(error.message || error).slice(0, 1000)).setTimestamp()] });
  }
}

// ---------- Interaction router ----------
client.on('interactionCreate', async (interaction) => {
  try {
    if (interaction.isMessageContextMenuCommand()) {
      if (!allowed.has(interaction.user.id) || interaction.user.id !== OWNER_ID) {
        return interaction.reply({ content: 'Only the bot owner can delete Bankroll messages.', ephemeral: true });
      }
      const target = interaction.targetMessage;
      if (!interaction.channel?.isDMBased?.()) {
        return interaction.reply({ content: 'Use this in a direct message with Bankroll.', ephemeral: true });
      }
      if (!target || target.author?.id !== client.user.id) {
        return interaction.reply({ content: 'You can only delete messages sent by Bankroll.', ephemeral: true });
      }
      await target.delete();
      return interaction.reply({ content: 'Bankroll message deleted.', ephemeral: true });
    }
    if (interaction.isAutocomplete()) return await onAutocomplete(interaction);
    if (interaction.isButton() && interaction.customId.startsWith('bkpay:')) return await handleBankrollPayoutButton(interaction);
    if (interaction.isButton() && interaction.customId.startsWith('spam:')) {
      if (interaction.customId.startsWith('spam:stop:')) return await handleSpamStopButton(interaction);
      return await handleSpamConsent(interaction);
    }
    if (interaction.isButton()) return await onButton(interaction);
    if (interaction.isStringSelectMenu() && interaction.customId === 'monzo_review_select') return await showMonzoReviewModal(interaction);
    if (interaction.isStringSelectMenu() && interaction.customId === 'customer_preview_select') return await onCustomerPreviewSelect(interaction);
    if (interaction.isModalSubmit() && interaction.customId.startsWith('monzo_review_modal:')) return await saveMonzoReviewModal(interaction);
    if (!interaction.isChatInputCommand()) return;
    if (!commands.some((c) => c.name === interaction.commandName)) return;

    if (!allowed.has(interaction.user.id)) {
      return interaction.reply({
        content: "You aren't allowed to use this command.",
        ephemeral: true,
      });
    }

    switch (interaction.commandName) {
      case 'bankroll': {
        const bankrollSubcommand = interaction.options.getSubcommand();
        if (bankrollSubcommand === 'checkout') return await sendStripeCheckout(interaction);
        await interaction.deferReply({ ephemeral: true });
        return await cmdBankrollFinance(interaction);
      }
      case 'bankroll-monzo':
        return await sendMonzoInvoice(interaction);
      case 'monzo-review':
        await interaction.deferReply({ ephemeral: true });
        return await cmdMonzoReview(interaction);
      case 'test':
        return await cmdPaymentTest(interaction);
      case 'vault':
        return await cmdVault(interaction);
      case 'sendfile':
        return await startScheduledSend(interaction);
      case 'downloads':
        await interaction.deferReply({ ephemeral: true });
        return await cmdDownloads(interaction);
      case 'delivery':
        await interaction.deferReply({ ephemeral: true });
        return await cmdDelivery(interaction);
      case 'cancel':
        return await cmdCancel(interaction);
      case 'wipe':
        await interaction.deferReply({ flags: 64 });
        return await cmdWipe(interaction);
      case 'remind':
        return await cmdRemind(interaction);
      case 'send':
        return await cmdSend(interaction);
      case 'spam':
        return await cmdSpam(interaction);
      case 'stop':
        if (interaction.user.id !== OWNER_ID) return interaction.reply({ content: 'Only the bot owner can stop all spam sessions.', ephemeral: true });
        {
          const active = [...spamSessions.entries()];
          for (const [id, session] of active) {
            clearTimeout(session.timeout);
            spamSessions.delete(id);
            await session.sourceMessage.reply({ content: 'The owner stopped this message session.', allowedMentions: { parse: [] } }).catch(() => {});
          }
          for (const [id, request] of spamRequests.entries()) {
            spamRequests.delete(id);
          }
          return interaction.reply({ content: 'Stopped ' + active.length + ' active spam session(s) and cleared pending requests.', ephemeral: true });
        }
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
      case 'export':
        await interaction.deferReply({ ephemeral: true });
        return await cmdExport(interaction);
      case 'bot':
        await interaction.deferReply({ ephemeral: true });
        return await cmdBotAdmin(interaction);
      default:
        await interaction.deferReply({ ephemeral: true });
        if (interaction.commandName === 'payments') return await cmdPayments(interaction);
        if (interaction.commandName === 'total') return await cmdTotal(interaction);
        if (interaction.commandName === 'customer') return await cmdCustomer(interaction);
        if (interaction.commandName === 'history') return await cmdHistory(interaction);
        if (interaction.commandName === 'preview') return await cmdCustomerPreview(interaction);
    }
  } catch (err) {
    void alertOwner(`/${interaction.commandName ?? interaction.customId?.split(':')[0]} failed`, err);
    try {
      if (interaction.deferred || interaction.replied) {
        await interaction.editReply({ content: `Something went wrong: ${err.message}` });
      } else {
        await interaction.reply({ content: `Something went wrong: ${err.message}`, flags: 64 });
      }
    } catch {}
  }
});

// ---------- Monzo Webhook Listener ----------
const app = express();
// Render puts a proxy in front of the bot; trust one hop so req.ip is the real visitor (per-visitor download limit).
app.set('trust proxy', 1);

// Liveness and readiness endpoints for Render or external uptime monitors.
function healthSnapshot() {
  const mem = process.memoryUsage();
  return { service: 'bankroll', status: 'ok', uptimeSeconds: Math.floor(process.uptime()), discordReady: client.isReady(), memory: { rssBytes: mem.rss, heapUsedBytes: mem.heapUsed, heapTotalBytes: mem.heapTotal, externalBytes: mem.external, arrayBuffersBytes: mem.arrayBuffers || 0 }, eventLoopDelayMs: { mean: Number.isFinite(eventLoopMonitor.mean) ? Number((eventLoopMonitor.mean / 1e6).toFixed(2)) : 0, max: Number.isFinite(eventLoopMonitor.max) ? Number((eventLoopMonitor.max / 1e6).toFixed(2)) : 0 }, lastBackupAt: lastBackupAt || null, lastBackupAgeSeconds: lastBackupAt ? Math.floor((Date.now() - lastBackupAt) / 1000) : null, storage: DATA_DIR_STATUS, checkedAt: new Date().toISOString() };
}
app.get('/healthz', (_req, res) => res.status(200).json(healthSnapshot()));
app.get('/readyz', (_req, res) => {
  let storageWritable = true;
  try { fs.accessSync(DATA_DIR, fs.constants.W_OK); } catch { storageWritable = false; }
  const ready = client.isReady() && storageWritable;
  res.status(ready ? 200 : 503).json({ ...healthSnapshot(), status: ready ? 'ready' : 'not_ready', storageWritable });
});

app.post('/monzo-webhook', express.json(), async (req, res) => {
  if (MONZO_WEBHOOK_TOKEN && req.query.token !== MONZO_WEBHOOK_TOKEN) {
    console.error('Monzo webhook rejected: bad or missing token');
    return res.status(403).send('Forbidden');
  }
  console.log('Monzo webhook hit! Type:', req.body?.type);
  try {
    const data = req.body?.data;
    if (req.body?.type !== 'transaction.created' || !data) return res.status(200).send('Ignored');
    if (data.id && hasProcessedWebhook(`monzo:${data.id}`)) return res.status(200).send('Duplicate');
    if (MONZO_ACCOUNT_ID && data.account_id && data.account_id !== MONZO_ACCOUNT_ID) return res.status(200).send('Other account');

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
    if (tx.decline_reason) return res.status(200).send('Declined');

    const amountPaid = Math.round(Number(tx.amount ?? data.amount ?? 0)) / 100;
    if (!(amountPaid > 0)) return res.status(200).send('No incoming amount');
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
        .setTitle('PAID | MONZO PAYMENT RECEIVED')
        .setDescription(d.description || 'No description')
        .addFields(
          { name: 'Customer', value: d.customerId ? `<@${d.customerId}>` : 'Unknown', inline: true },
          { name: 'Sales Agent', value: d.agentId ? `<@${d.agentId}>` : 'Unknown', inline: true },
          { name: 'Amount Paid', value: money(amountPaid, 'gbp'), inline: true },
          { name: 'Payment Status', value: 'PAID', inline: true },
          { name: 'Paid At', value: `<t:${Math.floor(Date.now()/1000)}:F>`, inline: true },
          { name: 'Payment reference', value: String(d.reference || key || 'Not recorded'), inline: true },
          { name: 'Transaction ID', value: String(tx.id || data.id || key || 'Not supplied by Monzo') }
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
    if (data.id) markWebhookProcessed(`monzo:${data.id}`, 'monzo.transaction.created');
    return res.status(200).send('OK');
  } catch (e) {
    alertOwner('Monzo webhook error', e);
    return res.status(500).send('Webhook processing failed; retry required.');
  }
});

// ---------- Stripe webhook ----------

const stripeEventsInFlight = new Set();
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

  if (hasProcessedWebhook(event.id)) return res.json({ received: true, duplicate: true });
  // Stripe retries while a slow delivery is still running; don't process the same event twice at once.
  if (stripeEventsInFlight.has(event.id)) return res.status(202).json({ received: true, inFlight: true });
  stripeEventsInFlight.add(event.id);
  try {
  lastWebhookAt = Date.now();
  lastWebhookType = event.type;

  if (event.type === 'checkout.session.completed') {
    const s = event.data.object;
    if (s.payment_status === 'paid') {
      if (deliveries[s.id]) {
        Object.assign(deliveries[s.id], { paymentConfirmed:true, paid:true, paymentConfirmedAt:event.created ? event.created*1000 : Date.now(), amount:(s.amount_total || 0)/100, currency:String(s.currency || CURRENCY).toLowerCase(), paymentIntentId:s.payment_intent || null });
        saveDeliveries();
      }
      void updateStripeLedgerFromPI(s.payment_intent);
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
            { name: 'Amount', value: money(total, s.currency || CURRENCY), inline: true },
            { name: 'Payment Status', value: 'PAID', inline: true },
            { name: 'Paid At', value: `<t:${event.created}:F>`, inline: true },
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

  if (event.type === 'charge.refunded') {
    const charge = event.data.object;
    const piId = typeof charge.payment_intent === 'string' ? charge.payment_intent : charge.payment_intent?.id;
    const pi = piId ? await stripe.paymentIntents.retrieve(piId).catch(() => null) : null;
    const refunded = Number(charge.amount_refunded || 0)/100;
    const refundEmbed = new EmbedBuilder().setColor(RED).setTitle('REFUND PROCESSED')
      .setDescription('Stripe reported a refund for a Bankroll payment.')
      .addFields(
        { name:'Refunded Amount', value:money(refunded,charge.currency || CURRENCY), inline:true },
        { name:'Original Amount', value:money(Number(charge.amount || 0)/100,charge.currency || CURRENCY), inline:true },
        { name:'Payment Intent', value:String(piId || 'Unknown') },
        { name:'Customer', value:pi?.metadata?.customer_id ? `<@${pi.metadata.customer_id}>` : 'Not available', inline:true },
        { name:'Refund Status', value:charge.refunded ? 'Fully refunded' : 'Partially refunded', inline:true }
      );
    if (piId && stripeLedger[piId]) { stripeLedger[piId].refunded = Number(charge.amount_refunded || 0); saveStripeLedger(); }
    else if (piId) void updateStripeLedgerFromPI(piId);
    if (piId) await recordInStripe(piId,{refunded_at:new Date(event.created*1000).toISOString(),refund_amount:refunded,refund_status:charge.refunded?'refunded':'partially_refunded'});
    await notifyOwner({embeds:[refundEmbed]},{notificationType:'payment.refunded',notificationStatus:'warning'});
  }

  if (event.type === 'charge.dispute.created') {
    await onDisputeCreated(event.data.object).catch((e) => alertOwner('Dispute alert failed', e));
  }
  if (event.type === 'charge.dispute.closed') {
    await onDisputeClosed(event.data.object).catch((e) => alertOwner('Dispute close alert failed', e));
  }

  markWebhookProcessed(event.id, event.type);
  res.json({ received: true });
  } catch (webhookError) {
    void alertOwner(`Stripe webhook ${event.type} failed`, webhookError);
    if (!res.headersSent) res.status(500).send('Webhook processing failed; retry required.');
  } finally {
    stripeEventsInFlight.delete(event.id);
  }
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
  if(!t||(t.used&&!t.multiUse)||activeDownloadTokens.has(token)||(t.expiresAt != null && Date.now()>Number(t.expiresAt))||!safePath||!fs.existsSync(t.path)){recordDownloadEvent('access_denied',{file:t?.name,customerId:t?.customerId,token,detail:'Expired, used, missing or invalid download token/path.'});return res.status(410).send('This download link has already been used or has expired.');}

  const ua = req.headers['user-agent'] || '';
  if (req.method === 'HEAD' || /bot|crawler|spider|preview|embed/i.test(ua)) {
    return res.status(200).send('Download link');
  }
  if (!t.multiUse) t.used = true; activeDownloadTokens.add(token); saveDownloads();
  const downloadStartedAt = Date.now();
  const expectedBytes = fs.statSync(t.path).size;
  res.setHeader('Content-Type', 'application/octet-stream');
  res.setHeader('Content-Length', String(expectedBytes));
  res.setHeader('Content-Disposition', `attachment; filename*=UTF-8''${encodeURIComponent(t.name)}`);
  let finished = false;
  res.on('finish', () => {
    finished = true;
    const downloadedAt = Date.now();
    const elapsedMs = downloadedAt - downloadStartedAt;
    if (!t.multiUse) {
      try { fs.unlinkSync(t.path); } catch {}
      delete downloads[token];
    }
    activeDownloadTokens.delete(token);
    saveDownloads();
    recordDownloadEvent('download_completed', {
      file:t.name, customerId:t.customerId, agentId:t.agentId, token,
      detail:`${t.testMode ? '[TEST] ' : ''}Completed file transfer: ${expectedBytes} bytes in ${elapsedMs} ms at ${new Date(downloadedAt).toISOString()}.`
    });
    notifyOwner({
      embeds: [new EmbedBuilder().setColor(GREEN).setTitle(t.testMode ? '[TEST] File downloaded' : 'File downloaded')
        .setDescription(`<@${t.customerId}> successfully downloaded **${short(String(t.name || 'file'), 150)}**.`)
        .addFields(
          { name: 'Completed', value: `<t:${Math.floor(downloadedAt/1000)}:F>`, inline: true },
          { name: 'File size', value: `${(expectedBytes/(1024*1024)).toFixed(2)} MB`, inline: true }
        ).setTimestamp()]
    }, { notificationType: 'download.completed', notificationStatus: 'success' });
    recordInStripe(t.paymentIntentId, {
      downloaded_at: new Date(downloadedAt).toISOString(),
      downloaded_file: String(t.name || '').slice(0,100),
      downloaded_bytes: String(expectedBytes)
    });
  });
  res.on('close', () => { if(!finished){t.used=false;activeDownloadTokens.delete(token);saveDownloads();recordDownloadEvent('download_interrupted',{file:t.name,customerId:t.customerId,token,detail:'Connection closed before the file finished; link re-enabled.'});} });
  const stream = fs.createReadStream(t.path);
  stream.on('error', (e) => { t.used=false;activeDownloadTokens.delete(token);saveDownloads();recordDownloadEvent('download_failed',{file:t.name,customerId:t.customerId,token,detail:short(e.message,180)});res.destroy(); });
  stream.pipe(res);
});

app.get('/', (_, res) => res.send('Bankroll bot running'));
app.listen(PORT, () => console.log(`Webhook server listening on :${PORT}`));

// Monitor process memory and event-loop stalls. Configure MEMORY_WARN_MB in Render if needed.
// Resolution is milliseconds; 20,000 would make the monitor itself report a ~20-second baseline.
const eventLoopMonitor = monitorEventLoopDelay({ resolution: 20 });
eventLoopMonitor.enable();
const configuredMemoryWarnMb = Number(process.env.MEMORY_WARN_MB || 384);
const MEMORY_WARN_MB = Number.isFinite(configuredMemoryWarnMb) ? Math.max(128, configuredMemoryWarnMb) : 384;
let memoryWarningActive = false;
let lastMemorySampleAt = 0;
function sampleRuntimeHealth() {
  const now = Date.now();
  const mem = process.memoryUsage();
  const rssMb = mem.rss / 1024 / 1024;
  const heapMb = mem.heapUsed / 1024 / 1024;
  const loopMeanMs = Number.isFinite(eventLoopMonitor.mean) ? eventLoopMonitor.mean / 1e6 : 0;
  const loopMaxMs = Number.isFinite(eventLoopMonitor.max) ? eventLoopMonitor.max / 1e6 : 0;
  if (now - lastMemorySampleAt >= 5 * 60 * 1000) {
    console.log(`[HEALTH] RSS ${rssMb.toFixed(0)} MB; heap ${heapMb.toFixed(0)} MB; external ${(mem.external/1024/1024).toFixed(0)} MB; event-loop mean ${loopMeanMs.toFixed(1)} ms, max ${loopMaxMs.toFixed(1)} ms`);
    lastMemorySampleAt = now;
  }
  if (rssMb >= MEMORY_WARN_MB && !memoryWarningActive) {
    memoryWarningActive = true;
    void alertOwner('High memory usage', new Error(`Bankroll RSS is ${rssMb.toFixed(0)} MB (alert threshold ${MEMORY_WARN_MB} MB); heap ${heapMb.toFixed(0)} MB, external ${(mem.external/1024/1024).toFixed(0)} MB.`));
  } else if (memoryWarningActive && rssMb < MEMORY_WARN_MB * 0.9) {
    memoryWarningActive = false;
    console.log(`[HEALTH] Memory usage recovered to ${rssMb.toFixed(0)} MB RSS.`);
  }
  if (loopMaxMs >= 2000) console.warn(`[HEALTH] Event loop was blocked for up to ${loopMaxMs.toFixed(0)} ms since the previous sample.`);
  eventLoopMonitor.reset();
}
setInterval(sampleRuntimeHealth, 60 * 1000).unref?.();

client.on('error', (e) => console.error('Client error:', e));
client.on('shardError', (e) => console.error('Shard error:', e));
client.on('shardDisconnect', (ev, id) => console.error('Shard disconnected', id, ev.code));
client.on('warn', (m) => console.warn('Warn:', m));
client.on('shardReady', (id) => console.log('Shard ready', id));
client.on('shardReconnecting', (id) => console.log('Shard reconnecting', id));

// A failed login used to be logged once and the web server kept running ("Bankroll bot running")
// while the bot sat offline forever. Retry with backoff and say WHY it failed.
let loginAttempt = 0;
async function loginWithRetry() {
  loginAttempt++;
  try {
    await client.login(DISCORD_TOKEN);
    console.log(`login() resolved (attempt ${loginAttempt})`);
  } catch (e) {
    const text = String(e?.code || e?.message || e);
    console.error(`Login failed (attempt ${loginAttempt}):`, e?.message || e);
    if (/TokenInvalid|invalid token|An invalid token/i.test(text)) {
      console.error('LOGIN HINT: DISCORD_TOKEN is invalid. Reset the token in the Discord Developer Portal and update it in the Render Environment tab. Not retrying.');
      return;
    }
    if (/DisallowedIntents|disallowed intents/i.test(text)) {
      console.error('LOGIN HINT: a privileged intent is not enabled for this bot in the Developer Portal. Not retrying.');
      return;
    }
    if (/429|1015|rate limit|Cloudflare|banned|403/i.test(text)) {
      console.error('LOGIN HINT: Discord/Cloudflare may be rate-limiting or blocking this server IP. Redeploying to another region or waiting usually fixes it.');
    }
    // 30s, 60s, 2m, 4m ... capped at 10 minutes. Slow retries avoid making an IP block worse.
    const delay = Math.min(30000 * 2 ** Math.min(loginAttempt - 1, 5), 10 * 60 * 1000);
    console.error(`Retrying Discord login in ${Math.round(delay / 1000)}s.`);
    setTimeout(() => { void loginWithRetry(); }, delay);
  }
}
void loginWithRetry();
