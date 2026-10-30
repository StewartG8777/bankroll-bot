require('dotenv').config();
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const express = require('express');
const Stripe = require('stripe');
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

async function notifyOwner(payload, { log = true } = {}) {
  try {
    const owner = await client.users.fetch(OWNER_ID);
    await owner.send(payload);
  } catch (e) {
    console.error('Owner DM failed:', e.message);
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
const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, 'data');
const FILES_DIR = path.join(DATA_DIR, 'files');
const DELIVERIES_FILE = path.join(DATA_DIR, 'deliveries.json');
const MONZO_FILE = path.join(DATA_DIR, 'monzo_payments.json');

// Write to a temp file then rename, so a crash mid-write can never leave a half-written data file.
function atomicWrite(file, data) {
  const tmp = `${file}.tmp`;
  fs.writeFileSync(tmp, data);
  fs.renameSync(tmp, file);
}

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
let restoreDone = false;
let backupTimer = null;
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
function cleanDownloads() {
  for (const [t, dl] of Object.entries(downloads)) {
    if (Date.now() > dl.expiresAt) {
      try {
        fs.unlinkSync(dl.path);
      } catch {}
      delete downloads[t];
    }
  }
  saveDownloads();
}
cleanDownloads();
setInterval(cleanDownloads, 60 * 60 * 1000);

function makeDownloadLink(d, paymentIntentId) {
  const token = crypto.randomBytes(24).toString('hex');
  downloads[token] = {
    path: d.path,
    name: d.name,
    customerId: d.customerId,
    agentId: d.agentId,
    vaultKey: d.vaultKey ?? null,
    paymentIntentId: paymentIntentId ?? null,
    expiresAt: Date.now() + 24 * 60 * 60 * 1000,
    used: false,
  };
  saveDownloads();
  const slug =
    (d.description || '')
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, '-')
      .replace(/^-+|-+$/g, '')
      .slice(0, 40)
      .replace(/-+$/, '') || 'order';
  return `${BASE_URL}/bankroll-${slug}-download/${token}`;
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
const BLOB_CHUNK = 9 * 1024 * 1024; // Discord allows about 10 MB per upload, so big files go up in 9 MB parts
const blobBackedUp = {}; // relative path -> size already in the backup channel
function relData(p) {
  if (!p) return null;
  const rel = path.relative(DATA_DIR, path.resolve(String(p)));
  return rel && !rel.startsWith('..') && !path.isAbsolute(rel) ? rel.split(path.sep).join('/') : null;
}
function dataFilePaths() {
  const set = new Set();
  for (const v of Object.values(vault)) if (v?.path) set.add(v.path);
  for (const d of [...Object.values(deliveries), ...Object.values(downloads)]) {
    if (!d?.path) continue;
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
async function backupDataOnce() {
  if (!restoreDone) return;
  try {
    const ch = BACKUP_CHANNEL_ID
      ? await client.channels.fetch(BACKUP_CHANNEL_ID).catch(() => null)
      : findBackupChannel() ?? (await getLogChannel());
    if (!ch) return;

    const files = [];
    for (const f of [DELIVERIES_FILE, DOWNLOADS_FILE, VAULT_FILE, MONZO_FILE]) {
      try {
        const buffer = fs.readFileSync(f);
        if (buffer.length > 2) files.push({ name: path.basename(f), buffer });
      } catch {}
    }
    if (files.length) {
      const hash = crypto.createHash('sha256').update(Buffer.concat(files.map((f) => f.buffer))).digest('hex');
      if (hash !== lastBackupHash) {
        const sentMsg = await ch.send({
          content: 'Data backup (waiting files, download links, vault index and Monzo payment records)',
          files: files.map((f) => new AttachmentBuilder(f.buffer, { name: f.name })),
          allowedMentions: { parse: [] },
        });
        const prevMsg = lastJsonMsg;
        lastJsonMsg = sentMsg;
        if (prevMsg) prevMsg.delete().catch(() => {});
        lastBackupHash = hash;
        lastBackupAt = Date.now();
      }
    }

    // The files themselves, so everything comes back after a redeploy (any size: big files go up in parts).
    let sentParts = 0;
    for (const fp of dataFilePaths()) {
      const rel = relData(fp);
      let st;
      try {
        st = fs.statSync(fp);
      } catch {
        continue;
      }
      if (!st.size || blobBackedUp[rel] === st.size) continue;
      const total = Math.ceil(st.size / BLOB_CHUNK);
      if (sentParts > 0 && sentParts + total > 15) {
        scheduleBackup(); // carry on with the rest in a moment
        break;
      }
      let fd;
      try {
        fd = fs.openSync(fp, 'r');
        for (let i = 0; i < total; i++) {
          const len = Math.min(BLOB_CHUNK, st.size - i * BLOB_CHUNK);
          const buf = Buffer.alloc(len);
          fs.readSync(fd, buf, 0, len, i * BLOB_CHUNK);
          await ch.send({
            content: `datafile:${rel}|${st.size}|${i + 1}/${total}`,
            files: [new AttachmentBuilder(buf, { name: total === 1 ? path.basename(fp) : `${path.basename(fp)}.part${i + 1}of${total}` })],
            allowedMentions: { parse: [] },
          });
          sentParts += 1;
        }
        blobBackedUp[rel] = st.size;
        lastBackupAt = Date.now();
      } catch (e) {
        console.error(`File backup failed for ${rel}:`, e.message);
        scheduleBackup();
      } finally {
        if (fd !== undefined) {
          try {
            fs.closeSync(fd);
          } catch {}
        }
      }
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

async function deliverFile(key, paymentIntentId) {
  const d = deliveries[key];
  if (!d) return;

  let owner = null;
  try {
    owner = await client.users.fetch(OWNER_ID);
  } catch {}

  const noFile = !d.link && !d.path;
  let dlink = d.link || null;
  let hosted = false;
  if (!dlink && BASE_URL && d.mode !== 'direct' && d.path && fs.existsSync(d.path)) {
    dlink = makeDownloadLink(d, paymentIntentId);
    hosted = true;
  }
  let buffer = null;
  if (!dlink && !noFile) {
    try {
      buffer = fs.readFileSync(d.path);
    } catch {
      if (owner) {
        await owner
          .send(`Payment received for ${key}, but the file "${d.name}" is gone. Please send it manually to <@${d.customerId}>.`)
          .catch(() => {});
      }
      delete deliveries[key];
      saveDeliveries();
      return;
    }
  }

  const payload = () =>
    noFile
      ? { content: `<@${d.customerId}> payment received. Thank you!` }
      : dlink
      ? { content: `<@${d.customerId}> payment received. Here is your download link${hosted ? ' (works once, expires in 24 hours)' : ''}:\n<${dlink}>` }
      : {
          content: `<@${d.customerId}> payment received. Here is the file for this purchase.`,
          files: [new AttachmentBuilder(buffer, { name: d.name })],
        };

  let sentTo = null;
  let ownerGotIt = false;
  let msgUrl = null;
  let reachedCustomer = false;

  const live = liveInteractions.get(key);
  if (live && Date.now() - live.at < 14 * 60 * 1000) {
    try {
      const original = await live.interaction.fetchReply();
      const old = original.embeds[0];
      if (old) {
        const paidEmbed = EmbedBuilder.from(old).setColor(GREY).setTitle(`PAID - ${old.title}`);
        if (d.type === 'monzo') paidEmbed.setDescription('Payment received. Thank you!');
        await live.interaction.editReply({
          embeds: [paidEmbed],
          components: [],
        });
      }
      const msg = await live.interaction.followUp(payload());
      msgUrl = msg.url;
      reachedCustomer = true;
      sentTo = 'a reply under the invoice message';
    } catch (e) {
      console.log('Could not reply under invoice:', e.message);
    }
  }
  liveInteractions.delete(key);

  if (!sentTo) try {
    const ch = await client.channels.fetch(d.channelId);
    const msg = await ch.send(payload());
    msgUrl = msg.url;
    reachedCustomer = true;
    sentTo = 'the channel where the invoice was created';
  } catch (e) {
    console.log('Could not post in original channel:', e.message);
  }

  if (!sentTo) {
    try {
      const u = await client.users.fetch(d.customerId);
      const msg = await u.send(payload());
      msgUrl = msg.url;
      reachedCustomer = true;
      sentTo = "the customer's DMs";
    } catch (e) {
      console.log('Could not DM customer:', e.message);
    }
  }

  if (!sentTo && owner) {
    try {
      await owner.send(
        dlink
          ? { content: `Couldn't reach <@${d.customerId}>. Here is their download link:\n<${dlink}>` }
          : {
              content: `Couldn't reach <@${d.customerId}>. Here is their file.`,
              files: [new AttachmentBuilder(buffer, { name: d.name })],
            }
      );
      sentTo = 'your DMs';
      ownerGotIt = true;
    } catch (e) {
      console.error('Could not deliver file anywhere:', e.message);
    }
  }

  if (sentTo) {
    if (d.path && !hosted) {
      try {
        fs.unlinkSync(d.path);
      } catch {}
    }
    delete deliveries[key];
    saveDeliveries();
    if (reachedCustomer && paymentIntentId) {
      await recordInStripe(paymentIntentId, {
        delivered_at: new Date().toISOString(),
        delivered_to: sentTo.slice(0, 200),
        delivered_file: d.name.slice(0, 100),
        delivery_message: msgUrl ?? '',
      });
    }
    if (owner && !ownerGotIt) {
      await notifyOwner({
        content: noFile
          ? `Payment confirmed for "${d.description || 'invoice'}" and <@${d.customerId}> was told (${sentTo}).`
          : `"${d.name}" was sent to ${sentTo}.`,
      });
    }
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
);

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
        .setDescription('deliveries.json or downloads.json from the backup channel')
        .setRequired(true)
    )
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

const commands = [bankrollCmd, monzoCmd, vaultCmd, cancelCmd, remindCmd, unpaidCmd, statusCmd, restoreCmd, currencyCmd, paymentsCmd, totalCmd, customerCmd, historyCmd, refundCmd, setupCmd];

let started = false;
async function autoRestoreOnStartup() {
  try {
    const ch = BACKUP_CHANNEL_ID
      ? await client.channels.fetch(BACKUP_CHANNEL_ID).catch(() => null)
      : findBackupChannel() ?? (await getLogChannel());
    if (!ch) return;

    // Newest first; scan further back only when something is still missing
    const msgs = [];
    let before;
    let exhausted = false;
    const loadPages = async (n) => {
      for (let i = 0; i < n && !exhausted; i++) {
        const batch = await ch.messages.fetch({ limit: 100, ...(before ? { before } : {}) });
        if (!batch.size) {
          exhausted = true;
          break;
        }
        msgs.push(...batch.values());
        before = batch.last().id;
        if (batch.size < 100) exhausted = true;
      }
    };
    await loadPages(3);

    const targets = {
      'deliveries.json': DELIVERIES_FILE,
      'downloads.json': DOWNLOADS_FILE,
      'vault.json': VAULT_FILE,
      'monzo_payments.json': MONZO_FILE,
    };
    const needs = (f) => !fs.existsSync(f) || fs.readFileSync(f, 'utf8').length < 5;
    const done = new Set();
    const restoreJson = async () => {
      for (const msg of msgs) {
        if (msg.author.id !== client.user.id) continue;
        for (const att of msg.attachments.values()) {
          const fname = att.name.toLowerCase();
          const target = targets[fname];
          if (!target || done.has(fname) || !needs(target)) continue;
          try {
            const res = await fetch(att.url);
            const text = await res.text();
            JSON.parse(text); // validate json
            atomicWrite(target, text);
            done.add(fname);
            console.log(`Auto-recovered ${fname} from backup channel.`);
          } catch (e) {
            console.error(`Failed to auto-restore ${fname}:`, e.message);
          }
        }
      }
    };
    await restoreJson();
    if (!exhausted && Object.entries(targets).some(([f, t]) => !done.has(f) && needs(t))) {
      await loadPages(27);
      await restoreJson();
    }

    // Reload files into memory
    try {
      deliveries = JSON.parse(fs.readFileSync(DELIVERIES_FILE, 'utf8'));
    } catch {}
    try {
      downloads = JSON.parse(fs.readFileSync(DOWNLOADS_FILE, 'utf8'));
    } catch {}
    try {
      vault = JSON.parse(fs.readFileSync(VAULT_FILE, 'utf8'));
    } catch {}
    try {
      monzoLedger = JSON.parse(fs.readFileSync(MONZO_FILE, 'utf8'));
    } catch {}

    // Bring every stored file back (vault files, files waiting for payment, hosted downloads)
    const vaultRoot = path.resolve(VAULT_DIR) + path.sep;
    for (const [key, item] of Object.entries(vault)) {
      if (!item.path || !path.resolve(String(item.path)).startsWith(vaultRoot)) {
        item.path = path.join(VAULT_DIR, `${key}_${String(item.originalName || 'file').replace(/[^\w.\-]/g, '_')}`);
      }
    }
    const wanted = dataFilePaths();
    const missing = () => wanted.filter((fp) => !fs.existsSync(fp));

    const blobIndex = () => {
      const m = new Map();
      for (const msg of msgs) {
        if (msg.author.id !== client.user.id || !msg.content.startsWith('datafile:')) continue;
        const bits = msg.content.slice('datafile:'.length).split('|');
        const pt = String(bits.pop());
        const size = Number(bits.pop());
        const rel = bits.join('|');
        const [pi, tot] = pt.split('/').map(Number);
        const att = msg.attachments.first();
        if (!rel || !att || !pi || !tot || !Number.isFinite(size)) continue;
        const list = m.get(rel) || [];
        m.set(rel, list);
        let g = list.find((x) => x.size === size && x.total === tot);
        if (!g) {
          g = { size, total: tot, parts: new Map() };
          list.push(g);
        }
        if (!g.parts.has(pi)) g.parts.set(pi, att);
      }
      return m;
    };
    const completeGroup = (idx, rel) => (idx.get(rel) || []).find((g) => g.parts.size === g.total);

    let idx = blobIndex();
    let scanned = 0;
    while (!exhausted && scanned < 60 && missing().some((fp) => !completeGroup(idx, relData(fp)))) {
      await loadPages(10);
      scanned += 10;
      idx = blobIndex();
    }

    // Older backups stored vault files one per message as "vaultfile:<key>"
    const vaultAtts = {};
    for (const msg of msgs) {
      if (msg.author.id !== client.user.id || !msg.content.startsWith('vaultfile:')) continue;
      const key = msg.content.slice('vaultfile:'.length).trim();
      const att = msg.attachments.first();
      if (key && att && !vaultAtts[key]) vaultAtts[key] = att;
    }

    let recovered = 0;
    const lost = [];
    for (const fp of missing()) {
      const rel = relData(fp);
      try {
        let buf = null;
        const g = completeGroup(idx, rel);
        if (g) {
          const chunks = [];
          for (let i = 1; i <= g.total; i++) {
            const res = await fetch(g.parts.get(i).url);
            if (!res.ok) throw new Error(`HTTP ${res.status}`);
            chunks.push(Buffer.from(await res.arrayBuffer()));
          }
          buf = Buffer.concat(chunks);
          if (buf.length !== g.size) throw new Error('size mismatch');
        } else {
          const vk = Object.keys(vault).find((k) => vault[k].path === fp);
          const att = vk && vaultAtts[vk];
          if (att) {
            const res = await fetch(att.url);
            if (!res.ok) throw new Error(`HTTP ${res.status}`);
            buf = Buffer.from(await res.arrayBuffer());
          }
        }
        if (!buf) {
          lost.push(path.basename(fp));
          continue;
        }
        fs.mkdirSync(path.dirname(fp), { recursive: true });
        atomicWrite(fp, buf);
        blobBackedUp[rel] = buf.length;
        recovered += 1;
      } catch (e) {
        console.error(`Could not recover ${rel}:`, e.message);
        lost.push(path.basename(fp));
      }
    }
    // Invoice files that are just links to a vault file: link them again from the restored vault file
    for (const e of [...Object.values(deliveries), ...Object.values(downloads)]) {
      if (!e?.vaultKey || !e.path || fs.existsSync(e.path)) continue;
      const v = vault[e.vaultKey];
      if (!v || !fs.existsSync(v.path)) continue;
      try {
        fs.mkdirSync(path.dirname(e.path), { recursive: true });
        try {
          fs.linkSync(v.path, e.path);
        } catch {
          fs.copyFileSync(v.path, e.path);
        }
      } catch (err) {
        console.error('Could not re-link a vault file:', err.message);
      }
    }
    // Files that survived on disk and already have a backup do not need uploading again
    for (const fp of wanted) {
      const rel = relData(fp);
      try {
        const g = completeGroup(idx, rel);
        if (g && !blobBackedUp[rel] && fs.statSync(fp).size === g.size) blobBackedUp[rel] = g.size;
      } catch {}
    }
    if (Object.keys(vault).length) saveVault();
    if (recovered) console.log(`Recovered ${recovered} file(s) from backup channel.`);
    recoveryInfo = { ready: wanted.length - lost.length, total: wanted.length, restored: recovered, lost: [...lost] };
    recoveryNote =
      (wanted.length ? ` Files: ${wanted.length - lost.length} of ${wanted.length} ready${recovered ? ` (${recovered} restored from backup)` : ''}.` : '') +
      (lost.length ? ` Could not recover: ${lost.join(', ')}. Re-add them (/vault add or a new invoice).` : '');
  } catch (e) {
    console.error('Auto-restore on startup failed:', e.message);
  } finally {
    restoreDone = true;
  }
}

const onReady = async () => {
  if (started) return;
  started = true;
  console.log(`Logged in as ${client.user.tag}`);
  try {
    await client.application.commands.set(commands.map((c) => c.toJSON()));
    console.log('Slash commands registered');
  } catch (e) {
    console.error('Command registration failed:', e);
  }

  await autoRestoreOnStartup();

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

function checkinRow(payUrl) {
  if (!payUrl) return [];
  return [new ActionRowBuilder().addComponents(new ButtonBuilder().setLabel('Pay link').setStyle(ButtonStyle.Link).setURL(payUrl))];
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

        const embed = checkinEmbed({ pct, customerId, amount, currency, description, expiresSec: expiresAt, msgUrl: s.metadata?.invoice_msg_url });
        const delivered = await sendCheckin(
          agentId,
          { embeds: [embed], components: checkinRow(s.url) },
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
          { embeds: [embed], components: checkinRow(payUrl) },
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
          .setLabel('Get my file')
          .setStyle(ButtonStyle.Secondary)
      );
    }

    const sentInvoice = await interaction.editReply({ content: `<@${customer.id}>`, embeds: [embed], components: [row] });
    if (sentInvoice?.url) {
      stripe.checkout.sessions.update(session.id, { metadata: { invoice_msg_url: sentInvoice.url } }).catch(() => {});
    }
    
    // GUARANTEED LOGGING
    (async () => {
      let logFiles;
      const dp = deliveries[session.id]?.path;
      if (fileData && dp && fileData.size <= 8 * 1024 * 1024) {
        try {
          logFiles = [{ buffer: await fs.promises.readFile(dp), name: fileData.name }];
        } catch {}
      }
      await logToServer({ embeds: [embed], files: logFiles });
    })().catch((e) => console.error('Logging failed:', e.message));

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
    if (fileData) embed.addFields({ name: 'File', value: `${fileData.name} (sent here as soon as payment lands)` });
    if (linkUrl) embed.addFields({ name: 'Download', value: 'Link sent here as soon as payment lands' });
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
        new ButtonBuilder().setCustomId(`getfile:${key}`).setLabel('Get my file').setStyle(ButtonStyle.Secondary)
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
    await logToServer({ embeds: [embed] });

    liveInteractions.set(key, { interaction, at: Date.now() });
    setTimeout(() => liveInteractions.delete(key), 14 * 60 * 1000);
  } catch (err) {
    console.error(err);
    await interaction.editReply({ content: `Failed to create Monzo invoice: ${err.message}` });
  }
}

// ---------- Vault Command Handler ----------
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

    vault[key] = {
      path: filePath,
      originalName: att.name,
      size: info.size,
      sha256: info.sha256,
      addedAt: Date.now(),
    };
    delete blobBackedUp[relData(filePath)];
    saveVault();

    await interaction.editReply(`Successfully added **${att.name}** to the vault as \`${key}\`! Saving a backup copy now...`);
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

  if (sub === 'remove') {
    const key = interaction.options.getString('name');
    if (!vault[key]) return interaction.editReply(`Vault item \`${key}\` not found.`);

    try {
      fs.unlinkSync(vault[key].path);
    } catch {}
    delete blobBackedUp[relData(vault[key].path)];
    delete vault[key];
    saveVault();

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
      { name: 'Hosted links', value: BASE_URL ? 'On' : 'Off (PUBLIC_URL not set)', inline: true },
      {
        name: 'File storage',
        value: process.env.DATA_DIR ? 'Persistent folder set' : 'Default folder (wiped on redeploy)',
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
  const target = fname.startsWith('deliveries') ? 'deliveries' : fname.startsWith('downloads') ? 'downloads' : fname.startsWith('vault') ? 'vault' : fname.startsWith('monzo') ? 'monzo' : null;
  if (!target) return interaction.editReply('Pick deliveries.json, downloads.json, vault.json or monzo_payments.json from the backup channel.');

  let data;
  try {
    const res = await fetch(att.url);
    data = JSON.parse(await res.text());
  } catch {
    return interaction.editReply('That file could not be read as a backup.');
  }
  if (!data || typeof data !== 'object' || Array.isArray(data)) {
    return interaction.editReply('That file does not look like a backup.');
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
    } else if (target !== 'vault' && target !== 'monzo') {
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
      (interaction.commandName === 'vault' && ['remove', 'info'].includes(interaction.options.getSubcommand())) ||
      interaction.commandName === 'bankroll' ||
      interaction.commandName === 'bankroll-monzo'
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
    return interaction.followUp({
      content: 'The file was already sent or is no longer available.',
      ephemeral: true,
    });
  }
  if (interaction.user.id !== d.customerId && !allowed.has(interaction.user.id)) {
    return interaction.reply({ content: 'Only the customer can get this file.', ephemeral: true });
  }

  if (d.type === 'monzo') {
    if (!d.paid) {
      return interaction.reply({ content: 'Payment has not been received yet.', ephemeral: true });
    }
    await interaction.reply({ content: 'Sending your file now.', ephemeral: true });
    return deliverFile(key, null);
  }

  const session = await stripe.checkout.sessions.retrieve(key);
  if (session.payment_status !== 'paid') {
    return interaction.reply({
      content: 'Payment has not been received yet.',
      ephemeral: true,
    });
  }

  const checkPi = typeof session.payment_intent === 'string' ? session.payment_intent : session.payment_intent?.id;
  if (checkPi) {
    const pi = await stripe.paymentIntents.retrieve(checkPi, { expand: ['latest_charge'] });
    if (pi.latest_charge && typeof pi.latest_charge === 'object' && pi.latest_charge.refunded) {
      discardPending(key);
      return interaction.reply({
        content: 'This payment was refunded.',
        ephemeral: true,
      });
    }
  }

  let dlink = d.link || null;
  let hosted = false;
  if (!dlink && BASE_URL && d.mode !== 'direct' && d.path && fs.existsSync(d.path)) {
    dlink = makeDownloadLink(d, checkPi);
    hosted = true;
  }
  let buffer = null;
  if (!dlink) {
    try {
      buffer = fs.readFileSync(d.path);
    } catch {
      delete deliveries[key];
      saveDeliveries();
      return interaction.reply({
        content: 'Sorry, the file is no longer available.',
        ephemeral: true,
      });
    }
  }

  const old = interaction.message.embeds[0];
  await interaction.update({
    embeds: old ? [EmbedBuilder.from(old).setColor(GREY).setTitle(old.title.startsWith('PAID') ? old.title : `PAID - ${old.title}`)] : [],
    components: [],
  });
  const msg = await interaction.followUp(
    dlink
      ? { content: `<@${d.customerId}> here is your download link:\n<${dlink}>` }
      : {
          content: `<@${d.customerId}> here is the file for this purchase.`,
          files: [new AttachmentBuilder(buffer, { name: d.name })],
        }
  );

  if (d.path && !hosted) {
    try {
      fs.unlinkSync(d.path);
    } catch {}
  }
  delete deliveries[key];
  saveDeliveries();
  liveInteractions.delete(key);

  await recordInStripe(checkPi, {
    delivered_at: new Date().toISOString(),
    delivered_to: 'Get my file button pressed',
    delivered_file: d.name.slice(0, 100),
    delivery_message: msg.url ?? '',
  });

  await notifyOwner({ content: `"${d.name}" was sent to <@${d.customerId}>.` });
}

async function onButton(interaction) {
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

app.get('/:name/:token', (req, res) => {
  if (!req.params.name.startsWith('bankroll-')) return res.status(404).send('Not found');
  const token = req.params.token;
  const t = downloads[token];
  if (!t || t.used || Date.now() > t.expiresAt || !fs.existsSync(t.path)) {
    return res.status(410).send('This download link has already been used or has expired.');
  }
  const ua = req.headers['user-agent'] || '';
  if (req.method === 'HEAD' || /bot|crawler|spider|preview|embed/i.test(ua)) {
    return res.status(200).send('Download link');
  }
  t.used = true;
  res.setHeader('Content-Type', 'application/octet-stream');
  res.setHeader('Content-Disposition', `attachment; filename*=UTF-8''${encodeURIComponent(t.name)}`);
  let finished = false;
  res.on('finish', () => {
    finished = true;
    try {
      fs.unlinkSync(t.path);
    } catch {}
    delete downloads[token];
    saveDownloads();
    notifyOwner({ content: `<@${t.customerId}> downloaded "${t.name}".` });
    recordInStripe(t.paymentIntentId, { downloaded_at: new Date().toISOString() });
  });
  res.on('close', () => {
    if (!finished) t.used = false;
  });
  const stream = fs.createReadStream(t.path);
  stream.on('error', () => {
    t.used = false;
    res.destroy();
  });
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
