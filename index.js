require('dotenv').config();
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const express = require('express');
const Stripe = require('stripe');
const { pipeline } = require('stream/promises');
const { Readable } = require('stream');
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
  MONZO_ACCESS_TOKEN = '',
  MONZO_ACCOUNT_ID = '',
  MONZO_USERNAME = 'sstewart32',
} = process.env;

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
      await ch.send({
        content: payload.content || undefined,
        embeds: payload.embeds,
        files: payload.files?.map((f) => new AttachmentBuilder(f.buffer, { name: f.name })),
        allowedMentions: { parse: [] },
      });
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

const VAULT_DIR = path.join(DATA_DIR, 'vault');
const VAULT_FILE = path.join(DATA_DIR, 'vault.json');
fs.mkdirSync(VAULT_DIR, { recursive: true });

let vault = {};
try {
  vault = JSON.parse(fs.readFileSync(VAULT_FILE, 'utf8'));
  let vaultChanged = false;
  for (const [k, item] of Object.entries(vault)) {
    if (!item.path || !fs.existsSync(item.path)) {
      delete vault[k];
      vaultChanged = true;
    }
  }
  if (vaultChanged) {
    fs.writeFileSync(VAULT_FILE, JSON.stringify(vault, null, 2));
  }
} catch {}

const saveVault = () => {
  try {
    fs.writeFileSync(VAULT_FILE, JSON.stringify(vault, null, 2));
  } catch (e) {
    console.error('Could not save vault:', e.message);
  }
};

const BASE_URL = PUBLIC_URL.replace(/\/+$/, '');
const MAX_FILE_BYTES = (BASE_URL ? 100 : 10) * 1024 * 1024;
fs.mkdirSync(FILES_DIR, { recursive: true });

let deliveries = {};
try {
  deliveries = JSON.parse(fs.readFileSync(DELIVERIES_FILE, 'utf8'));
} catch {}
const saveDeliveries = () => {
  try {
    fs.writeFileSync(DELIVERIES_FILE, JSON.stringify(deliveries));
  } catch (e) {
    console.error('Could not save deliveries:', e.message);
  }
};

for (const [k, d] of Object.entries(deliveries)) {
  if (Date.now() - d.savedAt > 7 * 24 * 60 * 60 * 1000) {
    if (d.path) {
      try {
        fs.unlinkSync(d.path);
      } catch {}
    }
    delete deliveries[k];
  }
}
saveDeliveries();

const liveInteractions = new Map();

async function downloadAttachment(att, hosted) {
  const limit = (hosted ? 100 : 10) * 1024 * 1024;
  if (att.size > limit) throw new Error(`That file is over ${hosted ? 100 : 10} MB. ${hosted ? '' : 'Use the hosted link option for bigger files.'}`);
  const res = await fetch(att.url);
  if (!res.ok) throw new Error('Could not download the file you attached.');
  return { buffer: Buffer.from(await res.arrayBuffer()), name: att.name };
}

function saveDelivery(key, fileData, interaction, customer, type = 'stripe', amount = 0) {
  const safeName = fileData.name.replace(/[^\w.\-]/g, '_');
  const filePath = path.join(FILES_DIR, `${key.replace(/[^\w\-]/g, '_')}_${safeName}`);
  fs.writeFileSync(filePath, fileData.buffer);
  deliveries[key] = {
    type,
    amount,
    paid: false,
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

function saveLinkDelivery(key, url, interaction, customer, type = 'stripe', amount = 0) {
  deliveries[key] = {
    type,
    amount,
    paid: false,
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
    fs.writeFileSync(DOWNLOADS_FILE, JSON.stringify(downloads));
  } catch (e) {
    console.error('Could not save downloads:', e.message);
  }
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
async function backupData() {
  try {
    const ch = BACKUP_CHANNEL_ID
      ? await client.channels.fetch(BACKUP_CHANNEL_ID).catch(() => null)
      : findBackupChannel() ?? (await getLogChannel());
    if (!ch) return;
    const files = [];
    for (const f of [DELIVERIES_FILE, DOWNLOADS_FILE, VAULT_FILE]) {
      try {
        const buffer = fs.readFileSync(f);
        if (buffer.length > 2) files.push({ name: path.basename(f), buffer });
      } catch {}
    }
    if (!files.length) return;
    const hash = crypto.createHash('sha256').update(Buffer.concat(files.map((f) => f.buffer))).digest('hex');
    if (hash === lastBackupHash) return;
    await ch.send({
      content: 'Data backup (records of files waiting, download links, and vault index)',
      files: files.map((f) => new AttachmentBuilder(f.buffer, { name: f.name })),
      allowedMentions: { parse: [] },
    });
    lastBackupHash = hash;
    lastBackupAt = Date.now();
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

  let dlink = d.link || null;
  let hosted = false;
  if (!dlink && BASE_URL && d.mode !== 'direct' && d.path && fs.existsSync(d.path)) {
    dlink = makeDownloadLink(d, paymentIntentId);
    hosted = true;
  }
  let buffer = null;
  if (!dlink) {
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
    dlink
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
        await live.interaction.editReply({
          embeds: [EmbedBuilder.from(old).setColor(GREY).setTitle(`PAID - ${old.title}`)],
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
      await notifyOwner({ content: `"${d.name}" was sent to ${sentTo}.` });
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
    .setDescription('Create payment links for Stripe or Monzo')
    .addSubcommand((sub) =>
      sub
        .setName('stripe')
        .setDescription('Create a Stripe payment link / invoice for a customer')
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
    )
    .addSubcommand((sub) =>
      sub
        .setName('monzo')
        .setDescription('Create a Monzo bank transfer invoice with vault & automated amount tracking')
        .addUserOption((o) => o.setName('customer').setDescription('Who is paying').setRequired(true))
        .addNumberOption((o) => o.setName('amount').setDescription('Amount to charge').setMinValue(1).setRequired(true))
        .addStringOption((o) => o.setName('description').setDescription('What the purchase is for').setRequired(true))
        .addAttachmentOption((o) => o.setName('file').setDescription('File to send once paid'))
        .addStringOption((o) => o.setName('vault_file').setDescription('Or pick a pre-saved file from your vault').setAutocomplete(true))
        .addStringOption((o) => o.setName('delivery').setDescription('Delivery method').addChoices({ name: 'Hosted link', value: 'link' }, { name: 'Direct upload', value: 'direct' }))
        .addStringOption((o) => o.setName('link').setDescription('Download link to send once paid'))
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

const commands = [bankrollCmd, vaultCmd, cancelCmd, remindCmd, unpaidCmd, statusCmd, restoreCmd, currencyCmd, paymentsCmd, totalCmd, customerCmd, historyCmd, refundCmd, setupCmd];

let started = false;
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
  restoreReminders().then((n) =>
    notifyOwner({ content: `Bot is back online.${n ? ` Reminders rebuilt: ${n}.` : ''}` }, { log: false })
  );
  setTimeout(backupData, 2 * 60 * 1000);
  setInterval(backupData, 6 * 60 * 60 * 1000);
};
client.once('ready', onReady);
client.once('clientReady', onReady);

// ---------- Reminder ----------
const reminderTimers = new Map();

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

function scheduleReminders({ customerId, agentId, sessionId, url, amount, description, expiresAt, currency = CURRENCY, fireAt = null }) {
  if (reminderTimers.has(sessionId)) return;
  const total = expiresAt * 1000 - Date.now();
  const delay = Math.max(fireAt ? fireAt - Date.now() : total * 0.5, 1000);
  const timer = setTimeout(async () => {
    reminderTimers.delete(sessionId);
    try {
      const s = await stripe.checkout.sessions.retrieve(sessionId);
      if (s.status !== 'open' || s.metadata?.reminded_at) return;

      const embed = new EmbedBuilder()
        .setColor(GREEN)
        .setTitle('Payment reminder')
        .setDescription(
          `You have an unpaid invoice for **${money(amount, currency)}** (${description}).\n` +
            `The link expires <t:${expiresAt}:R>.`
        );
      const row = new ActionRowBuilder().addComponents(
        new ButtonBuilder().setLabel('Pay invoice').setStyle(ButtonStyle.Link).setURL(url)
      );

      let sent = false;
      try {
        const user = await client.users.fetch(customerId);
        await user.send({ embeds: [embed], components: [row] });
        sent = true;
      } catch {}

      if (!sent) {
        const agent = await client.users.fetch(agentId);
        await agent.send(
          `Reminder due: <@${customerId}> still hasn't paid ${money(amount, currency)} (${description}). ` +
            `Open your DM with them and run /remind customer:@them`
        );
      }
      await stripe.checkout.sessions
        .update(sessionId, { metadata: { reminded_at: new Date().toISOString() } })
        .catch(() => {});
    } catch (e) {
      alertOwner('Reminder failed', e);
    }
  }, delay);
  reminderTimers.set(sessionId, timer);
}

async function restoreReminders() {
  try {
    const list = await stripe.checkout.sessions.list({ limit: 100, status: 'open' });
    let n = 0;
    for (const s of list.data) {
      const m = s.metadata ?? {};
      if (m.kind !== 'invoice' || m.reminded_at || !m.agent_id || !m.customer_id || !s.expires_at) continue;
      if (reminderTimers.has(s.id)) continue;
      if (s.expires_at * 1000 - Date.now() < 5 * 60 * 1000) continue;
      scheduleReminders({
        customerId: m.customer_id,
        agent_id: m.agent_id,
        sessionId: s.id,
        url: s.url,
        amount: (s.amount_total ?? 0) / 100,
        description: m.description || 'your order',
        expiresAt: s.expires_at,
        currency: s.currency,
        fireAt: (s.created + (s.expires_at - s.created) * 0.5) * 1000,
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

// ---------- Stripe checkout (/bankroll stripe) ----------
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
      fileData = {
        buffer: fs.readFileSync(item.path),
        name: item.originalName,
      };
    }

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
            file_sha256: crypto.createHash('sha256').update(fileData.buffer).digest('hex'),
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
    if (fileData) saveDelivery(session.id, fileData, interaction, customer, 'stripe', amount);
    if (linkUrl) saveLinkDelivery(session.id, linkUrl, interaction, customer, 'stripe', amount);
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

    await interaction.editReply({ content: `<@${customer.id}>`, embeds: [embed], components: [row] });
    
    // GUARANTEED LOGGING
    await logToServer({
      embeds: [embed],
      files: fileData ? [{ buffer: fileData.buffer, name: fileData.name }] : undefined,
    });

    if (fileData || linkUrl) {
      liveInteractions.set(session.id, { interaction, at: Date.now() });
      setTimeout(() => liveInteractions.delete(session.id), 14 * 60 * 1000);
    }

    if (expiresAt) {
      scheduleReminders({
        customerId: customer.id,
        agentId: agent.id,
        sessionId: session.id,
        url: session.url,
        amount,
        description,
        expiresAt,
        currency: cur,
      });
    }
  } catch (err) {
    console.error(err);
    await interaction.editReply({ content: `Failed to create invoice: ${err.message}` });
  }
}

// ---------- Monzo checkout (/bankroll monzo) ----------
async function sendMonzoCheckout(interaction) {
  const customer = interaction.options.getUser('customer');
  const amount = interaction.options.getNumber('amount');
  const description = interaction.options.getString('description');
  const agent = interaction.user;
  const file = interaction.options.getAttachment('file');
  const vaultKey = interaction.options.getString('vault_file');
  const linkRaw = interaction.options.getString('link');
  const invoiceKey = `MONZO-${Math.floor(1000 + Math.random() * 9000)}`;

  await interaction.deferReply();

  try {
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
      fileData = {
        buffer: fs.readFileSync(item.path),
        name: item.originalName,
      };
    }

    if (fileData) saveDelivery(invoiceKey, fileData, interaction, customer, 'monzo', amount);
    else if (linkUrl) saveLinkDelivery(invoiceKey, linkUrl, interaction, customer, 'monzo', amount);
    else {
      deliveries[invoiceKey] = {
        type: 'monzo',
        amount,
        paid: false,
        name: 'download link',
        description,
        channelId: interaction.channelId,
        customerId: customer.id,
        agentId: agent.id,
        savedAt: Date.now(),
      };
      saveDeliveries();
    }

    const monzoUrl = `https://monzo.me/${MONZO_USERNAME}/${amount.toFixed(2)}`;
    const where = whereFrom(interaction, customer);

    const embed = new EmbedBuilder()
      .setColor(GREEN)
      .setTitle('Monzo Bank Transfer Invoice')
      .setDescription('Please click the button below to pay via Monzo.me. Once the exact amount debits to your account, the file/link will unlock automatically!')
      .addFields(
        { name: 'Sales Agent', value: `<@${agent.id}>` },
        { name: 'Customer', value: `<@${customer.id}>` },
        { name: 'Amount to Pay', value: money(amount, 'gbp') },
        { name: 'Description', value: description },
        { name: 'Status', value: '⏳ Waiting for incoming payment match...' }
      )
      .setFooter({ text: `Created by ${agent.username} | Invoice Ref: ${invoiceKey}` });

    const row = new ActionRowBuilder().addComponents(
      new ButtonBuilder().setLabel('Pay with Monzo').setStyle(ButtonStyle.Link).setURL(monzoUrl)
    );

    await interaction.editReply({ content: `<@${customer.id}>`, embeds: [embed], components: [row] });
    
    await logToServer({
      embeds: [embed],
      files: fileData ? [{ buffer: fileData.buffer, name: fileData.name }] : undefined,
    });

    liveInteractions.set(invoiceKey, { interaction, at: Date.now() });
    setTimeout(() => liveInteractions.delete(invoiceKey), 14 * 60 * 1000);
  } catch (err) {
    console.error(err);
    await interaction.editReply({ content: `Could not create Monzo invoice: ${err.message}` });
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
    
    if (vault[key]) {
      return interaction.editReply(`A vault item with the name **${key}** already exists. Remove it first or pick a different name.`);
    }

    if (att.size > 100 * 1024 * 1024) {
      return interaction.editReply('That file is over 100 MB.');
    }

    const filePath = path.join(VAULT_DIR, `${key}_${att.name.replace(/[^\w.\-]/g, '_')}`);

    try {
      const res = await fetch(att.url);
      if (!res.ok || !res.body) throw new Error('Could not download the file.');
      
      const fileStream = fs.createWriteStream(filePath);
      await pipeline(Readable.fromWeb(res.body), fileStream);
    } catch (err) {
      try { fs.unlinkSync(filePath); } catch {}
      return interaction.editReply(`Failed to download and save file: ${err.message}`);
    }

    vault[key] = {
      path: filePath,
      originalName: att.name,
      size: att.size,
      addedAt: Date.now(),
    };
    saveVault();

    return interaction.editReply(`Successfully added **${att.name}** to the vault as \`${key}\`!`);
  }

  if (sub === 'list') {
    const keys = Object.keys(vault);
    if (!keys.length) return interaction.editReply('Your vault is currently empty.');

    const lines = keys.map((k) => `• \`${k}\` — **${vault[k].originalName}** (${(vault[k].size / (1024 * 1024)).toFixed(1)} MB)`);
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
  if (!input.startsWith('cs_')) {
    return interaction.editReply('Please provide a valid Checkout ID starting with `cs_...`.');
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
  if (!s) return interaction.editReply(`<@${user.id}> has no unpaid invoice.`);

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
  const res = await stripe.checkout.sessions.list({ limit: 100, status: 'open' });
  const mine = res.data.filter((s) => s.metadata?.agent_id);
  if (!mine.length) return interaction.editReply('No unpaid invoices right now.');

  const lines = mine
    .slice(0, 20)
    .map(
      (s) =>
        `${money((s.amount_total ?? 0) / 100, s.currency)} - <@${s.metadata.customer_id}> - ` +
        `${short(s.metadata.description, 40)} - expires <t:${s.expires_at}:R>`
    );
  const embed = new EmbedBuilder()
    .setColor(GREEN)
    .setTitle(`Unpaid invoices (${mine.length})`)
    .setDescription(lines.join('\n'))
    .setFooter({ text: mine.length > 20 ? `Showing 20 of ${mine.length}.` : 'Links that are still open.' });
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
      { name: 'Reminders queued', value: String(reminderTimers.size), inline: true },
      { name: 'Files waiting', value: String(Object.keys(deliveries).length), inline: true },
      { name: 'Vault items', value: String(Object.keys(vault).length), inline: true },
      { name: 'Hosted links', value: BASE_URL ? 'On' : 'Off (PUBLIC_URL not set)', inline: true },
      {
        name: 'File storage',
        value: process.env.DATA_DIR ? 'Persistent folder set' : 'Default folder (wiped on redeploy)',
        inline: true,
      },
      { name: 'Last Stripe notice', value: lastWebhookAt ? `${when(lastWebhookAt)} (${lastWebhookType})` : 'None since start', inline: true },
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
  const target = fname.startsWith('deliveries') ? 'deliveries' : fname.startsWith('downloads') ? 'downloads' : fname.startsWith('vault') ? 'vault' : null;
  if (!target) return interaction.editReply('Pick deliveries.json, downloads.json, or vault.json from the backup channel.');

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

  const store = target === 'deliveries' ? deliveries : target === 'downloads' ? downloads : vault;
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
    } else if (target !== 'vault') {
      skipped += 1;
      continue;
    }
    store[k] = v;
    added += 1;
  }
  if (target === 'deliveries') saveDeliveries();
  else if (target === 'downloads') saveDownloads();
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
  const list = (await succeededPayments()).slice(0, count);
  if (!list.length) return interaction.editReply('No card payments found yet.');

  const lines = list.map(
    (pi, i) =>
      `**${i + 1}.** ${money(pi.amount_received / 100, pi.currency)} - <@${pi.metadata.customer_id}> - ` +
      `${short(pi.metadata.description, 40)} - <t:${pi.created}:d>`
  );
  const embed = new EmbedBuilder()
    .setColor(GREEN)
    .setTitle(`Last ${list.length} card payments`)
    .setDescription(lines.join('\n'));
  return interaction.editReply({ embeds: [embed] });
}

async function cmdTotal(interaction) {
  const period = interaction.options.getString('period');
  const since = periodStart(period);
  const everything = await succeededPayments(since);
  const list = everything.filter((pi) => pi.currency === CURRENCY.toLowerCase());
  const otherCurrency = everything.length - list.length;

  const sum = list.reduce((a, pi) => a + pi.amount_received, 0) / 100;

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
      { name: 'Payments', value: String(list.length), inline: true },
      { name: 'By sales agent', value: agentLines.join('\n') || '—' }
    )
    .setFooter({
      text:
        'Card payments only. Stripe does not return its fee on refunds.' +
        (otherCurrency ? ` ${otherCurrency} payment(s) in other currencies not counted.` : '') +
        (missingFees ? ` Fee unavailable for ${missingFees} payment(s).` : ''),
    });
  return interaction.editReply({ embeds: [embed] });
}

async function cmdCustomer(interaction) {
  const user = interaction.options.getUser('user');
  const res = await stripe.paymentIntents.search({
    query: `status:'succeeded' AND metadata['customer_id']:'${user.id}'`,
    limit: 100,
  });
  const list = res.data;
  if (!list.length) return interaction.editReply(`<@${user.id}> hasn't paid you anything by card yet.`);

  const sum = list.reduce((a, pi) => a + pi.amount_received, 0) / 100;
  const lines = list
    .slice(0, 15)
    .map(
      (pi) =>
        `${money(pi.amount_received / 100, pi.currency)} - ${short(pi.metadata.description, 40)} - <t:${pi.created}:d>`
    );
  const embed = new EmbedBuilder()
    .setColor(GREEN)
    .setTitle(`Payments from ${user.username}`)
    .setDescription(lines.join('\n'))
    .addFields(
      { name: 'Total paid', value: money(sum, CURRENCY), inline: true },
      { name: 'Payments', value: String(list.length), inline: true }
    );
  return interaction.editReply({ embeds: [embed] });
}

async function cmdHistory(interaction) {
  const user = interaction.options.getUser('user');
  const sessions = await stripe.checkout.sessions.list({ limit: 100 }).autoPagingToArray({ limit: 500 });
  const mine = sessions.filter((s) => s.metadata?.customer_id === user.id);
  if (!mine.length) return interaction.editReply(`No links found for <@${user.id}>.`);

  const status = (s) =>
    s.payment_status === 'paid' ? 'Paid' : s.status === 'expired' ? 'Expired' : 'Unpaid';
  const lines = mine
    .slice(0, 20)
    .map(
      (s) =>
        `**${status(s)}** - ${money((s.amount_total ?? 0) / 100, s.currency)} - ` +
        `${short(s.metadata?.description, 40)} - <t:${s.created}:d>`
    );
  const embed = new EmbedBuilder()
    .setColor(GREEN)
    .setTitle(`Links made for ${user.username}`)
    .setDescription(lines.join('\n'))
    .setFooter({ text: `Showing ${lines.length} of ${mine.length}. Card links only.` });
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
    
    // Vault remove/info autocomplete or bankroll file autocomplete
    if (
      (interaction.commandName === 'vault' && ['remove', 'info'].includes(interaction.options.getSubcommand())) ||
      (interaction.commandName === 'bankroll' && interaction.options.getFocused(true).name === 'vault_file')
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
      const sessions = await stripe.checkout.sessions.list({ limit: 50, status: 'open' });
      const choices = sessions.data
        .filter((s) => s.metadata?.agent_id)
        .filter((s) => !typed || `${s.metadata.description} ${s.id}`.toLowerCase().includes(typed))
        .slice(0, 25)
        .map((s) => ({
          name: `${money((s.amount_total ?? 0) / 100, s.currency)} - ${short(s.metadata.description, 50)} - ID: ${s.id}`.slice(0, 100),
          value: s.id,
        }));
      return await interaction.respond(choices);
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
      case 'bankroll': {
        const sub = interaction.options.getSubcommand();
        if (sub === 'stripe') return await sendStripeCheckout(interaction);
        if (sub === 'monzo') return await sendMonzoCheckout(interaction);
        break;
      }
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

// ---------- Express App & Webhooks (Stripe + Monzo) ----------
const app = express();
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

app.get('/', (_, res) => res.send('Bankroll bot and Webhooks are active!'));

// --- Monzo Webhook Listener (Amount-Based Tracking) ---
app.post('/monzo-webhook', async (req, res) => {
  try {
    const data = req.body?.data;
    if (req.body?.type === 'transaction.create' && data) {
      const amountPaid = data.amount / 100; // Monzo amounts are in minor units (pence/cents)

      if (amountPaid > 0) {
        for (const [key, delivery] of Object.entries(deliveries)) {
          if (delivery.type === 'monzo' && !delivery.paid) {
            if (Math.abs(amountPaid - delivery.amount) < 0.01) {
              delivery.paid = true;
              saveDeliveries();
              await deliverFile(key, null);
              console.log(`Monzo payment amount-matched and delivered for invoice: ${key} (${amountPaid})`);
              break;
            }
          }
        }
      }
    }
    res.status(200).send('OK');
  } catch (e) {
    console.error('Monzo webhook error:', e.message);
    res.status(500).send('Error processing webhook');
  }
});

// --- Stripe Webhook Listener ---
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
