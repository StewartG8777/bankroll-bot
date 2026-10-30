require('dotenv').config();
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const express = require('express');
const Stripe = require('stripe');
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

// ---------- Express Server for Webhooks ----------
const app = express();
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// Basic uptime check route
app.get('/', (req, res) => res.send('Bot and Webhooks are active!'));

// --- Monzo Webhook Listener ---
app.post('/monzo-webhook', async (req, res) => {
  try {
    const data = req.body?.data;
    if (req.body?.type === 'transaction.create' && data) {
      const amountPaid = data.amount / 100; // Monzo amounts are in pence/cents
      const description = (data.description || '').toLowerCase();
      const notes = (data.notes || '').toLowerCase();
      const referenceText = `${description} ${notes}`;

      // Check if this incoming payment matches any pending manual bank invoices
      for (const [key, delivery] of Object.entries(deliveries)) {
        if (delivery.type === 'monzo' && !delivery.paid) {
          // Match if amount is correct and description/notes contain the unique reference code
          if (Math.abs(amountPaid - delivery.amount) < 0.01 && referenceText.includes(key.toLowerCase())) {
            delivery.paid = true;
            saveDeliveries();
            await deliverFile(key, null);
            console.log(`Monzo payment matched and delivered for invoice: ${key}`);
            break;
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

// ---------- File / link delivery ----------
const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, 'data');
const FILES_DIR = path.join(DATA_DIR, 'files');
const DELIVERIES_FILE = path.join(DATA_DIR, 'deliveries.json');
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
  if (att.size > limit) throw new Error(`That file is over ${hosted ? 100 : 10} MB.`);
  const res = await fetch(att.url);
  if (!res.ok) throw new Error('Could not download the file you attached.');
  return { buffer: Buffer.from(await res.arrayBuffer()), name: att.name };
}

function saveDelivery(key, fileData, interaction, customer, type = 'stripe', amount = 0) {
  const safeName = fileData ? fileData.name.replace(/[^\w.\-]/g, '_') : 'link';
  const filePath = fileData ? path.join(FILES_DIR, `${key.replace(/[^\w\-]/g, '_')}_${safeName}`) : null;
  if (fileData) fs.writeFileSync(filePath, fileData.buffer);

  deliveries[key] = {
    type,
    amount,
    paid: false,
    path: filePath,
    name: fileData ? fileData.name : 'download link',
    description: interaction.options.getString('description') ?? '',
    mode: interaction.options.getString('delivery') ?? (BASE_URL ? 'link' : 'direct'),
    channelId: interaction.channelId,
    customerId: customer.id,
    agentId: interaction.user.id,
    savedAt: Date.now(),
    link: interaction.options.getString('link') || null,
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
    description: interaction.options.getString('description') ?? '',
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
async function backupData() {
  try {
    const ch = BACKUP_CHANNEL_ID
      ? await client.channels.fetch(BACKUP_CHANNEL_ID).catch(() => null)
      : findBackupChannel() ?? (await getLogChannel());
    if (!ch) return;
    const files = [];
    for (const f of [DELIVERIES_FILE, DOWNLOADS_FILE]) {
      try {
        const buffer = fs.readFileSync(f);
        if (buffer.length > 2) files.push({ name: path.basename(f), buffer });
      } catch {}
    }
    if (!files.length) return;
    const hash = crypto.createHash('sha256').update(Buffer.concat(files.map((f) => f.buffer))).digest('hex');
    if (hash === lastBackupHash) return;
    await ch.send({
      content: 'Data backup (records of files waiting and download links)',
      files: files.map((f) => new AttachmentBuilder(f.buffer, { name: f.name })),
      allowedMentions: { parse: [] },
    });
    lastBackupHash = hash;
  } catch (e) {
    console.error('Backup failed:', e.message);
  }
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
  if (!dlink && d.path) {
    try {
      buffer = fs.readFileSync(d.path);
    } catch {}
  }

  const payload = () =>
    dlink
      ? { content: `<@${d.customerId}> payment received. Here is your download link${hosted ? ' (works once, expires in 24 hours)' : ''}:
<${dlink}>` }
      : buffer
      ? {
          content: `<@${d.customerId}> payment received. Here is the file for this purchase.`,
          files: [new AttachmentBuilder(buffer, { name: d.name })],
        }
      : { content: `<@${d.customerId}> payment received!` };

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
          ? { content: `Couldn't reach <@${d.customerId}>. Here is their download link:
<${dlink}>` }
          : buffer
          ? {
              content: `Couldn't reach <@${d.customerId}>. Here is their file.`,
              files: [new AttachmentBuilder(buffer, { name: d.name })],
            }
          : { content: `Payment received for <@${d.customerId}>.` }
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
    if (owner && !ownerGotIt) {
      await notifyOwner({ content: `"${d.name}" was sent to ${sentTo}.` });
    }
  }
}

// ---------- Slash Commands ----------
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
    .setDescription('Create a payment link for a customer')
    .addUserOption((o) => o.setName('customer').setDescription('Who is paying').setRequired(true))
    .addNumberOption((o) => o.setName('amount').setDescription('Amount to charge').setMinValue(1).setRequired(true))
    .addStringOption((o) => o.setName('description').setDescription('What the purchase is for').setRequired(true))
    .addAttachmentOption((o) => o.setName('file').setDescription('File to send once paid'))
    .addStringOption((o) => o.setName('delivery').setDescription('Delivery method').addChoices({ name: 'Hosted link', value: 'link' }, { name: 'Direct upload', value: 'direct' }))
    .addStringOption((o) => o.setName('currency').setDescription('Currency').addChoices({ name: 'GBP £', value: 'gbp' }, { name: 'USD $', value: 'usd' }, { name: 'EUR €', value: 'eur' }))
    .addStringOption((o) => o.setName('link').setDescription('Download link to send once paid'))
);

const invoiceCmd = common(
  new SlashCommandBuilder()
    .setName('invoice')
    .setDescription('Create an expiring card payment link with reminders')
    .addUserOption((o) => o.setName('customer').setDescription('Who is paying').setRequired(true))
    .addNumberOption((o) => o.setName('amount').setDescription('Amount to charge').setMinValue(1).setRequired(true))
    .addStringOption((o) => o.setName('description').setDescription('What the purchase is for').setRequired(true))
    .addAttachmentOption((o) => o.setName('file').setDescription('File to send once paid'))
    .addStringOption((o) => o.setName('delivery').setDescription('Delivery method').addChoices({ name: 'Hosted link', value: 'link' }, { name: 'Direct upload', value: 'direct' }))
    .addStringOption((o) => o.setName('currency').setDescription('Currency').addChoices({ name: 'GBP £', value: 'gbp' }, { name: 'USD $', value: 'usd' }, { name: 'EUR €', value: 'eur' }))
    .addStringOption((o) => o.setName('link').setDescription('Download link'))
    .addStringOption((o) => o.setName('expires').setDescription('Expiry time').addChoices({ name: '30 minutes', value: '30' }, { name: '1 hour', value: '60' }, { name: '6 hours', value: '360' }, { name: '24 hours', value: '1440' }))
);

const monzoCmd = common(
  new SlashCommandBuilder()
    .setName('monzo')
    .setDescription('Create a Monzo.me bank transfer invoice with automated tracking')
    .addUserOption((o) => o.setName('customer').setDescription('Who is paying').setRequired(true))
    .addNumberOption((o) => o.setName('amount').setDescription('Amount to charge').setMinValue(1).setRequired(true))
    .addStringOption((o) => o.setName('description').setDescription('What the purchase is for').setRequired(true))
    .addAttachmentOption((o) => o.setName('file').setDescription('File to send once paid'))
    .addStringOption((o) => o.setName('link').setDescription('Link to send once paid'))
);

const commands = [bankrollCmd, invoiceCmd, monzoCmd];

// ---------- Bot Startup & Interaction Handling ----------
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
  setTimeout(backupData, 2 * 60 * 1000);
  setInterval(backupData, 6 * 60 * 60 * 1000);
};
client.once('ready', onReady);
client.once('clientReady', onReady);

client.on('interactionCreate', async (interaction) => {
  if (!interaction.isChatInputCommand()) return;

  if (interaction.commandName === 'monzo') {
    const customer = interaction.options.getUser('customer');
    const amount = interaction.options.getNumber('amount');
    const description = interaction.options.getString('description');
    const file = interaction.options.getAttachment('file');
    const linkRaw = interaction.options.getString('link');
    const invoiceKey = `DISCORD-${Math.floor(1000 + Math.random() * 9000)}`;

    await interaction.deferReply();
    let fileData = null;
    if (file) fileData = await downloadAttachment(file, false);

    if (fileData) saveDelivery(invoiceKey, fileData, interaction, customer, 'monzo', amount);
    else if (linkRaw) saveLinkDelivery(invoiceKey, linkRaw, interaction, customer, 'monzo', amount);
    else saveDelivery(invoiceKey, null, interaction, customer, 'monzo', amount);

    const monzoUrl = `https://monzo.me/${MONZO_USERNAME}/${amount.toFixed(2)}`;

    const embed = new EmbedBuilder()
      .setColor(GREEN)
      .setTitle('Monzo Bank Transfer Invoice')
      .setDescription(`Please click the button below to pay via Monzo.me.\n**Important:** Ensure you include reference code \\`${invoiceKey}\\` in the payment notes if prompted!`)
      .addFields(
        { name: 'Amount to Pay', value: money(amount, 'gbp') },
        { name: 'Reference Required', value: `\\`${invoiceKey}\\`` },
        { name: 'Description', value: description },
        { name: 'Status', value: '⏳ Waiting for automated payment match...' }
      )
      .setFooter({ text: `Invoice Ref: ${invoiceKey}` });

    const row = new ActionRowBuilder().addComponents(
      new ButtonBuilder().setLabel('Pay with Monzo').setStyle(ButtonStyle.Link).setURL(monzoUrl)
    );

    await interaction.editReply({ content: `<@${customer.id}>`, embeds: [embed], components: [row] });
    liveInteractions.set(invoiceKey, { interaction, at: Date.now() });
  }

  if (interaction.commandName === 'bankroll' || interaction.commandName === 'invoice') {
    const customer = interaction.options.getUser('customer');
    const amount = interaction.options.getNumber('amount');
    const description = interaction.options.getString('description');
    const agent = interaction.user;
    const cur = (interaction.options.getString('currency') ?? CURRENCY).toLowerCase();

    await interaction.deferReply();

    try {
      const file = interaction.options.getAttachment('file');
      const linkRaw = interaction.options.getString('link');
      if (file && linkRaw) throw new Error('Use either a file or a link, not both.');

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
      const fileData = file ? await downloadAttachment(file, mode === 'link') : null;

      const record = {
        created_at: new Date().toISOString(),
        created_in: whereFrom(interaction, customer).guild,
        channel_id: String(interaction.channelId),
        kind: interaction.commandName,
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
      };

      const session = await stripe.checkout.sessions.create(params);
      if (fileData) saveDelivery(session.id, fileData, interaction, customer, 'stripe', amount);
      if (linkUrl) saveLinkDelivery(session.id, linkUrl, interaction, customer, 'stripe', amount);

      const embed = new EmbedBuilder()
        .setColor(GREEN)
        .setTitle('Bankroll Checkout Created')
        .setDescription('Please use the link below to complete your purchase.')
        .addFields(
          { name: 'Customer', value: `<@${customer.id}>` },
          { name: 'Amount', value: money(amount, cur) },
          { name: 'Description', value: description }
        )
        .setFooter({ text: `Checkout ID: ${session.id}` });

      const row = new ActionRowBuilder().addComponents(
        new ButtonBuilder().setLabel('Pay invoice').setStyle(ButtonStyle.Link).setURL(session.url)
      );

      await interaction.editReply({ content: `<@${customer.id}>`, embeds: [embed], components: [row] });
      liveInteractions.set(session.id, { interaction, at: Date.now() });
    } catch (e) {
      await interaction.editReply({ content: `Could not create checkout: ${e.message}`, embeds: [], components: [] });
    }
  }
});

// Start Express web server alongside Discord bot
app.listen(PORT, () => {
  console.log(`Web server listening on port ${PORT}`);
});

client.login(DISCORD_TOKEN);
