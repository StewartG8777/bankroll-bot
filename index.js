require('dotenv').config();
const fs = require('fs');
const path = path = require('path');
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
} = process.env;

const stripe = new Stripe(STRIPE_SECRET_KEY);
const client = new Client({
  intents: [GatewayIntentBits.Guilds, GatewayIntentBits.DirectMessages],
  partials: [Partials.Channel],
});

const allowed = new Set(
  [OWNER_ID, ...ALLOWED_USER_IDS.split(',').map((s) => s.trim())].filter(Boolean)
);

const GREEN = 0x2ecc40;
const GREY = 0x95a5a6;
const symbols = { gbp: '£', usd: '$', eur: '€' };
const money = (amount, cur) =>
  `${symbols[cur.toLowerCase()] ?? cur.toUpperCase() + ' '}${amount.toFixed(2)}`;

// ---------- Express Server for Webhooks ----------
const app = express();
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

app.get('/', (req, res) => res.send('Bot and Webhooks are active!'));

// --- Monzo Webhook Listener ---
app.post('/monzo-webhook', async (req, res) => {
  try {
    const data = req.body?.data;
    if (req.body?.type === 'transaction.create' && data) {
      const amountPaid = data.amount / 100; // Monzo handles amounts in pence/cents
      const description = (data.description || '').toLowerCase();
      const notes = (data.notes || '').toLowerCase();
      const referenceText = `${description} ${notes}`;

      for (const [key, delivery] of Object.entries(deliveries)) {
        if (delivery.type === 'monzo' && !delivery.paid) {
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

// --- Stripe Webhook Listener ---
app.post('/webhook', express.raw({ type: 'application/json' }), async (req, res) => {
  const sig = req.headers['stripe-signature'];
  let event;
  try {
    event = stripe.webhooks.constructEvent(req.body, sig, STRIPE_WEBHOOK_SECRET);
  } catch (err) {
    return res.status(400).send(`Webhook Error: ${err.message}`);
  }

  if (event.type === 'checkout.session.completed') {
    const session = event.data.object;
    await deliverFile(session.id, session.payment_intent);
  }
  res.json({ received: true });
});

// ---------- Data Storage ----------
const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, 'data');
const FILES_DIR = path.join(DATA_DIR, 'files');
const DELIVERIES_FILE = path.join(DATA_DIR, 'deliveries.json');
const BASE_URL = PUBLIC_URL.replace(/\/+$/, '');
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

const liveInteractions = new Map();

async function downloadAttachment(att) {
  if (att.size > 100 * 1024 * 1024) throw new Error('File is too large.');
  const res = await fetch(att.url);
  if (!res.ok) throw new Error('Could not download attached file.');
  return { buffer: Buffer.from(await res.arrayBuffer()), name: att.name };
}

function saveDelivery(key, fileData, interaction, customer, type = 'stripe', amount = 0) {
  const safeName = fileData ? fileData.name.replace(/[^\w.\-]/g, '_') : 'link';
  const filePath = fileData ? path.join(FILES_DIR, `${key}_${safeName}`) : null;
  if (fileData) fs.writeFileSync(filePath, fileData.buffer);

  deliveries[key] = {
    type,
    amount,
    paid: false,
    path: filePath,
    name: fileData ? fileData.name : 'download link',
    link: interaction.options.getString('link') || null,
    description: interaction.options.getString('description') ?? '',
    channelId: interaction.channelId,
    customerId: customer.id,
    agentId: interaction.user.id,
    savedAt: Date.now(),
  };
  saveDeliveries();
}

async function deliverFile(key) {
  const d = deliveries[key];
  if (!d) return;

  let buffer = null;
  if (d.path && fs.existsSync(d.path)) {
    try { buffer = fs.readFileSync(d.path); } catch {}
  }

  const payload = buffer
    ? { content: `<@${d.customerId}> payment received! Here is your file:`, files: [new AttachmentBuilder(buffer, { name: d.name })] }
    : { content: `<@${d.customerId}> payment received! Here is your link:\n<${d.link}>` };

  const live = liveInteractions.get(key);
  if (live) {
    try {
      await live.interaction.followUp(payload);
    } catch {}
  } else {
    try {
      const ch = await client.channels.fetch(d.channelId);
      await ch.send(payload);
    } catch {
      const u = await client.users.fetch(d.customerId);
      await u.send(payload).catch(() => {});
    }
  }

  if (d.path) { try { fs.unlinkSync(d.path); } catch {} }
  delete deliveries[key];
  saveDeliveries();
}

// ---------- Slash Commands Definition ----------
const common = (b) => b
  .setIntegrationTypes([ApplicationIntegrationType.GuildInstall, ApplicationIntegrationType.UserInstall])
  .setContexts([InteractionContextType.Guild, InteractionContextType.BotDM, InteractionContextType.PrivateChannel]);

const bankrollCmd = common(
  new SlashCommandBuilder()
    .setName('bankroll')
    .setDescription('Create a Stripe payment link')
    .addUserOption((o) => o.setName('customer').setDescription('Who is paying').setRequired(true))
    .addNumberOption((o) => o.setName('amount').setDescription('Amount').setMinValue(1).setRequired(true))
    .addStringOption((o) => o.setName('description').setDescription('Description').setRequired(true))
);

const monzoInvoiceCmd = common(
  new SlashCommandBuilder()
    .setName('monzo')
    .setDescription('Create a manual Monzo bank transfer invoice')
    .addUserOption((o) => o.setName('customer').setDescription('Who is paying').setRequired(true))
    .addNumberOption((o) => o.setName('amount').setDescription('Amount to charge').setMinValue(1).setRequired(true))
    .addStringOption((o) => o.setName('description').setDescription('What the purchase is for').setRequired(true))
    .addAttachmentOption((o) => o.setName('file').setDescription('File to send once paid'))
    .addStringOption((o) => o.setName('link').setDescription('Link to send once paid'))
);

const commands = [bankrollCmd, monzoInvoiceCmd];

// ---------- Handling Interactions ----------
client.on('interactionCreate', async (interaction) => {
  if (!interaction.isChatInputCommand()) return;

  if (interaction.commandName === 'monzo') {
    const customer = interaction.options.getUser('customer');
    const amount = interaction.options.getNumber('amount');
    const description = interaction.options.getString('description');
    const file = interaction.options.getAttachment('file');
    const invoiceKey = `DISCORD-${Math.floor(1000 + Math.random() * 9000)}`;

    await interaction.deferReply();
    const fileData = file ? await downloadAttachment(file) : null;
    
    saveDelivery(invoiceKey, fileData, interaction, customer, 'monzo', amount);

    const embed = new EmbedBuilder()
      .setColor(GREEN)
      .setTitle('Monzo Bank Transfer Invoice')
      .setDescription(`Please send a bank transfer using your Monzo app with the exact details below.`)
      .addFields(
        { name: 'Amount to Send', value: `£${amount.toFixed(2)}` },
        { name: 'Reference / Notes Required', value: `\`${invoiceKey}\`` },
        { name: 'Description', value: description },
        { name: 'Status', value: '⏳ Waiting for incoming payment match...' }
      )
      .setFooter({ text: `Invoice Ref: ${invoiceKey}` });

    await interaction.editReply({ content: `<@${customer.id}>`, embeds: [embed] });
    liveInteractions.set(invoiceKey, { interaction, at: Date.now() });
  }
});

// ---------- Bot Startup ----------
let started = false;
client.once('ready', async () => {
  if (started) return;
  started = true;
  console.log(`Logged in as ${client.user.tag}`);
  try {
    await client.application.commands.set(commands.map((c) => c.toJSON()));
    console.log('Slash commands registered');
  } catch (e) {
    console.error('Command registration failed:', e);
  }
});

app.listen(PORT, () => {
  console.log(`Web server listening on port ${PORT}`);
});

client.login(DISCORD_TOKEN);
