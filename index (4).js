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
  ChannelType,
  PermissionFlagsBits,
} = require('discord.js');

const {
  DISCORD_TOKEN,
  STRIPE_SECRET_KEY,
  STRIPE_WEBHOOK_SECRET,
  MONZO_USERNAME = 'sstewart32',
  MONZO_ACCESS_TOKEN = '',
  MONZO_ACCOUNT_ID = '',
  OWNER_ID,
  ALLOWED_USER_IDS = '',
  CURRENCY = 'gbp',
  SUCCESS_URL = 'https://stripe.com',
  LOG_WEBHOOK_URL = '',
  LOG_CHANNEL_ID = '',
  PORT = 3000,
  PUBLIC_URL = '',
  BACKUP_CHANNEL_ID = '',
} = process.env;

const stripe = new Stripe(STRIPE_SECRET_KEY);
const client = new Client({
  intents: [GatewayIntentBits.Guilds, GatewayIntentBits.DirectMessages],
  partials: [Partials.Channel],
});

const allowed = new Set(
  [OWNER_ID, ...ALLOWED_USER_IDS.split(',').map((s) => s.trim())].filter(Boolean)
);

const GREEN = 0x00D26A;
const GREY = 0x95a5a6;
const RED = 0xe74c3c;
const symbols = { gbp: '£', usd: '$', eur: '€' };
const money = (amount, cur) =>
  `${symbols[cur.toLowerCase()] ?? cur.toUpperCase() + ' '}${Number(amount).toFixed(2)}`;
const short = (text, n) => (text && text.length > n ? text.slice(0, n - 1) + '…' : text || '—');

// ---------- Logging & Notifications ----------
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

function whereFrom(interaction, customer) {
  return interaction.guild
    ? { guild: interaction.guild.name, channel: `#${interaction.channel?.name ?? 'unknown'}` }
    : { guild: 'Direct Message', channel: `DM with ${customer.username}` };
}

// ---------- Vault & File Storage ----------
const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, 'data');
const FILES_DIR = path.join(DATA_DIR, 'files');
const DELIVERIES_FILE = path.join(DATA_DIR, 'deliveries.json');
const VAULT_DIR = path.join(DATA_DIR, 'vault');
const VAULT_FILE = path.join(DATA_DIR, 'vault.json');

fs.mkdirSync(VAULT_DIR, { recursive: true });
fs.mkdirSync(FILES_DIR, { recursive: true });

let vault = {};
try {
  vault = JSON.parse(fs.readFileSync(VAULT_FILE, 'utf8'));
} catch {}
const saveVault = () => {
  try {
    fs.writeFileSync(VAULT_FILE, JSON.stringify(vault, null, 2));
  } catch (e) {
    console.error('Could not save vault:', e.message);
  }
};

let deliveries = {};
try {
  deliveries = JSON.parse(fs.readFileSync(DELIVERIES_FILE, 'utf8'));
} catch {}
const saveDeliveries = () => {
  try {
    fs.writeFileSync(DELIVERIES_FILE, JSON.stringify(deliveries, null, 2));
  } catch (e) {
    console.error('Could not save deliveries:', e.message);
  }
};

async function downloadAttachment(att) {
  if (att.size > 50 * 1024 * 1024) throw new Error('That file is over 50 MB.');
  const res = await fetch(att.url);
  if (!res.ok) throw new Error('Could not download the file you attached.');
  return { buffer: Buffer.from(await res.arrayBuffer()), name: att.name };
}

function saveDelivery(key, fileData, interaction, customer, amount, description) {
  const safeName = fileData.name.replace(/[^\w.\-]/g, '_');
  const filePath = path.join(FILES_DIR, `${key}_${safeName}`);
  fs.writeFileSync(filePath, fileData.buffer);
  deliveries[key] = {
    path: filePath,
    name: fileData.name,
    amount: amount.toFixed(2),
    description,
    channelId: interaction.channelId,
    customerId: customer.id,
    agentId: interaction.user.id,
    savedAt: Date.now(),
  };
  saveDeliveries();
}

// ---------- Slash Commands Definition ----------
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
    .setDescription('Create a Stripe payment link for a customer')
    .addUserOption((o) => o.setName('customer').setDescription('Who is paying').setRequired(true))
    .addNumberOption((o) => o.setName('amount').setDescription('Amount to charge').setMinValue(1).setRequired(true))
    .addStringOption((o) => o.setName('description').setDescription('What the purchase is for').setRequired(true))
    .addAttachmentOption((o) => o.setName('file').setDescription('File to send upon payment'))
    .addStringOption((o) => o.setName('vault_file').setDescription('Or pick pre-saved file from vault').setAutocomplete(true))
    .addStringOption((o) => o.setName('currency').setDescription('Currency (default GBP)').addChoices({ name: 'GBP £', value: 'gbp' }, { name: 'USD $', value: 'usd' }, { name: 'EUR €', value: 'eur' }))
);

const monzoCmd = common(
  new SlashCommandBuilder()
    .setName('monzo')
    .setDescription('Create a Monzo.me payment checkout and delivery vault')
    .addUserOption((o) => o.setName('customer').setDescription('Who is paying').setRequired(true))
    .addNumberOption((o) => o.setName('amount').setDescription('Amount in GBP (e.g. 15.00)').setMinValue(0.01).setRequired(true))
    .addStringOption((o) => o.setName('description').setDescription('What the purchase is for').setRequired(true))
    .addAttachmentOption((o) => o.setName('file').setDescription('File to deliver upon Monzo payment'))
    .addStringOption((o) => o.setName('vault_file').setDescription('Or pick pre-saved file from vault').setAutocomplete(true))
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
        .addStringOption((o) => o.setName('name').setDescription('Short key/name for autocomplete').setRequired(true))
    )
    .addSubcommand((sub) => sub.setName('list').setDescription('List all files in your vault'))
    .addSubcommand((sub) =>
      sub
        .setName('remove')
        .setDescription('Remove a file from the vault')
        .addStringOption((o) => o.setName('name').setDescription('Vault key to remove').setAutocomplete(true).setRequired(true))
    )
);

const paymentsCmd = common(new SlashCommandBuilder().setName('payments').setDescription('List recent Stripe payments'));
const totalCmd = common(
  new SlashCommandBuilder()
    .setName('total')
    .setDescription('Show Stripe takings')
    .addStringOption((o) => o.setName('period').setDescription('Period').setRequired(true).addChoices({ name: 'Today', value: 'today' }, { name: 'This week', value: 'week' }, { name: 'This month', value: 'month' }))
);
const statusCmd = common(new SlashCommandBuilder().setName('status').setDescription('Check bot health'));
const setupCmd = new SlashCommandBuilder()
  .setName('setup')
  .setDescription('Create private #bankroll-log channel')
  .setIntegrationTypes([ApplicationIntegrationType.GuildInstall])
  .setContexts([InteractionContextType.Guild]);

const commands = [bankrollCmd, monzoCmd, vaultCmd, paymentsCmd, totalCmd, statusCmd, setupCmd];

let started = false;
const onReady = async () => {
  if (started) return;
  started = true;
  console.log(`Logged in as ${client.user.tag}`);
  try {
    await client.application.commands.set(commands.map((c) => c.toJSON()));
    console.log('Dual Stripe & Monzo Bankroll commands registered');
  } catch (e) {
    console.error('Command registration failed:', e);
  }
  notifyOwner({ content: 'Dual Stripe & Monzo Bankroll bot is online.' }, { log: false });
};
client.once('ready', onReady);
client.once('clientReady', onReady);

// ---------- Stripe Checkout Handler ----------
async function createStripeCheckout(interaction) {
  const customer = interaction.options.getUser('customer');
  const amount = interaction.options.getNumber('amount');
  const description = interaction.options.getString('description');
  const agent = interaction.user;
  const cur = (interaction.options.getString('currency') ?? CURRENCY).toLowerCase();

  await interaction.deferReply();

  try {
    const file = interaction.options.getAttachment('file');
    const vaultKey = interaction.options.getString('vault_file');
    if (file && vaultKey) throw new Error('Use only one of: attached file or vault file.');

    let fileData = null;
    if (file) {
      fileData = await downloadAttachment(file);
    } else if (vaultKey) {
      const item = vault[vaultKey];
      if (!item || !fs.existsSync(item.path)) throw new Error(`Vault item "${vaultKey}" not found.`);
      fileData = { buffer: fs.readFileSync(item.path), name: item.originalName };
    }

    const session = await stripe.checkout.sessions.create({
      mode: 'payment',
      line_items: [{ quantity: 1, price_data: { currency: cur, unit_amount: Math.round(amount * 100), product_data: { name: description } } }],
      success_url: SUCCESS_URL,
      metadata: { customer_id: customer.id, agent_id: agent.id, description: description.slice(0, 400) },
      payment_intent_data: { metadata: { customer_id: customer.id, agent_id: agent.id, description: description.slice(0, 400) } },
    });

    if (fileData) saveDelivery(session.id, fileData, interaction, customer, amount, description);
    const where = whereFrom(interaction, customer);

    const embed = new EmbedBuilder()
      .setColor(GREEN)
      .setTitle('💳 Stripe Checkout Created')
      .setDescription('Please use the link below to complete your card purchase.')
      .addFields(
        { name: 'Sales Agent', value: `<@${agent.id}>`, inline: true },
        { name: 'Customer', value: `<@${customer.id}>`, inline: true },
        { name: 'Amount', value: money(amount, cur), inline: true },
        { name: 'Description', value: description, inline: false }
      )
      .setFooter({ text: `Checkout ID: ${session.id}` })
      .setTimestamp();

    const row = new ActionRowBuilder().addComponents(
      new ButtonBuilder().setLabel('Pay via Stripe').setStyle(ButtonStyle.Link).setURL(session.url)
    );

    await interaction.editReply({ content: `<@${customer.id}>`, embeds: [embed], components: [row] });
    await logToServer({ embeds: [embed] });
  } catch (err) {
    console.error(err);
    await interaction.editReply({ content: `Failed to create Stripe checkout: ${err.message}` });
  }
}

// ---------- Monzo Checkout Handler ----------
async function createMonzoCheckout(interaction) {
  const customer = interaction.options.getUser('customer');
  const amount = interaction.options.getNumber('amount');
  const description = interaction.options.getString('description');
  const agent = interaction.user;

  await interaction.deferReply();

  try {
    const file = interaction.options.getAttachment('file');
    const vaultKey = interaction.options.getString('vault_file');
    if (file && vaultKey) throw new Error('Use only one of: attached file or vault file.');

    let fileData = null;
    if (file) {
      fileData = await downloadAttachment(file);
    } else if (vaultKey) {
      const item = vault[vaultKey];
      if (!item || !fs.existsSync(item.path)) throw new Error(`Vault item "${vaultKey}" not found.`);
      fileData = { buffer: fs.readFileSync(item.path), name: item.originalName };
    }

    const fixedAmount = Number(amount).toFixed(2);
    const paymentLink = `https://monzo.me/${MONZO_USERNAME}/${fixedAmount}`;
    const invoiceId = `monzo_${Date.now()}_${Math.random().toString(36).substring(2, 7)}`;

    if (fileData) {
      saveDelivery(invoiceId, fileData, interaction, customer, amount, description);
    }

    const embed = new EmbedBuilder()
      .setColor(GREEN)
      .setTitle(`💳 Monzo Secure Vault: ${description}`)
      .setDescription(`Click below to pay **${money(amount, 'gbp')}** via Monzo.\n\n*After paying, click **"Check Payment & Unlock"** below to receive your file instantly!*`)
      .addFields(
        { name: 'Sales Agent', value: `<@${agent.id}>`, inline: true },
        { name: 'Customer', value: `<@${customer.id}>`, inline: true },
        { name: 'Amount', value: money(amount, 'gbp'), inline: true },
        { name: 'File Included', value: fileData ? fileData.name : 'None', inline: false }
      )
      .setFooter({ text: `Invoice ID: ${invoiceId}` })
      .setTimestamp();

    const row = new ActionRowBuilder().addComponents(
      new ButtonBuilder().setLabel(`Pay ${money(amount, 'gbp')} via Monzo`).setStyle(ButtonStyle.Link).setURL(paymentLink)
    );

    if (fileData) {
      row.addComponents(
        new ButtonBuilder()
          .setCustomId(`verify_monzo:${invoiceId}:${fixedAmount}`)
          .setLabel('Check Payment & Unlock')
          .setStyle(ButtonStyle.Success)
      );
    }

    await interaction.editReply({ content: `<@${customer.id}>`, embeds: [embed], components: [row] });
    await logToServer({ embeds: [embed] });
  } catch (err) {
    console.error(err);
    await interaction.editReply({ content: `Failed to create Monzo checkout: ${err.message}` });
  }
}

// ---------- Vault Handler ----------
async function cmdVault(interaction) {
  if (!allowed.has(interaction.user.id)) {
    return interaction.reply({ content: 'Only authorized users can manage the vault.', ephemeral: true });
  }
  const sub = interaction.options.getSubcommand();

  if (sub === 'add') {
    await interaction.deferReply({ ephemeral: true });
    const att = interaction.options.getAttachment('file');
    const key = interaction.options.getString('name').toLowerCase().trim().replace(/[^a-z0-9\-_]/g, '-');
    if (vault[key]) return interaction.editReply(`Vault item **${key}** already exists.`);

    const fileData = await downloadAttachment(att);
    const filePath = path.join(VAULT_DIR, `${key}_${fileData.name.replace(/[^\w.\-]/g, '_')}`);
    fs.writeFileSync(filePath, fileData.buffer);

    vault[key] = { path: filePath, originalName: fileData.name, size: att.size, addedAt: Date.now() };
    saveVault();
    return interaction.editReply(`Added **${fileData.name}** to vault as \`${key}\`!`);
  }

  if (sub === 'list') {
    const keys = Object.keys(vault);
    if (!keys.length) return interaction.reply({ content: 'Vault is empty.', ephemeral: true });
    const lines = keys.map((k) => `• \`${k}\` — **${vault[k].originalName}** (${(vault[k].size / (1024 * 1024)).toFixed(1)} MB)`);
    const embed = new EmbedBuilder().setColor(GREEN).setTitle(`Vault Inventory (${keys.length})`).setDescription(lines.join('\n'));
    return interaction.reply({ embeds: [embed], ephemeral: true });
  }

  if (sub === 'remove') {
    await interaction.deferReply({ ephemeral: true });
    const key = interaction.options.getString('name');
    if (!vault[key]) return interaction.editReply(`Vault item \`${key}\` not found.`);
    try { fs.unlinkSync(vault[key].path); } catch {}
    delete vault[key];
    saveVault();
    return interaction.editReply(`Removed \`${key}\` from vault.`);
  }
}

// ---------- Autocomplete ----------
async function onAutocomplete(interaction) {
  try {
    if (!allowed.has(interaction.user.id)) return await interaction.respond([]);
    const typed = interaction.options.getFocused().toLowerCase();
    const choices = Object.entries(vault)
      .filter(([k, v]) => !typed || k.includes(typed) || v.originalName.toLowerCase().includes(typed))
      .slice(0, 25)
      .map(([k, v]) => ({ name: `[Vault] ${k} - ${v.originalName}`, value: k }));
    await interaction.respond(choices);
  } catch {
    try { await interaction.respond([]); } catch {}
  }
}

// ---------- Button Interaction: Monzo Verification ----------
async function onButton(interaction) {
  if (!interaction.customId.startsWith('verify_monzo:')) return;
  const [, invoiceId, targetAmount] = interaction.customId.split(':');
  const d = deliveries[invoiceId];

  if (!d) return interaction.reply({ content: 'Invoice already fulfilled or expired.', ephemeral: true });
  if (interaction.user.id !== d.customerId && !allowed.has(interaction.user.id)) {
    return interaction.reply({ content: 'Only the customer can unlock this file.', ephemeral: true });
  }

  await interaction.deferUpdate();

  try {
    const response = await fetch(`https://api.monzo.com/transactions?account_id=${MONZO_ACCOUNT_ID}&limit=20`, {
      headers: { 'Authorization': `Bearer ${MONZO_ACCESS_TOKEN}` }
    });
    const data = await response.json();
    if (!data.transactions) return interaction.followUp({ content: '❌ Failed to reach Monzo API.', ephemeral: true });

    let paymentFound = false;
    for (const tx of data.transactions) {
      if (tx.amount > 0 && (tx.amount / 100).toFixed(2) === targetAmount) {
        paymentFound = true;
        break;
      }
    }

    if (paymentFound) {
      let buffer = fs.readFileSync(d.path);
      const successEmbed = new EmbedBuilder()
        .setColor(GREEN)
        .setTitle('🎉 Payment Verified - Vault Unlocked!')
        .setDescription(`Payment of **£${targetAmount}** for **${d.description}** confirmed via Monzo.`)
        .setTimestamp();

      await interaction.editReply({ embeds: [successEmbed], components: [] });
      await interaction.followUp({
        content: `<@${d.customerId}> payment confirmed! Here is your file:`,
        files: [new AttachmentBuilder(buffer, { name: d.name })],
      });

      const logEmbed = new EmbedBuilder()
        .setColor(GREEN)
        .setTitle('💰 Monzo Sale Completed')
        .addFields({ name: 'Product', value: d.description, inline: true }, { name: 'Amount', value: `£${targetAmount}`, inline: true }, { name: 'Customer', value: `<@${d.customerId}>`, inline: false })
        .setTimestamp();
      await logToServer({ embeds: [logEmbed] });
      await notifyOwner({ embeds: [logEmbed] });

      try { fs.unlinkSync(d.path); } catch {}
      delete deliveries[invoiceId];
      saveDeliveries();
    } else {
      const waitingEmbed = new EmbedBuilder()
        .setColor(0xF1C40F)
        .setTitle(`⏳ Waiting for Payment (£${targetAmount})...`)
        .setDescription(`No matching payment found yet in Monzo. Wait 10 seconds and click **Check Payment & Unlock** again.`);
      const row = new ActionRowBuilder().addComponents(
        new ButtonBuilder().setLabel(`Pay £${targetAmount} via Monzo`).setStyle(ButtonStyle.Link).setURL(`https://monzo.me/${MONZO_USERNAME}/${targetAmount}`),
        new ButtonBuilder().setCustomId(`verify_monzo:${invoiceId}:${targetAmount}`).setLabel('Check Payment & Unlock').setStyle(ButtonStyle.Success)
      );
      await interaction.editReply({ embeds: [waitingEmbed], components: [row] });
    }
  } catch (err) {
    console.error(err);
    await interaction.followUp({ content: '⚠️ Error verifying Monzo transaction.', ephemeral: true });
  }
}

// ---------- Router ----------
client.on('interactionCreate', async (interaction) => {
  try {
    if (interaction.isAutocomplete()) return await onAutocomplete(interaction);
    if (interaction.isButton()) return await onButton(interaction);
    if (!interaction.isChatInputCommand()) return;

    if (!allowed.has(interaction.user.id)) {
      return interaction.reply({ content: "You aren't allowed to use this command.", ephemeral: true });
    }

    switch (interaction.commandName) {
      case 'bankroll':
        return await createStripeCheckout(interaction);
      case 'monzo':
        return await createMonzoCheckout(interaction);
      case 'vault':
        return await cmdVault(interaction);
      case 'status':
        await interaction.deferReply({ ephemeral: true });
        const up = Math.floor(process.uptime());
        const embed = new EmbedBuilder()
          .setColor(GREEN)
          .setTitle('Bot Status (Stripe & Monzo)')
          .addFields({ name: 'Uptime', value: `${Math.floor(up / 3600)}h`, inline: true }, { name: 'Active Deliveries', value: String(Object.keys(deliveries).length), inline: true }, { name: 'Vault Items', value: String(Object.keys(vault).length), inline: true });
        return interaction.editReply({ embeds: [embed] });
      case 'setup':
        if (interaction.user.id !== OWNER_ID) return interaction.reply({ content: 'Owner only.', ephemeral: true });
        await interaction.deferReply({ ephemeral: true });
        const ch = await interaction.guild.channels.create({ name: 'bankroll-log', type: ChannelType.GuildText, topic: 'Sales logs' });
        return interaction.editReply(`Created <#${ch.id}>.`);
    }
  } catch (err) {
    alertOwner(`/${interaction.commandName} failed`, err);
  }
});

// ---------- Stripe Webhook Server ----------
const app = express();
app.post('/webhook', express.raw({ type: 'application/json' }), async (req, res) => {
  let event;
  try {
    event = stripe.webhooks.constructEvent(req.body, req.headers['stripe-signature'], STRIPE_WEBHOOK_SECRET);
  } catch (err) {
    return res.status(400).send(`Webhook Error: ${err.message}`);
  }

  if (event.type === 'checkout.session.completed') {
    const s = event.data.object;
    if (s.payment_status === 'paid') {
      const d = deliveries[s.id];
      if (d) {
        let buffer = null;
        try { buffer = fs.readFileSync(d.path); } catch {}
        if (buffer) {
          try {
            const user = await client.users.fetch(d.customerId);
            await user.send({ content: `<@${d.customerId}> Stripe payment confirmed! Here is your file:`, files: [new AttachmentBuilder(buffer, { name: d.name })] });
          } catch {}
          try { fs.unlinkSync(d.path); } catch {}
        }
        delete deliveries[s.id];
        saveDeliveries();
      }
    }
  }
  res.json({ received: true });
});

app.get('/', (_, res) => res.send('Bankroll dual bot running'));
app.listen(PORT, () => console.log(`Server listening on port ${PORT}`));

client.login(DISCORD_TOKEN);
