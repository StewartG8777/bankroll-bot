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
  OWNER_ID,
  ALLOWED_USER_IDS = '',
  CURRENCY = 'gbp',
  SUCCESS_URL = 'https://stripe.com',
  LOG_WEBHOOK_URL = '',
  LOG_CHANNEL_ID = '',
  PORT = 3000,
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
// Files are never posted there, and mentions never ping.
async function getLogChannel() {
  if (LOG_CHANNEL_ID) return client.channels.fetch(LOG_CHANNEL_ID).catch(() => null);
  for (const g of client.guilds.cache.values()) {
    const c = g.channels.cache.find((x) => x.name === 'bankroll-log' && x.isTextBased());
    if (c) return c;
  }
  return null;
}

async function logToServer(payload) {
  // Preferred: the bot posts in the #bankroll-log channel itself
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
  // Fallback: a channel webhook, if you set LOG_WEBHOOK_URL
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

// DMs the owner and also logs it in the server
async function notifyOwner(payload, { log = true } = {}) {
  try {
    const owner = await client.users.fetch(OWNER_ID);
    await owner.send(payload);
  } catch (e) {
    console.error('Owner DM failed:', e.message);
  }
  if (log) await logToServer({ content: payload.content, embeds: payload.embeds });
}

// ---------- Helpers ----------

function whereFrom(interaction, customer) {
  return interaction.guild
    ? { guild: interaction.guild.name, channel: `#${interaction.channel?.name ?? 'unknown'}` }
    : { guild: 'Direct Message', channel: `DM with ${customer.username}` };
}

// Midnight in UK time, so "today / this week / this month" match your day
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
    d -= (dow + 6) % 7; // Monday start
  }
  if (period === 'month') d = 1;
  const guess = Date.UTC(p.y, p.m - 1, d);
  return Math.floor((guess - londonOffset(guess)) / 1000);
}

// ---------- File delivery (file is sent after payment) ----------
// Files are kept in ./data on the server. On Render's free plan this folder is
// wiped on every redeploy, so don't redeploy while a paid-for file is pending.
const DATA_DIR = path.join(__dirname, 'data');
const FILES_DIR = path.join(DATA_DIR, 'files');
const DELIVERIES_FILE = path.join(DATA_DIR, 'deliveries.json');
const MAX_FILE_BYTES = 8 * 1024 * 1024;
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

// Clean up files that were never paid for after 7 days
for (const [k, d] of Object.entries(deliveries)) {
  if (Date.now() - d.savedAt > 7 * 24 * 60 * 60 * 1000) {
    try {
      fs.unlinkSync(d.path);
    } catch {}
    delete deliveries[k];
  }
}
saveDeliveries();

// Keeps the original /invoice interaction for 14 minutes, so the bot can reply under it
const liveInteractions = new Map();

async function downloadAttachment(att) {
  if (att.size > MAX_FILE_BYTES) throw new Error('That file is over 8 MB. Please use a smaller one.');
  const res = await fetch(att.url);
  if (!res.ok) throw new Error('Could not download the file you attached.');
  return { buffer: Buffer.from(await res.arrayBuffer()), name: att.name };
}

function saveDelivery(key, fileData, interaction, customer) {
  const safeName = fileData.name.replace(/[^\w.\-]/g, '_');
  const filePath = path.join(FILES_DIR, `${key.replace(/[^\w\-]/g, '_')}_${safeName}`);
  fs.writeFileSync(filePath, fileData.buffer);
  deliveries[key] = {
    path: filePath,
    name: fileData.name,
    channelId: interaction.channelId,
    customerId: customer.id,
    agentId: interaction.user.id,
    savedAt: Date.now(),
  };
  saveDeliveries();
}

// Throws away a file that is waiting to be delivered
function discardPending(key) {
  const d = deliveries[key];
  if (!d) return false;
  try {
    fs.unlinkSync(d.path);
  } catch {}
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

  let buffer;
  try {
    buffer = fs.readFileSync(d.path);
  } catch {
    if (owner) {
      await owner
        .send(`Payment received for ${key}, but the file "${d.name}" is gone (the server was restarted). Please send it manually to <@${d.customerId}>.`)
        .catch(() => {});
    }
    delete deliveries[key];
    saveDeliveries();
    return;
  }

  const payload = () => ({
    content: `<@${d.customerId}> payment received. Here is the file for this purchase.`,
    files: [new AttachmentBuilder(buffer, { name: d.name })],
  });

  let sentTo = null;
  let ownerGotIt = false;
  let msgUrl = null;
  let reachedCustomer = false;

  // 0. Reply under the invoice message (Discord only allows this for 15 minutes after it was created)
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

  // 1. Where the invoice was created (works in servers the bot is in)
  if (!sentTo) try {
    const ch = await client.channels.fetch(d.channelId);
    const msg = await ch.send(payload());
    msgUrl = msg.url;
    reachedCustomer = true;
    sentTo = 'the channel where the invoice was created';
  } catch (e) {
    console.log('Could not post in original channel:', e.message);
  }

  // 2. The customer's DMs
  if (!sentTo) {
    try {
      const u = await client.users.fetch(d.customerId);
      const msg = await u.send(payload());
      msgUrl = msg.url;
      reachedCustomer = true;
      sentTo = "the customer's DMs (the bot can't post in other people's private DMs)";
    } catch (e) {
      console.log('Could not DM customer:', e.message);
    }
  }

  // 3. Last resort: you
  if (!sentTo && owner) {
    try {
      await owner.send({
        content: `Couldn't reach <@${d.customerId}>. Here is their file, please send it to them.`,
        files: [new AttachmentBuilder(buffer, { name: d.name })],
      });
      sentTo = 'your DMs, because the customer could not be reached';
      ownerGotIt = true;
    } catch (e) {
      console.error('Could not deliver file anywhere:', e.message);
    }
  }

  if (sentTo) {
    try {
      fs.unlinkSync(d.path);
    } catch {}
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
      await notifyOwner({ content: `File "${d.name}" was sent to ${sentTo}.` });
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
    .setDescription('Create a payment link for a customer')
    .addUserOption((o) => o.setName('customer').setDescription('Who is paying').setRequired(true))
    .addNumberOption((o) =>
      o.setName('amount').setDescription('Amount to charge').setMinValue(1).setRequired(true)
    )
    .addStringOption((o) =>
      o.setName('description').setDescription('What the purchase is for').setRequired(true)
    )
    .addAttachmentOption((o) =>
      o.setName('file').setDescription('File to send the customer once they have paid (max 8 MB)')
    )
);

const invoiceCmd = common(
  new SlashCommandBuilder()
    .setName('invoice')
    .setDescription('Create a card payment link that expires, with reminders')
    .addUserOption((o) => o.setName('customer').setDescription('Who is paying').setRequired(true))
    .addNumberOption((o) =>
      o.setName('amount').setDescription('Amount to charge').setMinValue(1).setRequired(true)
    )
    .addStringOption((o) =>
      o.setName('description').setDescription('What the purchase is for').setRequired(true)
    )
    .addAttachmentOption((o) =>
      o.setName('file').setDescription('File to send the customer once they have paid (max 8 MB)')
    )
    .addStringOption((o) =>
      o
        .setName('expires')
        .setDescription('How long the link works (default: 24 hours)')
        .addChoices(
          { name: '30 minutes', value: '30' },
          { name: '1 hour', value: '60' },
          { name: '6 hours', value: '360' },
          { name: '24 hours', value: '1440' }
        )
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

const commands = [bankrollCmd, invoiceCmd, paymentsCmd, totalCmd, customerCmd, historyCmd, refundCmd, setupCmd];

// Works on both older ('ready') and newer ('clientReady') discord.js versions
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
};
client.once('ready', onReady);
client.once('clientReady', onReady);

// ---------- Reminders ----------
// Sent at 25%, 50% and 75% of the time the link is valid.
// Note: these timers live in memory, so a restart/redeploy cancels them.
function scheduleReminders({ customerId, sessionId, url, amount, description, expiresAt }) {
  const total = expiresAt * 1000 - Date.now();
  [0.25, 0.5, 0.75].forEach((fraction, i) => {
    setTimeout(async () => {
      try {
        const s = await stripe.checkout.sessions.retrieve(sessionId);
        if (s.status !== 'open') return; // already paid or expired
        const user = await client.users.fetch(customerId);
        const embed = new EmbedBuilder()
          .setColor(GREEN)
          .setTitle('Payment reminder')
          .setDescription(
            `You have an unpaid invoice for **${money(amount, CURRENCY)}** (${description}).\n` +
              `The link expires <t:${expiresAt}:R>.`
          );
        const row = new ActionRowBuilder().addComponents(
          new ButtonBuilder().setLabel('Pay invoice').setStyle(ButtonStyle.Link).setURL(url)
        );
        await user.send({ embeds: [embed], components: [row] });
      } catch (e) {
        console.error('Reminder failed:', e.message);
        if (i === 0) {
          try {
            const owner = await client.users.fetch(OWNER_ID);
            await owner.send(
              `Couldn't send a payment reminder to <@${customerId}> (they may not allow DMs from the bot).`
            );
          } catch {}
        }
      }
    }, Math.max(total * fraction, 1000));
  });
}

// ---------- Stripe checkout (used by /bankroll and /invoice) ----------
async function sendStripeCheckout(interaction, { expiresMins = null, kind }) {
  const customer = interaction.options.getUser('customer');
  const amount = interaction.options.getNumber('amount');
  const description = interaction.options.getString('description');
  const agent = interaction.user;

  await interaction.deferReply();

  try {
    const file = interaction.options.getAttachment('file');
    const fileData = file ? await downloadAttachment(file) : null;

    // Evidence record, stored inside Stripe so it survives restarts and redeploys
    const record = {
      created_at: new Date().toISOString(),
      created_in: (interaction.guild
        ? `${interaction.guild.name} #${interaction.channel?.name ?? 'unknown'}`
        : 'Direct message'
      ).slice(0, 200),
      channel_id: String(interaction.channelId),
      ...(fileData
        ? {
            file_name: fileData.name.slice(0, 100),
            file_sha256: crypto.createHash('sha256').update(fileData.buffer).digest('hex'),
          }
        : {}),
    };

    const params = {
      mode: 'payment',
      line_items: [
        {
          quantity: 1,
          price_data: {
            currency: CURRENCY,
            unit_amount: Math.round(amount * 100),
            product_data: { name: description },
          },
        },
      ],
      success_url: SUCCESS_URL,
      metadata: { customer_id: customer.id, agent_id: agent.id, description, ...record },
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
      if (expiresMins === 30) secs += 60; // Stripe needs at least 30 min
      if (expiresMins === 1440) secs -= 60; // and at most 24 h
      expiresAt = Math.floor(Date.now() / 1000) + secs;
      params.expires_at = expiresAt;
    }

    const session = await stripe.checkout.sessions.create(params);
    if (fileData) saveDelivery(session.id, fileData, interaction, customer);
    const where = whereFrom(interaction, customer);

    const embed = new EmbedBuilder()
      .setColor(GREEN)
      .setTitle(kind === 'invoice' ? 'Bankroll Invoice Created' : 'Bankroll Checkout Created')
      .setDescription('Please use the link below to complete your purchase.')
      .addFields(
        { name: 'Sales Agent', value: `<@${agent.id}>` },
        { name: 'Customer', value: `<@${customer.id}>` },
        { name: 'Amount', value: money(amount, CURRENCY) },
        { name: 'Description', value: description }
      );
    if (expiresAt) {
      embed.addFields({ name: 'Pay by', value: `<t:${expiresAt}:R> (<t:${expiresAt}:f>)` });
    }
    if (fileData) {
      embed.addFields({ name: 'File', value: `${fileData.name} (sent here after payment, or press Get my file)` });
    }
    embed
      .addFields(
        { name: 'Generated In Guild', value: where.guild },
        { name: 'Generated In Channel', value: where.channel }
      )
      .setFooter({ text: `Checkout created by ${agent.username} | Checkout ID: ${session.id}` });

    const row = new ActionRowBuilder().addComponents(
      new ButtonBuilder().setLabel('Pay invoice').setStyle(ButtonStyle.Link).setURL(session.url)
    );
    if (fileData) {
      row.addComponents(
        new ButtonBuilder()
          .setCustomId(`getfile:${session.id}`)
          .setLabel('Get my file')
          .setStyle(ButtonStyle.Secondary)
      );
    }

    await interaction.editReply({ content: `<@${customer.id}>`, embeds: [embed], components: [row] });
    await logToServer({
      embeds: [embed],
      files: fileData ? [{ buffer: fileData.buffer, name: fileData.name }] : undefined,
    });
    if (fileData) {
      liveInteractions.set(session.id, { interaction, at: Date.now() });
      setTimeout(() => liveInteractions.delete(session.id), 14 * 60 * 1000);
    }

    if (expiresAt) {
      scheduleReminders({
        customerId: customer.id,
        sessionId: session.id,
        url: session.url,
        amount,
        description,
        expiresAt,
      });
    }
  } catch (err) {
    console.error(err);
    await interaction.editReply({ content: `Failed to create checkout: ${err.message}` });
  }
}

// ---------- Stats commands (read from Stripe, so nothing is lost on restart) ----------
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
  const list = await succeededPayments(since);

  const sum = list.reduce((a, pi) => a + pi.amount_received, 0) / 100;

  // Stripe fees and refunds, so you can see what you actually keep
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

// ---------- /setup: creates the private log channel ----------
async function cmdSetup(interaction) {
  if (interaction.user.id !== OWNER_ID) {
    return interaction.reply({ content: 'Only the owner can run this.', ephemeral: true });
  }
  if (!interaction.guild) {
    return interaction.reply({ content: 'Run this inside your server, not in a DM.', ephemeral: true });
  }
  await interaction.deferReply({ ephemeral: true });

  const guild = interaction.guild;
  const existing = guild.channels.cache.find((c) => c.name === 'bankroll-log' && c.isTextBased());
  if (existing) {
    return interaction.editReply(`The log channel already exists: <#${existing.id}>. Logs will go there.`);
  }

  const view = [PermissionFlagsBits.ViewChannel, PermissionFlagsBits.ReadMessageHistory];
  const overwrites = [
    { id: guild.roles.everyone.id, deny: [PermissionFlagsBits.ViewChannel] },
    {
      id: client.user.id,
      allow: [...view, PermissionFlagsBits.SendMessages, PermissionFlagsBits.EmbedLinks],
    },
  ];
  // You and anyone on your allowed list who is in this server can read it
  const seen = new Set();
  for (const id of allowed) {
    if (seen.has(id)) continue;
    seen.add(id);
    const member = await guild.members.fetch(id).catch(() => null);
    if (member) overwrites.push({ id, allow: view });
  }

  try {
    const ch = await guild.channels.create({
      name: 'bankroll-log',
      type: ChannelType.GuildText,
      topic: 'Private log of Bankroll invoices, payments, refunds and disputes',
      permissionOverwrites: overwrites,
    });
    await ch.send({ content: 'Log channel ready. Invoices, payments, refunds and disputes will appear here.' });
    return interaction.editReply(`Created <#${ch.id}>. Only you and your allowed users can see it.`);
  } catch (e) {
    return interaction.editReply(
      `I couldn't create the channel: ${e.message}. The bot needs the **Manage Channels** permission in this server.`
    );
  }
}

// ---------- Refunds (owner only, with a confirm step) ----------
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
    if (!piId) throw new Error('That checkout has not been paid, so there is nothing to refund.');
    return stripe.paymentIntents.retrieve(piId, expand);
  }
  if (id.startsWith('pi_')) return stripe.paymentIntents.retrieve(id, expand);
  throw new Error('Pick a payment from the list, or paste a Checkout ID (cs_...) or payment ID (pi_...).');
}

async function onAutocomplete(interaction) {
  try {
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
    return interaction.editReply('That payment was not completed, so there is nothing to refund.');
  }
  if (charge.disputed) {
    return interaction.editReply('That payment is under dispute. Handle it from the dispute in Stripe instead of refunding.');
  }

  const refundable = charge.amount - charge.amount_refunded;
  if (refundable <= 0) return interaction.editReply('That payment has already been fully refunded.');

  const wanted = interaction.options.getNumber('amount');
  const pence = wanted ? Math.round(wanted * 100) : refundable;
  if (pence > refundable) {
    return interaction.editReply(`You can refund at most ${money(refundable / 100, pi.currency)} on that payment.`);
  }

  const code = interaction.options.getString('reason') ?? 'r';
  const m = pi.metadata ?? {};
  const embed = new EmbedBuilder()
    .setColor(RED)
    .setTitle('Confirm refund')
    .setDescription(pence === refundable ? 'This refunds everything left on the payment.' : 'This is a partial refund.')
    .addFields(
      { name: 'Refund amount', value: money(pence / 100, pi.currency), inline: true },
      { name: 'Originally paid', value: money(pi.amount_received / 100, pi.currency), inline: true },
      { name: 'Reason', value: REFUND_REASONS[code].label, inline: true },
      { name: 'Customer', value: m.customer_id ? `<@${m.customer_id}>` : 'Unknown', inline: true },
      { name: 'Description', value: short(m.description, 200) },
      { name: 'Payment ID', value: pi.id }
    )
    .setFooter({ text: "Refunds can't be undone. Stripe keeps its processing fee. A file already sent can't be taken back." });

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
      { idempotencyKey: `refund-${interaction.message.id}` } // a double click can't refund twice
    );

    await recordInStripe(piId, {
      last_refund_at: new Date().toISOString(),
      last_refund_amount: (pence / 100).toFixed(2),
      last_refund_reason: reason.api,
    });

    // Look at the payment again: who paid, and was it refunded in full?
    const pi = await stripe.paymentIntents.retrieve(piId, { expand: ['latest_charge'] });
    const m = pi.metadata ?? {};
    const fullyRefunded = !!(pi.latest_charge && typeof pi.latest_charge === 'object' && pi.latest_charge.refunded);

    // Full refund: a file that hasn't been handed over yet is deleted
    let fileNote = 'No file attached';
    if (fullyRefunded) {
      const sess = await stripe.checkout.sessions.list({ payment_intent: piId, limit: 1 });
      const sid = sess.data[0]?.id;
      fileNote = sid && discardPending(sid) ? 'Pending file deleted, so it will not be sent' : 'No pending file';
    } else if (m.file_name) {
      fileNote = 'Partial refund, so the file can still be sent';
    }

    // Tell the customer
    let customerNote = 'No customer on record';
    if (m.customer_id) {
      try {
        const u = await client.users.fetch(m.customer_id);
        await u.send({
          embeds: [
            new EmbedBuilder()
              .setColor(GREEN)
              .setTitle('Refund issued')
              .setDescription(
                `You've been refunded **${money(refund.amount / 100, refund.currency)}** for **${short(m.description, 100)}**.\n` +
                  'It usually shows in your account within 5 to 10 days.'
              ),
          ],
        });
        customerNote = 'Customer was told by DM';
      } catch {
        customerNote = "Couldn't DM the customer (their DMs may be closed). Please tell them yourself.";
      }
    }

    const done = new EmbedBuilder()
      .setColor(GREEN)
      .setTitle('Refund sent')
      .addFields(
        { name: 'Amount', value: money(refund.amount / 100, refund.currency), inline: true },
        { name: 'Status', value: refund.status, inline: true },
        { name: 'Reason', value: reason.label, inline: true },
        { name: 'Customer', value: m.customer_id ? `<@${m.customer_id}>` : 'Unknown', inline: true },
        { name: 'Refunded by', value: `<@${interaction.user.id}>`, inline: true },
        { name: 'Order', value: short(m.description, 200) },
        { name: 'Customer notice', value: customerNote },
        { name: 'File', value: fileNote },
        { name: 'Refund ID', value: refund.id }
      )
      .setFooter({ text: 'The customer usually sees it in 5 to 10 days.' })
      .setTimestamp();
    await interaction.editReply({ content: '', embeds: [done], components: [] });
    await logToServer({ embeds: [done] });
  } catch (err) {
    console.error('Refund failed:', err);
    await interaction.editReply({ content: `Refund failed: ${err.message}`, embeds: [], components: [] });
  }
}

// ---------- Evidence record + dispute alerts ----------
async function recordInStripe(paymentIntentId, fields) {
  if (!paymentIntentId) return;
  try {
    await stripe.paymentIntents.update(paymentIntentId, { metadata: fields });
  } catch (e) {
    console.error('Could not save record to Stripe:', e.message);
  }
}

// Keeps the message's other buttons (like Pay invoice), removes Get my file
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
      content: 'The file was already sent, or is no longer available. Please contact the seller.',
      ephemeral: true,
    });
  }
  if (interaction.user.id !== d.customerId && !allowed.has(interaction.user.id)) {
    return interaction.reply({ content: 'Only the customer can get this file.', ephemeral: true });
  }

  const session = await stripe.checkout.sessions.retrieve(key);
  if (session.payment_status !== 'paid') {
    return interaction.reply({
      content:
        session.status === 'expired'
          ? 'This invoice has expired. Please ask the seller for a new one.'
          : 'Payment has not been received yet. Pay first, then press this button again.',
      ephemeral: true,
    });
  }

  // Never hand over the file for a payment that was fully refunded
  const checkPi = typeof session.payment_intent === 'string' ? session.payment_intent : session.payment_intent?.id;
  if (checkPi) {
    const pi = await stripe.paymentIntents.retrieve(checkPi, { expand: ['latest_charge'] });
    if (pi.latest_charge && typeof pi.latest_charge === 'object' && pi.latest_charge.refunded) {
      discardPending(key);
      await logToServer({
        content: `Blocked file "${d.name}": <@${d.customerId}> tried to get it after the payment was refunded.`,
      });
      return interaction.reply({
        content: 'This payment was refunded, so the file can no longer be sent.',
        ephemeral: true,
      });
    }
  }

  let buffer;
  try {
    buffer = fs.readFileSync(d.path);
  } catch {
    await notifyOwner({
      content: `<@${d.customerId}> paid but their file "${d.name}" is gone (server restarted). Please send it manually.`,
    });
    delete deliveries[key];
    saveDeliveries();
    return interaction.reply({
      content: 'Sorry, the file is no longer available. The seller has been told and will send it to you.',
      ephemeral: true,
    });
  }

  // Turn the invoice into a PAID message with no buttons, then post the file under it
  const old = interaction.message.embeds[0];
  await interaction.update({
    embeds: old
      ? [EmbedBuilder.from(old).setColor(GREY).setTitle(old.title.startsWith('PAID') ? old.title : `PAID - ${old.title}`)]
      : [],
    components: [],
  });
  const msg = await interaction.followUp({
    content: `<@${d.customerId}> here is the file for this purchase.`,
    files: [new AttachmentBuilder(buffer, { name: d.name })],
  });

  try {
    fs.unlinkSync(d.path);
  } catch {}
  delete deliveries[key];
  saveDeliveries();
  liveInteractions.delete(key);

  const piId = typeof session.payment_intent === 'string' ? session.payment_intent : session.payment_intent?.id;
  await recordInStripe(piId, {
    delivered_at: new Date().toISOString(),
    delivered_to: 'a reply under the invoice message (customer pressed Get my file)',
    delivered_file: d.name.slice(0, 100),
    delivery_message: msg.url ?? '',
  });

  await notifyOwner({ content: `File "${d.name}" was sent to <@${d.customerId}> under their invoice.` });
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
    value: `<t:${Math.floor(now.getTime() / 1000)}:f> by <@${interaction.user.id}>`,
  });
  await interaction.update({ embeds: [updated], components: [] });
}

async function onDisputeCreated(d) {
  const piId = typeof d.payment_intent === 'string' ? d.payment_intent : d.payment_intent?.id;
  const pi = piId ? await stripe.paymentIntents.retrieve(piId) : null;
  const m = pi?.metadata ?? {};
  const dueBy = d.evidence_details?.due_by;
  const dash = `https://dashboard.stripe.com${d.livemode ? '' : '/test'}/disputes/${d.id}`;

  const when = (iso) => {
    const t = iso ? Math.floor(new Date(iso).getTime() / 1000) : null;
    return t ? `<t:${t}:f>` : 'Not recorded';
  };

  const embed = new EmbedBuilder()
    .setColor(RED)
    .setTitle('DISPUTE OPENED')
    .setDescription(
      `A customer disputed a payment. **Evidence is due ${dueBy ? `<t:${dueBy}:R> (<t:${dueBy}:f>)` : 'soon'}.**`
    )
    .addFields(
      { name: 'Amount', value: money(d.amount / 100, d.currency), inline: true },
      { name: 'Reason', value: String(d.reason ?? 'unknown'), inline: true },
      { name: 'Dispute ID', value: d.id, inline: true },
      { name: 'Customer', value: m.customer_id ? `<@${m.customer_id}>\n(${m.customer_id})` : 'Unknown', inline: true },
      { name: 'Sales Agent', value: m.agent_id ? `<@${m.agent_id}>` : 'Unknown', inline: true },
      { name: 'Description', value: short(m.description, 200) },
      { name: 'Order created', value: `${when(m.created_at)}\n${m.created_in ?? ''}`.trim() },
      { name: 'Paid', value: when(m.paid_at), inline: true },
      {
        name: 'Delivered',
        value: m.delivered_at
          ? `${when(m.delivered_at)}\n${m.delivered_to ?? ''}${m.delivery_message ? `\n${m.delivery_message}` : ''}`.slice(0, 1000)
          : '**Nothing recorded**',
        inline: true,
      },
      { name: 'Open in Stripe', value: dash }
    );

  // Save a DRAFT of the evidence in Stripe (nothing is submitted)
  let drafted = false;
  if (pi && m.created_at) {
    try {
      let email = null;
      const sessions = await stripe.checkout.sessions.list({ payment_intent: piId, limit: 1 });
      email = sessions.data[0]?.customer_details?.email ?? null;

      const log = [
        `Order created: ${m.created_at} by sales agent (Discord ID ${m.agent_id}) in ${m.created_in}.`,
        `Customer Discord ID: ${m.customer_id}.`,
        `Payment completed: ${m.paid_at ?? 'not recorded'}.`,
        m.delivered_at
          ? `Delivered: ${m.delivered_at} (${m.delivered_to}).${m.delivered_file ? ` File: ${m.delivered_file} (SHA-256 ${m.file_sha256}).` : ''}${m.delivery_message ? ` Message: ${m.delivery_message}` : ''}`
          : 'No delivery has been recorded by the system.',
      ].join('\n');

      await stripe.disputes.update(d.id, {
        evidence: {
          product_description: m.description,
          access_activity_log: log,
          ...(email ? { customer_email_address: email } : {}),
        },
      });
      drafted = true;
    } catch (e) {
      console.error('Could not draft evidence:', e.message);
    }
  }

  embed.setFooter({
    text: drafted
      ? 'A draft of the evidence was saved in Stripe. Review it and submit before the deadline. Nothing has been submitted.'
      : 'Add your evidence in Stripe and submit it before the deadline.',
  });

  const owner = await client.users.fetch(OWNER_ID);
  await notifyOwner({ embeds: [embed] });
}

async function onDisputeClosed(d) {
  const owner = await client.users.fetch(OWNER_ID);
  const won = d.status === 'won';
  const embed = new EmbedBuilder()
    .setColor(won ? GREEN : RED)
    .setTitle(`Dispute ${d.status.replace(/_/g, ' ')}`)
    .addFields(
      { name: 'Amount', value: money(d.amount / 100, d.currency), inline: true },
      { name: 'Dispute ID', value: d.id, inline: true }
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
        return await sendStripeCheckout(interaction, { kind: 'checkout' });
      case 'invoice':
        return await sendStripeCheckout(interaction, {
          kind: 'invoice',
          expiresMins: parseInt(interaction.options.getString('expires') ?? '1440', 10),
        });
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
    console.error('Interaction error:', err);
    try {
      if (interaction.deferred || interaction.replied) {
        await interaction.editReply({ content: `Something went wrong: ${err.message}` });
      } else {
        await interaction.reply({ content: `Something went wrong: ${err.message}`, ephemeral: true });
      }
    } catch {}
  }
});

// ---------- Stripe webhook ----------
const app = express();

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
            { name: 'Customer', value: customer_id ? `<@${customer_id}>\n(${customer_id})` : 'Unknown', inline: true },
            { name: 'Sales Agent', value: agent_id ? `<@${agent_id}>\n(${agent_id})` : 'Unknown', inline: true },
            { name: 'Purchase Amount', value: total.toFixed(2), inline: true },
            { name: 'Description', value: description || 'No description' },
            { name: 'Transaction ID', value: s.id }
          )
          .setFooter({ text: 'Thank you for your purchase using our alternative payment method.' })
          .setTimestamp();

        // Orders with a file are recorded automatically; others get a button
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
      await deliverFile(s.id, s.payment_intent).catch((e) => console.error('File delivery failed:', e));
    }
  }

  if (event.type === 'checkout.session.expired') {
    const s = event.data.object;
    if (s.metadata?.agent_id) {
      const hadFile = discardPending(s.id);
      const embed = new EmbedBuilder()
        .setColor(GREY)
        .setTitle('Invoice expired unpaid')
        .addFields(
          { name: 'Customer', value: s.metadata.customer_id ? `<@${s.metadata.customer_id}>` : 'Unknown', inline: true },
          { name: 'Sales Agent', value: `<@${s.metadata.agent_id}>`, inline: true },
          { name: 'Amount', value: money((s.amount_total ?? 0) / 100, s.currency), inline: true },
          { name: 'Description', value: short(s.metadata.description, 200) },
          { name: 'Checkout ID', value: s.id }
        )
        .setFooter({ text: hadFile ? 'The attached file was deleted.' : 'No file attached.' })
        .setTimestamp();
      await notifyOwner({ embeds: [embed] });
    }
  }

  if (event.type === 'charge.dispute.created') {
    await onDisputeCreated(event.data.object).catch((e) => console.error('Dispute alert failed:', e));
  }
  if (event.type === 'charge.dispute.closed') {
    await onDisputeClosed(event.data.object).catch((e) => console.error('Dispute close alert failed:', e));
  }

  res.json({ received: true });
});

app.get('/', (_, res) => res.send('Bankroll bot running'));
app.listen(PORT, () => console.log(`Webhook server listening on :${PORT}`));

// ---------- Discord login (with logging so failures are visible) ----------
client.on('error', (e) => console.error('Client error:', e));
client.on('shardError', (e) => console.error('Shard error:', e));
client.on('shardDisconnect', (ev, id) => console.error('Shard disconnected', id, ev.code));
client.on('warn', (m) => console.warn('Warn:', m));
client.on('shardReady', (id) => console.log('Shard ready', id));
client.on('shardReconnecting', (id) => console.log('Shard reconnecting', id));
console.log('Token present:', !!DISCORD_TOKEN, 'length:', DISCORD_TOKEN?.length);

setTimeout(() => {
  if (!started) console.error('NOT logged in after 45 seconds');
}, 45000);

client
  .login(DISCORD_TOKEN)
  .then(() => console.log('login() resolved'))
  .catch((e) => console.error('Login failed:', e));
