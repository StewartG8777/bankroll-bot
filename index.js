require('dotenv').config();
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
} = require('discord.js');

const {
  DISCORD_TOKEN,
  STRIPE_SECRET_KEY,
  STRIPE_WEBHOOK_SECRET,
  OWNER_ID,
  ALLOWED_USER_IDS = '',
  CURRENCY = 'gbp',
  SUCCESS_URL = 'https://stripe.com',
  PORT = 3000,
} = process.env;

const stripe = new Stripe(STRIPE_SECRET_KEY);
const client = new Client({
  intents: [GatewayIntentBits.Guilds, GatewayIntentBits.DirectMessages],
  partials: [Partials.Channel],
});

// Who is allowed to run /bankroll (owner is always allowed)
const allowed = new Set(
  [OWNER_ID, ...ALLOWED_USER_IDS.split(',').map((s) => s.trim())].filter(Boolean)
);

const GREEN = 0x2ecc40;
const symbols = { gbp: '£', usd: '$', eur: '€' };
const money = (amount, cur) =>
  `${symbols[cur.toLowerCase()] ?? cur.toUpperCase() + ' '}${amount.toFixed(2)}`;

// ---------- Slash command ----------
const command = new SlashCommandBuilder()
  .setName('bankroll')
  .setDescription('Create a Stripe checkout link for a customer')
  .addUserOption((o) =>
    o.setName('customer').setDescription('Who is paying').setRequired(true)
  )
  .addNumberOption((o) =>
    o.setName('amount').setDescription('Amount to charge').setMinValue(1).setRequired(true)
  )
  .addStringOption((o) =>
    o.setName('description').setDescription('What the purchase is for').setRequired(true)
  )
  // Works in servers, bot DMs and any DM/group DM (user-installed app)
  .setIntegrationTypes([
    ApplicationIntegrationType.GuildInstall,
    ApplicationIntegrationType.UserInstall,
  ])
  .setContexts([
    InteractionContextType.Guild,
    InteractionContextType.BotDM,
    InteractionContextType.PrivateChannel,
  ]);

client.once('clientReady', async () => {
  console.log(`Logged in as ${client.user.tag}`);
  await client.application.commands.set([command.toJSON()]);
  console.log('Slash command registered');
});

client.on('interactionCreate', async (interaction) => {
  if (!interaction.isChatInputCommand() || interaction.commandName !== 'bankroll') return;

  if (!allowed.has(interaction.user.id)) {
    return interaction.reply({
      content: "You aren't allowed to use this command.",
      ephemeral: true,
    });
  }

  const customer = interaction.options.getUser('customer');
  const amount = interaction.options.getNumber('amount');
  const description = interaction.options.getString('description');
  const agent = interaction.user;

  await interaction.deferReply();

  try {
    const session = await stripe.checkout.sessions.create({
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
      metadata: {
        customer_id: customer.id,
        agent_id: agent.id,
        description,
      },
      payment_intent_data: {
        metadata: { customer_id: customer.id, agent_id: agent.id },
      },
    });

    const where = interaction.guild
      ? { guild: interaction.guild.name, channel: `#${interaction.channel?.name ?? 'unknown'}` }
      : {
          guild: 'Direct Message',
          channel: `DM with ${customer.username}`,
        };

    const embed = new EmbedBuilder()
      .setColor(GREEN)
      .setTitle('Bankroll Checkout Created')
      .setDescription('Please use the link below to complete your purchase.')
      .addFields(
        { name: 'Sales Agent', value: `<@${agent.id}>` },
        { name: 'Customer', value: `<@${customer.id}>` },
        { name: 'Amount', value: money(amount, CURRENCY) },
        { name: 'Description', value: description },
        { name: 'Generated In Guild', value: where.guild },
        { name: 'Generated In Channel', value: where.channel }
      )
      .setFooter({
        text: `Checkout created by ${agent.username} | Checkout ID: ${session.id}`,
      });

    const row = new ActionRowBuilder().addComponents(
      new ButtonBuilder().setLabel('Pay invoice').setStyle(ButtonStyle.Link).setURL(session.url)
    );

    await interaction.editReply({
      content: `<@${customer.id}>`,
      embeds: [embed],
      components: [row],
    });
  } catch (err) {
    console.error(err);
    await interaction.editReply({ content: `Failed to create checkout: ${err.message}` });
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

        const owner = await client.users.fetch(OWNER_ID);
        await owner.send({ embeds: [embed] });
      } catch (err) {
        console.error('Failed to DM owner:', err);
      }
    }
  }

  res.json({ received: true });
});

app.get('/', (_, res) => res.send('Bankroll bot running'));
app.listen(PORT, () => console.log(`Webhook server listening on :${PORT}`));

client.login(DISCORD_TOKEN);
