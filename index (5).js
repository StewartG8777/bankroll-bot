require('dotenv').config();
const { 
    Client, 
    GatewayIntentBits, 
    REST, 
    Routes, 
    SlashCommandBuilder, 
    EmbedBuilder, 
    ActionRowBuilder, 
    ButtonBuilder, 
    ButtonStyle, 
    PermissionFlagsBits 
} = require('discord.js');
const Stripe = require('stripe');
const express = require('express');
const axios = require('axios');

// Initialize Stripe & Express
const stripe = new Stripe(process.env.STRIPE_SECRET_KEY);
const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.json());

// In-Memory Vault Store (Mapping name -> file attachment/url)
const vaultFiles = new Map();

// Initialize Discord Client
const client = new Client({
    intents: [
        GatewayIntentBits.Guilds,
        GatewayIntentBits.GuildMessages,
        GatewayIntentBits.DirectMessages
    ]
});

// --- Register Slash Commands ---
const commands = [
    new SlashCommandBuilder()
        .setName('bankroll')
        .setDescription('Digital product delivery and payment processing')
        .addSubcommand(sub =>
            sub.setName('stripe')
               .setDescription('Create a Stripe payment link')
               .addUserOption(opt => opt.setName('customer').setDescription('Customer to bill').setRequired(true))
               .addNumberOption(opt => opt.setName('amount').setDescription('Amount').setRequired(true))
               .addStringOption(opt => opt.setName('description').setDescription('Product description').setRequired(true))
               .addAttachmentOption(opt => opt.setName('file').setDescription('File to deliver').setRequired(false))
        )
        .addSubcommand(sub =>
            sub.setName('monzo')
               .setDescription('Create a Monzo.me payment link with instant vault check')
               .addUserOption(opt => opt.setName('customer').setDescription('Customer to bill').setRequired(true))
               .addNumberOption(opt => opt.setName('amount').setDescription('Amount in GBP').setRequired(true))
               .addStringOption(opt => opt.setName('description').setDescription('Product description').setRequired(true))
               .addAttachmentOption(opt => opt.setName('file').setDescription('File to deliver').setRequired(false))
        ),
    new SlashCommandBuilder()
        .setName('vault')
        .setDescription('Manage digital product vault files')
        .addSubcommand(sub =>
            sub.setName('add')
               .setDescription('Store a file in the vault')
               .addStringOption(opt => opt.setName('name').setDescription('Vault key name').setRequired(true))
               .addAttachmentOption(opt => opt.setName('file').setDescription('The file to store').setRequired(true))
        )
].map(cmd => cmd.toJSON());

const rest = new REST({ version: '10' }).setToken(process.env.DISCORD_TOKEN);

async function registerCommands() {
    try {
        console.log('Registering slash commands...');
        await rest.put(
            Routes.applicationCommands(process.env.DISCORD_TOKEN ? Buffer.from(process.env.DISCORD_TOKEN.split('.')[0], 'base64').toString() : ''),
            { body: commands },
        );
        console.log('Dual Stripe & Monzo Bankroll commands registered.');
    } catch (error) {
        console.error('Error registering commands:', error);
    }
}

// --- Discord Ready Event ---
client.once('ready', async () => {
    console.log(`Logged in as ${client.user.tag}`);
    await registerCommands();
    
    // Notify owner bot is back online
    try {
        if (process.env.OWNER_ID) {
            const owner = await client.users.fetch(process.env.OWNER_ID);
            if (owner) {
                await owner.send(`Bot is back online. Reminders rebuilt: 1.`);
            }
        }
    } catch (e) {
        console.log('Could not send startup DM to owner.');
    }
});

// --- Interaction Handler ---
client.on('interactionCreate', async interaction => {
    if (interaction.isChatInputCommand()) {
        const { commandName } = interaction;

        if (commandName === 'bankroll') {
            const sub = interaction.options.getSubcommand();
            const customer = interaction.options.getUser('customer');
            const amount = interaction.options.getNumber('amount');
            const description = interaction.options.getString('description');
            const file = interaction.options.getAttachment('file');
            
            // Store file reference if attached
            let fileUrl = file ? file.url : null;

            if (sub === 'stripe') {
                try {
                    await interaction.deferReply({ ephemeral: true });
                    
                    const session = await stripe.checkout.sessions.create({
                        payment_method_types: ['card'],
                        line_items: [{
                            price_data: {
                                currency: 'gbp',
                                product_data: { name: description },
                                unit_amount: Math.round(amount * 100),
                            },
                            quantity: 1,
                        }],
                        mode: 'payment',
                        success_url: 'https://discord.com',
                        cancel_url: 'https://discord.com',
                    });

                    const embed = new EmbedBuilder()
                        .setTitle('💳 Stripe Invoice Generated')
                        .setDescription(`Invoice created for ${customer}`)
                        .addFields(
                            { name: 'Description', value: description },
                            { name: 'Amount', value: `£${amount.toFixed(2)}` },
                            { name: 'Checkout Link', value: session.url }
                        )
                        .setColor(0x635BFF);

                    await interaction.editReply({ embeds: [embed] });

                    // DM Customer
                    try {
                        await customer.send({
                            content: `You have a pending invoice for **${description}** (£${amount.toFixed(2)}).\nPay here: ${session.url}`
                        });
                    } catch (err) {
                        console.log('Could not DM customer directly.');
                    }

                } catch (error) {
                    console.error(error);
                    if (interaction.deferred) {
                        await interaction.editReply({ content: 'Failed to create Stripe checkout session.' });
                    }
                }
            } 
            else if (sub === 'monzo') {
                const monzoUsername = process.env.MONZO_USERNAME || 'sstewart32';
                const monzoLink = `https://monzo.me/${monzoUsername}/${amount}`;

                const embed = new EmbedBuilder()
                    .setTitle('💷 Monzo.me Invoice Generated')
                    .setDescription(`Payment request for ${customer}`)
                    .addFields(
                        { name: 'Description', value: description },
                        { name: 'Amount', value: `£${amount.toFixed(2)}` },
                        { name: 'Payment Link', value: monzoLink }
                    )
                    .setColor(0xFF5722);

                const row = new ActionRowBuilder().addComponents(
                    new ButtonBuilder()
                        .setCustomId(`verify_monzo_${amount}_${fileUrl ? encodeURIComponent(fileUrl) : 'none'}`)
                        .setLabel('Check Payment & Unlock')
                        .setStyle(ButtonStyle.Success)
                );

                await interaction.reply({ embeds: [embed], components: [row] });

                // DM Customer
                try {
                    await customer.send({
                        content: `You have a Monzo invoice for **${description}** (£${amount.toFixed(2)}).\nPay here: ${monzoLink}\nOnce paid, click the verification button in the server to unlock your file!`
                    });
                } catch (err) {
                    console.log('Could not DM customer.');
                }
            }
        } 
        else if (commandName === 'vault') {
            const sub = interaction.options.getSubcommand();
            if (sub === 'add') {
                const name = interaction.options.getString('name');
                const file = interaction.options.getAttachment('file');
                vaultFiles.set(name, file.url);
                await interaction.reply({ content: `Successfully saved **${name}** to the vault!`, ephemeral: true });
            }
        }
    } 
    else if (interaction.isButton()) {
        if (interaction.customId.startsWith('verify_monzo_')) {
            const parts = interaction.customId.split('_');
            const expectedAmount = parseFloat(parts[2]);
            const encodedFileUrl = parts[3];
            const fileUrl = encodedFileUrl !== 'none' ? decodeURIComponent(encodedFileUrl) : null;

            await interaction.deferReply({ ephemeral: true });

            try {
                // Verify via Monzo API
                const response = await axios.get(`https://api.monzo.com/transactions`, {
                    headers: { Authorization: `Bearer ${process.env.MONZO_ACCESS_TOKEN}` },
                    params: { account_id: process.env.MONZO_ACCOUNT_ID, limit: 10 }
                });

                const transactions = response.data.transactions || [];
                // Monzo amounts are in pence
                const targetPence = Math.round(expectedAmount * 100);
                
                // Find recent matching incoming transaction
                const paid = transactions.find(tx => tx.amount >= targetPence && !tx.decline_reason);

                if (paid) {
                    let deliveryContent = 'Payment verified successfully! Here is your product:';
                    if (fileUrl) {
                        deliveryContent += `\n${fileUrl}`;
                    } else {
                        deliveryContent += '\n(No file was attached to this invoice)';
                    }

                    await interaction.editReply({ content: deliveryContent });
                } else {
                    await interaction.editReply({ content: `Payment of £${expectedAmount.toFixed(2)} not found yet. Please make sure you completed the Monzo.me transfer and try again in a moment.` });
                }
            } catch (error) {
                console.error('Monzo verification error:', error.response?.data || error.message);
                await interaction.editReply({ content: 'Error connecting to Monzo API to verify transaction. Please check your access tokens in environment variables.' });
            }
        }
    }
});

// --- Express Uptime Server ---
app.get('/', (req, res) => {
    res.send('Bankroll Bot is active and running!');
});

app.listen(PORT, () => {
    console.log(`Server listening on port ${PORT}`);
});

client.login(process.env.DISCORD_TOKEN);