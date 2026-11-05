'use strict';
// Extracted from the current main branch; preserves v2 encryption and legacy v1 recovery.
module.exports = function createGoogleDriveSystem(deps) {
const { fs, path, crypto, Buffer, Readable, Transform, pipeline, fetch, DATA_DIR, DATA_DIR_STATUS, DELIVERIES_FILE, DOWNLOADS_FILE, DOWNLOAD_HISTORY_FILE, NOTIFICATIONS_FILE, PRESENCE_FILE, SEND_JOBS_FILE, VAULT_FILE, WIPED_CUSTOMERS_FILE, GOOGLE_AUTH_BACKUP_NAME, GOOGLE_OAUTH_FILE, GOOGLE_TOKEN_FILE, GOOGLE_DRIVE_CONTENT_FOLDER_NAMES, GOOGLE_DRIVE_FOLDER_NAMES, GOOGLE_DRIVE_ROOT_NAME, PUBLIC_URL, client, atomicWrite, resolveBackupChannel, relData, sha256FileCached, safeBackupRel, googleDriveFolderForDatabaseFile, isManagedContentAddressedBackup } = deps;
function googleAuthBackupKey({ legacy = false } = {}) {
  // Discord tokens rotate. They must never be the long-term encryption key for
  // credentials that are expected to survive a bot-token rotation.
  const secret = legacy
    ? (process.env.BANKROLL_BACKUP_ENCRYPTION_KEY || process.env.DISCORD_TOKEN)
    : process.env.BANKROLL_BACKUP_ENCRYPTION_KEY;
  if (!secret) throw new Error('Set a stable BANKROLL_BACKUP_ENCRYPTION_KEY in Render before saving Google credentials. Do not use DISCORD_TOKEN: it changes when the bot token is rotated.');
  return crypto.createHash('sha256').update(String(secret)).digest();
}
function encryptGoogleAuthBundle(bundle) {
  const iv = crypto.randomBytes(12);
  const cipher = crypto.createCipheriv('aes-256-gcm', googleAuthBackupKey(), iv);
  const encrypted = Buffer.concat([cipher.update(JSON.stringify(bundle), 'utf8'), cipher.final()]);
  return Buffer.from(JSON.stringify({ version: 2, iv: iv.toString('base64'), tag: cipher.getAuthTag().toString('base64'), data: encrypted.toString('base64') }), 'utf8');
}
function decryptGoogleAuthBundle(buffer) {
  const envelope = JSON.parse(buffer.toString('utf8'));
  if (![1, 2].includes(envelope.version)) throw new Error('Unsupported encrypted Google credential backup version.');
  // Version 1 used the Discord token as a fallback. Keep compatibility while
  // that token remains unchanged; all new bundles use the dedicated stable key.
  const key = googleAuthBackupKey({ legacy: envelope.version === 1 });
  const decipher = crypto.createDecipheriv('aes-256-gcm', key, Buffer.from(envelope.iv, 'base64'));
  decipher.setAuthTag(Buffer.from(envelope.tag, 'base64'));
  return JSON.parse(Buffer.concat([decipher.update(Buffer.from(envelope.data, 'base64')), decipher.final()]).toString('utf8'));
}
async function persistGoogleAuthBundle() {
  if (!fs.existsSync(GOOGLE_OAUTH_FILE)) return;
  const ch = await resolveBackupChannel();
  if (!ch) throw new Error('Cannot persist Google credentials: #bankroll-backup was not found.');
  const bundle = { client: JSON.parse(fs.readFileSync(GOOGLE_OAUTH_FILE, 'utf8')), token: fs.existsSync(GOOGLE_TOKEN_FILE) ? JSON.parse(fs.readFileSync(GOOGLE_TOKEN_FILE, 'utf8')) : null, savedAt: Date.now() };
  const attachment = encryptGoogleAuthBundle(bundle);
  await ch.send({ content: 'Encrypted Google Drive recovery bundle. Only Bankroll can decrypt this using its server secret.', files: [{ attachment, name: GOOGLE_AUTH_BACKUP_NAME }] });
}
async function restoreGoogleAuthBundleFromMessages(messages) {
  if (fs.existsSync(GOOGLE_OAUTH_FILE) && fs.existsSync(GOOGLE_TOKEN_FILE)) return false;
  for (const msg of messages) {
    if (msg.author?.id !== client.user.id) continue;
    const att = msg.attachments?.find?.((a) => a.name === GOOGLE_AUTH_BACKUP_NAME);
    if (!att) continue;
    try {
      const response = await fetch(att.url);
      if (!response.ok) throw new Error('HTTP ' + response.status);
      const bundle = decryptGoogleAuthBundle(Buffer.from(await response.arrayBuffer()));
      if (!bundle.client || typeof bundle.client !== 'object') throw new Error('Missing OAuth client data.');
      atomicWrite(GOOGLE_OAUTH_FILE, JSON.stringify(bundle.client, null, 2));
      if (bundle.token && typeof bundle.token === 'object') atomicWrite(GOOGLE_TOKEN_FILE, JSON.stringify(bundle.token, null, 2));
      try { fs.chmodSync(GOOGLE_OAUTH_FILE, 0o600); if (fs.existsSync(GOOGLE_TOKEN_FILE)) fs.chmodSync(GOOGLE_TOKEN_FILE, 0o600); } catch {}
      console.log('Restored encrypted Google Drive credentials from #bankroll-backup.');
      return true;
    } catch (error) { console.error('Encrypted Google credential recovery failed:', error.message); }
  }
  return false;
}

const GOOGLE_OAUTH_STATES = new Map();
function googleRedirectUri() { const base = String(PUBLIC_URL || process.env.RENDER_EXTERNAL_URL || '').replace(/\/+$/, ''); return base ? base + '/google/oauth/callback' : ''; }
function readGoogleClientConfig() {
  if (!fs.existsSync(GOOGLE_OAUTH_FILE)) throw new Error('Upload your Google OAuth client JSON first with /drive-credentials.');
  const parsed = JSON.parse(fs.readFileSync(GOOGLE_OAUTH_FILE, 'utf8'));
  const config = parsed.web || parsed.installed || parsed;
  if (!config.client_id || !config.client_secret) throw new Error('The saved OAuth file is missing client_id/client_secret.');
  if (parsed.installed && !parsed.web) throw new Error('This is a Google Desktop (installed) OAuth client. Render needs a Google Web application OAuth client. Create a Web application client, add the redirect URI shown by /drive connect, then upload that JSON with /drive-credentials.');
  const redirectUri = googleRedirectUri();
  if (!redirectUri) throw new Error('Set PUBLIC_URL in Render to your public HTTPS service URL before connecting Google Drive.');
  return { clientId: config.client_id, clientSecret: config.client_secret, redirectUri };
}
function makeGoogleOAuthClient() {
  const { google } = require('googleapis');
  const config = readGoogleClientConfig();
  const oauth = new google.auth.OAuth2(config.clientId, config.clientSecret, config.redirectUri);
  if (fs.existsSync(GOOGLE_TOKEN_FILE)) { try { oauth.setCredentials(JSON.parse(fs.readFileSync(GOOGLE_TOKEN_FILE, 'utf8'))); } catch (e) { console.error('Google Drive token file could not be read:', e.message); } }
  oauth.on('tokens', (fresh) => { try { let current = {}; try { current = JSON.parse(fs.readFileSync(GOOGLE_TOKEN_FILE, 'utf8')); } catch {} atomicWrite(GOOGLE_TOKEN_FILE, JSON.stringify({ ...current, ...fresh }, null, 2)); try { fs.chmodSync(GOOGLE_TOKEN_FILE, 0o600); } catch {} } catch (e) { console.error('Could not persist refreshed Google Drive token:', e.message); } });
  return oauth;
}
function googleDriveIsConnected() { try { const t = JSON.parse(fs.readFileSync(GOOGLE_TOKEN_FILE, 'utf8')); return !!(t.refresh_token || t.access_token); } catch { return false; } }

// Resolve the BANKROLL root and its eight direct child folders. Legacy top-level
// folders are moved into BANKROLL only when they are unique and on the same Drive.
async function listGoogleDriveFolders(drive, name, parentId = null) {
  const escaped = String(name).replace(/\\/g, '\\\\').replace(/'/g, "\\'");
  let q = "name = '" + escaped + "' and mimeType = 'application/vnd.google-apps.folder' and trashed = false";
  if (parentId) q = "'" + parentId + "' in parents and " + q;
  const matches = []; let pageToken;
  do {
    const page = await drive.files.list({
      q, pageSize: 1000, pageToken,
      fields: 'nextPageToken,files(id,name,mimeType,parents,driveId,capabilities(canAddChildren,canEdit))',
      supportsAllDrives: true, includeItemsFromAllDrives: true,
    });
    matches.push(...(page.data.files || []));
    pageToken = page.data.nextPageToken;
  } while (pageToken);
  return matches;
}

async function resolveGoogleDriveFolders(drive) {
  const rootMatches = await listGoogleDriveFolders(drive, GOOGLE_DRIVE_ROOT_NAME);
  if (rootMatches.length > 1) {
    throw new Error('More than one BANKROLL root folder is visible to the connected Google account. Keep one BANKROLL folder or remove the duplicate from this account before continuing; Bankroll will not guess which one is correct.');
  }
  let root = rootMatches[0] || null;
  if (!root) {
    const created = await drive.files.create({
      requestBody: { name: GOOGLE_DRIVE_ROOT_NAME, mimeType: 'application/vnd.google-apps.folder' },
      fields: 'id,name,mimeType,parents,driveId,capabilities(canAddChildren,canEdit)',
      supportsAllDrives: true,
    });
    root = created.data;
    console.log('Created Google Drive root folder: BANKROLL');
  }
  if (!root?.id) throw new Error('Google Drive did not return an ID for the BANKROLL root folder.');
  if (root.capabilities?.canAddChildren === false) {
    throw new Error('The connected Google account cannot add folders or files inside BANKROLL. Give the OAuth account Editor access to the BANKROLL folder.');
  }

  let myDriveRootId = null;
  try {
    const rootInfo = await drive.files.get({ fileId: 'root', fields: 'id,driveId' });
    myDriveRootId = rootInfo.data?.id || null;
  } catch (error) {
    console.warn('Could not inspect the My Drive root for legacy folder migration:', error.message);
  }

  const folders = { ROOT: root };
  for (const name of GOOGLE_DRIVE_FOLDER_NAMES) {
    const directMatches = await listGoogleDriveFolders(drive, name, root.id);
    if (directMatches.length > 1) {
      throw new Error('BANKROLL contains more than one "' + name + '" folder. Rename or remove the duplicate inside BANKROLL; no data has been moved.');
    }
    let folder = directMatches[0] || null;

    // Adopt an old top-level Bankroll folder where it is unambiguous. This keeps
    // existing files and Drive IDs intact while placing the folder under BANKROLL.
    if (!folder && myDriveRootId) {
      const legacyMatches = await listGoogleDriveFolders(drive, name, myDriveRootId);
      if (legacyMatches.length > 1) {
        throw new Error('More than one top-level "' + name + '" folder exists. Bankroll cannot safely choose which one to move into BANKROLL.');
      }
      const legacy = legacyMatches[0] || null;
      const sameDrive = legacy && (legacy.driveId || null) === (root.driveId || null);
      if (legacy && sameDrive) {
        const moved = await drive.files.update({
          fileId: legacy.id,
          addParents: root.id,
          removeParents: myDriveRootId,
          fields: 'id,name,mimeType,parents,driveId,capabilities(canAddChildren,canEdit)',
          supportsAllDrives: true,
        });
        folder = moved.data;
        console.log('Moved legacy Google Drive folder into BANKROLL: ' + name);
      }
    }

    if (!folder) {
      const created = await drive.files.create({
        requestBody: { name, mimeType: 'application/vnd.google-apps.folder', parents: [root.id] },
        fields: 'id,name,mimeType,parents,driveId,capabilities(canAddChildren,canEdit)',
        supportsAllDrives: true,
      });
      folder = created.data;
      console.log('Created BANKROLL/' + name);
    }
    if (!folder?.id) throw new Error('Google Drive did not return an ID for BANKROLL/' + name + '.');
    if (folder.capabilities?.canAddChildren === false) {
      throw new Error('The connected Google account cannot add files to BANKROLL/' + name + '. Give the OAuth account Editor access to that folder.');
    }
    folders[name] = folder;
  }
  return folders;
}

// Direct non-payment object uploads are confirmed before vault changes are acknowledged.
// The existing local file is retained as a compatibility cache for ZIP editing and
// Discord attachment flows; payment files never pass through this helper.
async function uploadNonPaymentFileToDrive(localPath) {
  const relative = relData(localPath);
  if (!relative) throw new Error('File is outside the Bankroll data directory');
  if (!googleDriveIsConnected()) throw new Error('Google Drive must be connected before adding a vault file');
  const { google } = require('googleapis');
  const { createDriveObjectStore } = require('./systems/drive-object-store');
  const drive = google.drive({ version: 'v3', auth: makeGoogleOAuthClient() });
  const folders = await resolveGoogleDriveFolders(drive);
  const store = createDriveObjectStore({ drive, folders });
  return store.put(relative, fs.createReadStream(localPath));
}

// Restore missing non-payment cache objects from the direct Drive store.
// This runs before the bot starts serving requests, and never modifies payments.
// Recover current non-payment JSON objects before processing orders or downloads.
// Existing populated local data is preserved to avoid discarding offline changes.
// Payment records and OAuth credentials are never read from this object store.
async function recoverNonPaymentDatabaseFromDrive() {
  if (!googleDriveIsConnected()) return { restored: 0, skipped: 0 };
  const { google } = require('googleapis');
  const { createDriveObjectStore } = require('./systems/drive-object-store');
  const drive = google.drive({ version: 'v3', auth: makeGoogleOAuthClient() });
  const store = createDriveObjectStore({ drive, folders: await resolveGoogleDriveFolders(drive) });
  const entries = [
    ['vault.json', VAULT_FILE, 'object', value => { vault = value; }],
    ['deliveries.json', DELIVERIES_FILE, 'object', value => { deliveries = value; }],
    ['downloads.json', DOWNLOADS_FILE, 'object', value => { downloads = value; }],
    ['scheduled_sends.json', SEND_JOBS_FILE, 'object', value => { sendJobs = value; }],
    ['download_history.json', DOWNLOAD_HISTORY_FILE, 'array', value => { downloadHistory = value.slice(-500); }],
    ['presence.json', PRESENCE_FILE, 'object', value => { savedPresence = value; }],
    ['notifications.json', NOTIFICATIONS_FILE, 'array', value => { notificationHistory = value.slice(-100); }],
    ['wiped_customers.json', WIPED_CUSTOMERS_FILE, 'array', value => { wipedCustomers = new Set(value.map(String)); }],
  ];
  let restored = 0, skipped = 0;
  for (const [name, file, kind, apply] of entries) {
    // Drive wins on restart, even if an old local cache contains valid JSON.
    // If Drive has no object, retain the local cache for legacy migration.
    const remote = await store.get(name);
    if (!remote) { skipped++; continue; }
    const chunks = [];
    let bytes = 0;
    for await (const chunk of remote.stream) {
      bytes += chunk.length;
      if (bytes > 16 * 1024 * 1024) throw new Error('Drive database exceeds recovery limit: ' + name);
      chunks.push(Buffer.from(chunk));
    }
    const raw = Buffer.concat(chunks).toString('utf8');
    const parsed = JSON.parse(raw);
    if (!parsed || typeof parsed !== 'object' || (kind === 'array' ? !Array.isArray(parsed) : Array.isArray(parsed))) {
      throw new Error('Invalid Drive database structure: ' + name);
    }
    const temp = file + '.drive-db-' + crypto.randomBytes(6).toString('hex');
    try {
      await fs.promises.writeFile(temp, raw, { flag: 'wx' });
      await fs.promises.rename(temp, file);
    } catch (error) {
      await fs.promises.unlink(temp).catch(() => {});
      throw error;
    }
    apply(parsed);
    restored++;
  }
  return { restored, skipped };
}

async function hydrateNonPaymentCacheFromDrive() {
  if (!googleDriveIsConnected()) return { restored: 0, skipped: 0 };
  const { google } = require('googleapis');
  const { createDriveObjectStore } = require('./systems/drive-object-store');
  const drive = google.drive({ version: 'v3', auth: makeGoogleOAuthClient() });
  const folders = await resolveGoogleDriveFolders(drive);
  const store = createDriveObjectStore({ drive, folders });
  const candidates = new Set();
  for (const entry of Object.values(vault || {})) if (entry?.path) candidates.add(entry.path);
  for (const entry of [...Object.values(deliveries || {}), ...Object.values(downloads || {})]) {
    if (entry?.path && !entry.backupDownload) candidates.add(entry.path);
  }
  let restored = 0, skipped = 0;
  for (const filePath of candidates) {
    const relative = relData(filePath);
    if (!relative || !/^(vault|files|deliveries)\//.test(relative)) { skipped++; continue; }
    if (fs.existsSync(filePath)) { skipped++; continue; }
    const object = await store.get(relative);
    if (!object) { skipped++; continue; }
    await fs.promises.mkdir(path.dirname(filePath), { recursive: true });
    const temp = filePath + '.drive-cache-' + crypto.randomBytes(6).toString('hex');
    try {
      await pipeline(object.stream, fs.createWriteStream(temp, { flags: 'wx' }));
      await fs.promises.rename(temp, filePath);
      restored++;
    } catch (error) {
      await fs.promises.unlink(temp).catch(() => {});
      throw error;
    }
  }
  return { restored, skipped };
}

async function backupToGoogleDrive(databaseFiles, storedPaths) {
  if (!googleDriveIsConnected()) return { connected: false, uploaded: 0, skipped: true };
  const { google } = require('googleapis');
  const drive = google.drive({ version: 'v3', auth: makeGoogleOAuthClient() });
  const folders = await resolveGoogleDriveFolders(drive);
  const existingByFolder = new Map();
  for (const name of GOOGLE_DRIVE_FOLDER_NAMES) {
    const existing = new Map(); let pageToken;
    do {
      const page = await drive.files.list({
        q: "'" + folders[name].id + "' in parents and trashed = false",
        pageSize: 1000, pageToken,
        fields: 'nextPageToken,files(id,name,appProperties)',
        supportsAllDrives: true, includeItemsFromAllDrives: true,
      });
      for (const file of (page.data.files || [])) existing.set(file.name, file);
      pageToken = page.data.nextPageToken;
    } while (pageToken);
    existingByFolder.set(name, existing);
  }

  let uploaded = 0;
  const uploadOne = async (folderName, name, signature, body, mimeType) => {
    const existing = existingByFolder.get(folderName);
    if (!existing) throw new Error('Unknown Bankroll Drive folder: ' + folderName);
    const old = existing.get(name);
    if (old?.appProperties?.bankrollSignature === signature) return old.id || null;
    const requestBody = { name, appProperties: { bankrollSignature: signature, bankrollBackup: 'true' } };
    const media = { mimeType: mimeType || 'application/octet-stream', body };
    try {
      if (old?.id) {
        const updated = await drive.files.update({
          fileId: old.id, requestBody, media, fields: 'id,name,appProperties',
          supportsAllDrives: true,
        });
        existing.set(name, updated.data || { ...old, appProperties: requestBody.appProperties });
      } else {
        const created = await drive.files.create({
          requestBody: { ...requestBody, parents: [folders[folderName].id] },
          media, fields: 'id,name,appProperties', supportsAllDrives: true,
        });
        existing.set(name, created.data);
      }
    } catch (error) {
      if (/insufficient permissions|not have permission|forbidden/i.test(String(error.message || error))) {
        throw new Error('Google Drive cannot write to BANKROLL/' + folderName + '. Give the connected Google account Editor access. Google said: ' + String(error.message || error));
      }
      throw error;
    }
    uploaded++;
    console.log('Google Drive backup uploaded to BANKROLL/' + folderName + ': ' + name);
    return existing.get(name)?.id || null;
  };

  const databaseEntries = [];
  for (const file of databaseFiles || []) {
    const signature = crypto.createHash('sha256').update(file.buffer).digest('hex');
    const folderName = googleDriveFolderForDatabaseFile(file.name);
    const driveName = crypto.createHash('sha256').update(file.name + ':' + signature).digest('hex').slice(0, 24) + '_' + file.name;
    const fileId = await uploadOne(folderName, driveName, signature, Readable.from([file.buffer]), 'application/json');
    const current = existingByFolder.get(folderName).get(driveName);
    databaseEntries.push({
      name: file.name, driveName, folderName, fileId: fileId || current?.id,
      size: file.buffer.length, sha256: signature,
    });
  }

  // Delivery files that are not linked to a vault entry live in CUSTOMER DELIVERIES.
  // Vault-linked deliveries stay in VAULT and are backed up only once.
  const deliveryPaths = new Set();
  const addDeliveryRecord = (record) => {
    if (!record || record.backupDownload || record.vaultKey || !record.path) return;
    deliveryPaths.add(path.resolve(String(record.path)));
  };
  for (const record of Object.values(deliveries || {})) {
    if (Array.isArray(record?.files)) for (const file of record.files) addDeliveryRecord(file);
    else addDeliveryRecord(record);
  }
  for (const record of Object.values(downloads || {})) addDeliveryRecord(record);
  const vaultPaths = new Set(Object.values(vault || {}).filter((item) => item?.path).map((item) => path.resolve(String(item.path))));

  let vaultFileCount = 0;
  const vaultEntries = [];
  const customerDeliveryEntries = [];
  for (const fp of storedPaths || []) {
    if (!fp || !fs.existsSync(fp)) continue;
    const rel = relData(fp);
    if (!rel) continue;
    const stat = await fs.promises.stat(fp);
    if (!stat.isFile() || !stat.size) continue;
    if (stat.size > 150 * 1024 * 1024) throw new Error('Google Drive backup file exceeds 150 MiB: ' + rel);
    const signature = await sha256FileCached(fp, stat);
    const key = crypto.createHash('sha256').update(rel).digest('hex').slice(0, 16);
    const driveName = key + '_' + signature.slice(0, 24) + '_' + path.basename(rel).replace(/[^\w.\-]/g, '_');
    const isCustomerDelivery = deliveryPaths.has(path.resolve(fp)) && !vaultPaths.has(path.resolve(fp));
    const folderName = isCustomerDelivery ? 'CUSTOMER DELIVERIES' : 'VAULT';
    const fileId = await uploadOne(folderName, driveName, stat.size + ':' + signature, fs.createReadStream(fp));
    const entry = { path: rel, driveName, folderName, fileId: fileId || existingByFolder.get(folderName).get(driveName)?.id, size: stat.size, sha256: signature };
    if (isCustomerDelivery) customerDeliveryEntries.push(entry);
    else { vaultEntries.push(entry); vaultFileCount++; }
  }

  // A non-secret operational snapshot makes SYSTEM useful without copying .env,
  // OAuth client secrets, access tokens, or other credentials into Drive.
  const systemSnapshot = {
    format: 'bankroll-system-snapshot-v1',
    generatedAt: new Date().toISOString(),
    runtime: { node: process.version, platform: process.platform, dataDirectory: DATA_DIR_STATUS },
    records: {
      vault: Object.keys(vault || {}).length,
      deliveries: Object.keys(deliveries || {}).length,
      downloads: Object.keys(downloads || {}).length,
    },
    drive: { root: GOOGLE_DRIVE_ROOT_NAME, folders: [...GOOGLE_DRIVE_FOLDER_NAMES] },
  };
  const systemBuffer = Buffer.from(JSON.stringify(systemSnapshot, null, 2));
  const systemSignature = crypto.createHash('sha256').update(systemBuffer).digest('hex');
  const systemName = crypto.createHash('sha256').update('bankroll-system-state.json:' + systemSignature).digest('hex').slice(0, 24) + '_bankroll-system-state.json';
  const systemId = await uploadOne('SYSTEM', systemName, systemSignature, Readable.from([systemBuffer]), 'application/json');
  const systemEntries = [{
    name: 'bankroll-system-state.json', driveName: systemName,
    fileId: systemId || existingByFolder.get('SYSTEM').get(systemName)?.id,
    size: systemBuffer.length, sha256: systemSignature,
  }];

  const folderIds = Object.fromEntries(GOOGLE_DRIVE_FOLDER_NAMES.map((name) => [name, folders[name].id]));
  folderIds.ROOT = folders.ROOT.id;
  const manifest = {
    format: 'bankroll-drive-layout-v3',
    createdAt: new Date().toISOString(),
    databaseFiles: databaseEntries,
    vaultFiles: vaultEntries,
    customerDeliveryFiles: customerDeliveryEntries,
    systemFiles: systemEntries,
    vaultFileCount,
    customerDeliveryFileCount: customerDeliveryEntries.length,
    folders: folderIds,
  };
  const manifestBuffer = Buffer.from(JSON.stringify(manifest, null, 2));
  const manifestSignature = crypto.createHash('sha256').update(manifestBuffer).digest('hex');
  const pointName = 'bankroll-recovery-' + new Date().toISOString().replace(/[:.]/g, '-') + '.json';
  await uploadOne('BACKUPS', pointName, manifestSignature, Readable.from([manifestBuffer]), 'application/json');
  await uploadOne('BACKUPS', 'bankroll-drive-manifest.json', manifestSignature, Readable.from([manifestBuffer]), 'application/json');

  let retention = null;
  try {
    retention = await applyGoogleDriveRetention(drive, folders);
    console.log('Google Drive retention completed:', retention);
  } catch (retentionError) {
    console.error('Google Drive retention safely skipped; no incomplete cleanup was applied:', retentionError.message);
  }
  console.log('Google Drive BANKROLL backup complete: ' + uploaded + ' changed file(s) uploaded.');
  return { connected: true, uploaded, folders, retention, vaultFileCount, customerDeliveryFileCount: customerDeliveryEntries.length };
}

/**
 * Safe Drive recovery-point retention.
 * Keep the newest point for each of the 7 latest UTC days, 4 latest UTC weeks
 * and 3 latest UTC months. Delete only managed content-addressed objects that
 * are not referenced by any retained, fully validated recovery point.
 */
async function applyGoogleDriveRetention(drive, folders) {
  const listAll = async (folderId) => {
    const all = []; let pageToken;
    do {
      const page = await drive.files.list({
        q: "'" + folderId + "' in parents and trashed = false",
        pageSize: 1000, pageToken,
        fields: 'nextPageToken,files(id,name,mimeType,modifiedTime,appProperties)',
        supportsAllDrives: true, includeItemsFromAllDrives: true,
      });
      all.push(...(page.data.files || []));
      pageToken = page.data.nextPageToken;
    } while (pageToken);
    return all;
  };
  const backupFiles = await listAll(folders.BACKUPS.id);
  const points = backupFiles.filter((file) => /^bankroll-recovery-.*\.json$/i.test(file.name || ''))
    .sort((a, b) => new Date(b.modifiedTime || 0) - new Date(a.modifiedTime || 0));
  if (!points.length) return { kept: 0, deletedManifests: 0, deletedObjects: 0, skipped: 'No versioned recovery points' };

  const parsed = [];
  for (const file of points) {
    const response = await drive.files.get({ fileId: file.id, alt: 'media' }, { responseType: 'stream' });
    const chunks = [];
    for await (const chunk of response.data) chunks.push(Buffer.from(chunk));
    const manifest = JSON.parse(Buffer.concat(chunks).toString('utf8'));
    if (!['bankroll-drive-layout-v2', 'bankroll-drive-layout-v3'].includes(manifest.format) ||
        !Array.isArray(manifest.databaseFiles) || !Array.isArray(manifest.vaultFiles)) {
      throw new Error('Retention halted: recovery manifest "' + file.name + '" is malformed or unsupported. No Drive files were deleted.');
    }
    const entries = [
      ...manifest.databaseFiles,
      ...manifest.vaultFiles,
      ...(Array.isArray(manifest.customerDeliveryFiles) ? manifest.customerDeliveryFiles : []),
      ...(Array.isArray(manifest.systemFiles) ? manifest.systemFiles : []),
    ];
    for (const entry of entries) {
      if (!entry?.fileId || !/^[a-f0-9]{64}$/i.test(String(entry.sha256 || '')) ||
          !Number.isSafeInteger(Number(entry.size)) || Number(entry.size) < 0) {
        throw new Error('Retention halted: invalid object reference in "' + file.name + '". No Drive files were deleted.');
      }
      if (entry.folderName && !GOOGLE_DRIVE_CONTENT_FOLDER_NAMES.includes(entry.folderName)) {
        throw new Error('Retention halted: unknown content folder in "' + file.name + '". No Drive files were deleted.');
      }
    }
    parsed.push({ file, entries, time: new Date(manifest.createdAt || file.modifiedTime || 0) });
  }

  const dayKey = (date) => date.toISOString().slice(0, 10);
  const weekKey = (date) => {
    const value = new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate()));
    value.setUTCDate(value.getUTCDate() + 4 - (value.getUTCDay() || 7));
    const yearStart = new Date(Date.UTC(value.getUTCFullYear(), 0, 1));
    return value.getUTCFullYear() + '-W' + String(Math.ceil((((value - yearStart) / 86400000) + 1) / 7)).padStart(2, '0');
  };
  const monthKey = (date) => date.toISOString().slice(0, 7);
  const keep = new Set();
  const takeLatestBuckets = (keyFn, count) => {
    const buckets = new Set();
    for (const item of parsed) {
      if (!Number.isFinite(item.time.getTime())) throw new Error('Retention halted: invalid recovery timestamp. No Drive files were deleted.');
      const key = keyFn(item.time);
      if (!buckets.has(key) && buckets.size < count) {
        buckets.add(key);
        keep.add(item.file.id);
      }
    }
  };
  takeLatestBuckets(dayKey, 7);
  takeLatestBuckets(weekKey, 4);
  takeLatestBuckets(monthKey, 3);
  keep.add(parsed[0].file.id);

  const referenced = new Set();
  for (const item of parsed) if (keep.has(item.file.id)) {
    for (const entry of item.entries) referenced.add(entry.fileId);
  }

  const objects = [];
  for (const folderName of GOOGLE_DRIVE_CONTENT_FOLDER_NAMES) {
    objects.push(...await listAll(folders[folderName].id));
  }
  const objectIds = new Set(objects.map((file) => file.id));
  for (const id of referenced) {
    if (!objectIds.has(id)) {
      throw new Error('Retention halted: a retained recovery point references a missing Drive object. No Drive files were deleted.');
    }
  }

  const objectDeletes = objects.filter((file) =>
    file.appProperties?.bankrollBackup === 'true' &&
    isManagedContentAddressedBackup(file.name || '') &&
    !referenced.has(file.id));
  const manifestDeletes = points.filter((file) => !keep.has(file.id));
  let deletedObjects = 0;
  let deletedManifests = 0;
  for (const file of objectDeletes) {
    await drive.files.delete({ fileId: file.id, supportsAllDrives: true });
    deletedObjects++;
  }
  for (const file of manifestDeletes) {
    await drive.files.delete({ fileId: file.id, supportsAllDrives: true });
    deletedManifests++;
  }
  return { kept: keep.size, deletedManifests, deletedObjects };
}

async function restoreFromGoogleDrive({ restoreDatabase = true, restoreVault = true, restoreCustomerDeliveries = true, recoveryPointId = null, recoveryAt = null } = {}) {
  if (!googleDriveIsConnected()) throw new Error('Google Drive is not connected; connect it with /drive connect first.');
  const { google } = require('googleapis');
  const drive = google.drive({ version: 'v3', auth: makeGoogleOAuthClient() });
  const folders = await resolveGoogleDriveFolders(drive);
  const manifests = []; let manifestPageToken;
  do {
    const page = await drive.files.list({
      q: "'" + folders.BACKUPS.id + "' in parents and (name = 'bankroll-drive-manifest.json' or name contains 'bankroll-recovery-') and trashed = false",
      pageSize: 1000, pageToken: manifestPageToken, orderBy: 'modifiedTime desc',
      fields: 'nextPageToken,files(id,name,modifiedTime)',
      supportsAllDrives: true, includeItemsFromAllDrives: true,
    });
    manifests.push(...(page.data.files || []));
    manifestPageToken = page.data.nextPageToken;
  } while (manifestPageToken);
  manifests.sort((a, b) => new Date(b.modifiedTime || 0) - new Date(a.modifiedTime || 0));
  if (!manifests.length) throw new Error('No Bankroll recovery manifest was found in Google Drive BANKROLL/BACKUPS.');
  const { selectRecoveryPoint } = require('./systems/recovery-selection');
  const selectedPoint = selectRecoveryPoint(
    manifests.map(item => ({ id: item.id, createdAt: item.modifiedTime })),
    { id: recoveryPointId, ...(recoveryAt ? { at: recoveryAt } : {}) }
  );
  if (!selectedPoint) throw new Error('Requested Google Drive recovery point does not exist.');
  const manifestResponse = await drive.files.get({ fileId: selectedPoint.id, alt: 'media' }, { responseType: 'stream' });
  const manifestChunks = [];
  for await (const chunk of manifestResponse.data) manifestChunks.push(Buffer.from(chunk));
  const manifest = JSON.parse(Buffer.concat(manifestChunks).toString('utf8'));
  if (!['bankroll-drive-layout-v2', 'bankroll-drive-layout-v3'].includes(manifest.format)) {
    throw new Error('This Google Drive recovery point uses an unsupported manifest format. Run /drive backup once to create a current verified recovery point.');
  }
  // Never restore from an unverified or internally inconsistent recovery point.
  // Check the entire snapshot before changing any local files.
  const { auditRecoveryManifest } = require('./systems/drive-integrity');
  const audit = await auditRecoveryManifest(drive, manifest);
  if (!audit.ok) {
    throw new Error('Recovery point failed integrity verification (' + audit.issues.length +
      ' issue(s)): ' + audit.issues.slice(0, 3).map(issue => issue.path + ': ' + issue.reason).join(', '));
  }
  let restoredDatabase = 0;
  let restoredVault = 0;
  let restoredCustomerDeliveries = 0;
  let skipped = 0;

  const downloadVerified = async (entry, destination) => {
    if (!entry?.fileId || !/^[a-f0-9]{64}$/i.test(String(entry.sha256 || '')) ||
        !Number.isSafeInteger(Number(entry.size)) || Number(entry.size) < 0) {
      throw new Error('Recovery manifest contains an invalid file entry.');
    }
    const temp = destination + '.drive-restore-' + crypto.randomBytes(6).toString('hex');
    await fs.promises.mkdir(path.dirname(destination), { recursive: true });
    try {
      const response = await drive.files.get({ fileId: entry.fileId, alt: 'media' }, { responseType: 'stream' });
      const hash = crypto.createHash('sha256');
      let size = 0;
      const meter = new Transform({
        transform(chunk, enc, callback) {
          size += chunk.length;
          hash.update(chunk);
          callback(null, chunk);
        },
      });
      await pipeline(response.data, meter, fs.createWriteStream(temp, { flags: 'wx' }));
      if (size !== Number(entry.size) || hash.digest('hex') !== entry.sha256) {
        throw new Error('SHA-256/size verification failed for ' + (entry.name || entry.path || 'recovery file'));
      }
      await fs.promises.rename(temp, destination);
    } catch (error) {
      await fs.promises.unlink(temp).catch(() => {});
      throw error;
    }
  };

  if (restoreDatabase && Array.isArray(manifest.databaseFiles)) {
    for (const entry of manifest.databaseFiles) {
      const name = String(entry.name || '');
      if (!/^[a-z0-9_.-]+\.json$/i.test(name)) { skipped++; continue; }
      const destination = path.join(DATA_DIR, name);
      let needsRestore = false;
      try {
        const stat = await fs.promises.stat(destination);
        needsRestore = !stat.isFile() || stat.size === 0;
      } catch { needsRestore = true; }
      if (!needsRestore) { skipped++; continue; }
      await downloadVerified(entry, destination);
      restoredDatabase++;
    }
  }

  const restoreStoredEntries = async (entries, counter) => {
    for (const entry of entries || []) {
      const rel = safeBackupRel(entry.path);
      if (!rel || !(rel.startsWith('vault/') || rel.startsWith('files/') || rel.startsWith('deliveries/'))) {
        skipped++;
        continue;
      }
      const destination = path.resolve(DATA_DIR, rel);
      let exists = false;
      try { exists = (await fs.promises.stat(destination)).isFile(); } catch {}
      if (exists) { skipped++; continue; }
      await downloadVerified(entry, destination);
      if (counter === 'vault') restoredVault++;
      else restoredCustomerDeliveries++;
    }
  };
  if (restoreVault) await restoreStoredEntries(manifest.vaultFiles, 'vault');
  if (restoreCustomerDeliveries) await restoreStoredEntries(manifest.customerDeliveryFiles, 'delivery');
  return {
    restoredDatabase, restoredVault, restoredCustomerDeliveries, skipped,
    manifestCreatedAt: manifest.createdAt || null,
  };
}


return { googleAuthBackupKey, encryptGoogleAuthBundle, decryptGoogleAuthBundle, persistGoogleAuthBundle, restoreGoogleAuthBundleFromMessages, googleRedirectUri, readGoogleClientConfig, makeGoogleOAuthClient, googleDriveIsConnected, listGoogleDriveFolders, resolveGoogleDriveFolders, uploadNonPaymentFileToDrive, recoverNonPaymentDatabaseFromDrive, hydrateNonPaymentCacheFromDrive, backupToGoogleDrive, applyGoogleDriveRetention, restoreFromGoogleDrive, GOOGLE_OAUTH_STATES };
};
