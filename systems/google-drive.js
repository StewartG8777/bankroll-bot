'use strict';

// Google Drive OAuth, encrypted credential recovery, verified backup mirror,
// retention and restore. Runtime dependencies are injected from the entry point
// so the existing bot state and persistence queue remain the single source of truth.
module.exports = function createGoogleDriveSystem(deps) {
  const {
    fs, path, crypto, Buffer, Readable, Transform, pipeline, fetch,
    DATA_DIR, GOOGLE_OAUTH_FILE, GOOGLE_TOKEN_FILE, GOOGLE_DRIVE_FOLDER_NAMES,
    GOOGLE_AUTH_BACKUP_NAME, PUBLIC_URL, client, atomicWrite, resolveBackupChannel,
    relData, sha256FileCached, safeBackupRel,
  } = deps;

function googleAuthBackupKey() {
  const secret = process.env.BANKROLL_BACKUP_ENCRYPTION_KEY || process.env.DISCORD_TOKEN;
  if (!secret) throw new Error('Set BANKROLL_BACKUP_ENCRYPTION_KEY in Render to enable encrypted Google credential recovery.');
  return crypto.createHash('sha256').update(String(secret)).digest();
}
function encryptGoogleAuthBundle(bundle) {
  const iv = crypto.randomBytes(12);
  const cipher = crypto.createCipheriv('aes-256-gcm', googleAuthBackupKey(), iv);
  const encrypted = Buffer.concat([cipher.update(JSON.stringify(bundle), 'utf8'), cipher.final()]);
  return Buffer.from(JSON.stringify({ version: 1, iv: iv.toString('base64'), tag: cipher.getAuthTag().toString('base64'), data: encrypted.toString('base64') }), 'utf8');
}
function decryptGoogleAuthBundle(buffer) {
  const envelope = JSON.parse(buffer.toString('utf8'));
  if (envelope.version !== 1) throw new Error('Unsupported encrypted Google credential backup version.');
  const decipher = crypto.createDecipheriv('aes-256-gcm', googleAuthBackupKey(), Buffer.from(envelope.iv, 'base64'));
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

// Resolve the five existing folders by name; never write to a stale hard-coded folder ID.
async function resolveGoogleDriveFolders(drive) {
  const folders = {};
  for (const name of GOOGLE_DRIVE_FOLDER_NAMES) {
    const response = await drive.files.list({
      q: "name = '" + name + "' and mimeType = 'application/vnd.google-apps.folder' and trashed = false",
      pageSize: 100, fields: 'files(id,name,mimeType,capabilities(canAddChildren,canEdit))',
      supportsAllDrives: true, includeItemsFromAllDrives: true,
    });
    const matches = response.data.files || [];
    if (!matches.length) throw new Error('Google Drive folder "' + name + '" was not found. Make sure it exists and is shared with the connected Google account.');
    if (matches.length > 1) throw new Error('More than one Google Drive folder is named "' + name + '". Rename duplicates so Bankroll can safely select the correct folder.');
    const folder = matches[0];
    if (folder.capabilities?.canAddChildren === false) throw new Error('The connected Google account cannot add files to "' + name + '". Share that folder with the OAuth account as Editor.');
    folders[name] = folder;
  }
  return folders;
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
      const page = await drive.files.list({ q: "'" + folders[name].id + "' in parents and trashed = false", pageSize: 1000, pageToken, fields: 'nextPageToken,files(id,name,appProperties)', supportsAllDrives: true, includeItemsFromAllDrives: true });
      for (const f of (page.data.files || [])) existing.set(f.name, f);
      pageToken = page.data.nextPageToken;
    } while (pageToken);
    existingByFolder.set(name, existing);
  }
  let uploaded = 0;
  const uploadOne = async (folderName, name, signature, body, mimeType) => {
    const existing = existingByFolder.get(folderName);
    const old = existing.get(name);
    if (old?.appProperties?.bankrollSignature === signature) return;
    const requestBody = { name, appProperties: { bankrollSignature: signature, bankrollBackup: 'true' } };
    const media = { mimeType: mimeType || 'application/octet-stream', body };
    try {
      if (old?.id) await drive.files.update({ fileId: old.id, requestBody, media, fields: 'id,name' });
      else { const created = await drive.files.create({ requestBody: { ...requestBody, parents: [folders[folderName].id] }, media, fields: 'id,name' }); existing.set(name, created.data); }
    } catch (e) {
      if (/insufficient permissions|not have permission|forbidden/i.test(String(e.message || e))) throw new Error('Google Drive cannot write to "' + folderName + '". Share that folder with the connected Google account as Editor. Google said: ' + String(e.message || e));
      throw e;
    }
    uploaded++;
    const saved = existing.get(name);
    console.log('Google Drive backup uploaded to ' + folderName + ': ' + name);
    return saved?.id || null;
  };
  const databaseEntries = [];
  for (const file of databaseFiles || []) {
    const sig = crypto.createHash('sha256').update(file.buffer).digest('hex');
    const driveName = crypto.createHash('sha256').update(file.name + ':' + sig).digest('hex').slice(0, 24) + '_' + file.name;
    const fileId = await uploadOne('DATABASE', driveName, sig, Readable.from([file.buffer]), 'application/json');
    const current = existingByFolder.get('DATABASE').get(driveName);
    databaseEntries.push({ name: file.name, driveName, fileId: fileId || current?.id, size: file.buffer.length, sha256: sig });
  }
  let vaultFileCount = 0;
  const vaultEntries = [];
  for (const fp of storedPaths || []) {
    if (!fp || !fs.existsSync(fp)) continue;
    const rel = relData(fp); if (!rel) continue;
    const st = await fs.promises.stat(fp); if (!st.isFile() || !st.size) continue;
    if (st.size > 150 * 1024 * 1024) throw new Error('Google Drive backup file exceeds 150 MiB: ' + rel);
    const sig = await sha256FileCached(fp, st);
    const key = crypto.createHash('sha256').update(rel).digest('hex').slice(0, 16);
    const name = key + '_' + path.basename(rel).replace(/[^\w.\-]/g, '_');
    const driveName = key + '_' + sig.slice(0, 24) + '_' + path.basename(rel).replace(/[^\w.\-]/g, '_');
    const fileId = await uploadOne('VAULT', driveName, st.size + ':' + sig, fs.createReadStream(fp));
    const current = existingByFolder.get('VAULT').get(driveName);
    vaultEntries.push({ path: rel, name, driveName, fileId: fileId || current?.id, size: st.size, sha256: sig });
    vaultFileCount++;
  }
  const manifest = { format: 'bankroll-drive-layout-v2', createdAt: new Date().toISOString(), databaseFiles: databaseEntries, vaultFiles: vaultEntries, vaultFileCount, folders: Object.fromEntries(GOOGLE_DRIVE_FOLDER_NAMES.map((name) => [name, folders[name].id])) };
  const manifestBuffer = Buffer.from(JSON.stringify(manifest, null, 2));
  const manifestSig = crypto.createHash('sha256').update(manifestBuffer).digest('hex');
  const pointName = 'bankroll-recovery-' + new Date().toISOString().replace(/[:.]/g, '-') + '.json';
  await uploadOne('BACKUPS', pointName, manifestSig, Readable.from([manifestBuffer]), 'application/json');
  // Keep a compatibility pointer for existing status/restore code; versioned points
  // remain immutable and can be used for dated recovery once retention is enabled.
  await uploadOne('BACKUPS', 'bankroll-drive-manifest.json', manifestSig, Readable.from([manifestBuffer]), 'application/json');
  let retention = null;
  try { retention = await applyGoogleDriveRetention(drive, folders); console.log('Google Drive retention completed:', retention); }
  catch (retentionError) { console.error('Google Drive retention safely skipped; no incomplete cleanup was applied:', retentionError.message); }
  console.log('Google Drive backup complete: ' + uploaded + ' changed file(s) uploaded across the Bankroll folders.');
  return { connected: true, uploaded, folders, retention };
}

/**
 * Safe Drive recovery-point retention.
 * Keep the newest point for each of the 7 latest UTC days, 4 latest UTC weeks
 * and 3 latest UTC months. Delete only content-addressed objects proven unused
 * by every retained, successfully parsed v2 manifest. Any uncertainty aborts
 * the entire cleanup before the first delete.
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
  const points = backupFiles.filter((f) => /^bankroll-recovery-.*\.json$/i.test(f.name || ''))
    .sort((a, b) => new Date(b.modifiedTime || 0) - new Date(a.modifiedTime || 0));
  if (!points.length) return { kept: 0, deletedManifests: 0, deletedObjects: 0, skipped: 'No versioned recovery points' };

  const parsed = [];
  for (const file of points) {
    const response = await drive.files.get({ fileId: file.id, alt: 'media' }, { responseType: 'stream' });
    const chunks = [];
    for await (const chunk of response.data) chunks.push(Buffer.from(chunk));
    const manifest = JSON.parse(Buffer.concat(chunks).toString('utf8'));
    if (manifest.format !== 'bankroll-drive-layout-v2' ||
        !Array.isArray(manifest.databaseFiles) || !Array.isArray(manifest.vaultFiles)) {
      throw new Error('Retention halted: recovery manifest "' + file.name + '" is malformed or unsupported. No Drive files were deleted.');
    }
    for (const entry of [...manifest.databaseFiles, ...manifest.vaultFiles]) {
      if (!entry?.fileId || !/^[a-f0-9]{64}$/i.test(String(entry.sha256 || '')) ||
          !Number.isSafeInteger(Number(entry.size)) || Number(entry.size) < 0) {
        throw new Error('Retention halted: invalid object reference in "' + file.name + '". No Drive files were deleted.');
      }
    }
    parsed.push({ file, manifest, time: new Date(manifest.createdAt || file.modifiedTime || 0) });
  }
  const dayKey = (d) => d.toISOString().slice(0, 10);
  const weekKey = (d) => {
    const x = new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()));
    x.setUTCDate(x.getUTCDate() + 4 - (x.getUTCDay() || 7));
    const yearStart = new Date(Date.UTC(x.getUTCFullYear(), 0, 1));
    return x.getUTCFullYear() + '-W' + String(Math.ceil((((x - yearStart) / 86400000) + 1) / 7)).padStart(2, '0');
  };
  const monthKey = (d) => d.toISOString().slice(0, 7);
  const keep = new Set();
  const takeLatestBuckets = (keyFn, count) => {
    const buckets = new Set();
    for (const item of parsed) {
      if (!Number.isFinite(item.time.getTime())) throw new Error('Retention halted: invalid recovery timestamp. No Drive files were deleted.');
      const key = keyFn(item.time);
      if (!buckets.has(key) && buckets.size < count) { buckets.add(key); keep.add(item.file.id); }
    }
  };
  takeLatestBuckets(dayKey, 7);
  takeLatestBuckets(weekKey, 4);
  takeLatestBuckets(monthKey, 3);
  keep.add(parsed[0].file.id);

  const referenced = new Set();
  for (const item of parsed) if (keep.has(item.file.id)) {
    for (const entry of [...item.manifest.databaseFiles, ...item.manifest.vaultFiles]) referenced.add(entry.fileId);
  }
  // Validate every retained reference still exists before allowing any deletion.
  const dbFiles = await listAll(folders.DATABASE.id);
  const vaultFiles = await listAll(folders.VAULT.id);
  const objects = [...dbFiles, ...vaultFiles];
  const objectIds = new Set(objects.map((f) => f.id));
  for (const id of referenced) if (!objectIds.has(id)) {
    throw new Error('Retention halted: a retained recovery point references a missing Drive object. No Drive files were deleted.');
  }

  // Plan all deletes before executing. Only our clearly content-addressed objects
  // are eligible; legacy names and unknown files are always preserved.
  const objectDeletes = objects.filter((f) =>
    f.appProperties?.bankrollBackup === 'true' &&
    (/^[a-f0-9]{24}_[a-f0-9]{64}_/.test(f.name || '') ||
     /^[a-f0-9]{16}_[a-f0-9]{24}_/.test(f.name || '')) &&
    !referenced.has(f.id));
  const manifestDeletes = points.filter((f) => !keep.has(f.id));
  let deletedObjects = 0, deletedManifests = 0;
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

async function restoreFromGoogleDrive({ restoreDatabase = true, restoreVault = true } = {}) {
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
  if (!manifests.length) throw new Error('No Bankroll recovery manifest was found in Google Drive BACKUPS.');
  const manifestResponse = await drive.files.get({ fileId: manifests[0].id, alt: 'media' }, { responseType: 'stream' });
  const manifestChunks = [];
  for await (const chunk of manifestResponse.data) manifestChunks.push(Buffer.from(chunk));
  const manifest = JSON.parse(Buffer.concat(manifestChunks).toString('utf8'));
  if (manifest.format !== 'bankroll-drive-layout-v2') throw new Error('This Google Drive recovery point uses the legacy manifest format. Run /drive backup once to create a verified v2 recovery manifest.');
  let restoredDatabase = 0, restoredVault = 0, skipped = 0;
  const downloadVerified = async (entry, destination) => {
    if (!entry?.fileId || !/^[a-f0-9]{64}$/i.test(String(entry.sha256 || '')) || !Number.isSafeInteger(Number(entry.size)) || Number(entry.size) < 0) throw new Error('Recovery manifest contains an invalid file entry.');
    const temp = destination + '.drive-restore-' + crypto.randomBytes(6).toString('hex');
    await fs.promises.mkdir(path.dirname(destination), { recursive: true });
    try {
      const response = await drive.files.get({ fileId: entry.fileId, alt: 'media' }, { responseType: 'stream' });
      const hash = crypto.createHash('sha256');
      let size = 0;
      const meter = new Transform({ transform(chunk, enc, callback) { size += chunk.length; hash.update(chunk); callback(null, chunk); } });
      await pipeline(response.data, meter, fs.createWriteStream(temp, { flags: 'wx' }));
      const actual = hash.digest('hex');
      if (size !== Number(entry.size) || actual !== entry.sha256) throw new Error('SHA-256/size verification failed for ' + (entry.name || entry.path || 'recovery file'));
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
      try { const st = await fs.promises.stat(destination); needsRestore = !st.isFile() || st.size === 0; }
      catch { needsRestore = true; }
      if (!needsRestore) { skipped++; continue; }
      await downloadVerified(entry, destination);
      restoredDatabase++;
    }
  }
  if (restoreVault && Array.isArray(manifest.vaultFiles)) {
    for (const entry of manifest.vaultFiles) {
      const rel = safeBackupRel(entry.path);
      if (!rel || !(rel.startsWith('vault/') || rel.startsWith('files/'))) { skipped++; continue; }
      const destination = path.resolve(DATA_DIR, rel);
      let exists = false;
      try { exists = (await fs.promises.stat(destination)).isFile(); } catch {}
      if (exists) { skipped++; continue; }
      await downloadVerified(entry, destination);
      restoredVault++;
    }
  }
  return { restoredDatabase, restoredVault, skipped, manifestCreatedAt: manifest.createdAt || null };
}

  return {
    googleAuthBackupKey, encryptGoogleAuthBundle, decryptGoogleAuthBundle,
    persistGoogleAuthBundle, restoreGoogleAuthBundleFromMessages,
    googleRedirectUri, readGoogleClientConfig, makeGoogleOAuthClient,
    googleDriveIsConnected, resolveGoogleDriveFolders, backupToGoogleDrive,
    applyGoogleDriveRetention, restoreFromGoogleDrive,
  };
};
