'use strict';

// Google Drive immutable snapshot storage for Bankroll.
// This module deliberately does not change the live storage mode by itself.
// Callers must flush every application save before snapshot(), and must restore
// before loading application state. See docs/GOOGLE-DRIVE-STORAGE.md.
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const { pipeline } = require('node:stream/promises');
const { Transform, Readable } = require('node:stream');

const FORMAT = 'bankroll-primary-snapshot-v1';
const MAX_FILE_BYTES = 150 * 1024 * 1024;
const EXCLUDED_DIRS = new Set(['tmp', 'logs', 'backups', 'node_modules']);
const EXCLUDED_FILES = new Set(['google-oauth.json', '.env']);

function sha256(buffer) {
  return crypto.createHash('sha256').update(buffer).digest('hex');
}
function safeRelativePath(value) {
  const input = String(value || '').replace(/\\/g, '/');
  if (!input || input.startsWith('/') || /^[a-z]:/i.test(input)) return null;
  const parts = input.split('/');
  if (parts.some((part) => !part || part === '.' || part === '..')) return null;
  return parts.join('/');
}
function isAllowedRelativePath(rel) {
  const safe = safeRelativePath(rel);
  if (!safe) return false;
  const parts = safe.split('/');
  if (parts.some((part) => EXCLUDED_DIRS.has(part.toLowerCase()))) return false;
  if (EXCLUDED_FILES.has(parts[parts.length - 1].toLowerCase())) return false;
  return true;
}
async function walkFiles(root, current = root, out = []) {
  const entries = await fs.promises.readdir(current, { withFileTypes: true });
  for (const entry of entries) {
    if (entry.isSymbolicLink()) continue;
    const absolute = path.join(current, entry.name);
    const rel = path.relative(root, absolute).split(path.sep).join('/');
    if (!isAllowedRelativePath(rel)) continue;
    if (entry.isDirectory()) await walkFiles(root, absolute, out);
    else if (entry.isFile()) {
      const stat = await fs.promises.stat(absolute);
      if (stat.size > MAX_FILE_BYTES) throw new Error('File exceeds the 150 MiB Google Drive snapshot limit: ' + rel);
      out.push({ absolute, relative: rel, size: stat.size });
    }
  }
  return out;
}
function createGoogleDrivePrimaryStorage({ google, auth, dataDir, folderIds, logger = console }) {
  if (!google || !auth || !dataDir || !folderIds?.DATABASE || !folderIds?.VAULT || !folderIds?.BACKUPS) {
    throw new Error('Google Drive storage requires googleapis, OAuth auth, DATA_DIR, and DATABASE/VAULT/BACKUPS folder IDs.');
  }
  const drive = google.drive({ version: 'v3', auth });
  let chain = Promise.resolve();
  const serial = (fn) => {
    const run = chain.then(fn, fn);
    chain = run.catch(() => {});
    return run;
  };
  const uploadObject = async (folderId, name, buffer, mimeType = 'application/octet-stream') => {
    const response = await drive.files.create({
      requestBody: {
        name,
        parents: [folderId],
        appProperties: { bankrollPrimary: 'true', sha256: sha256(buffer) },
      },
      media: { mimeType, body: Readable.from([buffer]) },
      fields: 'id,name,size,md5Checksum',
      supportsAllDrives: true,
    });
    if (!response.data?.id) throw new Error('Google Drive did not return an uploaded file ID for ' + name);
    return response.data.id;
  };
  const downloadObject = async (entry, stagingPath) => {
    if (!entry?.fileId || !/^[a-f0-9]{64}$/i.test(String(entry.sha256 || '')) ||
        !Number.isSafeInteger(Number(entry.size)) || Number(entry.size) < 0) {
      throw new Error('Snapshot contains an invalid file reference.');
    }
    await fs.promises.mkdir(path.dirname(stagingPath), { recursive: true });
    const hash = crypto.createHash('sha256');
    let size = 0;
    const meter = new Transform({
      transform(chunk, enc, callback) {
        size += chunk.length;
        hash.update(chunk);
        callback(null, chunk);
      },
    });
    const response = await drive.files.get({ fileId: entry.fileId, alt: 'media', supportsAllDrives: true }, { responseType: 'stream' });
    await pipeline(response.data, meter, fs.createWriteStream(stagingPath, { flags: 'wx' }));
    if (size !== Number(entry.size) || hash.digest('hex') !== entry.sha256) {
      await fs.promises.unlink(stagingPath).catch(() => {});
      throw new Error('Snapshot checksum or size verification failed for ' + entry.path);
    }
  };
  async function snapshot({ flush = async () => {} } = {}) {
    await flush();
    const root = path.resolve(dataDir);
    const files = await walkFiles(root);
    if (!files.length) throw new Error('Refusing to create an empty Bankroll snapshot.');
    const entries = [];
    for (const file of files) {
      const buffer = await fs.promises.readFile(file.absolute);
      const digest = sha256(buffer);
      const folderId = file.relative.startsWith('vault/') || file.relative.startsWith('files/')
        ? folderIds.VAULT : folderIds.DATABASE;
      const objectName = digest + '_' + path.basename(file.relative).replace(/[^\w.-]/g, '_');
      const fileId = await uploadObject(folderId, objectName, buffer);
      entries.push({ path: file.relative, fileId, size: buffer.length, sha256: digest });
    }
    const manifest = {
      format: FORMAT,
      generation: crypto.randomUUID(),
      createdAt: new Date().toISOString(),
      files: entries,
    };
    const manifestBuffer = Buffer.from(JSON.stringify(manifest, null, 2));
    const manifestId = await uploadObject(folderIds.BACKUPS,
      'bankroll-primary-' + manifest.createdAt.replace(/[:.]/g, '-') + '-' + manifest.generation + '.json',
      manifestBuffer, 'application/json');
    // Only publish the current pointer after every object and the immutable
    // manifest have uploaded successfully.
    const pointer = Buffer.from(JSON.stringify({
      format: FORMAT, manifestId, generation: manifest.generation,
      createdAt: manifest.createdAt, sha256: sha256(manifestBuffer),
    }, null, 2));
    await publishCurrentPointer(pointer);
    logger.info?.('[DriveStorage] Snapshot committed: ' + entries.length + ' files; generation ' + manifest.generation);
    return { generation: manifest.generation, manifestId, files: entries.length, createdAt: manifest.createdAt };
  }
  async function publishCurrentPointer(buffer) {
    const q = "name = 'bankroll-primary-current.json' and '" + folderIds.BACKUPS + "' in parents and trashed = false";
    const found = await drive.files.list({ q, pageSize: 10, fields: 'files(id)', supportsAllDrives: true, includeItemsFromAllDrives: true });
    const existing = found.data.files || [];
    if (existing.length > 1) throw new Error('Multiple current Drive snapshot pointers exist; refusing ambiguous promotion.');
    if (existing.length) {
      await drive.files.update({
        fileId: existing[0].id,
        requestBody: { appProperties: { bankrollPrimaryPointer: 'true', sha256: sha256(buffer) } },
        media: { mimeType: 'application/json', body: Readable.from([buffer]) },
        supportsAllDrives: true,
      });
    } else {
      await drive.files.create({
        requestBody: { name: 'bankroll-primary-current.json', parents: [folderIds.BACKUPS], appProperties: { bankrollPrimaryPointer: 'true', sha256: sha256(buffer) } },
        media: { mimeType: 'application/json', body: Readable.from([buffer]) },
        fields: 'id', supportsAllDrives: true,
      });
    }
  }
  async function readCurrentManifest() {
    const q = "name = 'bankroll-primary-current.json' and '" + folderIds.BACKUPS + "' in parents and trashed = false";
    const found = await drive.files.list({ q, pageSize: 10, fields: 'files(id)', supportsAllDrives: true, includeItemsFromAllDrives: true });
    const pointers = found.data.files || [];
    if (pointers.length !== 1) throw new Error(pointers.length ? 'Multiple current snapshot pointers exist.' : 'No current primary snapshot pointer exists.');
    const pointerResponse = await drive.files.get({ fileId: pointers[0].id, alt: 'media', supportsAllDrives: true });
    const pointerChunks = [];
    for await (const chunk of pointerResponse.data) pointerChunks.push(Buffer.from(chunk));
    const pointerBuffer = Buffer.concat(pointerChunks);
    const pointer = JSON.parse(pointerBuffer.toString('utf8'));
    if (pointer.format !== FORMAT || !pointer.manifestId || !/^[a-f0-9]{64}$/i.test(String(pointer.sha256 || ''))) {
      throw new Error('Current Drive snapshot pointer is invalid or unsupported.');
    }
    const manifestResponse = await drive.files.get({ fileId: pointer.manifestId, alt: 'media', supportsAllDrives: true });
    const manifestChunks = [];
    for await (const chunk of manifestResponse.data) manifestChunks.push(Buffer.from(chunk));
    const manifestBuffer = Buffer.concat(manifestChunks);
    if (sha256(manifestBuffer) !== pointer.sha256) throw new Error('Current Drive snapshot manifest checksum does not match its pointer.');
    const manifest = JSON.parse(manifestBuffer.toString('utf8'));
    if (manifest.format !== FORMAT || manifest.generation !== pointer.generation || !Array.isArray(manifest.files) || !manifest.files.length) {
      throw new Error('Current Drive snapshot manifest is malformed.');
    }
    for (const entry of manifest.files) {
      if (!isAllowedRelativePath(entry.path) || !entry.fileId || !/^[a-f0-9]{64}$/i.test(String(entry.sha256 || '')) ||
          !Number.isSafeInteger(Number(entry.size)) || Number(entry.size) < 0) throw new Error('Current Drive snapshot contains an unsafe or invalid file entry.');
    }
    return { pointer, manifest };
  }
  async function restore({ beforeApply = async () => {} } = {}) {
    return serial(async () => {
      const { pointer, manifest } = await readCurrentManifest();
      const root = path.resolve(dataDir);
      const staging = path.join(root, '.drive-restore-' + crypto.randomUUID());
      const prepared = [];
      try {
        // Download and verify every file before replacing any local state.
        for (const entry of manifest.files) {
          const relative = safeRelativePath(entry.path);
          const destination = path.resolve(root, relative);
          if (!destination.startsWith(root + path.sep)) throw new Error('Snapshot path escapes DATA_DIR.');
          const staged = path.join(staging, relative);
          await downloadObject(entry, staged);
          prepared.push({ staged, destination });
        }
        await beforeApply({ generation: pointer.generation, files: prepared.length });
        for (const item of prepared) {
          await fs.promises.mkdir(path.dirname(item.destination), { recursive: true });
          const temp = item.destination + '.drive-new-' + crypto.randomBytes(6).toString('hex');
          await fs.promises.copyFile(item.staged, temp);
          await fs.promises.rename(temp, item.destination);
        }
        logger.info?.('[DriveStorage] Restored generation ' + pointer.generation + ' (' + prepared.length + ' files).');
        return { generation: pointer.generation, files: prepared.length, createdAt: manifest.createdAt };
      } finally {
        await fs.promises.rm(staging, { recursive: true, force: true }).catch(() => {});
      }
    });
  }
  async function status() {
    const { pointer, manifest } = await readCurrentManifest();
    return { connected: true, generation: pointer.generation, createdAt: manifest.createdAt, files: manifest.files.length };
  }
  return { snapshot: (options) => serial(() => snapshot(options)), restore, status };
}
module.exports = { FORMAT, createGoogleDrivePrimaryStorage, safeRelativePath, isAllowedRelativePath, walkFiles };
