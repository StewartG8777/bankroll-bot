'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs/promises');
const path = require('node:path');
const os = require('node:os');
const { Readable } = require('node:stream');
const { createGoogleDrivePrimaryStorage } = require('../systems/google-drive-primary-storage');
const { createSerializedStorageQueue } = require('../systems/serialized-storage-queue');
const { verifiedOfflineRollback } = require('../systems/verified-drive-rollback');

const folders = Object.freeze({
  DATABASE: 'folder-db', VAULT: 'folder-vault', BACKUPS: 'folder-backups',
  'CUSTOMER DELIVERIES': 'folder-deliveries', 'PAYMENT RECORDS': 'folder-payments',
  LOGS: 'folder-logs', SYSTEM: 'folder-system',
  'TEMPORARY PROCESSING': 'folder-temporary'
});
function fakeDrive() {
  const items = new Map();
  let next = 0;
  async function bytesFrom(stream) {
    const chunks = [];
    for await (const chunk of stream) chunks.push(Buffer.from(chunk));
    return Buffer.concat(chunks);
  }
  const drive = {files: {
    async create({ requestBody, media }) {
      const id = 'file-' + (++next);
      items.set(id, { id, name: requestBody.name, folder: requestBody.parents[0],
        bytes: await bytesFrom(media.body), createdTime: new Date().toISOString() });
      return {data:{id}};
    },
    async update({fileId, media}) {
      const found = items.get(fileId);
      if (!found) throw new Error('Drive file not found: ' + fileId);
      found.bytes = await bytesFrom(media.body);
      return {data:{id:fileId}};
    },
    async list({q}) {
      const parent = /'([^']+)' in parents/.exec(q)?.[1];
      const exact = /name = '([^']+)'/.exec(q)?.[1];
      const contains = /name contains '([^']+)'/.exec(q)?.[1];
      const files = [...items.values()].filter(f =>
        (!parent || f.folder === parent) && (!exact || f.name === exact) &&
        (!contains || f.name.includes(contains))
      ).map(({id,name,createdTime})=>({id,name,createdTime}));
      return {data:{files}};
    },
    async get({fileId}) {
      const f = items.get(fileId);
      if (!f) throw new Error('Drive file not found: ' + fileId);
      return {data:Readable.from([f.bytes])};
    }
  }};
  return { drive, items };
}
async function setup(t) {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'bankroll-drive-test-'));
  t.after(async () => { await fs.rm(root,{recursive:true,force:true}); });
  await fs.mkdir(path.join(root,'vault'),{recursive:true});
  await fs.mkdir(path.join(root,'files'),{recursive:true});
  await fs.mkdir(path.join(root,'logs'),{recursive:true});
  await fs.writeFile(path.join(root,'vault','car.bin'),'original car');
  await fs.writeFile(path.join(root,'files','buyer.bin'),'delivered');
  await fs.writeFile(path.join(root,'monzo_payments.json'),JSON.stringify({paid:true}));
  await fs.writeFile(path.join(root,'orders.json'),JSON.stringify({orders:1}));
  await fs.writeFile(path.join(root,'notifications.json'),JSON.stringify({count:1}));
  await fs.writeFile(path.join(root,'google-drive-token.json'),'secret; should not upload');
  await fs.writeFile(path.join(root,'.env'),'secret; should not upload');
  const fake=fakeDrive();
  const store=createGoogleDrivePrimaryStorage({
    google:{drive:()=>fake.drive},auth:{},dataDir:root,folderIds:folders,logger:{info(){},warn(){}}
  });
  return {root,store,...fake};
}
test('end-to-end snapshot and restore preserve eight-folder routing and SHA-256', async (t) => {
  const {root,store,items} = await setup(t);
  const first=await store.snapshot();
  assert.equal(first.files,5);
  const objectFiles=[...items.values()];
  assert.equal(objectFiles.find(f=>f.name.endsWith('_car.bin')).folder,folders.VAULT);
  assert.equal(objectFiles.find(f=>f.name.endsWith('_buyer.bin')).folder,folders['CUSTOMER DELIVERIES']);
  assert.equal(objectFiles.find(f=>f.name.endsWith('_monzo_payments.json')).folder,folders['PAYMENT RECORDS']);
  assert.equal(objectFiles.find(f=>f.name.endsWith('_orders.json')).folder,folders.DATABASE);
  assert.equal(objectFiles.find(f=>f.name.endsWith('_notifications.json')).folder,folders.LOGS);
  assert.equal(objectFiles.filter(f=>f.name.includes('google-drive-token.json')).length,0);
  await fs.writeFile(path.join(root,'vault','car.bin'),'broken on disk');
  const restored=await store.restore();
  assert.equal(restored.files,5);
  assert.equal(await fs.readFile(path.join(root,'vault','car.bin'),'utf8'),'original car');
  assert.equal((await store.status()).generation,first.generation);
});
test('restore rejects tampered Drive object before replacing ANY local files', async (t) => {
  const {root,store,items}=await setup(t);
  await store.snapshot();
  const file=[...items.values()].find(f=>f.name.endsWith('_buyer.bin'));
  file.bytes=Buffer.from('tampered bytes');
  await fs.writeFile(path.join(root,'vault','car.bin'),'retain local modified car');
  await assert.rejects(store.restore(),/verification failed/);
  assert.equal(await fs.readFile(path.join(root,'vault','car.bin'),'utf8'),'retain local modified car');
});
test('offline verified rollback creates safety point, restores older data and moves current pointer',async (t)=>{
  const {root,store}=await setup(t);
  const older=await store.snapshot();
  await fs.writeFile(path.join(root,'orders.json'),JSON.stringify({orders:2}));
  const newer=await store.snapshot();
  assert.notEqual(older.generation,newer.generation);
  const result=await verifiedOfflineRollback({
    queue:createSerializedStorageQueue(),store,generation:older.generation,confirm:true,
    assertStopped:async()=>true
  });
  assert.equal((await store.status()).generation,older.generation);
  assert.equal(JSON.parse(await fs.readFile(path.join(root,'orders.json'),'utf8')).orders,1);
  assert.equal(result.verifiedFiles,5);
  assert.ok((await store.listGenerations()).some(x=>x.generation===result.safetyGeneration));
});
test('rollback refuses a corrupt generation without changing current pointer or local records',async(t)=>{
  const {root,store,items}=await setup(t);
  const first=await store.snapshot();
  await fs.writeFile(path.join(root,'orders.json'),JSON.stringify({orders:2}));
  const second=await store.snapshot();
  const oldBlob=[...items.values()].find(f=>f.name.endsWith('_orders.json')&&f.bytes.toString().includes('"orders":1'));
  oldBlob.bytes=Buffer.from('corrupt');
  await assert.rejects(verifiedOfflineRollback({
    queue:createSerializedStorageQueue(),store,generation:first.generation,confirm:true,assertStopped:async()=>true
  }),/verification failed/);
  assert.equal((await store.status()).generation,second.generation);
  assert.equal(JSON.parse(await fs.readFile(path.join(root,'orders.json'),'utf8')).orders,2);
});
