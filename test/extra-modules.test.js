'use strict';
const test=require('node:test');const assert=require('node:assert/strict');const AdmZip=require('adm-zip');const {zipSpawnCodes}=require('../systems/spawn-code-parser');const {sha256File}=require('../systems/file-hash');const {randomSearchGb}=require('../systems/send-progress');const fs=require('fs');const os=require('os');const path=require('path');const crypto=require('crypto');
test('spawn code reads vehicles.meta',()=>{const z=new AdmZip();z.addFile('data/vehicles.meta',Buffer.from('<modelName>stew_bear</modelName>'));assert.deepEqual(zipSpawnCodes(z),['stew_bear']);});
test('spawn code falls back to yft',()=>{const z=new AdmZip();z.addFile('stream/stew_bear_hi.yft',Buffer.from('x'));assert.deepEqual(zipSpawnCodes(z),['stew_bear']);});
test('SHA256 matches expected digest',async()=>{const p=path.join(os.tmpdir(),'bankroll-hash-'+crypto.randomBytes(6).toString('hex'));try{fs.writeFileSync(p,'abc');assert.equal(await sha256File(p),crypto.createHash('sha256').update('abc').digest('hex'));}finally{fs.rmSync(p,{force:true});}});
test('progress scan estimate has minimum',()=>{assert.ok(randomSearchGb(2)>=2);});
