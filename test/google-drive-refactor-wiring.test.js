'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
test('extracted Google Drive module uses local module paths and injected runtime state',()=>{
  const source=fs.readFileSync(path.join(__dirname,'../systems/google-drive.js'),'utf8');
  assert.equal(source.includes("require('./systems/"),false);
  for(const name of ['vault','deliveries','downloads','sendJobs','downloadHistory','savedPresence','notificationHistory','wipedCustomers']){
    assert.ok(source.includes('state.'+name),'missing state ref '+name);
  }
  const index=fs.readFileSync(path.join(__dirname,'../index.js'),'utf8');
  assert.ok(index.includes('createGoogleDriveSystem({ state: {'),'runtime state injected from entry point');
});
