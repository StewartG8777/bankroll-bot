'use strict';
const test=require('node:test');
const assert=require('node:assert/strict');
const {Readable}=require('node:stream');
const {runDriveSelftest}=require('../systems/drive-selftest');
test('Drive selftest uses isolated object, verifies hash and cleans up',async()=>{
  let uploaded=null,deleted=false;
  const drive={
    about:{get:async()=>({data:{user:{emailAddress:'test@example.invalid'}}})},
    files:{
      get:async({fileId,alt})=>alt==='media'
        ?{data:Readable.from([uploaded])}
        :{data:{id:fileId,mimeType:'application/vnd.google-apps.folder',trashed:false}},
      create:async({media})=>{
        const chunks=[];for await(const part of media.body)chunks.push(Buffer.from(part));
        uploaded=Buffer.concat(chunks);return {data:{id:'test-object'}};
      },
      delete:async({fileId})=>{assert.equal(fileId,'test-object');deleted=true;}
    }
  };
  const folders=Object.fromEntries(['DATABASE','VAULT','BACKUPS','CUSTOMER DELIVERIES','PAYMENT RECORDS','LOGS'].map(x=>[x,x]));
  const result=await runDriveSelftest({drive,folderIds:folders,primaryContext:null});
  assert.equal(result.failures,0);
  assert.equal(deleted,true);
  assert.match(result.report,/SHA-256/);
  assert.match(result.report,/NOT RUN/);
  assert.match(result.report,/NOT CERTIFIED/);
});
test('Drive selftest reports failures and skips download when upload fails',async()=>{
  const drive={
    about:{get:async()=>{throw Error('auth denied')}},
    files:{get:async()=>{throw Error('folder inaccessible')},create:async()=>{throw Error('upload denied')},delete:async()=>{throw Error('must not delete')}}
  };
  const result=await runDriveSelftest({drive,folderIds:{BACKUPS:'backup'},primaryContext:null});
  assert.ok(result.failures>=3);
  assert.match(result.report,/FAIL/);
  assert.match(result.report,/No temporary object created/);
});
