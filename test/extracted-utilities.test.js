'use strict';
const test=require('node:test');const assert=require('node:assert/strict');const {periodBounds,londonParts}=require('../systems/london-time');const {parseExpiresToMinutes}=require('../systems/invoice-time');
test('invoice expiration units',()=>{assert.equal(parseExpiresToMinutes('2h'),120);assert.equal(parseExpiresToMinutes('1 day'),1440);assert.equal(parseExpiresToMinutes('45'),45);assert.equal(parseExpiresToMinutes('invalid'),null);});
test('London date helper returns components',()=>{const p=londonParts(Date.UTC(2026,0,1,12));assert.equal(p.y,2026);assert.equal(p.m,1);assert.equal(p.d,1);});
test('all period is unbounded',()=>{assert.deepEqual(periodBounds('all'),{since:null,until:null});});
