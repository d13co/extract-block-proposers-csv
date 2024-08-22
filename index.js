#!/usr/bin/node

import algosdk from 'algosdk';
import { existsSync } from 'fs';
import msgpack from 'algo-msgpack-with-bigint';
import stream from 'stream';
import DBStream from './db-stream.js';

function error(msg) { console.error(msg); process.exit(1) }

const dbPath = process.argv[2] ?? '/var/lib/algorand/fnet-v1/ledger.block.sqlite';
const start = process.argv[3] ? Number(process.argv[3]) : 0;

const sql = `select rnd, hdrdata, certdata from blocks where rnd > ${start} order by rnd`;

console.log("rnd,proposer,payout");

stream.pipeline(
    [
        new DBStream( { sql, db: dbPath } ),
        new stream.Transform( { objectMode: true, transform:( data, enc, cb ) => cb( null, print(data) ) } ),
        process.stdout
    ],
    err => { err && console.error(err); }
)

function print(row) {
    const { prop: { oprop: propRaw } } = msgpack.decode(row.certdata);
    const prop = algosdk.encodeAddress(propRaw);
    const { pp = '' } = msgpack.decode(row.hdrdata);
    const { rnd } = row;
    return `${rnd},${prop},${pp}\n`;
}
