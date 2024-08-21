#!/usr/bin/node

import algosdk from 'algosdk';
import { existsSync } from 'fs';
import msgpack from 'algo-msgpack-with-bigint';
import stream from 'stream';
import DBStream from './db-stream.js';

function error(msg) { console.error(msg); process.exit(1) }

const dbPath = process.argv[2] ?? '/var/lib/algorand/fnet-v1/ledger.block.sqlite';
const start = process.argv[3] ? Number(process.argv[3]) : 0;

const sql = `select rnd, certdata from blocks where rnd > ${start} order by rnd`;

stream.pipeline(
    [
        new DBStream( { sql, db: dbPath } ),
        new stream.Transform( { objectMode: true, transform:( data, enc, cb ) => cb( null, print(data) ) } ),
        process.stdout
    ],
    err => { err && console.error(err); process.exit(0) }
)

function print(row) {
    const certdata = msgpack.decode(row.certdata);
    const propRaw = certdata.prop.oprop;
    const prop = algosdk.encodeAddress(propRaw);
    const { rnd } = row;
    return `${rnd},${prop}\n`;
}
