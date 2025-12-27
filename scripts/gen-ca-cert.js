import { Buffer } from 'buffer';
import { createPrivateKey, createPublicKey, sign, createHash } from 'crypto';

function die(msg) {
  process.stderr.write(String(msg || 'error') + '\n');
  process.exit(1);
}

const keyPem = [];
for await (const chunk of process.stdin) keyPem.push(chunk);
const keyData = Buffer.concat(keyPem).toString('utf8').trim();
if (!keyData.includes('BEGIN PRIVATE KEY')) die('expected PEM private key on stdin');

const privateKey = createPrivateKey(keyData);
const publicKey = createPublicKey(privateKey);
const pubDer = publicKey.export({ type: 'spki', format: 'der' });
const pubKeyBits = pubDer.slice(-32);

function encodeLen(len) {
  if (len < 128) return Buffer.from([len]);
  if (len < 256) return Buffer.from([0x81, len]);
  return Buffer.from([0x82, (len >> 8) & 0xff, len & 0xff]);
}

function seq(items) {
  const c = Buffer.concat(items);
  return Buffer.concat([Buffer.from([0x30]), encodeLen(c.length), c]);
}

function set(items) {
  const c = Buffer.concat(items);
  return Buffer.concat([Buffer.from([0x31]), encodeLen(c.length), c]);
}

function oid(o) {
  const p = o.split('.').map(Number);
  const b = [p[0] * 40 + p[1]];
  for (let i = 2; i < p.length; i++) {
    let v = p[i];
    if (v === 0) { b.push(0); continue; }
    const e = [];
    while (v > 0) { e.unshift(v & 0x7f); v >>= 7; }
    for (let j = 0; j < e.length - 1; j++) e[j] |= 0x80;
    b.push(...e);
  }
  return Buffer.concat([Buffer.from([0x06, b.length]), Buffer.from(b)]);
}

function utf8(s) {
  const b = Buffer.from(s, 'utf8');
  return Buffer.concat([Buffer.from([0x0c]), encodeLen(b.length), b]);
}

function int(v) {
  if (typeof v === 'number') {
    if (v === 0) return Buffer.from([0x02, 0x01, 0x00]);
    const b = [];
    while (v > 0) { b.unshift(v & 0xff); v >>= 8; }
    if (b[0] & 0x80) b.unshift(0);
    return Buffer.concat([Buffer.from([0x02, b.length]), Buffer.from(b)]);
  }
  const buf = Buffer.isBuffer(v) ? v : Buffer.from(v);
  const pad = buf[0] & 0x80;
  const c = pad ? Buffer.concat([Buffer.from([0]), buf]) : buf;
  return Buffer.concat([Buffer.from([0x02]), encodeLen(c.length), c]);
}

function bits(buf) {
  return Buffer.concat([Buffer.from([0x03]), encodeLen(buf.length + 1), Buffer.from([0]), buf]);
}

function utc(d) {
  const s = [d.getUTCFullYear() % 100, d.getUTCMonth() + 1, d.getUTCDate(),
             d.getUTCHours(), d.getUTCMinutes(), d.getUTCSeconds()]
             .map(x => String(x).padStart(2, '0')).join('') + 'Z';
  return Buffer.concat([Buffer.from([0x17, s.length]), Buffer.from(s, 'ascii')]);
}

function genTime(d) {
  const s = String(d.getUTCFullYear()) + 
            [d.getUTCMonth() + 1, d.getUTCDate(), d.getUTCHours(), d.getUTCMinutes(), d.getUTCSeconds()]
            .map(x => String(x).padStart(2, '0')).join('') + 'Z';
  return Buffer.concat([Buffer.from([0x18, s.length]), Buffer.from(s, 'ascii')]);
}

function oct(buf) {
  return Buffer.concat([Buffer.from([0x04]), encodeLen(buf.length), buf]);
}

function ctx(tag, content) {
  return Buffer.concat([Buffer.from([0xa0 | tag]), encodeLen(content.length), content]);
}

function bool(v) {
  return Buffer.from([0x01, 0x01, v ? 0xff : 0x00]);
}

const serial = Buffer.from([0x01]);
const notBefore = new Date(Date.UTC(2024, 0, 1, 0, 0, 0));
const notAfter = new Date(Date.UTC(2049, 11, 31, 23, 59, 59));
const cn = 'znode-bridge-ca';

const rdnCn = seq([oid('2.5.4.3'), utf8(cn)]);
const name = seq([set([rdnCn])]);
const validity = seq([utc(notBefore), genTime(notAfter)]);
const alg = seq([oid('1.3.101.112')]);
const spki = seq([alg, bits(pubKeyBits)]);

const bcVal = seq([bool(true)]);
const bc = seq([oid('2.5.29.19'), bool(true), oct(bcVal)]);

const kuBits = Buffer.from([0x01, 0x06]);
const ku = seq([oid('2.5.29.15'), bool(true), oct(Buffer.concat([Buffer.from([0x03, 0x02]), kuBits]))]);

const skiHash = createHash('sha1').update(pubKeyBits).digest();
const skiExt = seq([oid('2.5.29.14'), oct(oct(skiHash))]);

const exts = ctx(3, seq([bc, ku, skiExt]));

const tbs = seq([
  ctx(0, int(2)),
  int(serial),
  alg,
  name,
  validity,
  name,
  spki,
  exts
]);

const sig = sign(null, tbs, privateKey);
const cert = seq([tbs, alg, bits(sig)]);

const b64 = cert.toString('base64');
const lines = b64.match(/.{1,64}/g) || [];
process.stdout.write('-----BEGIN CERTIFICATE-----\n' + lines.join('\n') + '\n-----END CERTIFICATE-----\n');
