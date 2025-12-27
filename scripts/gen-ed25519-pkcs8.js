import { Buffer } from 'buffer';

function die(msg) {
  process.stderr.write(String(msg || 'error') + '\n');
  process.exit(1);
}

const seedHex = (process.argv[2] || '').trim();
if (!/^[0-9a-fA-F]{64}$/.test(seedHex)) die('expected 32-byte hex seed');
const seed = Buffer.from(seedHex, 'hex');

const der = Buffer.concat([
  Buffer.from([0x30, 0x2e, 0x02, 0x01, 0x00, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x04, 0x22, 0x04, 0x20]),
  seed,
]);

const b64 = der.toString('base64');
const lines = b64.match(/.{1,64}/g) || [];
process.stdout.write(`-----BEGIN PRIVATE KEY-----\n${lines.join('\n')}\n-----END PRIVATE KEY-----\n`);
