import fs from 'fs';
import path from 'path';
import { ethers } from 'ethers';
function getBridgeDataDir() {
  const homeDir = process.env.HOME || process.cwd();
  const dataDir = process.env.BRIDGE_DATA_DIR || path.join(homeDir, '.znode-bridge');
  try { fs.mkdirSync(dataDir, { recursive: true, mode: 0o700 }); } catch {}
  return dataDir;
}
function getTxsetDir() {
  const dir = path.join(getBridgeDataDir(), 'withdrawal_txsets');
  try { fs.mkdirSync(dir, { recursive: true, mode: 0o700 }); } catch {}
  return dir;
}
function isTxsetHash(hash) {
  return typeof hash === 'string' && /^0x[0-9a-fA-F]{64}$/.test(hash);
}
function getTxsetPath(txsetHash) {
  const h = String(txsetHash || '').toLowerCase();
  return path.join(getTxsetDir(), `${h.slice(2)}.txt`);
}
export function computeTxsetHash(txDataHex) {
  if (typeof txDataHex !== 'string' || !txDataHex) return null;
  try {
    return ethers.keccak256(ethers.toUtf8Bytes(txDataHex));
  } catch {
    return null;
  }
}
export function putTxset(node, txsetHash, txDataHex) {
  const h = typeof txsetHash === 'string' ? txsetHash.toLowerCase() : '';
  if (!isTxsetHash(h)) return { ok: false, reason: 'invalid_txset_hash' };
  if (typeof txDataHex !== 'string' || !txDataHex) return { ok: false, reason: 'missing_txset' };
  const filePath = getTxsetPath(h);
  const tmpPath = `${filePath}.tmp.${process.pid}.${Date.now()}`;
  try {
    fs.writeFileSync(tmpPath, txDataHex, { mode: 0o600 });
    fs.renameSync(tmpPath, filePath);
  } catch (e) {
    try { fs.unlinkSync(tmpPath); } catch {}
    return { ok: false, reason: e && e.message ? e.message : String(e) };
  }
  if (node) {
    if (!node._withdrawalTxsetIndex) node._withdrawalTxsetIndex = new Map();
    node._withdrawalTxsetIndex.set(h, { path: filePath, size: txDataHex.length, createdAt: Date.now() });
  }
  return { ok: true, path: filePath, size: txDataHex.length };
}
export function getTxset(node, txsetHash) {
  const h = typeof txsetHash === 'string' ? txsetHash.toLowerCase() : '';
  if (!isTxsetHash(h)) return null;
  const fromIndex = node && node._withdrawalTxsetIndex ? node._withdrawalTxsetIndex.get(h) : null;
  const filePath = fromIndex && fromIndex.path ? fromIndex.path : getTxsetPath(h);
  try {
    return fs.readFileSync(filePath, 'utf8');
  } catch {
    return null;
  }
}
function getExpectedMap(node) {
  if (!node._withdrawalExpectedSignSteps) node._withdrawalExpectedSignSteps = new Map();
  return node._withdrawalExpectedSignSteps;
}
function getResultsMap(node) {
  if (!node._withdrawalSignStepResults) node._withdrawalSignStepResults = new Map();
  return node._withdrawalSignStepResults;
}
function makeSignStepKey(withdrawalKey, stepIndex, signer) {
  const k = String(withdrawalKey || '').toLowerCase();
  const s = String(signer || '').toLowerCase();
  const i = Number(stepIndex);
  if (!k || !s || !Number.isFinite(i) || i < 0) return '';
  return `${k}:${i}:${s}`;
}
export function setExpectedSignStep(node, { withdrawalKey, stepIndex, signer, txsetHashIn }) {
  if (!node) return;
  const key = makeSignStepKey(withdrawalKey, stepIndex, signer);
  const h = typeof txsetHashIn === 'string' ? txsetHashIn.toLowerCase() : '';
  if (!key || !isTxsetHash(h)) return;
  getExpectedMap(node).set(key, { txsetHashIn: h, createdAt: Date.now() });
}
export function recordSignStepResult(node, { withdrawalKey, stepIndex, signer, txsetHashIn, txDataHexOut, stale, busy }) {
  if (!node) return { ok: false, reason: 'no_node' };
  const key = makeSignStepKey(withdrawalKey, stepIndex, signer);
  if (!key) return { ok: false, reason: 'invalid_request' };
  const expected = getExpectedMap(node).get(key);
  const hIn = typeof txsetHashIn === 'string' ? txsetHashIn.toLowerCase() : '';
  if (!expected || !expected.txsetHashIn || !isTxsetHash(expected.txsetHashIn)) {
    return { ok: false, reason: 'no_expected_sign_step' };
  }
  if (!isTxsetHash(hIn) || hIn !== expected.txsetHashIn) {
    return { ok: false, reason: 'txset_hash_in_mismatch' };
  }
  if (stale) {
    getResultsMap(node).set(key, { stale: true, createdAt: Date.now() });
    getExpectedMap(node).delete(key);
    return { ok: true, stale: true };
  }
  if (busy) {
    getResultsMap(node).set(key, { busy: true, createdAt: Date.now() });
    getExpectedMap(node).delete(key);
    return { ok: true, busy: true };
  }
  if (typeof txDataHexOut !== 'string' || !txDataHexOut) {
    return { ok: false, reason: 'missing_tx_data_hex_out' };
  }
  const hOut = computeTxsetHash(txDataHexOut);
  if (!hOut || !isTxsetHash(hOut)) {
    return { ok: false, reason: 'invalid_txset_hash_out' };
  }
  const stored = putTxset(node, hOut, txDataHexOut);
  if (!stored.ok) return stored;
  getResultsMap(node).set(key, { txsetHashOut: hOut, size: stored.size, createdAt: Date.now() });
  getExpectedMap(node).delete(key);
  return { ok: true, txsetHashOut: hOut, size: stored.size };
}
export function takeSignStepResult(node, { withdrawalKey, stepIndex, signer }) {
  if (!node) return null;
  const key = makeSignStepKey(withdrawalKey, stepIndex, signer);
  if (!key) return null;
  const map = getResultsMap(node);
  const res = map.get(key);
  if (res) map.delete(key);
  return res || null;
}
