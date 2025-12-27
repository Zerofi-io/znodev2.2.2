import { ethers } from 'ethers';
import { tryParseJSON } from '../json.js';
import { deriveRound } from '../pbft-utils.js';
import { parseAtomicAmount } from '../amounts.js';
function normalizeAttempt(v) {
  const n = Number(v);
  if (!Number.isFinite(n) || n < 0) return 0;
  return Math.floor(n);
}
function normalizeHexHash(v) {
  const s = typeof v === 'string' ? v.toLowerCase() : '';
  if (!/^0x[0-9a-f]{64}$/.test(s)) return '';
  return s;
}
function normalizeAddress(v) {
  if (v == null) return '';
  const s = String(v);
  return s;
}
function normalizeBurnRecord(w) {
  if (!w) return null;
  const txHash = normalizeHexHash(w.txHash);
  if (!txHash) return null;
  const xmrAddress = normalizeAddress(w.xmrAddress);
  if (!xmrAddress) return null;
  const amt = parseAtomicAmount(w.xmrAmountAtomic != null ? w.xmrAmountAtomic : w.xmrAmount);
  if (amt == null || amt <= 0n) return null;
  const user = w.user != null ? String(w.user) : null;
  const zxmrAmount = w.zxmrAmount != null ? String(w.zxmrAmount) : null;
  const blockNumber = typeof w.blockNumber === 'number' && Number.isFinite(w.blockNumber) ? Math.floor(w.blockNumber) : null;
  const timestamp = typeof w.timestamp === 'number' && Number.isFinite(w.timestamp) ? Math.floor(w.timestamp) : null;
  const priorityClass = w.priorityClass != null ? String(w.priorityClass) : null;
  return {
    txHash,
    user,
    xmrAddress,
    xmrAmountAtomic: amt.toString(),
    zxmrAmount,
    blockNumber,
    timestamp,
    priorityClass,
  };
}
function buildDestinationsFromBurns(burns) {
  const map = new Map();
  let total = 0n;
  for (const b of burns) {
    if (!b || !b.xmrAddress || !b.xmrAmountAtomic) continue;
    const addr = String(b.xmrAddress);
    const amt = parseAtomicAmount(b.xmrAmountAtomic);
    if (amt == null || amt <= 0n) continue;
    map.set(addr, (map.get(addr) || 0n) + amt);
    total += amt;
  }
  const destinations = Array.from(map.entries())
    .sort((a, b) => String(a[0]).localeCompare(String(b[0])))
    .map(([address, amountAtomic]) => ({ address, amountAtomic: amountAtomic.toString() }));
  return { destinations, totalAmountAtomic: total.toString() };
}
function canonicalBaseManifest(clusterId, batchId, attempt, burns) {
  const { destinations, totalAmountAtomic } = buildDestinationsFromBurns(burns);
  return {
    type: 'withdrawal-batch-manifest',
    clusterId,
    batchId,
    attempt,
    burns,
    destinations,
    totalAmountAtomic,
  };
}
function computeManifestHash(base) {
  const payload = JSON.stringify(base);
  return ethers.keccak256(ethers.toUtf8Bytes(payload)).toLowerCase();
}
export function deriveBatchManifestRound(batchId, attempt) {
  const key = String(batchId || '').toLowerCase();
  if (!key) return 0;
  const a = normalizeAttempt(attempt);
  return deriveRound('withdrawal-batch-manifest', `${key}:${a}`);
}
export function buildBatchManifest(node, batchId, attempt, withdrawals) {
  if (!node || !node._activeClusterId || !batchId || !Array.isArray(withdrawals)) {
    return { ok: false, reason: 'invalid_args' };
  }
  const clusterId = String(node._activeClusterId).toLowerCase();
  if (!clusterId) return { ok: false, reason: 'no_cluster_id' };
  const batchKey = String(batchId).toLowerCase();
  if (!batchKey) return { ok: false, reason: 'no_batch_id' };
  const a = normalizeAttempt(attempt);
  const burns = withdrawals
    .map(normalizeBurnRecord)
    .filter(Boolean)
    .sort((x, y) => String(x.txHash).localeCompare(String(y.txHash)));
  if (burns.length === 0) return { ok: false, reason: 'no_burns' };
  const base = canonicalBaseManifest(clusterId, batchKey, a, burns);
  const manifestHash = computeManifestHash(base);
  const manifest = { ...base, manifestHash };
  return { ok: true, manifest, manifestHash, attempt: a };
}
export function validateBatchManifest(node, batchId, attempt, raw) {
  if (!node || !node._activeClusterId || !batchId) return null;
  const clusterId = String(node._activeClusterId).toLowerCase();
  const batchKey = String(batchId).toLowerCase();
  const data = raw && typeof raw === 'object' ? raw : null;
  if (!data || data.type !== 'withdrawal-batch-manifest') return null;
  const c = typeof data.clusterId === 'string' ? data.clusterId.toLowerCase() : '';
  if (!c || c !== clusterId) return null;
  const b = typeof data.batchId === 'string' ? data.batchId.toLowerCase() : '';
  if (!b || b !== batchKey) return null;
  const a = normalizeAttempt(data.attempt);
  const expectedAttempt = normalizeAttempt(attempt);
  if (a !== expectedAttempt) return null;
  const burnsIn = Array.isArray(data.burns) ? data.burns : [];
  const burns = burnsIn
    .map((w) => normalizeBurnRecord(w))
    .filter(Boolean)
    .sort((x, y) => String(x.txHash).localeCompare(String(y.txHash)));
  if (burns.length === 0) return null;
  const base = canonicalBaseManifest(clusterId, batchKey, a, burns);
  const computedHash = computeManifestHash(base);
  const providedHash = typeof data.manifestHash === 'string' ? data.manifestHash.toLowerCase() : '';
  if (providedHash && providedHash !== computedHash) return null;
  const destinationsIn = Array.isArray(data.destinations) ? data.destinations : null;
  if (destinationsIn) {
    const expected = base.destinations;
    if (expected.length !== destinationsIn.length) return null;
    for (let i = 0; i < expected.length; i += 1) {
      const e = expected[i];
      const d = destinationsIn[i];
      if (!d || String(d.address || '') !== String(e.address)) return null;
      if (String(d.amountAtomic || '') !== String(e.amountAtomic)) return null;
    }
  }
  const totalIn = data.totalAmountAtomic != null ? String(data.totalAmountAtomic) : null;
  if (totalIn && totalIn !== base.totalAmountAtomic) return null;
  return { manifest: { ...base, manifestHash: computedHash }, manifestHash: computedHash, attempt: a };
}
export async function broadcastBatchManifest(node, batchId, attempt, manifest) {
  if (!node || !node.p2p || !node._activeClusterId || !batchId || !manifest) return false;
  const clusterId = node._activeClusterId;
  const sessionId = node._sessionId || 'bridge';
  const round = deriveBatchManifestRound(batchId, attempt);
  if (!round) return false;
  const payload = JSON.stringify(manifest);
  try {
    await node.p2p.broadcastRoundData(clusterId, sessionId, round, payload);
    return true;
  } catch (e) {
    const msg = e && e.message ? String(e.message) : String(e);
    if (msg.includes('round payload overwrite rejected')) return true;
    return false;
  }
}
export async function readBatchManifestOnce(node, batchId, attempt, coordinatorLc) {
  if (!node || !node.p2p || !node._activeClusterId || !batchId) return null;
  const clusterId = node._activeClusterId;
  const sessionId = node._sessionId || 'bridge';
  const round = deriveBatchManifestRound(batchId, attempt);
  if (!round) return null;
  let roundData = null;
  try {
    roundData = await node.p2p.getRoundData(clusterId, sessionId, round);
  } catch {
    return null;
  }
  const entries = roundData && typeof roundData === 'object' ? Object.entries(roundData) : [];
  if (entries.length === 0) return null;
  const memberSet = new Set(
    Array.isArray(node._clusterMembers)
      ? node._clusterMembers.map((a) => String(a || '').toLowerCase()).filter(Boolean)
      : [],
  );
  for (const [sender, raw] of entries) {
    const senderLc = sender ? String(sender).toLowerCase() : '';
    if (!senderLc || (memberSet.size > 0 && !memberSet.has(senderLc))) continue;
    if (coordinatorLc && senderLc !== coordinatorLc) continue;
    const data = tryParseJSON(raw, `batch-manifest from ${senderLc}`);
    const validated = validateBatchManifest(node, batchId, attempt, data);
    if (validated) return validated;
  }
  return null;
}
export async function waitForBatchManifest(node, batchId, attempt, coordinatorLc, timeoutMs) {
  const ms0 = Number(timeoutMs);
  const ms = Number.isFinite(ms0) && ms0 > 0 ? Math.floor(ms0) : 30000;
  const startedAt = Date.now();
  while (Date.now() - startedAt < ms) {
    const res = await readBatchManifestOnce(node, batchId, attempt, coordinatorLc);
    if (res) return res;
    await new Promise((r) => setTimeout(r, 500));
  }
  return null;
}
export function manifestToWithdrawals(manifest) {
  const burns = manifest && Array.isArray(manifest.burns) ? manifest.burns : [];
  const withdrawals = [];
  for (const b of burns) {
    const txHash = normalizeHexHash(b && b.txHash);
    const xmrAddress = normalizeAddress(b && b.xmrAddress);
    const amt = parseAtomicAmount(b && b.xmrAmountAtomic);
    if (!txHash || !xmrAddress || amt == null || amt <= 0n) continue;
    withdrawals.push({
      txHash,
      user: b && b.user != null ? b.user : null,
      xmrAddress,
      zxmrAmount: b && b.zxmrAmount != null ? b.zxmrAmount : null,
      xmrAmount: amt.toString(),
      xmrAmountAtomic: amt,
      blockNumber: b && typeof b.blockNumber === 'number' ? b.blockNumber : null,
      timestamp: b && typeof b.timestamp === 'number' ? b.timestamp : null,
      priorityClass: b && b.priorityClass != null ? b.priorityClass : null,
    });
  }
  return withdrawals;
}
export function getManifestTxHashSet(manifest) {
  const burns = manifest && Array.isArray(manifest.burns) ? manifest.burns : [];
  const set = new Set();
  for (const b of burns) {
    const txHash = normalizeHexHash(b && b.txHash);
    if (txHash) set.add(txHash);
  }
  return set;
}
