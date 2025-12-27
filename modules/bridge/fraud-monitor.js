import { ethers } from 'ethers';
import { requestFraudSlashBatch } from '../cluster/fraud-slash.js';
import { isAuthorizedTxid, recordAuthorizedTxids } from '../utils/authorized-txids.js';
import { SWEEP_SYNC_ROUND } from './withdrawals/config.js';
const FRAUD_MONITOR_INTERVAL_MS = Number(process.env.FRAUD_MONITOR_INTERVAL_MS || 60000);
const FRAUD_MIN_CONFIRMATIONS = Number(process.env.FRAUD_MIN_CONFIRMATIONS || 10);
let fraudMonitorRunning = false;
let lastCheckedOutgoingTxids = new Set();
function toFraudId(reason) {
  try {
    return ethers.keccak256(ethers.toUtf8Bytes(String(reason || 'fraud')));
  } catch {
    return null;
  }
}
function ingestWithdrawalTxids(node) {
  if (!node) return;
  if (node._pendingWithdrawals && node._pendingWithdrawals.size > 0) {
    for (const [, pending] of node._pendingWithdrawals) {
      if (!pending) continue;
      const hashes = pending.xmrTxHashes;
      if (!Array.isArray(hashes) || hashes.length === 0) continue;
      recordAuthorizedTxids(node, hashes);
    }
  }
  if (node._processedWithdrawalsMeta && node._processedWithdrawalsMeta.size > 0) {
    for (const [, meta] of node._processedWithdrawalsMeta) {
      if (!meta) continue;
      const hashes = meta.xmrTxHashes;
      if (!Array.isArray(hashes) || hashes.length === 0) continue;
      recordAuthorizedTxids(node, hashes);
    }
  }
}
async function ingestSweepTxids(node) {
  if (!node || !node.p2p || !node._activeClusterId) return;
  const round = SWEEP_SYNC_ROUND + 3;
  try {
    const roundData = await node.p2p.getRoundData(node._activeClusterId, node._sessionId || 'bridge', round);
    if (!roundData || typeof roundData !== 'object') return;
    for (const [, raw] of Object.entries(roundData)) {
      try {
        const data = typeof raw === 'string' ? JSON.parse(raw) : raw;
        if (!data || data.type !== 'sweep-txhashes') continue;
        if (!Array.isArray(data.txHashList) || data.txHashList.length === 0) continue;
        recordAuthorizedTxids(node, data.txHashList);
      } catch {}
    }
  } catch {}
}
async function getOutgoingTransfers(node, minHeight) {
  if (!node || !node.monero) return [];
  try {
    const result = await node.monero.call('get_transfers', {
      out: true,
      in: false,
      pending: false,
      pool: false,
      failed: false,
      filter_by_height: minHeight > 0,
      min_height: minHeight || 0,
      account_index: 0,
    }, 30000);
    if (!result || !Array.isArray(result.out)) return [];
    return result.out.map((tx) => ({
      txid: tx.txid,
      confirmations: tx.confirmations || 0,
    }));
  } catch (e) {
    console.log('[FraudMonitor] Failed to get outgoing transfers:', e.message || String(e));
    return [];
  }
}
async function slashClusterForFraud(node, reason) {
  if (!node || !Array.isArray(node._clusterMembers) || node._clusterMembers.length === 0) return;
  const self = node.wallet && node.wallet.address ? node.wallet.address.toLowerCase() : '';
  const targets = [];
  for (const member of node._clusterMembers) {
    const memberLc = String(member || '').toLowerCase();
    if (!memberLc || memberLc === self) continue;
    targets.push(member);
  }
  if (targets.length === 0) return;
  const fraudId = toFraudId(reason);
  if (!fraudId) return;
  console.log(`[FraudMonitor] FRAUD DETECTED: ${reason}`);
  await requestFraudSlashBatch(node, targets, fraudId, reason);
}
async function checkForUnauthorizedOutgoing(node) {
  if (!node || !node.monero || !node._activeClusterId) return;
  ingestWithdrawalTxids(node);
  await ingestSweepTxids(node);
  let height = 0;
  try {
    height = await node.monero.getHeight();
  } catch {
    height = 0;
  }
  const minHeight = height > 100 ? height - 100 : 0;
  const outgoing = await getOutgoingTransfers(node, minHeight);
  if (!outgoing || outgoing.length === 0) return;
  for (const tx of outgoing) {
    if (!tx || !tx.txid) continue;
    if (lastCheckedOutgoingTxids.has(tx.txid)) continue;
    if ((tx.confirmations || 0) < FRAUD_MIN_CONFIRMATIONS) continue;
    lastCheckedOutgoingTxids.add(tx.txid);
    if (lastCheckedOutgoingTxids.size > 1000) {
      const arr = Array.from(lastCheckedOutgoingTxids);
      lastCheckedOutgoingTxids = new Set(arr.slice(-500));
    }
    if (isAuthorizedTxid(node, tx.txid)) {
      continue;
    }
    await ingestSweepTxids(node);
    ingestWithdrawalTxids(node);
    if (isAuthorizedTxid(node, tx.txid)) {
      continue;
    }
    await slashClusterForFraud(node, `unauthorized_outgoing_tx:${tx.txid}`);
    return;
  }
}
export function startFraudMonitor(node) {
  if (fraudMonitorRunning) return;
  if (!node || !node.monero || !node.staking || !node.p2p) {
    console.log('[FraudMonitor] Cannot start');
    return;
  }
  fraudMonitorRunning = true;
  const runCheck = async () => {
    if (!fraudMonitorRunning) return;
    await checkForUnauthorizedOutgoing(node);
    if (fraudMonitorRunning) {
      const ms = Number.isFinite(FRAUD_MONITOR_INTERVAL_MS) && FRAUD_MONITOR_INTERVAL_MS > 0 ? FRAUD_MONITOR_INTERVAL_MS : 60000;
      setTimeout(runCheck, ms);
    }
  };
  setTimeout(runCheck, 5000);
}
export function stopFraudMonitor() {
  fraudMonitorRunning = false;
}
export default { startFraudMonitor, stopFraudMonitor };
