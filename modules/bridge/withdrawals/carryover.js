import { scheduleWithdrawalStateSave } from './pending-store.js';
import { logSwallowedError } from '../../utils/rate-limited-log.js';
import { WITHDRAWAL_MAX_RETRY_WINDOW_MS, WITHDRAWAL_ORPHAN_TTL_MS } from './config.js';

export const CARRYOVER_STATUS = {
  WAITING_UNLOCKED: 'carryover_waiting_unlocked',
  WAITING_CLUSTER: 'carryover_waiting_cluster',
  WAITING_CAPACITY: 'carryover_waiting_capacity',
  ORPHANED: 'orphaned',
};

export const PRIORITY_CLASS = {
  RETRY: 'retry',
  BRIBE: 'bribe',
  STANDARD: 'standard',
};

const CARRYOVER_VALIDATION_INTERVAL_MS = Number(process.env.CARRYOVER_VALIDATION_INTERVAL_MS || 300000);
const CARRYOVER_MAX_AGE_MS = (() => {
  const envVal = Number(process.env.CARRYOVER_MAX_AGE_MS || 0);
  if (envVal > 0) return envVal;
  return Math.max(WITHDRAWAL_ORPHAN_TTL_MS, WITHDRAWAL_MAX_RETRY_WINDOW_MS, 1800000);
})();

function getPriorityRank(p) {
  if (p === PRIORITY_CLASS.RETRY) return 0;
  if (p === PRIORITY_CLASS.BRIBE) return 1;
  return 2;
}

export function canonicalWithdrawalSort(a, b) {
  const pA = getPriorityRank(a.priorityClass);
  const pB = getPriorityRank(b.priorityClass);
  if (pA !== pB) return pA - pB;
  const firstSeenA = typeof a.firstSeenAtMs === 'number' && Number.isFinite(a.firstSeenAtMs) ? a.firstSeenAtMs : Number.MAX_SAFE_INTEGER;
  const firstSeenB = typeof b.firstSeenAtMs === 'number' && Number.isFinite(b.firstSeenAtMs) ? b.firstSeenAtMs : Number.MAX_SAFE_INTEGER;
  if (firstSeenA !== firstSeenB) return firstSeenA - firstSeenB;
  const blockA = typeof a.blockNumber === 'number' && Number.isFinite(a.blockNumber) ? a.blockNumber : 0;
  const blockB = typeof b.blockNumber === 'number' && Number.isFinite(b.blockNumber) ? b.blockNumber : 0;
  if (blockA !== blockB) return blockA - blockB;
  const txA = String(a.txHash || '').toLowerCase();
  const txB = String(b.txHash || '').toLowerCase();
  return txA.localeCompare(txB);
}

export function isCarryoverStatus(status) {
  if (!status) return false;
  return status === CARRYOVER_STATUS.WAITING_UNLOCKED ||
         status === CARRYOVER_STATUS.WAITING_CLUSTER ||
         status === CARRYOVER_STATUS.WAITING_CAPACITY ||
         status === CARRYOVER_STATUS.ORPHANED;
}

export function isActiveCarryover(status) {
  if (!status) return false;
  return status === CARRYOVER_STATUS.WAITING_UNLOCKED ||
         status === CARRYOVER_STATUS.WAITING_CLUSTER ||
         status === CARRYOVER_STATUS.WAITING_CAPACITY;
}

export function addToCarryover(node, withdrawal, status, epochIndex) {
  if (!node || !withdrawal || !withdrawal.txHash) return false;
  const key = String(withdrawal.txHash).toLowerCase();
  node._pendingWithdrawals = node._pendingWithdrawals || new Map();
  const existing = node._pendingWithdrawals.get(key);
  const attemptCount = existing && typeof existing.attemptCount === 'number' ? existing.attemptCount + 1 : 1;
  const firstSeenAtMs = existing && typeof existing.firstSeenAtMs === 'number' ? existing.firstSeenAtMs : Date.now();
  const incomingPriorityClass = withdrawal.priorityClass || PRIORITY_CLASS.STANDARD;
  const existingPriorityClass = existing && existing.priorityClass ? existing.priorityClass : null;
  const priorityClass =
    existingPriorityClass && getPriorityRank(existingPriorityClass) <= getPriorityRank(incomingPriorityClass)
      ? existingPriorityClass
      : incomingPriorityClass;
  const entry = {
    txHash: key,
    blockNumber: typeof withdrawal.blockNumber === 'number' ? withdrawal.blockNumber : 0,
    user: withdrawal.user || null,
    xmrAddress: withdrawal.xmrAddress || null,
    xmrAmountAtomic: withdrawal.xmrAmountAtomic != null ? withdrawal.xmrAmountAtomic : (withdrawal.xmrAmount || null),
    zxmrAmount: withdrawal.zxmrAmount || null,
    xmrAmount: withdrawal.xmrAmount || null,
    priorityClass,
    status,
    firstSeenAtMs,
    attemptCount,
    lastTriedEpochIndex: typeof epochIndex === 'number' ? epochIndex : null,
    timestamp: withdrawal.timestamp || Date.now(),
  };
  node._pendingWithdrawals.set(key, entry);
  scheduleWithdrawalStateSave(node);
  return true;
}

export function getCarryoverQueue(node, statusFilter = null) {
  if (!node || !node._pendingWithdrawals) return [];
  const result = [];
  for (const [txHash, data] of node._pendingWithdrawals) {
    if (!data || !isCarryoverStatus(data.status)) continue;
    if (statusFilter && data.status !== statusFilter) continue;
    result.push({ txHash, ...data });
  }
  return result;
}

export function getActiveCarryoverQueue(node) {
  if (!node || !node._pendingWithdrawals) return [];
  const result = [];
  const now = Date.now();
  for (const [txHash, data] of node._pendingWithdrawals) {
    if (!data || !isActiveCarryover(data.status)) continue;
    const firstSeenAtMs = typeof data.firstSeenAtMs === 'number' ? data.firstSeenAtMs : now;
    const age = now - firstSeenAtMs;
    if (CARRYOVER_MAX_AGE_MS > 0 && age > CARRYOVER_MAX_AGE_MS) continue;
    result.push({ txHash, ...data });
  }
  return result;
}

export function removeFromCarryover(node, txHash) {
  if (!node || !node._pendingWithdrawals || !txHash) return false;
  const key = String(txHash).toLowerCase();
  const existed = node._pendingWithdrawals.has(key);
  if (existed) {
    node._pendingWithdrawals.delete(key);
    scheduleWithdrawalStateSave(node);
  }
  return existed;
}

export function markAsOrphaned(node, txHash) {
  if (!node || !node._pendingWithdrawals || !txHash) return false;
  const key = String(txHash).toLowerCase();
  const entry = node._pendingWithdrawals.get(key);
  if (!entry) return false;
  entry.status = CARRYOVER_STATUS.ORPHANED;
  entry.orphanedAtMs = Date.now();
  scheduleWithdrawalStateSave(node);
  return true;
}

export function getCarryoverStats(node) {
  const queue = getCarryoverQueue(node);
  const stats = {
    total: queue.length,
    waitingUnlocked: 0,
    waitingCluster: 0,
    orphaned: 0,
    oldestFirstSeenAtMs: null,
  };
  for (const item of queue) {
    if (item.status === CARRYOVER_STATUS.WAITING_UNLOCKED) stats.waitingUnlocked++;
    else if (item.status === CARRYOVER_STATUS.WAITING_CLUSTER) stats.waitingCluster++;
    else if (item.status === CARRYOVER_STATUS.ORPHANED) stats.orphaned++;
    if (typeof item.firstSeenAtMs === 'number') {
      if (stats.oldestFirstSeenAtMs === null || item.firstSeenAtMs < stats.oldestFirstSeenAtMs) {
        stats.oldestFirstSeenAtMs = item.firstSeenAtMs;
      }
    }
  }
  return stats;
}

async function validateCarryoverEntry(node, txHash, data) {
  if (!node || !node.bridge || !txHash) return { valid: false, reason: 'missing_params' };
  if (!data || !data.user) return { valid: false, reason: 'missing_data' };
  const clusterId = node._activeClusterId;
  if (!clusterId) return { valid: true, reason: 'no_cluster_skip' };
  try {
    const receipt = await node.provider.getTransactionReceipt(txHash);
    if (!receipt) return { valid: false, reason: 'no_receipt' };
    if (receipt.status === 0) return { valid: false, reason: 'tx_reverted' };
    const logs = Array.isArray(receipt.logs) ? receipt.logs : [];
    const bridgeAddr = (node.bridge.target || node.bridge.address || '').toLowerCase();
    let foundBurn = false;
    for (const log of logs) {
      const logAddr = (log.address || '').toLowerCase();
      if (bridgeAddr && logAddr !== bridgeAddr) continue;
      let parsed;
      try {
        parsed = node.bridge.interface.parseLog(log);
      } catch {
        continue;
      }
      if (!parsed || parsed.name !== 'TokensBurned') continue;
      const args = parsed.args || [];
      const user = args[0] ?? args.user;
      const logClusterId = args[1] ?? args.clusterId;
      if (!user) continue;
      const userLc = String(user).toLowerCase();
      const dataUserLc = String(data.user).toLowerCase();
      if (userLc !== dataUserLc) continue;
      if (logClusterId && String(logClusterId).toLowerCase() !== String(clusterId).toLowerCase()) continue;
      foundBurn = true;
      break;
    }
    if (!foundBurn) return { valid: false, reason: 'burn_not_found' };
    return { valid: true, reason: 'verified' };
  } catch (e) {
    logSwallowedError('carryover_verify_tx_error', e);
    return { valid: true, reason: 'verification_error_skip' };
  }
}

export async function validateCarryoverQueue(node) {
  if (!node || !node._pendingWithdrawals || !node.bridge || !node.provider) return { validated: 0, removed: 0, errors: 0 };
  const active = getActiveCarryoverQueue(node);
  if (active.length === 0) return { validated: 0, removed: 0, errors: 0 };
  const now = Date.now();
  let validated = 0;
  let removed = 0;
  let errors = 0;
  const toRemove = [];
  for (const item of active) {
    if (!item.txHash) continue;
    const firstSeenAtMs = typeof item.firstSeenAtMs === 'number' ? item.firstSeenAtMs : now;
    const age = now - firstSeenAtMs;
    if (CARRYOVER_MAX_AGE_MS > 0 && age > CARRYOVER_MAX_AGE_MS) {
      toRemove.push({ txHash: item.txHash, reason: 'max_age_exceeded' });
      continue;
    }
    const result = await validateCarryoverEntry(node, item.txHash, item);
    validated++;
    if (!result.valid) {
      if (result.reason !== 'verification_error_skip' && result.reason !== 'no_cluster_skip') {
        toRemove.push({ txHash: item.txHash, reason: result.reason });
      } else {
        errors++;
      }
    }
  }
  for (const { txHash, reason } of toRemove) {
    const entry = node._pendingWithdrawals.get(txHash);
    if (entry) {
      entry.status = CARRYOVER_STATUS.ORPHANED;
      entry.orphanedAtMs = now;
      entry.orphanReason = reason;
      removed++;
    }
  }
  if (removed > 0) {
    scheduleWithdrawalStateSave(node);
    console.log(`[Carryover] Validation: ${validated} checked, ${removed} orphaned`);
  }
  return { validated, removed, errors };
}

export function startCarryoverValidation(node) {
  if (!node || node._carryoverValidationTimer) return;
  const runValidation = async () => {
    if (!node._withdrawalMonitorRunning) return;
    try {
      await validateCarryoverQueue(node);
    } catch (e) {
      console.log('[Carryover] Validation error:', e.message || String(e));
    }
  };
  node._carryoverValidationTimer = setInterval(runValidation, CARRYOVER_VALIDATION_INTERVAL_MS);
  setTimeout(runValidation, 30000);
}

export function stopCarryoverValidation(node) {
  if (!node || !node._carryoverValidationTimer) return;
  clearInterval(node._carryoverValidationTimer);
  node._carryoverValidationTimer = null;
}
