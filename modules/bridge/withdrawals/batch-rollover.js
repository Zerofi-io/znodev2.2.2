import { scheduleWithdrawalStateSave } from './pending-store.js';
import { scheduleClearWithdrawalRounds } from './pbft-utils.js';
import { clearLocalSignStepCacheForWithdrawal } from './sign-step-cache.js';
import { CARRYOVER_STATUS, PRIORITY_CLASS, addToCarryover } from './carryover.js';

function parseAtomicAmount(v) {
  if (typeof v === 'bigint') return v;
  if (typeof v === 'number') {
    if (!Number.isFinite(v) || v <= 0) return 0n;
    return BigInt(Math.floor(v));
  }
  if (typeof v === 'string' && v.length) {
    try { return BigInt(v); } catch { return 0n; }
  }
  return 0n;
}

export function rolloverBatchPendingToCarryover(
  node,
  batchKey,
  pending,
  {
    status = CARRYOVER_STATUS.WAITING_CAPACITY,
    epochIndex = null,
    removeBatchKey = true,
  } = {},
) {
  if (!node || !batchKey || !pending || !pending.isBatch) {
    return { ok: false, reason: 'invalid_args' };
  }
  const key = String(batchKey).toLowerCase();
  if (!key) return { ok: false, reason: 'invalid_key' };
  const batchWithdrawals = Array.isArray(pending.batchWithdrawals) ? pending.batchWithdrawals : [];
  if (batchWithdrawals.length === 0) return { ok: false, reason: 'no_batch_withdrawals' };

  node._pendingWithdrawals = node._pendingWithdrawals || new Map();
  node._processedWithdrawals = node._processedWithdrawals || new Set();

  let added = 0;
  let skipped = 0;
  for (const w of batchWithdrawals) {
    const txHash = w && w.txHash ? String(w.txHash).toLowerCase() : '';
    if (!txHash) {
      skipped += 1;
      continue;
    }
    if (node._processedWithdrawals.has(txHash)) {
      skipped += 1;
      continue;
    }
    const atomic = parseAtomicAmount(w.xmrAmountAtomic || w.xmrAmount);
    const withdrawal = {
      txHash,
      user: w.user || null,
      xmrAddress: w.xmrAddress || null,
      zxmrAmount: w.zxmrAmount || null,
      xmrAmount: typeof w.xmrAmount === 'string' ? w.xmrAmount : atomic.toString(),
      xmrAmountAtomic: atomic,
      blockNumber: typeof w.blockNumber === 'number' ? w.blockNumber : 0,
      timestamp: typeof w.timestamp === 'number' ? w.timestamp : Date.now(),
      priorityClass: PRIORITY_CLASS.RETRY,
    };
    if (addToCarryover(node, withdrawal, status, epochIndex)) {
      added += 1;
    } else {
      skipped += 1;
    }
  }

  if (removeBatchKey) {
    clearLocalSignStepCacheForWithdrawal(node, key);
    scheduleClearWithdrawalRounds(node, key);
    node._pendingWithdrawals.delete(key);
  }
  scheduleWithdrawalStateSave(node);
  return { ok: true, added, skipped };
}
