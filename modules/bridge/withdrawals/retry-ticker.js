import { saveBridgeState } from '../bridge-state.js';
import { getDefaultTracker, withIdempotency } from '../idempotency.js';
import { scheduleClearWithdrawalRounds } from './pbft-utils.js';
import { scheduleWithdrawalStateSave } from './pending-store.js';
import { markWithdrawalCompleted } from './processed-tracking.js';
import { withMoneroLock } from '../monero-lock.js';
import { clearLocalSignStepCacheForWithdrawal } from './sign-step-cache.js';
import { getTxset } from './txset-store.js';
import { isCarryoverStatus } from './carryover.js';
import { rolloverBatchPendingToCarryover } from './batch-rollover.js';
import {
  WITHDRAWAL_MAX_RETRY_WINDOW_MS,
  WITHDRAWAL_RETRY_FAILED_PERMANENT,
  WITHDRAWAL_RETRY_INTERVAL_MS,
} from './config.js';
function isRetriableWithdrawal(pending) {
  if (!pending) return false;
  const isBatch = !!pending.isBatch;
  const status = String(pending.status || '');
  const err = String(pending.error || '');
  if (status === 'completed') return false;
  if (status === 'pending' || status === 'deferred') return isBatch;
  if (status === 'signed') return true;
  if (status === 'submitting') return true;
  if (status === 'submit_failed' || status === 'failed' || status === 'error') return true;
  if (status === 'failed_permanent') return WITHDRAWAL_RETRY_FAILED_PERMANENT;
  if (status === 'signing_failed' || status === 'sign_failed') return true;
  if (err === 'tx_pbft_error' || err === 'tx_pbft_failed') return true;
  if (err.startsWith('multisig_sync_failed:')) return true;
  return false;
}
function isLikelyDuplicateSubmitError(msg) {
  const m = String(msg || '').toLowerCase();
  if (!m) return false;
  if (m.includes('already') && (m.includes('submitted') || m.includes('in pool') || m.includes('in blockchain') || m.includes('known') || m.includes('exists'))) return true;
  if (m.includes('key image') && m.includes('spent')) return true;
  if (m.includes('double spend')) return true;
  return false;
}

function buildWithdrawalFromPending(key, data) {
  const txHash = data.txHash || key;
  let xmrAmountAtomic = data.xmrAmountAtomic;
  if (typeof xmrAmountAtomic === 'string') {
    try { xmrAmountAtomic = BigInt(xmrAmountAtomic); } catch { xmrAmountAtomic = 0n; }
  }
  if (typeof xmrAmountAtomic !== 'bigint') {
    try { xmrAmountAtomic = BigInt(data.xmrAmount || '0'); } catch { xmrAmountAtomic = 0n; }
  }
  const xmrAmountStr = typeof data.xmrAmount === 'string' ? data.xmrAmount : xmrAmountAtomic.toString();
  return {
    txHash,
    user: data.user,
    xmrAddress: data.xmrAddress,
    zxmrAmount: data.zxmrAmount || xmrAmountStr,
    xmrAmount: xmrAmountStr,
    xmrAmountAtomic,
    blockNumber: data.blockNumber,
    timestamp: data.timestamp || data.startedAt || Date.now(),
  };
}
export function createWithdrawalRetryTicker({ executeWithdrawalWithCascade } = {}) {
  if (typeof executeWithdrawalWithCascade !== 'function') {
    throw new TypeError('createWithdrawalRetryTicker requires executeWithdrawalWithCascade');
  }
  return function tickPendingWithdrawals(node) {
    if (!node || !node._pendingWithdrawals || node._pendingWithdrawals.size === 0) return;
    const now = Date.now();
    for (const [key, data] of node._pendingWithdrawals) {
      if (!data) continue;
      const isBatch = !!data.isBatch;
      if (isBatch && node._withdrawalBatchInProgressKey && String(node._withdrawalBatchInProgressKey).toLowerCase() === String(key).toLowerCase()) {
        continue;
      }
      if (!isBatch && (isCarryoverStatus(data.status) || data.status === 'waiting_cluster_health')) {
        continue;
      }
      if (data.status === 'completed') {
        clearLocalSignStepCacheForWithdrawal(node, key);
        scheduleClearWithdrawalRounds(node, key);
        markWithdrawalCompleted(node, key, data, data.xmrTxHashes);
        node._pendingWithdrawals.delete(key);
        saveBridgeState(node);
        continue;
      }
      const started = typeof data.startedAt === 'number' ? data.startedAt : now;
      if (typeof data.firstAttemptAt !== 'number') {
        data.firstAttemptAt = started;
      }
      const ageMs = now - data.firstAttemptAt;
      const allowPermanentRetry = data.status === 'failed_permanent' && WITHDRAWAL_RETRY_FAILED_PERMANENT;
      if (!allowPermanentRetry && ageMs > WITHDRAWAL_MAX_RETRY_WINDOW_MS) {
        if (!data.permanentFailure) {
          data.permanentFailure = true;
          if (!data.error) data.error = 'withdrawal_failed_after_retry_window';
          if (!data.status || data.status === 'pending') data.status = 'failed_permanent';
          console.log(
            `[Withdrawal] Giving up on withdrawal ${key.slice(0, 18)} after ${Math.floor(
              ageMs / 1000,
            )}s; marking permanently failed`,
          );
        }
        clearLocalSignStepCacheForWithdrawal(node, key);
        scheduleClearWithdrawalRounds(node, key);
        continue;
      }
      if (!isRetriableWithdrawal(data)) continue;
      if (data.status === 'submitting') {
        const preAt = typeof data.submitPreAt === 'number' ? data.submitPreAt : 0;
        if (preAt && now - preAt < 60000) continue;
      }
      if (data.retryInProgress) continue;
      const last = typeof data.lastAttemptAt === 'number' ? data.lastAttemptAt : started;
      if (now - last < WITHDRAWAL_RETRY_INTERVAL_MS) continue;
      data.retryInProgress = true;
      data.lastAttemptAt = now;
      if (node.monero && (data.status === 'signed' || data.status === 'submit_failed' || data.status === 'submitting')) {
        const txHex =
          typeof data.txDataHexSigned === 'string' && data.txDataHexSigned.length
            ? data.txDataHexSigned
            : typeof data.txsetHashSigned === 'string' && data.txsetHashSigned.length
              ? getTxset(node, data.txsetHashSigned)
              : null;
        if (typeof txHex === 'string' && txHex.length) {
          (async () => {
            const lockKey = key;
            let lockAcquired = false;
            try {
              const existingLock = node._withdrawalMultisigLock;
              if (existingLock && existingLock !== lockKey) return;
              if (!existingLock) {
                node._withdrawalMultisigLock = lockKey;
                lockAcquired = true;
              }
              const current0 = node._pendingWithdrawals && node._pendingWithdrawals.get(key);
              let txsetHashRaw = '';
              if (current0 && typeof current0.txsetHashSigned === 'string') txsetHashRaw = String(current0.txsetHashSigned).toLowerCase();
              else if (typeof data.txsetHashSigned === 'string') txsetHashRaw = String(data.txsetHashSigned).toLowerCase();
              const tracker = getDefaultTracker();
              const submitIdemKey = tracker.generateKey('withdrawal_retry_submit_multisig', key, txsetHashRaw || '');
              const attemptRaw = current0 && typeof current0.attempt !== 'undefined' ? Number(current0.attempt) : NaN;
              const attempt = Number.isFinite(attemptRaw) && attemptRaw >= 0 ? Math.floor(attemptRaw) : 0;
              if (current0) {
                current0.status = 'submitting';
                current0.submitIdemKey = submitIdemKey;
                current0.submitPreAt = Date.now();
                current0.submitPreAttempt = attempt;
                current0.submitPreTxsetHash = txsetHashRaw || null;
                current0.submitPreTxsetSize = txHex.length;
              }
              const preSaved = saveBridgeState(node);
              if (!preSaved) {
                const current = node._pendingWithdrawals && node._pendingWithdrawals.get(key);
                if (current) {
                  current.status = 'submit_failed';
                  current.error = 'bridge_state_persist_failed_before_retry_submit';
                }
                return;
              }
              let submitResult = null;
              try {
                submitResult = await withIdempotency(
                  tracker,
                  submitIdemKey,
                  async () => {
                    return await withMoneroLock(node, { key: lockKey, op: 'withdrawal_retry_submit' }, async () => {
                      return await node.monero.call('submit_multisig', { tx_data_hex: txHex }, 180000);
                    });
                  },
                  { metadata: { withdrawalKey: key, txsetHash: txsetHashRaw || null } },
                );
                if (!submitResult) return;
              } catch (e) {
                const msg = (e && e.message) ? e.message : String(e);
                const current = node._pendingWithdrawals && node._pendingWithdrawals.get(key);
                if (current) {
                  if (isLikelyDuplicateSubmitError(msg)) {
                    current.status = 'completed';
                    current.submitAt = Date.now();
                    current.submitTxsetHash = txsetHashRaw || null;
                    current.submitTxsetSize = txHex.length;
                    current.submitDuplicateError = msg;
                    markWithdrawalCompleted(node, key, current, current.xmrTxHashes);
                    saveBridgeState(node);
                  } else {
                    current.status = 'submit_failed';
                    current.error = msg;
                  }
                }
                return;
              }
              const current = node._pendingWithdrawals && node._pendingWithdrawals.get(key);
              if (!current) return;
              current.status = 'completed';
              current.xmrTxHashes = submitResult.tx_hash_list;
              current.submitAt = Date.now();
              current.submitTxsetHash = txsetHashRaw || null;
              current.submitTxsetSize = txHex.length;
              current.submitDuplicateError = null;
              markWithdrawalCompleted(node, key, current, submitResult.tx_hash_list);
              const saved = saveBridgeState(node);
              if (saved) {
                clearLocalSignStepCacheForWithdrawal(node, key);
                scheduleClearWithdrawalRounds(node, key);
                node._pendingWithdrawals.delete(key);
                saveBridgeState(node);
              }
            } catch (e) {
              const msg = (e && e.message) ? e.message : String(e);
              const current = node._pendingWithdrawals && node._pendingWithdrawals.get(key);
              if (current) current.error = msg;
            } finally {
              if (lockAcquired && node._withdrawalMultisigLock === lockKey) {
                node._withdrawalMultisigLock = null;
              }
              const current = node._pendingWithdrawals && node._pendingWithdrawals.get(key);
              if (current) current.retryInProgress = false;
            }
          })();
          continue;
        }
      }
      if (isBatch) {
        const epochIndex = (() => {
          const parts = String(key || '').split(':');
          if (parts.length !== 2) return null;
          const n = Number(parts[1]);
          if (!Number.isFinite(n) || n < 0 || !Number.isInteger(n)) return null;
          return n;
        })();
        const res = rolloverBatchPendingToCarryover(node, key, data, { epochIndex });
        if (!res || !res.ok) {
          data.retryInProgress = false;
        }
        continue;
      }
      const withdrawal = buildWithdrawalFromPending(key, data);
      (async () => {
        try {
          await executeWithdrawalWithCascade(node, withdrawal);
        } catch (e) {
          console.log(
            '[Withdrawal] Retry execution error for withdrawal',
            key.slice(0, 18),
            e.message || String(e),
          );
        } finally {
          const current = node._pendingWithdrawals && node._pendingWithdrawals.get(key);
          if (current) current.retryInProgress = false;
        }
      })();
    }
    scheduleWithdrawalStateSave(node);
  };
}
