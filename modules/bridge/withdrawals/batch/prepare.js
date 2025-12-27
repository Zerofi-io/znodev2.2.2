import { scheduleWithdrawalStateSave } from '../pending-store.js';
import { markWithdrawalProcessedInMemory } from '../processed-tracking.js';
import { WITHDRAWAL_MAX_XMR, WITHDRAWAL_MAX_XMR_ATOMIC } from '../config.js';
import { withMoneroLock } from '../../monero-lock.js';
export function prepareBatchPendingState(
  node,
  {
    key,
    withdrawals,
    now,
    firstAttemptAt,
    electionResult,
    isCoordinator,
  },
) {
  if (!node || !key || !Array.isArray(withdrawals) || withdrawals.length === 0) {
    return { ok: false, reason: 'invalid_args' };
  }
  const destMap = new Map();
  for (const w of withdrawals) {
    if (!w || !w.xmrAddress || !w.txHash) continue;
    const addr = String(w.xmrAddress);
    let amt = w.xmrAmountAtomic;
    if (typeof amt === 'string') {
      try { amt = BigInt(amt); } catch { amt = 0n; }
    }
    if (typeof amt !== 'bigint') {
      try { amt = BigInt(w.xmrAmount || '0'); } catch { amt = 0n; }
    }
    if (amt <= 0n) continue;
    const prev = destMap.get(addr) || 0n;
    destMap.set(addr, prev + amt);
  }
  const destinations = [];
  let totalAmount = 0n;
  for (const [addr, amt] of destMap) {
    if (typeof amt !== 'bigint' || amt <= 0n) continue;
    totalAmount += amt;
    destinations.push({ address: addr, amount: amt });
  }
  if (destinations.length === 0) {
    for (const w of withdrawals) {
      if (w && w.txHash) markWithdrawalProcessedInMemory(node, w.txHash);
    }
    return { ok: false, reason: 'no_destinations' };
  }
  const batchWithdrawals = withdrawals.map((w) => ({
    txHash: w.txHash,
    user: w.user,
    xmrAddress: w.xmrAddress,
    zxmrAmount: w.zxmrAmount,
    xmrAmount: w.xmrAmount,
    xmrAmountAtomic: String(w.xmrAmountAtomic || w.xmrAmount || '0'),
    blockNumber: w.blockNumber,
    timestamp: w.timestamp,
  }));

  // Preserve certain fields across retries. Round payloads are write-once per sender,
  // so regenerating a different unsigned proposal for the same batch key can brick PBFT.
  const prev = node._pendingWithdrawals.get(key);
  const preserveUnsigned =
    prev &&
    typeof prev === 'object' &&
    prev.isBatch &&
    typeof prev.txsetHashUnsigned === 'string' &&
    prev.txsetHashUnsigned &&
    typeof prev.pbftUnsignedHash === 'string' &&
    prev.pbftUnsignedHash &&
    prev.error !== 'stale_multisig_tx';

  const pending0 = prev && typeof prev === 'object' ? prev : {};
  pending0.isBatch = true;
  pending0.batchId = key;
  pending0.ethTxHashes = batchWithdrawals.map((w) => String(w.txHash || '').toLowerCase());
  pending0.batchWithdrawals = batchWithdrawals;
  pending0.status = 'pending';
  pending0.startedAt = now;
  pending0.firstAttemptAt = firstAttemptAt;
  pending0.coordinator = electionResult && electionResult.coordinator ? String(electionResult.coordinator).toLowerCase() : null;
  pending0.isCoordinator = !!isCoordinator;

  // If we *must* rebuild (e.g. stale multisig), clear the immutable proposal fields.
  if (!preserveUnsigned) {
    pending0.signerSet = null;
    pending0.txsetHashUnsigned = null;
    pending0.txsetSizeUnsigned = null;
    pending0.pbftUnsignedHash = null;
  }

  // Never store raw tx hex in bridge_state; txset-store persists it separately.
  pending0.txDataHexUnsigned = null;

  if (pending0.txDataHexSigned === undefined) pending0.txDataHexSigned = null;
  if (pending0.txsetHashSigned === undefined) pending0.txsetHashSigned = null;
  if (pending0.txsetSizeSigned === undefined) pending0.txsetSizeSigned = null;
  if (pending0.pbftSignedHash === undefined) pending0.pbftSignedHash = null;

  node._pendingWithdrawals.set(key, pending0);
  if (WITHDRAWAL_MAX_XMR_ATOMIC > 0n) {
    for (const d of destinations) {
      if (!d || typeof d.amount !== 'bigint') continue;
      if (d.amount > WITHDRAWAL_MAX_XMR_ATOMIC) {
        const pending = node._pendingWithdrawals.get(key);
        if (pending) {
          pending.status = 'failed_permanent';
          pending.permanentFailure = true;
          pending.error = 'withdrawal_amount_exceeds_limit';
          pending.limitXmr = WITHDRAWAL_MAX_XMR;
          pending.excessAddress = d.address;
          pending.excessAmountAtomic = d.amount.toString();
        }
        scheduleWithdrawalStateSave(node);
        return { ok: false, reason: 'withdrawal_amount_exceeds_limit' };
      }
    }
  }
  scheduleWithdrawalStateSave(node);
  return {
    ok: true,
    destinations,
    totalAmount,
    batchWithdrawals,
  };
}
export async function validateBatchBalance(node, totalAmountAtomic) {
  if (!node || !node.monero) return { ok: false, reason: 'no_monero' };
  if (typeof totalAmountAtomic !== 'bigint' || totalAmountAtomic <= 0n) {
    return { ok: false, reason: 'invalid_amount' };
  }
  try {
    const balance = await withMoneroLock(node, { op: 'withdrawal_balance_precheck' }, async () => {
      return await node.monero.getBalance();
    });
    if (!balance) return { ok: false, reason: 'balance_fetch_failed' };
    const unlocked = typeof balance.unlockedBalance === 'number'
      ? BigInt(balance.unlockedBalance)
      : typeof balance.unlockedBalance === 'bigint'
        ? balance.unlockedBalance
        : 0n;
    if (unlocked < totalAmountAtomic) {
      console.log(`[Withdrawal] Insufficient unlocked balance: ${unlocked} < ${totalAmountAtomic}`);
      return {
        ok: false,
        reason: 'insufficient_unlocked_balance',
        unlockedBalance: unlocked,
        requiredAmount: totalAmountAtomic,
      };
    }
    return { ok: true, unlockedBalance: unlocked };
  } catch (e) {
    console.log('[Withdrawal] Balance check failed:', e.message || String(e));
    return { ok: false, reason: 'balance_check_error', error: e.message };
  }
}
