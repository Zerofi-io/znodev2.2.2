import { WITHDRAWAL_PARTIAL_EPOCH_MODE } from './config.js';
import {
  CARRYOVER_STATUS,
  canonicalWithdrawalSort,
  addToCarryover,
} from './carryover.js';
import { withMoneroLock } from '../monero-lock.js';
import { parseAtomicAmount } from './amounts.js';
export function selectWithdrawalsThatFit(node, withdrawals, unlockedBalance, epochIndex) {
  if (!WITHDRAWAL_PARTIAL_EPOCH_MODE) {
    return { selected: withdrawals, deferred: [] };
  }
  if (!Array.isArray(withdrawals) || withdrawals.length === 0) {
    return { selected: [], deferred: [] };
  }
  if (typeof unlockedBalance !== 'bigint' || unlockedBalance <= 0n) {
    for (const w of withdrawals) {
      addToCarryover(node, w, CARRYOVER_STATUS.WAITING_UNLOCKED, epochIndex);
    }
    return { selected: [], deferred: withdrawals };
  }
  const sorted = [...withdrawals].map((w) => {
    const amt = parseAtomicAmount(w.xmrAmountAtomic != null ? w.xmrAmountAtomic : w.xmrAmount);
    return { ...w, _amount: amt != null ? amt : 0n };
  });
  sorted.sort(canonicalWithdrawalSort);
  const selected = [];
  const deferred = [];
  let remaining = unlockedBalance;
  for (const w of sorted) {
    const amt = w._amount;
    if (amt <= remaining) {
      remaining -= amt;
      const { _amount: _, ...clean } = w;
      selected.push(clean);
    } else {
      const { _amount: _, ...clean } = w;
      deferred.push(clean);
    }
  }
  for (const w of deferred) {
    addToCarryover(node, w, CARRYOVER_STATUS.WAITING_UNLOCKED, epochIndex);
  }
  return { selected, deferred };
}
export function getUnlockedBalanceForFit(node) {
  return new Promise(async (resolve) => {
    if (!node || !node.monero) {
      resolve(null);
      return;
    }
    try {
      const bal = await withMoneroLock(node, { key: node._withdrawalMultisigLock, op: 'withdrawal_fit_balance' }, async () => {
        return await node.monero.getBalance();
      });
      if (!bal) {
        resolve(null);
        return;
      }
      const raw = bal.unlockedBalance;
      let unlocked = 0n;
      if (typeof raw === 'bigint') {
        unlocked = raw;
      } else if (typeof raw === 'number') {
        unlocked = BigInt(raw);
      } else if (typeof raw === 'string') {
        try { unlocked = BigInt(raw); } catch { unlocked = 0n; }
      }
      resolve(unlocked);
    } catch (e) {
      console.log('[FitSelection] Failed to get balance:', e.message || String(e));
      resolve(null);
    }
  });
}
