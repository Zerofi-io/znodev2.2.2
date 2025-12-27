import { scheduleWithdrawalStateSave } from '../pending-store.js';
import { WITHDRAWAL_SIGNED_INFO_WAIT_MS } from '../config.js';
import { waitForSignedWithdrawalInfo } from './waiters.js';
import { recordAuthorizedTxids } from '../../../utils/authorized-txids.js';
export function maybeStartSignedInfoWait(node, withdrawalKey, expectedSender, onComplete) {
  if (!node || !node._pendingWithdrawals || !withdrawalKey) return false;
  const key = String(withdrawalKey || '').toLowerCase();
  if (!key) return false;
  const pending = node._pendingWithdrawals.get(key);
  if (!pending) return false;
  const sender = expectedSender
    ? String(expectedSender).toLowerCase()
    : pending.coordinator
      ? String(pending.coordinator).toLowerCase()
      : '';
  if (!sender) return false;
  if (pending.signedInfoWaitInProgress) return false;
  pending.signedInfoWaitInProgress = true;
  (async () => {
    try {
      const info = await waitForSignedWithdrawalInfo(node, key, sender, WITHDRAWAL_SIGNED_INFO_WAIT_MS);
      if (!info) return;
      const current = node._pendingWithdrawals && node._pendingWithdrawals.get(key);
      if (!current || current.status === 'completed') return;
      current.status = 'completed';
      current.xmrTxHashes = info.txHashList || [];
      if (typeof info.txKey === 'string' && info.txKey) current.xmrTxKey = info.txKey;
      if (Array.isArray(info.moneroSigners) && info.moneroSigners.length > 0) {
        current.moneroSigners = info.moneroSigners;
      }
      if (Array.isArray(info.txHashList) && info.txHashList.length > 0) {
        recordAuthorizedTxids(node, info.txHashList);
      }
    } catch {} finally {
      const current = node._pendingWithdrawals && node._pendingWithdrawals.get(key);
      if (current) current.signedInfoWaitInProgress = false;
      scheduleWithdrawalStateSave(node);
      if (typeof onComplete === 'function') {
        try { onComplete(); } catch {}
      }
    }
  })();
  return true;
}
