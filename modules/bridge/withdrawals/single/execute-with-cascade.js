import { maybeStartSignedInfoWait } from '../signing/signed-info-wait.js';
import { runWithdrawalCoordinatorElection } from '../consensus.js';
import { scheduleWithdrawalStateSave } from '../pending-store.js';
import { executeWithdrawal } from './execute.js';
export async function executeWithdrawalWithCascade(node, withdrawal) {
  const members = Array.isArray(node._clusterMembers)
    ? node._clusterMembers.map((a) => String(a || '').toLowerCase())
    : [];
  if (!Array.isArray(members) || members.length === 0) return;
  const key = typeof withdrawal.txHash === 'string' ? withdrawal.txHash.toLowerCase() : '';
  if (!key) return;
  const self = node.wallet && node.wallet.address ? String(node.wallet.address).toLowerCase() : null;
  if (!self) return;
  node._pendingWithdrawals = node._pendingWithdrawals || new Map();
  const existing = node._pendingWithdrawals.get(key) || {};
  if (typeof existing.firstAttemptAt !== 'number') {
    existing.firstAttemptAt = Date.now();
  }
  node._pendingWithdrawals.set(key, existing);
  const electionResult = await runWithdrawalCoordinatorElection(node, key);
  if (!electionResult || !electionResult.success) {
    console.log(`[Withdrawal] Coordinator election failed for ${key.slice(0, 18)}: ${electionResult?.reason || 'unknown'}`);
    const pending = node._pendingWithdrawals.get(key);
    if (pending) {
      pending.status = 'failed';
      pending.error = `coordinator_election_failed:${electionResult?.reason || 'unknown'}`;
    }
    scheduleWithdrawalStateSave(node);
    return;
  }
  const pending0 = node._pendingWithdrawals.get(key);
  if (pending0) {
    pending0.coordinator = electionResult && electionResult.coordinator ? String(electionResult.coordinator).toLowerCase() : null;
    pending0.isCoordinator = !!electionResult.isCoordinator;
  }
  scheduleWithdrawalStateSave(node);
  if (!electionResult.isCoordinator) {
    maybeStartSignedInfoWait(node, key);
    console.log(`[Withdrawal] Not coordinator for ${key.slice(0, 18)}, participating in consensus only`);
    return;
  }
  console.log(`[Withdrawal] Starting withdrawal execution as coordinator for ${key.slice(0, 18)}...`);
  try {
    await executeWithdrawal(node, withdrawal);
  } catch (e) {
    console.log('[Withdrawal] Error executing withdrawal:', e.message || String(e));
  }
}
