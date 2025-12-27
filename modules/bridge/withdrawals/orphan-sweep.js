import {
  WITHDRAWAL_ORPHAN_TTL_MS,
  WITHDRAWAL_ORPHAN_SWEEP_INTERVAL_MS,
  WITHDRAWAL_PARTIAL_EPOCH_MODE,
} from './config.js';
import {
  getActiveCarryoverQueue,
  markAsOrphaned,
} from './carryover.js';
let _orphanSweepTimer = null;
export function startOrphanSweepTimer(node) {
  if (!WITHDRAWAL_PARTIAL_EPOCH_MODE) return;
  if (_orphanSweepTimer) return;
  _orphanSweepTimer = setInterval(() => {
    runOrphanSweep(node);
  }, WITHDRAWAL_ORPHAN_SWEEP_INTERVAL_MS);
  console.log(`[OrphanSweep] Started sweep timer (interval=${WITHDRAWAL_ORPHAN_SWEEP_INTERVAL_MS}ms, ttl=${WITHDRAWAL_ORPHAN_TTL_MS}ms)`);
}
export function stopOrphanSweepTimer() {
  if (_orphanSweepTimer) {
    clearInterval(_orphanSweepTimer);
    _orphanSweepTimer = null;
    console.log('[OrphanSweep] Stopped sweep timer');
  }
}
export function runOrphanSweep(node) {
  if (!WITHDRAWAL_PARTIAL_EPOCH_MODE) return;
  if (!node) return;
  const queue = getActiveCarryoverQueue(node);
  if (queue.length === 0) return;
  const now = Date.now();
  let orphanedCount = 0;
  for (const item of queue) {
    const firstSeen = typeof item.firstSeenAtMs === 'number' ? item.firstSeenAtMs : 0;
    if (firstSeen <= 0) continue;
    const age = now - firstSeen;
    if (age >= WITHDRAWAL_ORPHAN_TTL_MS) {
      const success = markAsOrphaned(node, item.txHash);
      if (success) {
        orphanedCount++;
        console.log(`[OrphanSweep] Marked withdrawal ${item.txHash.slice(0, 18)} as orphaned (age=${Math.floor(age / 1000)}s)`);
      }
    }
  }
  if (orphanedCount > 0) {
    console.log(`[OrphanSweep] Marked ${orphanedCount} withdrawal(s) as orphaned`);
  }
}
