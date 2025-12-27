import { loadBridgeState, startBridgeStateSaver, startCleanupTimer } from './bridge-state.js';
import { startFraudMonitor, stopFraudMonitor } from './fraud-monitor.js';
import { validateRoundAllocationsOnStartup } from '../utils/round-collision-check.js';
import { migrateLegacyPendingWithdrawals, scheduleWithdrawalStateSave, flushWithdrawalStateSave } from './withdrawals/pending-store.js';
import { getReservedCount } from './withdrawals/bribe-tracking.js';
import {
  isBridgeEnabled,
  WITHDRAWAL_RETRY_INTERVAL_MS,
  WITHDRAWAL_EVENT_POLL_INTERVAL_MS,
  WITHDRAWAL_MIN_CONFIRMATIONS,
  WITHDRAWAL_STARTUP_BACKFILL_BLOCKS,
  WITHDRAWAL_SECURITY_HARDEN,
  WITHDRAWAL_EPOCH_SECONDS,
  MAX_BRIBE_SLOTS,
  BRIBE_BASE_COST,
  BRIBE_WINDOW_BUFFER,
  WITHDRAWAL_PARTIAL_EPOCH_MODE,
  WITHDRAWAL_BATCH_MANIFEST_PBFT,
  WITHDRAWAL_MANIFEST_WAIT_MS,
} from './withdrawals/config.js';
import { getWithdrawalShortHash, scheduleClearWithdrawalRounds } from './withdrawals/pbft-utils.js';
import { withMoneroLock } from './monero-lock.js';
import { setupWithdrawalUnsignedProposalListener } from './withdrawals/listeners/unsigned-proposals.js';
import { setupMultisigSyncResponder } from './withdrawals/listeners/multisig-sync.js';
import { setupWithdrawalSignStepListener } from './withdrawals/listeners/sign-step.js';
import { formatXmrAtomic } from './withdrawals/amounts.js';
import { verifyTxsetMatchesPending, verifyPendingBurnsOnChain } from './withdrawals/verification.js';
import { createWithdrawalEpochRunner } from './withdrawals/epoch.js';
import { pollWithdrawalEvents } from './withdrawals/event-poller.js';
import { createWithdrawalEventHandlers } from './withdrawals/event-handlers.js';
import { runUnsignedWithdrawalTxConsensus, runWithdrawalCoordinatorElection, runWithdrawalBatchManifestConsensus } from './withdrawals/consensus.js';
import { createWithdrawalRetryTicker } from './withdrawals/retry-ticker.js';
import { executeWithdrawalWithCascade } from './withdrawals/single/execute-with-cascade.js';
import { prepareBatchPendingState, validateBatchBalance } from './withdrawals/batch/prepare.js';
import { buildBatchManifest, broadcastBatchManifest, waitForBatchManifest, manifestToWithdrawals, getManifestTxHashSet } from './withdrawals/batch/manifest.js';
import { resolveUnsignedBatchProposal } from './withdrawals/batch/unsigned-proposal.js';
import { runBatchCoordinatorSignAndSubmit } from './withdrawals/batch/coordinator-sign-submit.js';
import { runWithdrawalMultisigSync } from './withdrawals/signing/multisig-sync.js';
import { maybeStartSignedInfoWait } from './withdrawals/signing/signed-info-wait.js';
import { startOrphanSweepTimer, stopOrphanSweepTimer } from './withdrawals/orphan-sweep.js';
import { selectWithdrawalsThatFit, getUnlockedBalanceForFit } from './withdrawals/fit-selection.js';
import { addToCarryover, isCarryoverStatus, CARRYOVER_STATUS, startCarryoverValidation, stopCarryoverValidation } from './withdrawals/carryover.js';
import { clearLocalSignStepCacheForWithdrawal } from './withdrawals/sign-step-cache.js';
import { isClusterOperational } from '../core/health.js';
const epochRunner = createWithdrawalEpochRunner({ executeWithdrawalBatch });
const { handleBurnEvent, handleSlotReservedEvent, handleWithdrawalRequeuedEvent } = createWithdrawalEventHandlers({
  queueWithdrawalForEpoch: epochRunner.queueWithdrawalForEpoch,
});
const tickPendingWithdrawals = createWithdrawalRetryTicker({ executeWithdrawalBatch, executeWithdrawalWithCascade });
function normalizeEnvMs(name, fallback) {
  const raw = process.env[name];
  const n = raw != null ? Number(raw) : NaN;
  if (!Number.isFinite(n) || n <= 0) return fallback;
  return Math.floor(n);
}
const WITHDRAWAL_WATCHDOG_INTERVAL_MS = normalizeEnvMs('WITHDRAWAL_WATCHDOG_INTERVAL_MS', 10000);
const WITHDRAWAL_LOCK_WATCHDOG_MS = normalizeEnvMs('WITHDRAWAL_LOCK_WATCHDOG_MS', 600000);
const WITHDRAWAL_BATCH_WATCHDOG_MS = normalizeEnvMs('WITHDRAWAL_BATCH_WATCHDOG_MS', 900000);
function tickWithdrawalWatchdog(node) {
  if (!node || !node._withdrawalMonitorRunning) return;
  const now = Date.now();
  const lockKeyRaw = node._withdrawalMultisigLock ? String(node._withdrawalMultisigLock).toLowerCase() : '';
  if (lockKeyRaw) {
    if (node._withdrawalMultisigLockWatchKey !== lockKeyRaw) {
      node._withdrawalMultisigLockWatchKey = lockKeyRaw;
      node._withdrawalMultisigLockWatchAt = now;
    }
    const at = typeof node._withdrawalMultisigLockWatchAt === 'number' ? node._withdrawalMultisigLockWatchAt : now;
    const ageMs = now - at;
    if (WITHDRAWAL_LOCK_WATCHDOG_MS > 0 && ageMs > WITHDRAWAL_LOCK_WATCHDOG_MS) {
      console.log(`[Withdrawal] Watchdog clearing stuck multisig lock ${lockKeyRaw.slice(0, 18)} age=${Math.floor(ageMs / 1000)}s`);
      const pending = node._pendingWithdrawals && node._pendingWithdrawals.get(lockKeyRaw);
      if (pending && pending.status !== 'completed') {
        pending.status = 'failed';
        pending.error = 'watchdog_withdrawal_lock_timeout';
      }
      if (pending) scheduleClearWithdrawalRounds(node, lockKeyRaw);
      node._withdrawalMultisigLock = null;
      node._withdrawalMultisigLockWatchKey = null;
      node._withdrawalMultisigLockWatchAt = null;
      scheduleWithdrawalStateSave(node);
    }
  } else {
    node._withdrawalMultisigLockWatchKey = null;
    node._withdrawalMultisigLockWatchAt = null;
  }
  if (node._withdrawalBatchInProgress) {
    const batchKeyRaw = node._withdrawalBatchInProgressKey ? String(node._withdrawalBatchInProgressKey).toLowerCase() : '';
    if (batchKeyRaw) {
      if (node._withdrawalBatchWatchKey !== batchKeyRaw) {
        node._withdrawalBatchWatchKey = batchKeyRaw;
        node._withdrawalBatchWatchAt = now;
      }
      const at = typeof node._withdrawalBatchWatchAt === 'number' ? node._withdrawalBatchWatchAt : now;
      const ageMs = now - at;
      if (WITHDRAWAL_BATCH_WATCHDOG_MS > 0 && ageMs > WITHDRAWAL_BATCH_WATCHDOG_MS) {
        console.log(`[Withdrawal] Watchdog clearing stuck batch ${batchKeyRaw.slice(0, 18)} age=${Math.floor(ageMs / 1000)}s`);
        const pending = node._pendingWithdrawals && node._pendingWithdrawals.get(batchKeyRaw);
        if (pending && pending.status !== 'completed') {
          pending.status = 'failed';
          pending.error = 'watchdog_batch_timeout';
        }
        if (pending) scheduleClearWithdrawalRounds(node, batchKeyRaw);
        const lockNow = node._withdrawalMultisigLock ? String(node._withdrawalMultisigLock).toLowerCase() : '';
        if (lockNow && lockNow === batchKeyRaw) {
          node._withdrawalMultisigLock = null;
          node._withdrawalMultisigLockWatchKey = null;
          node._withdrawalMultisigLockWatchAt = null;
        }
        node._withdrawalBatchInProgress = false;
        node._withdrawalBatchInProgressKey = null;
        node._withdrawalBatchWatchKey = null;
        node._withdrawalBatchWatchAt = null;
        scheduleWithdrawalStateSave(node);
      }
    }
  } else {
    node._withdrawalBatchWatchKey = null;
    node._withdrawalBatchWatchAt = null;
  }
}
export async function startWithdrawalMonitor(node) {
  if (node._withdrawalMonitorRunning) return;
  validateRoundAllocationsOnStartup();
  const bridgeEnabled = isBridgeEnabled();
  if (!bridgeEnabled) {
    console.log('[Withdrawal] Withdrawal monitoring disabled by configuration (BRIDGE_ENABLED=0)');
    return;
  }
  if (!node._clusterFinalized || !node._clusterFinalAddress) {
    console.log('[Withdrawal] Cannot start withdrawal monitor: cluster not finalized');
    return;
  }
  if (!node.bridge) {
    console.log('[Withdrawal] Cannot start withdrawal monitor: bridge contract not configured');
    return;
  }
  node._withdrawalMonitorRunning = true;
  node.getWithdrawalEpochStatus = () => getWithdrawalEpochStatus(node);
  node.getBribeStatus = () => getBribeStatusAsync(node);
  loadBridgeState(node);
  startBridgeStateSaver(node);
  startCleanupTimer(node);
  node._processedWithdrawals = node._processedWithdrawals || new Set();
  node._processedWithdrawalsMeta = node._processedWithdrawalsMeta || new Map();
  node._pendingWithdrawals = node._pendingWithdrawals || new Map();
  migrateLegacyPendingWithdrawals(node);
  for (const [k, data] of node._pendingWithdrawals) {
    if (!data || typeof data !== 'object') continue;
    const key = k ? String(k).toLowerCase() : '';
    if (!key) continue;
    const hasBatchWithdrawals = Array.isArray(data.batchWithdrawals) && data.batchWithdrawals.length > 0;
    const hasEthTxHashes = Array.isArray(data.ethTxHashes) && data.ethTxHashes.length > 0;
    if (!data.isBatch && hasBatchWithdrawals) {
      data.isBatch = true;
      if (!data.batchId) data.batchId = key;
    }
    if ((data.isBatch || hasBatchWithdrawals) && hasBatchWithdrawals && !hasEthTxHashes) {
      const hashes = data.batchWithdrawals
        .map((w) => (w && w.txHash ? String(w.txHash).toLowerCase() : ''))
        .filter((h) => h);
      if (hashes.length > 0) data.ethTxHashes = Array.from(new Set(hashes));
    }
  }
  if (WITHDRAWAL_PARTIAL_EPOCH_MODE) {
    let migrated = 0;
    for (const [, data] of node._pendingWithdrawals) {
      if (!data || typeof data !== 'object') continue;
      if (data.isBatch) continue;
      if (data.status === 'waiting_cluster_health') {
        data.status = CARRYOVER_STATUS.WAITING_CLUSTER;
        if (typeof data.firstSeenAtMs !== 'number') data.firstSeenAtMs = Date.now();
        migrated += 1;
      }
    }
    if (migrated > 0) {
      console.log(`[Withdrawal] Migrated ${migrated} pending withdrawal(s) from waiting_cluster_health to carryover_waiting_cluster`);
      scheduleWithdrawalStateSave(node);
    }
  }
  if (node._withdrawalRetryTimer) clearInterval(node._withdrawalRetryTimer);
  node._tickPendingWithdrawals = tickPendingWithdrawals;
  node._withdrawalRetryTimer = setInterval(() => { tickPendingWithdrawals(node); }, WITHDRAWAL_RETRY_INTERVAL_MS);
  node._withdrawalEventPollInFlight = false;
  if (node._withdrawalEventPollTimer) {
    clearInterval(node._withdrawalEventPollTimer);
    node._withdrawalEventPollTimer = null;
  }
  console.log('[Withdrawal] Starting withdrawal event monitor...');
  try {
    const activeClusterId = node._activeClusterId;
    if (!activeClusterId) {
      console.log('[Withdrawal] Cannot start withdrawal monitor: missing activeClusterId');
      node._withdrawalMonitorRunning = false;
      return;
    }
    try {
      epochRunner.initEpochRunner(node);
    } catch (e) {
      console.log('[Withdrawal] Failed to init epoch runner:', e.message || String(e));
    }
    let head;
    try {
      head = await node.provider.getBlockNumber();
    } catch (e) {
      console.log('[Withdrawal] Failed to fetch current block for withdrawal startup scan:', e.message || String(e));
      head = null;
    }
    if (typeof head === 'number' && Number.isFinite(head) && head >= 0) {
      const safeHead = WITHDRAWAL_MIN_CONFIRMATIONS > 0 ? Math.max(0, head - WITHDRAWAL_MIN_CONFIRMATIONS) : head;
      const cursorOk =
        typeof node._withdrawalLastScanBlock === 'number' &&
        Number.isFinite(node._withdrawalLastScanBlock) &&
        node._withdrawalLastScanBlock >= 0;
      if (!cursorOk) {
        node._withdrawalLastScanBlock = Math.max(0, safeHead - WITHDRAWAL_STARTUP_BACKFILL_BLOCKS);
        scheduleWithdrawalStateSave(node);
      } else if (node._withdrawalLastScanBlock > safeHead) {
        node._withdrawalLastScanBlock = safeHead;
        scheduleWithdrawalStateSave(node);
      }
    }
    await pollWithdrawalEvents(node, { handleBurnEvent, handleSlotReservedEvent, handleWithdrawalRequeuedEvent });
    node._withdrawalEventPollTimer = setInterval(() => {
      if (!node._withdrawalMonitorRunning) return;
      if (node._withdrawalEventPollInFlight) return;
      node._withdrawalEventPollInFlight = true;
      pollWithdrawalEvents(node, { handleBurnEvent, handleSlotReservedEvent, handleWithdrawalRequeuedEvent })
        .catch((err) => {
          const msg = (err && err.message) ? err.message : String(err);
          console.log('[Withdrawal] Error in TokensBurned poller:', msg);
        })
        .finally(() => {
          node._withdrawalEventPollInFlight = false;
        });
    }, WITHDRAWAL_EVENT_POLL_INTERVAL_MS);
    console.log('[Withdrawal] Now polling TokensBurned events every ' + WITHDRAWAL_EVENT_POLL_INTERVAL_MS + 'ms');
    setupWithdrawalClaimListener(node);
    startFraudMonitor(node);
    startOrphanSweepTimer(node);
    startCarryoverValidation(node);
    if (node._withdrawalWatchdogTimer) clearInterval(node._withdrawalWatchdogTimer);
    if (WITHDRAWAL_WATCHDOG_INTERVAL_MS > 0) {
      node._withdrawalWatchdogTimer = setInterval(() => { tickWithdrawalWatchdog(node); }, WITHDRAWAL_WATCHDOG_INTERVAL_MS);
    }
  } catch (e) {
    console.log('[Withdrawal] Failed to setup withdrawal monitor:', e.message || String(e));
    node._withdrawalMonitorRunning = false;
    if (node._withdrawalEventPollTimer) {
      clearInterval(node._withdrawalEventPollTimer);
      node._withdrawalEventPollTimer = null;
    }
  }
}
export function stopWithdrawalMonitor(node) {
  try { epochRunner.stopEpochRunner(node); } catch {}
  node._withdrawalMonitorRunning = false;
  stopFraudMonitor();
  stopOrphanSweepTimer();
  stopCarryoverValidation(node);
  if (node._withdrawalRetryTimer) {
    clearInterval(node._withdrawalRetryTimer);
    node._withdrawalRetryTimer = null;
  }
  if (node._withdrawalEventPollTimer) {
    clearInterval(node._withdrawalEventPollTimer);
    node._withdrawalEventPollTimer = null;
  }
  node._withdrawalEventPollInFlight = false;
  if (node._withdrawalWatchdogTimer) {
    clearInterval(node._withdrawalWatchdogTimer);
    node._withdrawalWatchdogTimer = null;
  }
  console.log('[Withdrawal] Withdrawal monitor stopped');
}
function setupWithdrawalClaimListener(node) {
  if (!node.p2p || node._withdrawalClaimListenerSetup) return;
  node._withdrawalClaimListenerSetup = true;
  setupMultisigSyncResponder(node);
  setupWithdrawalSignStepListener(node, {
    verifyPendingBurnsOnChain,
    verifyTxsetMatchesPending,
  });
  setupWithdrawalUnsignedProposalListener(node, { runUnsignedWithdrawalTxConsensus });
}
async function executeWithdrawalBatch(node, batchId, withdrawals) {
  if (!node || !batchId || !Array.isArray(withdrawals) || withdrawals.length === 0) return;
  const members = Array.isArray(node._clusterMembers)
    ? node._clusterMembers.map((a) => String(a || '').toLowerCase())
    : [];
  if (!Array.isArray(members) || members.length === 0) return;
  const self = node.wallet && node.wallet.address ? String(node.wallet.address).toLowerCase() : null;
  if (!self) return;
  const key = String(batchId).toLowerCase();
  const shortEth = getWithdrawalShortHash(key);
  node._pendingWithdrawals = node._pendingWithdrawals || new Map();
  const existingBatch = node._pendingWithdrawals.get(key);
  const isRetryBatch = !!(existingBatch && existingBatch.isBatch);
  const batchEpochIndex = (() => {
    const parts = key.split(':');
    if (parts.length !== 2) return null;
    const n = Number(parts[1]);
    if (!Number.isFinite(n) || n < 0 || !Number.isInteger(n)) return null;
    return n;
  })();
  if (WITHDRAWAL_PARTIAL_EPOCH_MODE) {
    const clusterStatus = await isClusterOperational(node);
    if (!clusterStatus.operational) {
      if (isRetryBatch) {
        existingBatch.error = `cluster_not_operational:${clusterStatus.reason}`;
        scheduleWithdrawalStateSave(node);
      } else {
        if (existingBatch) node._pendingWithdrawals.delete(key);
        for (const w of withdrawals) {
          addToCarryover(node, w, CARRYOVER_STATUS.WAITING_CLUSTER, batchEpochIndex);
        }
        scheduleWithdrawalStateSave(node);
      }
      console.log(`[Withdrawal] Cluster not operational (${clusterStatus.reason}), deferring ${withdrawals.length} withdrawal(s)`);
      return;
    }
  }
  if (node._withdrawalBatchInProgress) {
    const pending = existingBatch || {};
    pending.isBatch = true;
    pending.batchId = key;
    pending.status = 'pending';
    pending.error = 'batch_in_progress';
    if (!Array.isArray(pending.batchWithdrawals) || pending.batchWithdrawals.length === 0) {
      pending.batchWithdrawals = withdrawals;
    }
    if (!Array.isArray(pending.ethTxHashes) || pending.ethTxHashes.length === 0) {
      const hashes = withdrawals
        .map((w) => (w && w.txHash ? String(w.txHash).toLowerCase() : ''))
        .filter((h) => h);
      pending.ethTxHashes = Array.from(new Set(hashes));
    }
    node._pendingWithdrawals.set(key, pending);
    scheduleWithdrawalStateSave(node);
    return;
  }
  node._withdrawalBatchInProgress = true;
  node._withdrawalBatchInProgressKey = key;
  const existing = existingBatch || {};
  existing.isBatch = true;
  existing.batchId = key;
  if (!Array.isArray(existing.batchWithdrawals) || existing.batchWithdrawals.length === 0) {
    existing.batchWithdrawals = withdrawals;
  }
  if (!Array.isArray(existing.ethTxHashes) || existing.ethTxHashes.length === 0) {
    const hashes = withdrawals
      .map((w) => (w && w.txHash ? String(w.txHash).toLowerCase() : ''))
      .filter((h) => h);
    existing.ethTxHashes = Array.from(new Set(hashes));
  }
  if (typeof existing.startedAt !== 'number') existing.startedAt = Date.now();
  if (typeof existing.firstAttemptAt !== 'number') {
    existing.firstAttemptAt = Date.now();
  }
  node._pendingWithdrawals.set(key, existing);
  const electionResult = await runWithdrawalCoordinatorElection(node, key);
  if (!electionResult || !electionResult.success) {
    console.log(`[Withdrawal] Coordinator election failed for batch ${shortEth}: ${electionResult?.reason || 'unknown'}`);
    const pending = node._pendingWithdrawals.get(key);
    if (pending) {
      pending.status = 'failed';
      pending.error = `coordinator_election_failed:${electionResult?.reason || 'unknown'}`;
    }
    node._withdrawalBatchInProgress = false;
    node._withdrawalBatchInProgressKey = null;
    return;
  }
  const isCoordinator = electionResult.isCoordinator;
  console.log(`[Withdrawal] Batch ${shortEth} coordinator election complete: isCoordinator=${isCoordinator}`);
  const pendingAfterElection = node._pendingWithdrawals.get(key);
  if (pendingAfterElection) {
    pendingAfterElection.coordinator = electionResult && electionResult.coordinator ? String(electionResult.coordinator).toLowerCase() : null;
    pendingAfterElection.isCoordinator = !!isCoordinator;
  }
  scheduleWithdrawalStateSave(node);
  const existingLock = node._withdrawalMultisigLock;
  if (existingLock && existingLock !== key) {
    const pending = node._pendingWithdrawals.get(key);
    if (pending) {
      pending.isBatch = true;
      pending.status = 'pending';
      pending.error = 'withdrawal_lock_active';
      if (!Array.isArray(pending.batchWithdrawals) || pending.batchWithdrawals.length === 0) {
        pending.batchWithdrawals = withdrawals;
      }
    }
    scheduleWithdrawalStateSave(node);
    node._withdrawalBatchInProgress = false;
    node._withdrawalBatchInProgressKey = null;
    return;
  }
  const lockAcquired = !existingLock;
  if (lockAcquired) node._withdrawalMultisigLock = key;
  try {
    clearLocalSignStepCacheForWithdrawal(node, key);
    const syncResult = await runWithdrawalMultisigSync(node, key);
    if (!syncResult || !syncResult.success) {
      console.log(
        '[Withdrawal] Batch withdrawal multisig sync failed:',
        syncResult && syncResult.reason ? syncResult.reason : 'unknown',
      );
      const pending = node._pendingWithdrawals.get(key);
      if (pending) {
        const curAttempt = typeof pending.attempt !== 'undefined' ? Number(pending.attempt) : 0;
        const nextAttempt = Number.isFinite(curAttempt) && curAttempt >= 0 ? Math.floor(curAttempt) + 1 : 1;
        pending.attempt = nextAttempt;
        pending.status = 'failed';
        pending.error = `multisig_sync_failed:${(syncResult && syncResult.reason) || 'unknown'}`;
      }
      scheduleClearWithdrawalRounds(node, key);
      return;
    }
    console.log(
      `[Withdrawal] Batch withdrawal multisig sync completed; importedOutputs=${
        typeof syncResult.importedOutputs === 'number' ? syncResult.importedOutputs : 'unknown'
      }`,
    );
    let withdrawalsToProcess = withdrawals;
    if (WITHDRAWAL_PARTIAL_EPOCH_MODE) {
      const unlockedBalance = await getUnlockedBalanceForFit(node);
      if (unlockedBalance === null) {
        console.log('[Withdrawal] Failed to get unlocked balance for fit-selection, deferring batch');
        const pending = node._pendingWithdrawals.get(key);
        if (pending) {
          pending.status = 'deferred';
          pending.error = 'balance_fetch_failed';
        }
        if (!isRetryBatch) {
          for (const w of withdrawals) {
            addToCarryover(node, w, CARRYOVER_STATUS.WAITING_UNLOCKED, batchEpochIndex);
          }
          node._pendingWithdrawals.delete(key);
        }
        scheduleWithdrawalStateSave(node);
        return;
      }
      const { selected, deferred } = selectWithdrawalsThatFit(node, withdrawals, unlockedBalance, batchEpochIndex);
      if (selected.length === 0) {
        console.log(`[Withdrawal] No withdrawals fit within unlocked balance (${formatXmrAtomic(unlockedBalance)} XMR), all ${deferred.length} deferred`);
        const pending = node._pendingWithdrawals.get(key);
        if (pending) {
          pending.status = 'deferred';
          pending.error = 'insufficient_unlocked_balance';
        }
        if (!isRetryBatch) {
          node._pendingWithdrawals.delete(key);
          scheduleWithdrawalStateSave(node);
        }
        return;
      }
      if (deferred.length > 0) {
        console.log(`[Withdrawal] Fit-selection: ${selected.length} selected, ${deferred.length} deferred to carryover`);
      }
      withdrawalsToProcess = selected;
    }
    let withdrawalsFinal = withdrawalsToProcess;
    if (WITHDRAWAL_BATCH_MANIFEST_PBFT) {
      const pendingForManifest = node._pendingWithdrawals.get(key);
      const attempt0 = pendingForManifest && typeof pendingForManifest.attempt !== 'undefined' ? Number(pendingForManifest.attempt) : 0;
      const attempt = Number.isFinite(attempt0) && attempt0 >= 0 ? Math.floor(attempt0) : 0;
      const coordinatorLc = pendingForManifest && pendingForManifest.coordinator ? String(pendingForManifest.coordinator).toLowerCase() : null;
      if (!coordinatorLc) {
        const pending = node._pendingWithdrawals.get(key);
        if (pending) {
          pending.status = 'failed';
          pending.error = 'manifest_missing_coordinator';
        }
        scheduleClearWithdrawalRounds(node, key);
        return;
      }
      let manifestRes = null;
      if (isCoordinator) {
        const built = buildBatchManifest(node, key, attempt, withdrawalsToProcess);
        if (!built || !built.ok) {
          const pending = node._pendingWithdrawals.get(key);
          if (pending) {
            pending.status = 'failed';
            pending.error = built && built.reason ? `manifest_build_failed:${built.reason}` : 'manifest_build_failed';
          }
          scheduleClearWithdrawalRounds(node, key);
          return;
        }
        const ok = await broadcastBatchManifest(node, key, attempt, built.manifest);
        if (!ok) {
          const pending = node._pendingWithdrawals.get(key);
          if (pending) {
            pending.status = 'failed';
            pending.error = 'manifest_broadcast_failed';
          }
          scheduleClearWithdrawalRounds(node, key);
          return;
        }
        manifestRes = { manifest: built.manifest, manifestHash: built.manifestHash, attempt: built.attempt };
      }
      if (!manifestRes) {
        manifestRes = await waitForBatchManifest(node, key, attempt, coordinatorLc, WITHDRAWAL_MANIFEST_WAIT_MS);
      }
      if (!manifestRes || !manifestRes.manifest || !manifestRes.manifestHash) {
        const pending = node._pendingWithdrawals.get(key);
        if (pending) {
          pending.status = 'failed';
          pending.error = 'manifest_wait_timeout';
        }
        scheduleClearWithdrawalRounds(node, key);
        return;
      }
      const pbftRes = await runWithdrawalBatchManifestConsensus(node, key, attempt, manifestRes.manifestHash);
      if (!pbftRes || !pbftRes.success) {
        const pending = node._pendingWithdrawals.get(key);
        if (pending) {
          pending.status = 'failed';
          pending.error = pbftRes && pbftRes.reason ? `manifest_pbft_failed:${pbftRes.reason}` : 'manifest_pbft_failed';
        }
        scheduleClearWithdrawalRounds(node, key);
        return;
      }
      const txSet = getManifestTxHashSet(manifestRes.manifest);
      if (txSet.size > 0) {
        const status = WITHDRAWAL_PARTIAL_EPOCH_MODE ? CARRYOVER_STATUS.WAITING_UNLOCKED : CARRYOVER_STATUS.WAITING_CAPACITY;
        for (const w of withdrawalsToProcess) {
          const txh = w && w.txHash ? String(w.txHash).toLowerCase() : '';
          if (txh && !txSet.has(txh)) addToCarryover(node, w, status, batchEpochIndex);
        }
      }
      withdrawalsFinal = manifestToWithdrawals(manifestRes.manifest);
      const pending = node._pendingWithdrawals.get(key);
      if (pending) {
        pending.manifestHash = manifestRes.manifestHash;
        pending.manifestAttempt = attempt;
        pending.burnsOnChainVerified = false;
        pending.txsetVerified = false;
      }
      scheduleWithdrawalStateSave(node);
    }
    const now = Date.now();
    const firstAttemptAt = existing.firstAttemptAt;
    const prep = prepareBatchPendingState(node, {
      key,
      withdrawals: withdrawalsFinal,
      now,
      firstAttemptAt,
      electionResult,
      isCoordinator,
    });
    if (!prep || !prep.ok) {
      console.log('[Withdrawal] prepareBatchPendingState failed:', prep?.reason || 'unknown');
      return;
    }
    const { destinations, totalAmount, batchWithdrawals } = prep;
    if (WITHDRAWAL_PARTIAL_EPOCH_MODE) {
      const removed = [];
      for (const w of withdrawalsFinal) {
        const txHash = w && w.txHash ? String(w.txHash).toLowerCase() : '';
        if (!txHash) continue;
        const entry = node._pendingWithdrawals.get(txHash);
        if (entry && isCarryoverStatus(entry.status)) {
          removed.push({ txHash, entry });
          node._pendingWithdrawals.delete(txHash);
        }
      }
      if (removed.length > 0) {
        const ok = flushWithdrawalStateSave(node);
        if (!ok) {
          for (const r of removed) {
            if (!r || !r.txHash || !r.entry) continue;
            node._pendingWithdrawals.set(r.txHash, r.entry);
          }
          if (existing) node._pendingWithdrawals.set(key, existing);
          else node._pendingWithdrawals.delete(key);
          scheduleWithdrawalStateSave(node);
          const pending = node._pendingWithdrawals.get(key);
          if (pending) {
            pending.status = 'deferred';
            pending.error = 'state_persist_failed';
          }
          return;
        }
      }
    }
    if (!WITHDRAWAL_PARTIAL_EPOCH_MODE) {
      const balanceCheck = await validateBatchBalance(node, totalAmount);
      if (!balanceCheck.ok) {
        console.log(`[Withdrawal] Balance pre-check failed: ${balanceCheck.reason}`);
        const pending = node._pendingWithdrawals.get(key);
        if (pending) {
          pending.status = 'deferred';
          pending.error = `balance_precheck_failed:${balanceCheck.reason}`;
        }
        return;
      }
    }
    console.log(`[Withdrawal] Executing batch withdrawal with ${withdrawalsFinal.length} burns`);
    console.log(`  Total amount: ${formatXmrAtomic(totalAmount)} XMR`);
    const pendingAfterSync = node._pendingWithdrawals.get(key);
    if (pendingAfterSync && Array.isArray(syncResult.signerSet) && syncResult.signerSet.length > 0) {
      const hasLockedUnsigned = !!(pendingAfterSync.pbftUnsignedHash && pendingAfterSync.txsetHashUnsigned);
      if (!hasLockedUnsigned) {
        pendingAfterSync.signerSet = syncResult.signerSet.map((a) => String(a || '').toLowerCase());
      }
    }
    const unsignedProposal = await resolveUnsignedBatchProposal(node, {
      key,
      isCoordinator,
      destinations,
      batchWithdrawals,
      pendingAfterSync,
    });
    if (!unsignedProposal) return;
    const txData = unsignedProposal.txData;
    const unsignedHash = unsignedProposal.unsignedHash;
    console.log(`[Withdrawal] Running unsigned tx PBFT consensus for batch ${shortEth}`);
    const txConsensus = await runUnsignedWithdrawalTxConsensus(node, key, unsignedHash);
    if (!txConsensus || !txConsensus.success) {
      const reason = txConsensus && txConsensus.reason ? txConsensus.reason : 'tx_pbft_failed';
      console.log(`[Withdrawal] Unsigned tx PBFT failed: ${reason}`);
      const pending = node._pendingWithdrawals.get(key);
      if (pending) {
        pending.status = 'failed';
        pending.error = reason;
      }
      return;
    }
    console.log('[Withdrawal] Unsigned tx PBFT consensus reached');
    if (!isCoordinator) {
      const releaseLockOnComplete = () => {
        if (lockAcquired && node._withdrawalMultisigLock === key) {
          node._withdrawalMultisigLock = null;
        }
      };
      maybeStartSignedInfoWait(node, key, null, releaseLockOnComplete);
      console.log('[Withdrawal] Non-coordinator waiting for signing requests...');
      return;
    }
    await runBatchCoordinatorSignAndSubmit(node, { key, txData, selfAddress: self });
  } catch (e) {
    console.log('[Withdrawal] Batch withdrawal execution error:', e.message || String(e));
    if (node._pendingWithdrawals) {
      const pending = node._pendingWithdrawals.get(key);
      if (pending) {
        pending.status = 'error';
        pending.error = e.message;
      }
    }
  } finally {
    if (isCoordinator && lockAcquired && node._withdrawalMultisigLock === key) {
      node._withdrawalMultisigLock = null;
    }
    node._withdrawalBatchInProgress = false;
    node._withdrawalBatchInProgressKey = null;
  }
}
export function getWithdrawalStatus(node, ethTxHash) {
  if (!node || !ethTxHash) return null;
  const key = String(ethTxHash).toLowerCase();
  if (node._pendingWithdrawals) {
    const direct = node._pendingWithdrawals.get(key);
    if (direct) return direct;
    for (const [, data] of node._pendingWithdrawals) {
      if (!data || !data.isBatch || !Array.isArray(data.ethTxHashes)) continue;
      for (const h of data.ethTxHashes) {
        if (typeof h === 'string' && h.toLowerCase() === key) return data;
      }
    }
  }
  if (node._withdrawalEpoch && Array.isArray(node._withdrawalEpoch.withdrawals)) {
    const epoch = node._withdrawalEpoch;
    const startMs = epoch.startSec * 1000;
    const endSec =
      typeof epoch.endSec === 'number'
        ? epoch.endSec
        : epoch.startSec + (typeof epoch.durationSec === 'number' ? epoch.durationSec : WITHDRAWAL_EPOCH_SECONDS);
    const endMs = endSec * 1000;
    for (const w of epoch.withdrawals) {
      if (!w || !w.txHash) continue;
      if (String(w.txHash).toLowerCase() === key) {
        return {
          status: 'epoch_pending',
          epochStartMs: startMs,
          epochEndMs: endMs,
          txHash: w.txHash,
          user: w.user,
          xmrAddress: w.xmrAddress,
          xmrAmount: w.xmrAmount,
        };
      }
    }
  }
  if (node._processedWithdrawalsMeta && typeof node._processedWithdrawalsMeta.get === 'function') {
    const meta = node._processedWithdrawalsMeta.get(key);
    if (meta) return meta;
  }
  return null;
}
export function getAllPendingWithdrawals(node) {
  if (!node._pendingWithdrawals) return [];
  const result = [];
  for (const [txHash, data] of node._pendingWithdrawals) {
    result.push({ ethTxHash: txHash, ...data });
  }
  return result;
}
export function getWithdrawalEpochStatus(node) {
  return epochRunner.getEpochQueueStatus(node);
}
async function getBribeStatusAsync(node) {
  const clusterId = node._activeClusterId;
  if (clusterId && node.bridge && typeof node.bridge.getBribeStatus === 'function') {
    try {
      const result = await node.bridge.getBribeStatus(clusterId);
      const currentEpoch = Number(result.currentEpoch ?? result[0]);
      const targetEpoch = Number(result.targetEpoch ?? result[1]);
      const reservedSlots = Number(result.reservedSlots ?? result[2]);
      const availableBribeSlots = Number(result.availableBribeSlots ?? result[3]);
      let nextSlotCostZfi = null;
      try {
        const atomic = BigInt(result.nextSlotCost ?? result[4] ?? 0);
        nextSlotCostZfi = Number(atomic / 1000000000000000000n);
      } catch {
        nextSlotCostZfi = Number(result.nextSlotCost ?? result[4]);
      }
      const windowOpen = Boolean(result.windowOpen ?? result[5]);
      const windowClosesInSeconds = Number(result.windowClosesInSeconds ?? result[6]);
      const currentEpochLocked = Boolean(result.currentEpochLocked ?? result[7]);
      return {
        currentEpoch,
        targetEpoch,
        reservedSlots,
        availableBribeSlots,
        nextSlotCostZfi,
        windowOpen,
        windowClosesInSeconds,
        currentEpochLocked,
      };
    } catch (e) {
      console.log('[Bribe] Failed to fetch bribe status from contract:', e.message || String(e));
    }
  }
  return await getBribeStatusFallback(node);
}
async function getBribeStatusFallback(node) {
  const epoch = node._withdrawalEpoch;
  const clusterId = node._activeClusterId;
  const currentEpoch = epoch ? epoch.epochIndex : null;
  const targetEpoch = currentEpoch !== null ? currentEpoch + 1 : null;
  const reservedSlots = targetEpoch !== null ? getReservedCount(node, targetEpoch) : 0;
  const availableBribeSlots = Math.max(0, MAX_BRIBE_SLOTS - reservedSlots);
  const nextSlotCost = reservedSlots >= MAX_BRIBE_SLOTS ? 0 : BRIBE_BASE_COST * Math.pow(2, reservedSlots);
  let windowOpen = false;
  let windowClosesInSeconds = 0;
  let currentEpochLocked = false;
  if (epoch && typeof epoch.startSec === 'number') {
    const epochEndSec =
      typeof epoch.endSec === 'number'
        ? epoch.endSec
        : epoch.startSec + (typeof epoch.durationSec === 'number' ? epoch.durationSec : WITHDRAWAL_EPOCH_SECONDS);
    const nowSec = Math.floor(Date.now() / 1000);
    const windowStartSec = epoch.startSec + BRIBE_WINDOW_BUFFER;
    const windowEndSec = epochEndSec - BRIBE_WINDOW_BUFFER;
    windowOpen = nowSec >= windowStartSec && nowSec <= windowEndSec;
    if (windowOpen) {
      windowClosesInSeconds = Math.max(0, windowEndSec - nowSec);
    }
  }
  if (clusterId && currentEpoch !== null && node.bridge && typeof node.bridge.epochLocked === 'function') {
    try {
      currentEpochLocked = await node.bridge.epochLocked(clusterId, currentEpoch);
    } catch (e) {
      console.log('[Bribe] Failed to check epoch locked status:', e.message || String(e));
    }
  }
  if (currentEpochLocked) {
    windowOpen = false;
  }
  return {
    currentEpoch,
    targetEpoch,
    reservedSlots,
    availableBribeSlots,
    nextSlotCostZfi: nextSlotCost,
    windowOpen,
    windowClosesInSeconds,
    currentEpochLocked,
  };
}
