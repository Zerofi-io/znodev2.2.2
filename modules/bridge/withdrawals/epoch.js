import {
  MAX_WITHDRAWALS_PER_BATCH,
  WITHDRAWAL_EPOCH_SECONDS,
  WITHDRAWAL_MIN_CONFIRMATIONS,
  WITHDRAWAL_PARTIAL_EPOCH_MODE,
} from './config.js';
import { scheduleSweepAfterWithdrawals, isSweepEnabled } from '../sweep.js';
import {
  initBribeTracking,
  getReservedCount,
  getAvailableStandardSlots,
  pruneOldEpochData,
  syncReservationsFromContract,
  isUserBribePriority,
  isNearEpochBoundary,
} from './bribe-tracking.js';
import {
  PRIORITY_CLASS,
  CARRYOVER_STATUS,
  getActiveCarryoverQueue,
  addToCarryover,
  canonicalWithdrawalSort,
} from './carryover.js';
import { scheduleWithdrawalStateSave } from './pending-store.js';
const LATEST_BLOCK_CACHE_MS = 1500;
const EPOCH_ENSURE_INTERVAL_MS = Number(process.env.WITHDRAWAL_EPOCH_ENSURE_INTERVAL_MS || 15000);
export function createWithdrawalEpochRunner({ executeWithdrawalBatch } = {}) {
  if (typeof executeWithdrawalBatch !== 'function') {
    throw new TypeError('createWithdrawalEpochRunner requires executeWithdrawalBatch');
  }
  async function getLatestBlock(node) {
    if (!node || !node.provider) return null;
    const now = Date.now();
    const cached = node._latestBlockCache;
    if (
      cached &&
      cached.block &&
      typeof cached.fetchedAtMs === 'number' &&
      (now - cached.fetchedAtMs) < LATEST_BLOCK_CACHE_MS
    ) {
      return cached.block;
    }
    try {
      const block = await node.provider.getBlock('latest');
      if (!block) return null;
      let timestamp = null;
      if (typeof block.timestamp === 'number') {
        timestamp = block.timestamp;
      } else if (typeof block.timestamp === 'bigint') {
        timestamp = Number(block.timestamp);
      }
      if (!timestamp || !Number.isFinite(timestamp) || timestamp <= 0) {
        return null;
      }
      const number =
        typeof block.number === 'number' && Number.isFinite(block.number)
          ? block.number
          : null;
      const normalized = { number, timestamp };
      node._latestBlockCache = { fetchedAtMs: now, block: normalized };
      return normalized;
    } catch (e) {
      console.log('[Epoch] Failed to get latest block:', e.message || String(e));
      return null;
    }
  }
  function computeEpochFromTimestamp(timestamp, epochSeconds) {
    if (!timestamp || timestamp <= 0) return 0;
    const seconds =
      typeof epochSeconds === 'number' && Number.isFinite(epochSeconds) && epochSeconds > 0
        ? epochSeconds
        : WITHDRAWAL_EPOCH_SECONDS;
    return Math.floor(timestamp / seconds);
  }
  async function getEpochDurationSeconds(node) {
    if (!node || !node.bridge || typeof node.bridge.epochDuration !== 'function') {
      return null;
    }
    const now = Date.now();
    const cached = node._epochDurationCache;
    if (
      cached &&
      typeof cached.fetchedAtMs === 'number' &&
      typeof cached.value === 'number' &&
      (now - cached.fetchedAtMs) < 10000
    ) {
      return cached.value;
    }
    try {
      const raw = await node.bridge.epochDuration();
      const v = typeof raw === 'bigint' ? Number(raw) : Number(raw);
      if (!Number.isFinite(v) || v <= 0) {
        return null;
      }
      const seconds = Math.floor(v);
      node._epochDurationCache = { fetchedAtMs: now, value: seconds };
      return seconds;
    } catch (e) {
      const last = node._epochDurationLastErrorAt || 0;
      if (now - last > 60000) {
        node._epochDurationLastErrorAt = now;
        console.log('[Epoch] Failed to fetch epochDuration from contract:', e.message || String(e));
      }
      return null;
    }
  }
  function getEpochCounts(epoch) {
    let retryCount = 0;
    let bribeCount = 0;
    let standardCount = 0;
    if (epoch && Array.isArray(epoch.withdrawals)) {
      for (const w of epoch.withdrawals) {
        if (!w) continue;
        if (w.priorityClass === PRIORITY_CLASS.RETRY) retryCount += 1;
        else if (w.priorityClass === PRIORITY_CLASS.BRIBE) bribeCount += 1;
        else standardCount += 1;
      }
    }
    return { retryCount, bribeCount, standardCount, total: retryCount + bribeCount + standardCount };
  }

  function normalizePriorityClass(node, epochIndex, w) {
    const p = w && w.priorityClass ? w.priorityClass : null;
    if (p === PRIORITY_CLASS.RETRY || p === PRIORITY_CLASS.BRIBE || p === PRIORITY_CLASS.STANDARD) return p;
    if (epochIndex !== null && w && w.user && isUserBribePriority(node, epochIndex, w.user)) {
      return PRIORITY_CLASS.BRIBE;
    }
    return PRIORITY_CLASS.STANDARD;
  }
  async function executeWithdrawalEpoch(node) {
    if (!node) return;
    const epoch = node._withdrawalEpoch;
    node._withdrawalEpoch = null;
    node._withdrawalEpochTimer = null;
    const epochIndex = epoch && typeof epoch.epochIndex === 'number' ? epoch.epochIndex : null;
    if (epochIndex !== null) pruneOldEpochData(node, epochIndex);
    const carryoverItems = getActiveCarryoverQueue(node);
    const epochItems = epoch && Array.isArray(epoch.withdrawals) ? epoch.withdrawals : [];
    const candidates = [];
    const seenTx = new Set();
    const addCandidate = (w, fromCarryover) => {
      if (!w || !w.txHash || !w.xmrAddress) return;
      const txHash = String(w.txHash).toLowerCase();
      if (!txHash || seenTx.has(txHash)) return;
      seenTx.add(txHash);
      const firstSeenAtMs =
        typeof w.firstSeenAtMs === 'number' && Number.isFinite(w.firstSeenAtMs)
          ? w.firstSeenAtMs
          : typeof w.timestamp === 'number' && Number.isFinite(w.timestamp)
            ? w.timestamp
            : Date.now();
      candidates.push({
        txHash,
        user: w.user || null,
        xmrAddress: w.xmrAddress || null,
        xmrAmountAtomic: w.xmrAmountAtomic,
        xmrAmount: w.xmrAmount,
        zxmrAmount: w.zxmrAmount,
        blockNumber: w.blockNumber,
        timestamp: w.timestamp,
        firstSeenAtMs,
        priorityClass: normalizePriorityClass(node, epochIndex, w),
        fromCarryover,
      });
    };
    for (const w of carryoverItems) addCandidate(w, true);
    for (const w of epochItems) addCandidate(w, false);
    if (candidates.length === 0) {
      if (isSweepEnabled()) scheduleSweepAfterWithdrawals(node);
      return;
    }
    candidates.sort(canonicalWithdrawalSort);
    const selected = candidates.slice(0, MAX_WITHDRAWALS_PER_BATCH);
    if (selected.length === 0) {
      if (isSweepEnabled()) scheduleSweepAfterWithdrawals(node);
      return;
    }
    const withdrawalsToExecute = selected.map((w) => ({
      txHash: w.txHash,
      user: w.user,
      xmrAddress: w.xmrAddress,
      xmrAmountAtomic: w.xmrAmountAtomic,
      xmrAmount: w.xmrAmount,
      zxmrAmount: w.zxmrAmount,
      blockNumber: w.blockNumber,
      timestamp: w.timestamp,
      priorityClass: w.priorityClass,
    }));
    const baseId = epoch && epoch.id ? epoch.id : withdrawalsToExecute[0].txHash;
    let promise = null;
    try {
      promise = executeWithdrawalBatch(node, baseId, withdrawalsToExecute);
    } catch (e) {
      console.log('[Withdrawal] Error executing epoch batch:', e.message || String(e));
    }
    const batchKey = String(baseId || '').toLowerCase();
    const batchEntry = node._pendingWithdrawals && batchKey ? node._pendingWithdrawals.get(batchKey) : null;
    const batchStarted = !!(promise && batchEntry && batchEntry.isBatch);
    if (!batchStarted) {
      for (const w of epochItems) {
        if (!w || !w.txHash || !w.xmrAddress) continue;
        addToCarryover(node, w, CARRYOVER_STATUS.WAITING_CAPACITY, epochIndex);
      }
      scheduleWithdrawalStateSave(node);
      try {
        if (promise) await promise;
      } catch (e) {
        console.log('[Withdrawal] Error executing epoch batch:', e.message || String(e));
      }
      if (isSweepEnabled()) scheduleSweepAfterWithdrawals(node);
      return;
    }
    let carryoverRemoved = 0;
    for (const w of selected) {
      if (!w || !w.fromCarryover) continue;
      const txKey = String(w.txHash || '').toLowerCase();
      if (!txKey || txKey === batchKey) continue;
      if (node._pendingWithdrawals && node._pendingWithdrawals.delete(txKey)) {
        carryoverRemoved += 1;
      }
    }
    let deferred = 0;
    for (let i = MAX_WITHDRAWALS_PER_BATCH; i < candidates.length; i += 1) {
      const w = candidates[i];
      if (!w || w.fromCarryover) continue;
      if (addToCarryover(node, w, CARRYOVER_STATUS.WAITING_CAPACITY, epochIndex)) {
        deferred += 1;
      }
    }
    if (carryoverRemoved > 0 || deferred > 0) {
      scheduleWithdrawalStateSave(node);
      if (deferred > 0) {
        console.log(`[Epoch] Deferred ${deferred} withdrawal(s) to carryover (capacity)`);
      }
    }
    try {
      if (promise) await promise;
    } catch (e) {
      console.log('[Withdrawal] Error executing epoch batch:', e.message || String(e));
    }
    node._lastWithdrawalCompletedAt = Date.now();
    if (isSweepEnabled()) {
      scheduleSweepAfterWithdrawals(node);
    }
  }
  async function ensureWithdrawalEpoch(node) {
    if (!node) return null;
    initBribeTracking(node);
    const latest = await getLatestBlock(node);
    const blockTimestamp = latest && typeof latest.timestamp === 'number' ? latest.timestamp : null;
    if (!blockTimestamp) {
      return node._withdrawalEpoch || null;
    }
    const epochDurationSec = await getEpochDurationSeconds(node);
    const effectiveEpochSeconds =
      typeof epochDurationSec === 'number' && Number.isFinite(epochDurationSec) && epochDurationSec > 0
        ? epochDurationSec
        : WITHDRAWAL_EPOCH_SECONDS;
    const epochIndex = computeEpochFromTimestamp(blockTimestamp, effectiveEpochSeconds);
    const epochStartSec = epochIndex * effectiveEpochSeconds;
    const epochEndSec = epochStartSec + effectiveEpochSeconds;
    const secondsUntilEnd = Math.max(0, epochEndSec - blockTimestamp);
    const isNewEpoch = !node._withdrawalEpoch || node._withdrawalEpoch.epochIndex !== epochIndex;
    const needsUpdate =
      !isNewEpoch &&
      node._withdrawalEpoch &&
      (
        node._withdrawalEpoch.startSec !== epochStartSec ||
        node._withdrawalEpoch.endSec !== epochEndSec ||
        node._withdrawalEpoch.durationSec !== effectiveEpochSeconds
      );
    if (isNewEpoch) {
      if (node._withdrawalEpoch && typeof node._withdrawalEpoch.epochIndex === 'number' && node._withdrawalEpoch.epochIndex < epochIndex) {
        if (node._withdrawalEpochTimer) {
          clearTimeout(node._withdrawalEpochTimer);
          node._withdrawalEpochTimer = null;
        }
        const hasEpochWithdrawals = Array.isArray(node._withdrawalEpoch.withdrawals) && node._withdrawalEpoch.withdrawals.length > 0;
        const carryoverNow = getActiveCarryoverQueue(node);
        const hasCarryover = Array.isArray(carryoverNow) && carryoverNow.length > 0;
        if (hasEpochWithdrawals || hasCarryover) {
          executeWithdrawalEpoch(node).catch(() => {});
        }
      }
      const clusterId = node._activeClusterId || '0x0';
      const id = `${clusterId.toLowerCase()}:${epochIndex}`;
      node._withdrawalEpoch = { id, epochIndex, startSec: epochStartSec, endSec: epochEndSec, durationSec: effectiveEpochSeconds, withdrawals: [] };
      if (node._withdrawalEpochTimer) {
        clearTimeout(node._withdrawalEpochTimer);
        node._withdrawalEpochTimer = null;
      }
      const headNumber = latest && typeof latest.number === 'number' ? latest.number : null;
      const minConf = Number.isFinite(WITHDRAWAL_MIN_CONFIRMATIONS) && WITHDRAWAL_MIN_CONFIRMATIONS > 0
        ? Math.floor(WITHDRAWAL_MIN_CONFIRMATIONS)
        : 0;
      const safeHead =
        typeof headNumber === 'number' && Number.isFinite(headNumber)
          ? Math.max(0, headNumber - minConf)
          : null;
      const includeEpochAfterNext = isNearEpochBoundary(epochEndSec, blockTimestamp);
      syncReservationsFromContract(node, { blockTag: safeHead, epochIndexHint: epochIndex, includeEpochAfterNext }).catch((e) => {
        console.log('[Epoch] Failed to sync reservations from contract:', e.message || String(e));
      });
    } else if (needsUpdate) {
      node._withdrawalEpoch.startSec = epochStartSec;
      node._withdrawalEpoch.endSec = epochEndSec;
      node._withdrawalEpoch.durationSec = effectiveEpochSeconds;
      if (node._withdrawalEpochTimer) {
        clearTimeout(node._withdrawalEpochTimer);
        node._withdrawalEpochTimer = null;
      }
    }
    if (!node._withdrawalEpochTimer) {
      const delayMs = (secondsUntilEnd + 5) * 1000;
      node._withdrawalEpochTimer = setTimeout(() => { executeWithdrawalEpoch(node); }, delayMs);
    }
    return node._withdrawalEpoch;
  }
  async function queueWithdrawalForEpoch(node, withdrawal) {
    if (!node || !withdrawal || !withdrawal.txHash) return;
    const epoch = await ensureWithdrawalEpoch(node);
    if (!epoch) return;
    const epochIndex = typeof epoch.epochIndex === 'number' ? epoch.epochIndex : null;
    if (!withdrawal.priorityClass) {
      withdrawal.priorityClass = normalizePriorityClass(node, epochIndex, withdrawal);
    }
    const txKey = String(withdrawal.txHash).toLowerCase();
    if (Array.isArray(epoch.withdrawals)) {
      for (const w of epoch.withdrawals) {
        if (!w || !w.txHash) continue;
        if (String(w.txHash).toLowerCase() === txKey) return;
      }
    }
    const { standardCount, total } = getEpochCounts(epoch);
    if (total >= MAX_WITHDRAWALS_PER_BATCH) {
      addToCarryover(node, withdrawal, CARRYOVER_STATUS.WAITING_CAPACITY, epochIndex);
      return;
    }
    if (WITHDRAWAL_PARTIAL_EPOCH_MODE && epochIndex !== null) {
      const maxStandard = getAvailableStandardSlots(node, epochIndex);
      const isStandard = withdrawal.priorityClass !== PRIORITY_CLASS.BRIBE;
      if (isStandard && standardCount >= maxStandard) {
        addToCarryover(node, withdrawal, CARRYOVER_STATUS.WAITING_CAPACITY, epochIndex);
        return;
      }
    }
    epoch.withdrawals.push(withdrawal);
  }
  function getEpochQueueStatus(node) {
    initBribeTracking(node);
    const epoch = node._withdrawalEpoch;
    const epochIndex = epoch ? epoch.epochIndex : null;
    const { retryCount, bribeCount, standardCount, total } = getEpochCounts(epoch);
    const reservedCount = epochIndex !== null ? getReservedCount(node, epochIndex) : 0;
    const maxStandard = getAvailableStandardSlots(node, epochIndex);
    const availableStandard = Math.max(0, maxStandard - standardCount);
    const availableTotal = Math.max(0, MAX_WITHDRAWALS_PER_BATCH - total);
    const isFull = total >= MAX_WITHDRAWALS_PER_BATCH;
    let epochSecondsRemaining = 0;
    if (epoch && typeof epoch.startSec === 'number') {
      const epochEndSec =
        typeof epoch.endSec === 'number'
          ? epoch.endSec
          : epoch.startSec + (typeof epoch.durationSec === 'number' ? epoch.durationSec : WITHDRAWAL_EPOCH_SECONDS);
      const nowSec = Math.floor(Date.now() / 1000);
      epochSecondsRemaining = Math.max(0, epochEndSec - nowSec);
    }
    return {
      currentCount: total,
      retryCount,
      bribeCount,
      standardCount,
      maxPerEpoch: MAX_WITHDRAWALS_PER_BATCH,
      maxStandardSlots: maxStandard,
      reservedCount,
      availableStandardSlots: availableStandard,
      availableSlots: availableTotal,
      isFull,
      epochSecondsRemaining,
      epochIndex,
      epochId: epoch ? epoch.id : null,
    };
  }
  function initEpochRunner(node) {
    initBribeTracking(node);
    if (node._withdrawalEpochEnsureTimer) return;
    node._withdrawalEpochEnsureTimer = setInterval(() => {
      ensureWithdrawalEpoch(node).catch(() => {});
    }, EPOCH_ENSURE_INTERVAL_MS);
    ensureWithdrawalEpoch(node).catch(() => {});
  }

  function stopEpochRunner(node) {
    if (!node) return;
    if (node._withdrawalEpochEnsureTimer) {
      clearInterval(node._withdrawalEpochEnsureTimer);
      node._withdrawalEpochEnsureTimer = null;
    }
    if (node._withdrawalEpochTimer) {
      clearTimeout(node._withdrawalEpochTimer);
      node._withdrawalEpochTimer = null;
    }
    node._withdrawalEpoch = null;
  }

  return {
    getEpochQueueStatus,
    queueWithdrawalForEpoch,
    initEpochRunner,
    stopEpochRunner,
  };
}
