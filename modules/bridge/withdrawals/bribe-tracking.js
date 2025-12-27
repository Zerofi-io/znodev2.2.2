import { MAX_BRIBE_SLOTS, MAX_WITHDRAWALS_PER_BATCH } from './config.js';
const SYNC_THROTTLE_MS = Number(process.env.BRIBE_SYNC_THROTTLE_MS || 5000);
const EPOCH_BOUNDARY_GRACE_MS = Number(process.env.BRIBE_EPOCH_BOUNDARY_GRACE_MS || 30000);
const PROCESSED_SLOT_RESERVATIONS_MAX = 5000;
export function initBribeTracking(node) {
  if (!node._epochReservations) {
    node._epochReservations = new Map();
  }
  if (!node._epochReservedUsers) {
    node._epochReservedUsers = new Map();
  }
  if (!node._processedSlotReservations) {
    node._processedSlotReservations = new Set();
  }
}
function normalizeEpoch(epoch) {
  const n = typeof epoch === 'bigint' ? Number(epoch) : Number(epoch);
  if (!Number.isSafeInteger(n) || n < 0) return null;
  return n;
}
function normalizeSlotIndex(slotIndex) {
  if (slotIndex == null) return null;
  const n = typeof slotIndex === 'bigint' ? Number(slotIndex) : Number(slotIndex);
  if (!Number.isSafeInteger(n) || n < 0) return null;
  return n;
}
export function isSlotReservationProcessed(node, txHash) {
  initBribeTracking(node);
  const key = String(txHash || '').toLowerCase();
  if (!key) return false;
  return node._processedSlotReservations.has(key);
}
export function markSlotReservationProcessed(node, txHash) {
  initBribeTracking(node);
  const key = String(txHash || '').toLowerCase();
  if (!key) return;
  node._processedSlotReservations.add(key);
  if (node._processedSlotReservations.size <= PROCESSED_SLOT_RESERVATIONS_MAX) return;
  const excess = node._processedSlotReservations.size - PROCESSED_SLOT_RESERVATIONS_MAX;
  let i = 0;
  for (const k of node._processedSlotReservations) {
    node._processedSlotReservations.delete(k);
    i += 1;
    if (i >= excess) break;
  }
}
export function trackSlotReservation(node, targetEpoch, user, txHash, slotIndex = null) {
  initBribeTracking(node);
  const epochNum = normalizeEpoch(targetEpoch);
  if (epochNum == null) return false;
  const userLc = String(user || '').toLowerCase();
  if (!userLc) return false;
  if (txHash && isSlotReservationProcessed(node, txHash)) {
    return false;
  }
  if (!node._epochReservedUsers.has(epochNum)) {
    node._epochReservedUsers.set(epochNum, new Set());
  }
  const users = node._epochReservedUsers.get(epochNum);
  let changed = false;
  if (!users.has(userLc)) {
    users.add(userLc);
    changed = true;
  }
  const prevCountRaw = node._epochReservations.get(epochNum) || 0;
  const prevCount = Number.isFinite(prevCountRaw) ? prevCountRaw : 0;
  const slotNum = normalizeSlotIndex(slotIndex);
  let nextCount = prevCount;
  if (slotNum != null) {
    nextCount = Math.max(prevCount, slotNum + 1);
  } else if (prevCount < MAX_BRIBE_SLOTS) {
    nextCount = prevCount + 1;
  }
  nextCount = Math.min(MAX_BRIBE_SLOTS, nextCount);
  if (nextCount !== prevCount) {
    node._epochReservations.set(epochNum, nextCount);
    changed = true;
  }
  if (txHash) {
    markSlotReservationProcessed(node, txHash);
  }
  return changed;
}
export function getReservedCount(node, epoch) {
  initBribeTracking(node);
  const epochNum = normalizeEpoch(epoch);
  if (epochNum == null) return 0;
  const raw = node._epochReservations.get(epochNum) || 0;
  const n = Number.isFinite(raw) ? raw : 0;
  return Math.max(0, Math.min(MAX_BRIBE_SLOTS, n));
}
export function getAvailableStandardSlots(node, epoch) {
  const reservedCount = getReservedCount(node, epoch);
  const maxStandard = MAX_WITHDRAWALS_PER_BATCH - reservedCount;
  return Math.max(0, maxStandard);
}
export function isNearEpochBoundary(epochEndSec, nowSec = null) {
  const end = Number(epochEndSec);
  const now = nowSec == null ? Math.floor(Date.now() / 1000) : Number(nowSec);
  if (!Number.isFinite(end) || end <= 0) return false;
  if (!Number.isFinite(now) || now <= 0) return false;
  const graceSeconds = EPOCH_BOUNDARY_GRACE_MS > 0 ? EPOCH_BOUNDARY_GRACE_MS / 1000 : 0;
  if (graceSeconds <= 0) return false;
  const secUntilEnd = end - now;
  return secUntilEnd >= 0 && secUntilEnd <= graceSeconds;
}
export function pruneOldEpochData(node, currentEpoch) {
  initBribeTracking(node);
  const epochNum = normalizeEpoch(currentEpoch);
  if (epochNum == null) return;
  const keepFrom = epochNum - 1;
  for (const epoch of Array.from(node._epochReservations.keys())) {
    if (epoch < keepFrom) node._epochReservations.delete(epoch);
  }
  for (const epoch of Array.from(node._epochReservedUsers.keys())) {
    if (epoch < keepFrom) node._epochReservedUsers.delete(epoch);
  }
}
export async function syncReservationsFromContract(
  node,
  { blockTag, forceSync = false, epochIndexHint = null, includeEpochAfterNext = false } = {},
) {
  if (!node.bridge || !node.bridge.epochReservedCount) return;
  const now = Date.now();
  const throttle = forceSync ? 0 : SYNC_THROTTLE_MS;
  if (node._lastReservationSyncAt && (now - node._lastReservationSyncAt) < throttle) {
    return;
  }
  initBribeTracking(node);
  const overrides =
    typeof blockTag === 'number' && Number.isFinite(blockTag) && blockTag >= 0
      ? { blockTag: Math.floor(blockTag) }
      : {};
  try {
    const epochs = [];
    const hintNum = normalizeEpoch(epochIndexHint);
    if (hintNum != null) {
      const base = BigInt(hintNum);
      epochs.push(base, base + 1n);
      if (includeEpochAfterNext) epochs.push(base + 2n);
    } else {
      if (!node.bridge.getCurrentEpoch) return;
      let currentEpoch = await node.bridge.getCurrentEpoch(overrides);
      if (typeof currentEpoch !== 'bigint') {
        try {
          currentEpoch = BigInt(currentEpoch);
        } catch {
          return;
        }
      }
      epochs.push(currentEpoch, currentEpoch + 1n);
    }
    for (const epochBi of epochs) {
      const count = await node.bridge.epochReservedCount(epochBi, overrides);
      const key = Number(epochBi);
      if (!Number.isSafeInteger(key) || key < 0) continue;
      const n = typeof count === 'bigint' ? Number(count) : Number(count);
      const clamped = Number.isFinite(n) && n >= 0 ? Math.min(MAX_BRIBE_SLOTS, Math.floor(n)) : 0;
      node._epochReservations.set(key, clamped);
    }
    node._lastReservationSyncAt = now;
  } catch (e) {
    console.log('[Bribe] Failed to sync reservations:', e.message || String(e));
  }
}
export async function onSlotReservedEvent(node, targetEpoch, user, slotIndex, txHash) {
  const changed = trackSlotReservation(node, targetEpoch, user, txHash, slotIndex);
  if (changed) {
    await syncReservationsFromContract(node, { forceSync: true, epochIndexHint: targetEpoch });
  }
  return changed;
}
export function isUserBribePriority(node, epochIndex, user) {
  initBribeTracking(node);
  const epochNum = normalizeEpoch(epochIndex);
  if (epochNum == null) return false;
  const userLc = String(user || '').toLowerCase();
  if (!userLc) return false;
  const users = node._epochReservedUsers.get(epochNum);
  if (!users) return false;
  return users.has(userLc);
}
