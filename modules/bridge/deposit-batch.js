import { buildTree, getProof } from './merkle.js';

const BATCH_WINDOW_MS = Number(process.env.DEPOSIT_BATCH_WINDOW_MS || 300000);
const BATCH_MIN_SIZE = Number(process.env.DEPOSIT_BATCH_MIN_SIZE || 1);
const BATCH_MAX_SIZE = Number(process.env.DEPOSIT_BATCH_MAX_SIZE || 100);
const BATCH_CUTOFF_BEFORE_WD_SEC = Number(process.env.DEPOSIT_BATCH_CUTOFF_BEFORE_WD || 300);
const DEPOSIT_MAX_AMOUNT_ATOMIC = (() => {
  const raw = process.env.DEPOSIT_MAX_AMOUNT_XMR;
  const parsed = raw != null && raw !== '' ? Number(raw) : NaN;
  const xmr = Number.isFinite(parsed) && parsed > 0 ? parsed : 5000;
  return BigInt(Math.floor(xmr)) * 1000000000000n;
})();

function normalizeXmrTxid(value) {
  if (!value || typeof value !== 'string') return null;
  const v = value.trim().toLowerCase();
  return v || null;
}
function parseAmountAtomic(value) {
  if (typeof value === 'bigint') return value;
  if (typeof value === 'number') {
    if (!Number.isFinite(value) || !Number.isSafeInteger(value)) return null;
    return BigInt(value);
  }
  if (typeof value === 'string') {
    const s = value.trim();
    if (!s || !/^\d+$/.test(s)) return null;
    try { return BigInt(s); } catch { return null; }
  }
  return null;
}

class DepositBatchManager {
  constructor() {
    this._pendingDeposits = new Map();
    this._batchWindowStart = null;
    this._activeBatch = null;
    this._completedBatches = new Map();
    this._depositToBatch = new Map();
    this._batchPrepareLock = false;
    this._batchPrepareKey = null;
  }
  addDeposit(xmrTxid, depositId, recipient, amount) {
    const txid = normalizeXmrTxid(xmrTxid);
    if (!txid) return { ok: false, reason: 'invalid_txid' };
    if (!depositId || typeof depositId !== 'string') return { ok: false, reason: 'invalid_deposit_id' };
    if (!recipient || typeof recipient !== 'string') return { ok: false, reason: 'invalid_recipient' };
    const amountAtomic = parseAmountAtomic(amount);
    if (amountAtomic === null || amountAtomic <= 0n) return { ok: false, reason: 'invalid_amount' };
    if (DEPOSIT_MAX_AMOUNT_ATOMIC > 0n && amountAtomic > DEPOSIT_MAX_AMOUNT_ATOMIC) {
      return { ok: false, reason: 'deposit_amount_exceeds_limit', limit: DEPOSIT_MAX_AMOUNT_ATOMIC.toString() };
    }
    if (this._pendingDeposits.has(txid)) return { ok: false, reason: 'already_pending' };
    if (this._depositToBatch.has(depositId)) return { ok: false, reason: 'already_batched' };
    if (this._batchPrepareLock) return { ok: false, reason: 'batch_locked' };
    if (!this._batchWindowStart) this._batchWindowStart = Date.now();
    this._pendingDeposits.set(txid, {
      xmrTxid: txid,
      depositId,
      recipient,
      amount: amountAtomic.toString(),
      detectedAt: Date.now(),
    });
    return { ok: true };
  }

  removeDeposit(xmrTxid) {
    const txid = normalizeXmrTxid(xmrTxid);
    if (!txid) return;
    this._pendingDeposits.delete(txid);
    if (this._pendingDeposits.size === 0) {
      this._batchWindowStart = null;
      this._activeBatch = null;
    }
  }

  lockActiveBatch(merkleRoot = null) {
    if (!this._activeBatch) return false;
    if (merkleRoot && this._activeBatch.merkleRoot !== merkleRoot) return false;
    this._activeBatch.locked = true;
    return true;
  }

  shouldProcessBatch(withdrawalEpochEndMs = null) {
    if (this._activeBatch && this._activeBatch.locked && Array.isArray(this._activeBatch.deposits) && this._activeBatch.deposits.length > 0) {
      return true;
    }
    const size = this._pendingDeposits.size;
    if (size === 0) return false;
    if (size >= BATCH_MAX_SIZE) return true;
    if (this._batchWindowStart) {
      const elapsed = Date.now() - this._batchWindowStart;
      if (elapsed >= BATCH_WINDOW_MS && size >= BATCH_MIN_SIZE) return true;
    }
    if (withdrawalEpochEndMs) {
      const timeUntilWd = withdrawalEpochEndMs - Date.now();
      if (timeUntilWd <= BATCH_CUTOFF_BEFORE_WD_SEC * 1000 && size >= BATCH_MIN_SIZE) return true;
    }
    return false;
  }

  getPendingDeposits() {
    return Array.from(this._pendingDeposits.values());
  }

  getPendingCount() {
    return this._pendingDeposits.size;
  }

  hasPendingDeposit(xmrTxid) {
    const txid = normalizeXmrTxid(xmrTxid);
    if (!txid) return false;
    return this._pendingDeposits.has(txid);
  }

  acquirePrepareLock(key) {
    if (this._batchPrepareLock && this._batchPrepareKey !== key) return false;
    this._batchPrepareLock = true;
    this._batchPrepareKey = key;
    return true;
  }

  releasePrepareLock(key) {
    if (this._batchPrepareKey !== key) return false;
    this._batchPrepareLock = false;
    this._batchPrepareKey = null;
    return true;
  }

  isBatchLocked() {
    return this._batchPrepareLock;
  }

  prepareBatch(clusterId) {
    if (this._activeBatch && this._activeBatch.clusterId === clusterId && this._activeBatch.locked) {
      return this._activeBatch;
    }
    if (!this._batchPrepareLock) {
      return null;
    }
    const pending = this.getPendingDeposits();
    if (pending.length === 0) return null;
    const deposits = pending.map((d) => ({
      xmrTxid: d.xmrTxid,
      depositId: d.depositId,
      recipient: d.recipient,
      amount: d.amount,
    }));
    deposits.sort((a, b) => String(a.depositId).localeCompare(String(b.depositId)));
    const tree = buildTree(deposits, clusterId);
    if (!tree) return null;
    const batch = {
      clusterId,
      merkleRoot: tree.root,
      tree,
      deposits,
      createdAt: Date.now(),
      locked: false,
    };
    this._activeBatch = batch;
    return batch;
  }

  finalizeBatch(batch, signatures) {
    const active =
      this._activeBatch && batch && this._activeBatch.merkleRoot === batch.merkleRoot
        ? this._activeBatch
        : batch;
    if (!active || !active.merkleRoot) return;
    const batchData = {
      clusterId: active.clusterId,
      merkleRoot: active.merkleRoot,
      deposits: active.deposits,
      signatures,
      tree: active.tree,
      createdAt: active.createdAt,
      finalizedAt: Date.now(),
    };
    this._completedBatches.set(active.merkleRoot, batchData);
    for (const d of active.deposits) {
      this._depositToBatch.set(d.depositId, active.merkleRoot);
      this._pendingDeposits.delete(d.xmrTxid);
    }
    this._activeBatch = null;
    this._batchWindowStart = this._pendingDeposits.size > 0 ? Date.now() : null;
    this._batchPrepareLock = false;
    this._batchPrepareKey = null;
    return batchData;
  }

  getBatch(merkleRoot) {
    return this._completedBatches.get(merkleRoot) || null;
  }

  getClaimData(depositId) {
    const merkleRoot = this._depositToBatch.get(depositId);
    if (!merkleRoot) return null;
    const batch = this._completedBatches.get(merkleRoot);
    if (!batch) return null;
    const deposit = batch.deposits.find((d) => d.depositId === depositId);
    if (!deposit) return null;
    const proof = getProof(batch.tree, depositId);
    if (!proof) return null;
    return {
      merkleRoot,
      proof,
      clusterId: batch.clusterId,
      depositId: deposit.depositId,
      recipient: deposit.recipient,
      amount: deposit.amount,
      signatures: batch.signatures || [],
    };
  }

  getAllBatches() {
    return Array.from(this._completedBatches.values());
  }

  getQueueStatus() {
    const pendingCount = this._pendingDeposits.size;
    const maxSlots = BATCH_MAX_SIZE;
    const availableSlots = Math.max(0, maxSlots - pendingCount);
    const isFull = pendingCount >= maxSlots;
    let windowSecondsRemaining = BATCH_WINDOW_MS / 1000;
    if (this._batchWindowStart && pendingCount > 0) {
      const elapsed = Date.now() - this._batchWindowStart;
      const remaining = Math.max(0, BATCH_WINDOW_MS - elapsed);
      windowSecondsRemaining = Math.ceil(remaining / 1000);
    }
    const activeMerkleRoot = this._activeBatch && this._activeBatch.merkleRoot ? this._activeBatch.merkleRoot : null;
    const activeLocked = this._activeBatch ? !!this._activeBatch.locked : false;
    const prepareLocked = this._batchPrepareLock;
    return { pendingCount, maxSlots, availableSlots, isFull, windowSecondsRemaining, activeMerkleRoot, activeLocked, prepareLocked };
  }

  cleanupOldBatches(maxAgeMs = 86400000) {
    const now = Date.now();
    for (const [root, batch] of this._completedBatches) {
      if (now - batch.finalizedAt > maxAgeMs) {
        for (const d of batch.deposits) {
          this._depositToBatch.delete(d.depositId);
        }
        this._completedBatches.delete(root);
      }
    }
  }

  exportState() {
    return {
      pendingDeposits: Array.from(this._pendingDeposits.entries()),
      batchWindowStart: this._batchWindowStart,
      activeBatch: this._activeBatch ? { ...this._activeBatch, tree: null } : null,
      completedBatches: Array.from(this._completedBatches.entries()).map(([k, v]) => [
        k,
        { ...v, tree: null },
      ]),
      depositToBatch: Array.from(this._depositToBatch.entries()),
    };
  }

  importState(state) {
    if (!state) return;
    if (Array.isArray(state.pendingDeposits)) {
      const next = new Map();
      for (const item of state.pendingDeposits) {
        if (!Array.isArray(item) || item.length < 2) continue;
        const key = normalizeXmrTxid(item[0]);
        const v = item[1] && typeof item[1] === 'object' ? { ...item[1] } : null;
        if (!key || !v) continue;
        v.xmrTxid = key;
        next.set(key, v);
      }
      this._pendingDeposits = next;
    }
    if (typeof state.batchWindowStart === 'number') {
      this._batchWindowStart = state.batchWindowStart;
    }
    if (state.activeBatch && typeof state.activeBatch === 'object') {
      const v = state.activeBatch;
      if (v && Array.isArray(v.deposits) && v.clusterId) {
        const tree = buildTree(v.deposits, v.clusterId);
        if (tree) {
          this._activeBatch = { ...v, tree, merkleRoot: tree.root };
        }
      }
    }
    if (Array.isArray(state.completedBatches)) {
      for (const [k, v] of state.completedBatches) {
        if (v && v.deposits && v.clusterId) {
          const tree = buildTree(v.deposits, v.clusterId);
          if (tree) {
            const root = v.merkleRoot || k || tree.root;
            this._completedBatches.set(root, { ...v, merkleRoot: root, tree });
          }
        }
      }
    }
    if (Array.isArray(state.depositToBatch)) {
      this._depositToBatch = new Map(state.depositToBatch);
    }
  }
}

export { DepositBatchManager, BATCH_WINDOW_MS, BATCH_MIN_SIZE, BATCH_MAX_SIZE, DEPOSIT_MAX_AMOUNT_ATOMIC };
