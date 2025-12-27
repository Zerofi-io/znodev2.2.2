import fs from 'fs';
import path from 'path';
import crypto from 'crypto';
import { recordAuthorizedTxids } from '../utils/authorized-txids.js';
export function atomicWriteFileSync(filePath, data, mode) {
  const dir = path.dirname(filePath);
  const base = path.basename(filePath);
  const tmp = path.join(dir, `.${base}.${process.pid}.${Date.now()}.tmp`);
  let fd;
  try {
    fd = fs.openSync(tmp, 'wx', mode);
    fs.writeFileSync(fd, data);
    fs.fsyncSync(fd);
  } finally {
    if (fd !== undefined) {
      try { fs.closeSync(fd); } catch {}
    }
  }
  try {
    fs.renameSync(tmp, filePath);
  } catch (e) {
    try { fs.unlinkSync(tmp); } catch {}
    throw e;
  }
  try {
    const dfd = fs.openSync(dir, 'r');
    try { fs.fsyncSync(dfd); } finally { fs.closeSync(dfd); }
  } catch {}
}
function calculateChecksum(data) {
  return crypto.createHash('sha256').update(data).digest('hex');
}
function saveWithChecksum(filePath, data, mode) {
  const parsedData = JSON.parse(data);
  const canonical = JSON.stringify(parsedData);
  const checksum = calculateChecksum(canonical);
  const dataWithChecksum = JSON.stringify(
    {
      checksum,
      data: parsedData,
    },
    null,
    2,
  );
  atomicWriteFileSync(filePath, dataWithChecksum, mode);
}
let _checksumFailureCount = 0;
let _lastChecksumAlertMs = 0;
const CHECKSUM_ALERT_INTERVAL_MS = Number(process.env.CHECKSUM_ALERT_INTERVAL_MS || 3600000);
function alertChecksumFailure(filePath, error) {
  _checksumFailureCount++;
  const now = Date.now();
  if (now - _lastChecksumAlertMs >= CHECKSUM_ALERT_INTERVAL_MS) {
    _lastChecksumAlertMs = now;
    console.error(`[ALERT] Bridge state checksum failure: ${error}`);
    console.error(`[ALERT] File: ${filePath}`);
    console.error(`[ALERT] Total checksum failures: ${_checksumFailureCount}`);
  }
}
export function getChecksumFailureCount() {
  return _checksumFailureCount;
}
function loadWithChecksumVerification(filePath) {
  const content = fs.readFileSync(filePath, 'utf8');
  const parsed = JSON.parse(content);
  if (!parsed || typeof parsed !== 'object' || !('checksum' in parsed) || !('data' in parsed)) {
    return parsed;
  }
  const checksum = parsed.checksum;
  const data = parsed.data;
  if (typeof checksum !== 'string' || !checksum) {
    return data;
  }
  const canonical = JSON.stringify(data);
  const canonicalChecksum = calculateChecksum(canonical);
  if (canonicalChecksum !== checksum) {
    const legacyPrettyChecksum = calculateChecksum(JSON.stringify(data, null, 2));
    if (legacyPrettyChecksum !== checksum) {
      alertChecksumFailure(filePath, 'checksum mismatch');
      throw new Error('Checksum verification failed');
    }
  }
  return data;
}
let _lastBackupRotationMs = 0;
const BACKUP_ROTATION_MIN_INTERVAL_MS = Number(process.env.BACKUP_ROTATION_MIN_INTERVAL_MS || 300000);
function rotateBackup(filePath, maxBackups = 3) {
  if (!fs.existsSync(filePath)) return;
  const now = Date.now();
  if (now - _lastBackupRotationMs < BACKUP_ROTATION_MIN_INTERVAL_MS) return;
  _lastBackupRotationMs = now;
  const dir = path.dirname(filePath);
  const base = path.basename(filePath);
  const backupPattern = new RegExp(`^${base}\\.backup\\.(\\d+)$`);
  let existingBackups = [];
  try {
    existingBackups = fs.readdirSync(dir)
      .filter((f) => backupPattern.test(f))
      .map((f) => {
        const match = f.match(backupPattern);
        return { file: f, index: parseInt(match[1], 10) };
      })
      .sort((a, b) => b.index - a.index);
  } catch (e) {
    console.log('[Bridge] Failed to read backup directory:', e.message || String(e));
    return;
  }
  for (const backup of existingBackups) {
    const backupFile = path.join(dir, backup.file);
    if (backup.index >= maxBackups) {
      try {
        fs.unlinkSync(backupFile);
      } catch (e) {
        console.log('[Bridge] Failed to remove old backup:', e.message || String(e));
      }
    } else {
      const newIndex = backup.index + 1;
      try {
        fs.renameSync(backupFile, path.join(dir, `${base}.backup.${newIndex}`));
      } catch (e) {
        console.log('[Bridge] Failed to rotate backup:', e.message || String(e));
      }
    }
  }
  const backupPath = path.join(dir, `${base}.backup.1`);
  try {
    const content = fs.readFileSync(filePath, 'utf8');
    atomicWriteFileSync(backupPath, content, 0o600);
  } catch (e) {
    console.log('[Bridge] Failed to create backup:', e.message || String(e));
  }
}
function getBridgeStatePath() {
  const homeDir = process.env.HOME || process.cwd();
  const dataDir = process.env.BRIDGE_DATA_DIR || path.join(homeDir, '.znode-bridge');
  try { fs.mkdirSync(dataDir, { recursive: true, mode: 0o700 }); } catch {}
  return path.join(dataDir, 'bridge_state.json');
}
function tryLoadFromBackups(statePath, maxBackups) {
  const dir = path.dirname(statePath);
  const base = path.basename(statePath);
  for (let i = 1; i <= maxBackups; i++) {
    const backupPath = path.join(dir, `${base}.backup.${i}`);
    if (!fs.existsSync(backupPath)) continue;
    try {
      const data = loadWithChecksumVerification(backupPath);
      console.log(`[Bridge] Loaded state from backup ${i}`);
      return data;
    } catch {
      if (process.env.BRIDGE_STATE_ALLOW_RAW_BACKUP_FALLBACK === '1') {
        try {
          const parsed = JSON.parse(fs.readFileSync(backupPath, 'utf8'));
          const data = parsed && typeof parsed === 'object' && 'data' in parsed ? parsed.data : parsed;
          console.log(`[Bridge] Loaded state from backup ${i} (raw fallback)`);
          return data;
        } catch {}
      }
    }
  }
  return null;
}
export function loadBridgeState(node) {
  if (!node || node._bridgeStateLoaded) return;
  node._bridgeStateLoaded = true;
  try {
    const statePath = getBridgeStatePath();
    if (!fs.existsSync(statePath)) return;
    let data;
    try {
      data = loadWithChecksumVerification(statePath);
    } catch (e) {
      console.warn(`[Bridge] Checksum verification failed: ${e.message}`);
      console.warn('[Bridge] Attempting to load from backup files...');
      const maxBackups = parseInt(process.env.BRIDGE_STATE_MAX_BACKUPS, 10) || 3;
      data = tryLoadFromBackups(statePath, maxBackups);
      if (!data) {
        throw new Error('Bridge state corrupted: main file checksum failed and no valid backups found');
      }
    }
    const now = Date.now();
    if (Array.isArray(data.processedDeposits)) {
      const ttlMs = Number(process.env.PROCESSED_DEPOSIT_TTL_MS || 86400000);
      const meta = data.processedDepositsMeta && typeof data.processedDepositsMeta === 'object'
        ? data.processedDepositsMeta
        : null;
      const defaultTs = typeof data.savedAt === 'number' ? data.savedAt : now;
      const set = new Set();
      const metaMap = new Map();
      for (const h of data.processedDeposits) {
        if (typeof h !== 'string') continue;
        const rawKey = h;
        const key = rawKey.toLowerCase();
        const m = meta ? (meta[key] || meta[rawKey]) : null;
        let ts = null;
        if (m && typeof m.timestamp === 'number') ts = m.timestamp;
        if (!ts) ts = defaultTs;
        if (ttlMs > 0 && now - ts > ttlMs) continue;
        set.add(key);
        const entry = { timestamp: ts }; if (m && typeof m.subaddrIndex === 'number') entry.subaddrIndex = m.subaddrIndex; if (m && typeof m.batchRoot === 'string') entry.batchRoot = m.batchRoot; metaMap.set(key, entry);
      }
      node._processedDeposits = set;
      node._processedDepositsMeta = metaMap;
      console.log(`[Bridge] Loaded ${node._processedDeposits.size} processed deposits from disk`);
    }
    if (data.pendingMintSignatures && typeof data.pendingMintSignatures === 'object') {
      const map = new Map();
      for (const [k, v] of Object.entries(data.pendingMintSignatures)) {
        if (typeof k !== 'string') continue;
        map.set(k.toLowerCase(), v);
      }
      node._pendingMintSignatures = map;
      console.log(`[Bridge] Loaded ${node._pendingMintSignatures.size} pending mint signatures from disk`);
    }
    if (data.pendingBatchSignatures && typeof data.pendingBatchSignatures === 'object') {
      const map = new Map();
      for (const [k, v] of Object.entries(data.pendingBatchSignatures)) {
        if (typeof k !== 'string') continue;
        map.set(k.toLowerCase(), v);
      }
      node._pendingBatchSignatures = map;
      console.log(`[Bridge] Loaded ${node._pendingBatchSignatures.size} pending batch signatures from disk`);
    }
    if (data.depositBatchState && typeof data.depositBatchState === 'object') {
      node._depositBatchState = data.depositBatchState;
    }
    if (data.pendingDepositRequests && typeof data.pendingDepositRequests === 'object') {
      const map = new Map();
      for (const [k, v] of Object.entries(data.pendingDepositRequests)) {
        if (typeof k !== 'string') continue;
        const key = /^[0-9a-fA-F]{16}$/.test(k) ? k.toLowerCase() : k;
        let entry = null;
        if (v && typeof v === 'object' && typeof v.ethAddress === 'string') {
          entry = {
            ethAddress: v.ethAddress,
            clusterId: v.clusterId || null,
            requestKey: v.requestKey || null,
          };
          if (typeof v.timestamp === 'number') entry.timestamp = v.timestamp;
        } else if (typeof v === 'string') {
          entry = { ethAddress: v, clusterId: null, requestKey: null };
        }
        if (!entry) continue;
        const prev = map.get(key);
        if (!prev) {
          map.set(key, entry);
          continue;
        }
        const prevTs = prev && typeof prev.timestamp === 'number' ? prev.timestamp : 0;
        const nextTs = entry && typeof entry.timestamp === 'number' ? entry.timestamp : 0;
        if (nextTs > prevTs) {
          map.set(key, entry);
        }
      }
      node._pendingDepositRequests = map;
      console.log(`[Bridge] Loaded ${node._pendingDepositRequests.size} pending deposit requests from disk`);
    }
    if (Array.isArray(data.processedWithdrawals)) {
      const ttlMs = Number(process.env.PROCESSED_WITHDRAWAL_TTL_MS || 0);
      const meta = data.processedWithdrawalsMeta && typeof data.processedWithdrawalsMeta === 'object'
        ? data.processedWithdrawalsMeta
        : null;
      const defaultTs = typeof data.savedAt === 'number' ? data.savedAt : now;
      const set = new Set();
      const metaMap = new Map();
      for (const h of data.processedWithdrawals) {
        if (typeof h !== 'string') continue;
        const key = h.toLowerCase();
        const m = meta && meta[key] && typeof meta[key] === 'object' ? meta[key] : null;
        let ts = null;
        if (m && typeof m.timestamp === 'number') ts = m.timestamp;
        if (!ts) ts = defaultTs;
        if (ttlMs > 0 && now - ts > ttlMs) continue;
        set.add(key);
        const info = { timestamp: ts };
        if (m && typeof m.status === 'string') info.status = m.status;
        if (m && Array.isArray(m.xmrTxHashes)) info.xmrTxHashes = m.xmrTxHashes;
        if (m && typeof m.xmrTxKey === 'string') info.xmrTxKey = m.xmrTxKey;
        if (m && Array.isArray(m.moneroSigners)) info.moneroSigners = m.moneroSigners;
        metaMap.set(key, info);
      }
      node._processedWithdrawals = set;
      node._processedWithdrawalsMeta = metaMap;
      console.log(`[Bridge] Loaded ${node._processedWithdrawals.size} processed withdrawals from disk`);
    }
    if (Array.isArray(data.authorizedTxids) && data.authorizedTxids.length > 0) {
      recordAuthorizedTxids(node, data.authorizedTxids);
    }
    if (node._processedWithdrawalsMeta && node._processedWithdrawalsMeta.size > 0) {
      const txids = [];
      for (const [, info] of node._processedWithdrawalsMeta) {
        if (!info || !Array.isArray(info.xmrTxHashes)) continue;
        for (const t of info.xmrTxHashes) {
          txids.push(t);
        }
      }
      recordAuthorizedTxids(node, txids);
    }
    if (data.pendingWithdrawals && typeof data.pendingWithdrawals === 'object') {
      const map = new Map();
      for (const [k, v] of Object.entries(data.pendingWithdrawals)) {
        const parsed = deserializeBigInts(v);
        map.set(k.toLowerCase(), parsed);
      }
      node._pendingWithdrawals = map;
      console.log(`[Bridge] Loaded ${node._pendingWithdrawals.size} pending withdrawals from disk`);
    }
    const scanRaw = data.withdrawalLastScanBlock;
    const scanNum = typeof scanRaw === 'number' ? scanRaw : typeof scanRaw === 'string' ? Number(scanRaw) : NaN;
    if (Number.isFinite(scanNum) && scanNum >= 0) {
      node._withdrawalLastScanBlock = Math.floor(scanNum);
    }
    const depRaw = data.depositLastScanHeight;
    const depNum = typeof depRaw === 'number' ? depRaw : typeof depRaw === 'string' ? Number(depRaw) : NaN;
    if (Number.isFinite(depNum) && depNum >= 0) {
      node._depositLastScanHeight = Math.floor(depNum);
    }
    if (Array.isArray(data.sweptDeposits)) {
      const ttlMs = Number(process.env.SWEPT_DEPOSIT_TTL_MS || 604800000);
      const meta = data.sweptDepositsMeta && typeof data.sweptDepositsMeta === 'object' ? data.sweptDepositsMeta : null;
      const defaultTs = typeof data.savedAt === 'number' ? data.savedAt : now;
      const set = new Set();
      const metaMap = new Map();
      for (const key of data.sweptDeposits) {
        if (typeof key !== 'string') continue;
        let ts = null;
        if (meta && meta[key] && typeof meta[key].timestamp === 'number') ts = meta[key].timestamp;
        if (!ts) ts = defaultTs;
        if (ttlMs > 0 && now - ts > ttlMs) continue;
        set.add(key);
        metaMap.set(key, { timestamp: ts, txHash: meta && meta[key] ? meta[key].txHash : null });
      }
      node._sweptDeposits = set;
      node._sweptDepositsMeta = metaMap;
      console.log(`[Bridge] Loaded ${node._sweptDeposits.size} swept deposits from disk`);
    }
  } catch (e) {
    console.log('[Bridge] Failed to load bridge state:', e.message || String(e));
  }
}
function serializeBigInts(obj) {
  if (obj === null || obj === undefined) return obj;
  if (typeof obj === 'bigint') return { __bigint__: obj.toString() };
  if (Array.isArray(obj)) return obj.map(serializeBigInts);
  if (typeof obj === 'object') {
    const out = {};
    for (const [k, v] of Object.entries(obj)) {
      out[k] = serializeBigInts(v);
    }
    return out;
  }
  return obj;
}
function deserializeBigInts(obj) {
  if (obj === null || obj === undefined) return obj;
  if (typeof obj === 'object' && obj.__bigint__ !== undefined) {
    try {
      return BigInt(obj.__bigint__);
    } catch {
      return obj.__bigint__;
    }
  }
  if (Array.isArray(obj)) return obj.map(deserializeBigInts);
  if (typeof obj === 'object') {
    const out = {};
    for (const [k, v] of Object.entries(obj)) {
      out[k] = deserializeBigInts(v);
    }
    return out;
  }
  return obj;
}
export function saveBridgeState(node) {
  try {
    const statePath = getBridgeStatePath();
    const processedDeposits = node._processedDeposits ? Array.from(node._processedDeposits) : [];
    const processedDepositsMeta = {};
    if (node._processedDepositsMeta && node._processedDepositsMeta.size > 0) {
      for (const [k, info] of node._processedDepositsMeta) {
        if (!info || typeof info.timestamp !== 'number') continue;
        const entry = { timestamp: info.timestamp }; if (typeof info.subaddrIndex === 'number') entry.subaddrIndex = info.subaddrIndex; if (typeof info.batchRoot === 'string') entry.batchRoot = info.batchRoot; processedDepositsMeta[k] = entry;
      }
    }
    const processedWithdrawals = node._processedWithdrawals ? Array.from(node._processedWithdrawals) : [];
    const processedWithdrawalsMeta = {};
    if (node._processedWithdrawalsMeta && node._processedWithdrawalsMeta.size > 0) {
      for (const [k, info] of node._processedWithdrawalsMeta) {
        if (!info || typeof info.timestamp !== 'number') continue;
        const entry = { timestamp: info.timestamp };
        if (typeof info.status === 'string') entry.status = info.status;
        if (Array.isArray(info.xmrTxHashes)) entry.xmrTxHashes = info.xmrTxHashes;
        if (typeof info.xmrTxKey === 'string') entry.xmrTxKey = info.xmrTxKey;
        if (Array.isArray(info.moneroSigners)) entry.moneroSigners = info.moneroSigners;
        processedWithdrawalsMeta[k] = entry;
      }
    }
    const sweptDeposits = node._sweptDeposits ? Array.from(node._sweptDeposits) : [];
    const sweptDepositsMeta = {};
    if (node._sweptDepositsMeta && node._sweptDepositsMeta.size > 0) {
      for (const [k, info] of node._sweptDepositsMeta) {
        if (!info || typeof info.timestamp !== "number") continue;
        const entry = { timestamp: info.timestamp };
        if (typeof info.txHash === "string") entry.txHash = info.txHash;
        sweptDepositsMeta[k] = entry;
      }
    }
    const pendingWithdrawalsObj = {};
    if (node._pendingWithdrawals && node._pendingWithdrawals.size > 0) {
      for (const [k, v] of node._pendingWithdrawals) {
        pendingWithdrawalsObj[k] = serializeBigInts(v);
      }
    }
    const data = {
      savedAt: Date.now(),
      processedDeposits,
      processedDepositsMeta,
      pendingMintSignatures: node._pendingMintSignatures ? Object.fromEntries(node._pendingMintSignatures) : {},
      pendingBatchSignatures: node._pendingBatchSignatures ? Object.fromEntries(node._pendingBatchSignatures) : {},
      depositBatchState: (node._depositBatchManager && typeof node._depositBatchManager.exportState === 'function') ? node._depositBatchManager.exportState() : (node._depositBatchState && typeof node._depositBatchState === 'object' ? node._depositBatchState : null),
      pendingDepositRequests: node._pendingDepositRequests ? Object.fromEntries(node._pendingDepositRequests) : {},
      processedWithdrawals,
      processedWithdrawalsMeta,
      authorizedTxids: node._authorizedTxids ? Array.from(node._authorizedTxids.keys()) : [],
      pendingWithdrawals: pendingWithdrawalsObj,
      withdrawalLastScanBlock: (
        typeof node._withdrawalLastScanBlock === 'number' && Number.isFinite(node._withdrawalLastScanBlock)
          ? node._withdrawalLastScanBlock
          : null
      ),
      depositLastScanHeight: (
        typeof node._depositLastScanHeight === 'number' && Number.isFinite(node._depositLastScanHeight)
          ? node._depositLastScanHeight
          : null
      ),
      sweptDeposits,
      sweptDepositsMeta,
    };
    const json = JSON.stringify(data, null, 2);
    const maxBackups = parseInt(process.env.BRIDGE_STATE_MAX_BACKUPS, 10) || 3;
    rotateBackup(statePath, maxBackups);
    saveWithChecksum(statePath, json, 0o600);
    return true;
  } catch (e) {
    console.log('[Bridge] Failed to save bridge state:', e.message || String(e));
    return false;
  }
}
export function startBridgeStateSaver(node) {
  const saveIntervalMs = Number(process.env.BRIDGE_STATE_SAVE_INTERVAL_MS || 60000);
  if (node._bridgeStateSaverTimer) clearInterval(node._bridgeStateSaverTimer);
  node._bridgeStateSaverTimer = setInterval(() => { saveBridgeState(node); }, saveIntervalMs);
}
function cleanupExpiredSignatures(node) {
  const ttlMs = Number(process.env.MINT_SIGNATURE_TTL_MS || 86400000);
  const now = Date.now();
  let expiredMint = 0;
  let expiredBatch = 0;
  if (node._pendingMintSignatures && node._pendingMintSignatures.size > 0) {
    for (const [txid, data] of node._pendingMintSignatures) {
      if (data && data.timestamp && now - data.timestamp > ttlMs) {
        node._pendingMintSignatures.delete(txid);
        expiredMint += 1;
      }
    }
  }
  if (node._pendingBatchSignatures && node._pendingBatchSignatures.size > 0) {
    for (const [root, data] of node._pendingBatchSignatures) {
      if (data && data.timestamp && now - data.timestamp > ttlMs) {
        node._pendingBatchSignatures.delete(root);
        expiredBatch += 1;
      }
    }
  }
  const expiredTotal = expiredMint + expiredBatch;
  if (expiredTotal > 0) {
    console.log(`[Bridge] Cleaned up ${expiredTotal} expired signature set(s)`);
    saveBridgeState(node);
  }
}
function isWithdrawalInFlight(data) {
  if (!data || typeof data !== 'object') return false;
  const status = typeof data.status === 'string' ? data.status : '';
  const inFlightStatuses = [
    'signing',
    'pending',
    'submitting',
    'waiting_signatures',
    'awaiting_confirmation',
  ];
  return inFlightStatuses.includes(status);
}
function cleanupOldProcessedDeposits(node) {
  if (!node._processedDeposits || node._processedDeposits.size === 0) return;
  const ttlMs = Number(process.env.PROCESSED_DEPOSIT_TTL_MS || 86400000);
  const now = Date.now();
  let ttlPruned = 0;
  if (ttlMs > 0) {
    node._processedDepositsMeta = node._processedDepositsMeta || new Map();
    for (const txid of Array.from(node._processedDeposits)) {
      const meta = node._processedDepositsMeta.get(txid) || {};
      const ts = typeof meta.timestamp === 'number' ? meta.timestamp : null;
      if (ts && now - ts > ttlMs) {
        node._processedDeposits.delete(txid);
        node._processedDepositsMeta.delete(txid);
        ttlPruned += 1;
      }
    }
    if (ttlPruned > 0) {
      console.log(`[Bridge] Pruned ${ttlPruned} old processed deposit entries by TTL`);
    }
  }
  const maxEntries = Number(process.env.MAX_PROCESSED_DEPOSITS || 10000);
  let countPruned = 0;
  if (node._processedDeposits.size > maxEntries) {
    const arr = Array.from(node._processedDeposits);
    const toRemove = arr.length - maxEntries;
    for (let i = 0; i < toRemove; i += 1) {
      const txid = arr[i];
      node._processedDeposits.delete(txid);
      if (node._processedDepositsMeta) node._processedDepositsMeta.delete(txid);
    }
    countPruned = toRemove;
    console.log(`[Bridge] Pruned ${toRemove} old processed deposit entries by max count`);
  }
  if (ttlPruned > 0 || countPruned > 0) {
    saveBridgeState(node);
  }
}
function cleanupOldPendingWithdrawals(node) {
  if (!node._pendingWithdrawals || node._pendingWithdrawals.size === 0) return;
  const ttlMs = Number(process.env.PENDING_WITHDRAWAL_TTL_MS || 86400000);
  const now = Date.now();
  let pruned = 0;
  for (const [key, data] of node._pendingWithdrawals) {
    if (!data) continue;
    if (isWithdrawalInFlight(data)) continue;
    const status = typeof data.status === 'string' ? data.status : '';
    if (
      status === 'carryover_waiting_unlocked' ||
      status === 'carryover_waiting_cluster' ||
      status === 'orphaned' ||
      status === 'waiting_cluster_health'
    ) {
      continue;
    }
    let started = null;
    if (typeof data.firstAttemptAt === 'number') started = data.firstAttemptAt;
    else if (typeof data.startedAt === 'number') started = data.startedAt;
    else if (typeof data.timestamp === 'number') started = data.timestamp;
    if (!started) continue;
    if (now - started > ttlMs) {
      node._pendingWithdrawals.delete(key);
      pruned += 1;
    }
  }
  if (pruned > 0) {
    console.log(`[Bridge] Pruned ${pruned} old pending withdrawal entries`);
    saveBridgeState(node);
  }
}
function cleanupOldPendingDepositRequests(node) {
  if (!node._pendingDepositRequests || node._pendingDepositRequests.size === 0) return;
  const ttlMs = Number(process.env.PENDING_DEPOSIT_REQUEST_TTL_MS || 86400000);
  const now = Date.now();
  let pruned = 0;
  for (const [key, data] of node._pendingDepositRequests) {
    if (!data || typeof data !== 'object') continue;
    const ts = typeof data.timestamp === 'number' ? data.timestamp : null;
    if (!ts) continue;
    if (now - ts > ttlMs) {
      node._pendingDepositRequests.delete(key);
      pruned += 1;
    }
  }
  if (pruned > 0) {
    console.log(`[Bridge] Pruned ${pruned} old pending deposit request entries`);
    saveBridgeState(node);
  }
}
function cleanupOldProcessedWithdrawals(node) {
  if (!node._processedWithdrawals || node._processedWithdrawals.size === 0) return;
  const ttlMs = Number(process.env.PROCESSED_WITHDRAWAL_TTL_MS || 0);
  const maxEntries = Number(process.env.MAX_PROCESSED_WITHDRAWALS || 200000);
  const now = Date.now();
  node._processedWithdrawalsMeta = node._processedWithdrawalsMeta || new Map();
  let pruned = 0;
  if (ttlMs > 0) {
    for (const hash of Array.from(node._processedWithdrawals)) {
      const meta = node._processedWithdrawalsMeta.get(hash) || {};
      const ts = typeof meta.timestamp === 'number' ? meta.timestamp : null;
      if (!ts) continue;
      if (now - ts > ttlMs) {
        node._processedWithdrawals.delete(hash);
        node._processedWithdrawalsMeta.delete(hash);
        pruned += 1;
      }
    }
  }
  if (maxEntries > 0 && node._processedWithdrawals.size > maxEntries) {
    const items = [];
    for (const hash of node._processedWithdrawals) {
      const meta = node._processedWithdrawalsMeta.get(hash) || {};
      const ts = typeof meta.timestamp === 'number' ? meta.timestamp : 0;
      items.push([hash, ts]);
    }
    items.sort((a, b) => a[1] - b[1]);
    const toRemove = items.length - maxEntries;
    for (let i = 0; i < toRemove; i += 1) {
      const hash = items[i][0];
      node._processedWithdrawals.delete(hash);
      node._processedWithdrawalsMeta.delete(hash);
      pruned += 1;
    }
  }
  if (pruned > 0) {
    console.log(`[Bridge] Pruned ${pruned} processed withdrawal entries`);
    saveBridgeState(node);
  }
}
export function startCleanupTimer(node) {
  const cleanupIntervalMs = Number(process.env.BRIDGE_CLEANUP_INTERVAL_MS || 3600000);
  if (node._bridgeCleanupTimer) clearInterval(node._bridgeCleanupTimer);
  node._bridgeCleanupTimer = setInterval(() => {
    cleanupExpiredSignatures(node);
    cleanupOldProcessedDeposits(node);
    cleanupOldPendingWithdrawals(node);
    cleanupOldPendingDepositRequests(node);
    cleanupOldProcessedWithdrawals(node);
  }, cleanupIntervalMs);
}
