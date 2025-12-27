import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { saveBridgeState } from '../bridge-state.js';
import { tryParseJSON } from './json.js';
import { markWithdrawalProcessedInMemory } from './processed-tracking.js';
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const BRIDGE_DIR = path.resolve(__dirname, '..');
export const WITHDRAWAL_PENDING_STORE_PATH = path.join(BRIDGE_DIR, '.withdrawals-pending.json');
let _pendingWithdrawalsSavePending = false;
export function loadPendingWithdrawalsStoreRaw() {
  try {
    const raw = fs.readFileSync(WITHDRAWAL_PENDING_STORE_PATH, 'utf8');
    const arr = tryParseJSON(raw, 'at line 210');
    return Array.isArray(arr) ? arr : [];
  } catch {
    return [];
  }
}
export function schedulePendingWithdrawalsStoreSave(node) {
  if (!node || !node._pendingWithdrawals || _pendingWithdrawalsSavePending) return;
  _pendingWithdrawalsSavePending = true;
  setTimeout(() => {
    try {
      const list = [];
      for (const [txHash, value] of node._pendingWithdrawals) {
        if (!value || typeof value !== 'object') continue;
        const entry = {};
        for (const [k, v] of Object.entries(value)) {
          if (typeof v === 'bigint') {
            entry[k] = v.toString();
          } else {
            entry[k] = v;
          }
        }
        list.push({ txHash, data: entry });
      }
      fs.writeFileSync(WITHDRAWAL_PENDING_STORE_PATH, JSON.stringify(list), { mode: 0o600 });
    } catch (e) {
      console.log('[Withdrawal] Failed to persist pending withdrawals:', e.message || String(e));
    }
    _pendingWithdrawalsSavePending = false;
  }, 50);
}
let _bridgeStateSavePending = false;
function scheduleBridgeStateSave(node) {
  if (!node || _bridgeStateSavePending) return;
  _bridgeStateSavePending = true;
  setTimeout(() => {
    saveBridgeState(node);
    _bridgeStateSavePending = false;
  }, 50);
}
export function scheduleWithdrawalStateSave(node) {
  scheduleBridgeStateSave(node);
  if (process.env.WITHDRAWAL_LEGACY_PENDING_STORE === '1') {
    schedulePendingWithdrawalsStoreSave(node);
  }
}
function savePendingWithdrawalsStoreSync(node) {
  if (!node || !node._pendingWithdrawals) return true;
  try {
    const list = [];
    for (const [txHash, value] of node._pendingWithdrawals) {
      if (!value || typeof value !== 'object') continue;
      const entry = {};
      for (const [k, v] of Object.entries(value)) {
        if (typeof v === 'bigint') entry[k] = v.toString();
        else entry[k] = v;
      }
      list.push({ txHash, data: entry });
    }
    fs.writeFileSync(WITHDRAWAL_PENDING_STORE_PATH, JSON.stringify(list), { mode: 0o600 });
    return true;
  } catch (e) {
    console.log('[Withdrawal] Failed to persist pending withdrawals:', e.message || String(e));
    return false;
  }
}
export function flushWithdrawalStateSave(node) {
  const ok = saveBridgeState(node);
  if (!ok) return false;
  if (process.env.WITHDRAWAL_LEGACY_PENDING_STORE === '1') {
    return savePendingWithdrawalsStoreSync(node);
  }
  return true;
}
export function getPendingRecencyMs(data) {
  if (!data) return 0;
  const a = typeof data.lastAttemptAt === 'number' ? data.lastAttemptAt : 0;
  const b = typeof data.firstAttemptAt === 'number' ? data.firstAttemptAt : 0;
  const c = typeof data.startedAt === 'number' ? data.startedAt : 0;
  const d = typeof data.timestamp === 'number' ? data.timestamp : 0;
  return Math.max(a, b, c, d, 0);
}
export function migrateLegacyPendingWithdrawals(node) {
  if (!node || node._withdrawalLegacyPendingMigrated) return;
  node._withdrawalLegacyPendingMigrated = true;
  let items;
  try {
    if (!fs.existsSync(WITHDRAWAL_PENDING_STORE_PATH)) return;
    items = loadPendingWithdrawalsStoreRaw();
  } catch {
    return;
  }
  if (!items || items.length === 0) return;
  node._pendingWithdrawals = node._pendingWithdrawals || new Map();
  for (const item of items) {
    if (!item || !item.txHash || !item.data) continue;
    const key = String(item.txHash || '').toLowerCase();
    if (!key) continue;
    const data = { ...item.data };
    if (data && typeof data.xmrAmountAtomic === 'string') {
      try {
        data.xmrAmountAtomic = BigInt(data.xmrAmountAtomic);
      } catch {}
    }
    if (data && data.status === 'completed') {
      markWithdrawalProcessedInMemory(node, key);
      continue;
    }
    const existing = node._pendingWithdrawals.get(key);
    if (!existing) {
      node._pendingWithdrawals.set(key, data);
      continue;
    }
    const prevMs = getPendingRecencyMs(existing);
    const nextMs = getPendingRecencyMs(data);
    if (nextMs > prevMs) {
      node._pendingWithdrawals.set(key, { ...existing, ...data });
    }
  }
  scheduleWithdrawalStateSave(node);
  try {
    const migratedPath = WITHDRAWAL_PENDING_STORE_PATH + '.migrated';
    if (!fs.existsSync(migratedPath)) {
      fs.renameSync(WITHDRAWAL_PENDING_STORE_PATH, migratedPath);
    }
  } catch {}
}
