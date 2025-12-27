import { parseAtomicAmount } from './amounts.js';
import { getBridgeAddressLower } from './bridge-utils.js';
import { withMoneroLock } from '../monero-lock.js';
function buildExpectedDestinationsFromPending(pending) {
  const dest = new Map();
  if (!pending) return dest;
  if (pending.isBatch && Array.isArray(pending.batchWithdrawals)) {
    for (const w of pending.batchWithdrawals) {
      if (!w || !w.xmrAddress) continue;
      const addr = String(w.xmrAddress);
      const amt = parseAtomicAmount(w.xmrAmountAtomic != null ? w.xmrAmountAtomic : w.xmrAmount);
      if (amt == null || amt <= 0n) continue;
      dest.set(addr, (dest.get(addr) || 0n) + amt);
    }
    return dest;
  }
  if (!pending.xmrAddress) return dest;
  const addr = String(pending.xmrAddress);
  const amt = parseAtomicAmount(pending.xmrAmountAtomic != null ? pending.xmrAmountAtomic : pending.xmrAmount);
  if (amt == null || amt <= 0n) return dest;
  dest.set(addr, amt);
  return dest;
}
async function describeTxsetRecipients(node, txDataHex) {
  if (!node || !node.monero || !txDataHex) return null;
  const paramsList = [
    { multisig_txset: txDataHex },
    { tx_data_hex: txDataHex },
    { unsigned_txset: txDataHex },
  ];
  for (const params of paramsList) {
    let result = null;
    try {
      result = await withMoneroLock(node, { key: node._withdrawalMultisigLock, op: 'withdrawal_describe_transfer', timeoutMs: 20000 }, async () => {
        return await node.monero.call('describe_transfer', params, 20000);
      });
    } catch {
      continue;
    }
    const desc = result && Array.isArray(result.desc) ? result.desc : null;
    if (!desc) continue;
    const map = new Map();
    for (const d of desc) {
      const recs = d && Array.isArray(d.recipients) ? d.recipients : [];
      for (const r of recs) {
        const addr = r && r.address ? String(r.address) : '';
        if (!addr) continue;
        const amt = parseAtomicAmount(r.amount);
        if (amt == null || amt < 0n) continue;
        map.set(addr, (map.get(addr) || 0n) + amt);
      }
    }
    return map;
  }
  return null;
}
export async function verifyTxsetMatchesPending(node, txDataHex, pending) {
  const expected = buildExpectedDestinationsFromPending(pending);
  if (!expected || expected.size === 0) return false;
  const actual = await describeTxsetRecipients(node, txDataHex);
  if (!actual || actual.size === 0) return false;
  if (expected.size != actual.size) return false;
  for (const [addr, amt] of expected) {
    if (!actual.has(addr)) return false;
    if (actual.get(addr) !== amt) return false;
  }
  return true;
}
export async function getBurnOnChainStatus(node, txHash, expected) {
  if (!node || !node.provider || !node.bridge || !txHash) return 'unknown';
  let receipt = null;
  try {
    receipt = await node.provider.getTransactionReceipt(txHash);
  } catch {
    return 'unknown';
  }
  const logs = receipt && Array.isArray(receipt.logs) ? receipt.logs : [];
  if (!receipt) return 'unknown';
  if (logs.length === 0) return 'missing';
  const bridgeAddr = getBridgeAddressLower(node);
  for (const log of logs) {
    if (bridgeAddr) {
      const addr = log && log.address ? String(log.address).toLowerCase() : '';
      if (!addr || addr !== bridgeAddr) continue;
    }
    let parsed;
    try {
      parsed = node.bridge.interface.parseLog(log);
    } catch {
      continue;
    }
    if (!parsed || parsed.name !== 'TokensBurned') continue;
    const args = parsed.args || [];
    const user = args[0] ?? args.user;
    const clusterId = args[1] ?? args.clusterId;
    const xmrAddress = args[2] ?? args.xmrAddress;
    const burnAmount = args[3] ?? args.burnAmount ?? args.amount;
    if (expected) {
      if (expected.user) {
        const u1 = String(expected.user).toLowerCase();
        const u2 = user ? String(user).toLowerCase() : '';
        if (!u2 || u2 != u1) continue;
      }
      if (expected.clusterId) {
        const c1 = String(expected.clusterId).toLowerCase();
        const c2 = clusterId ? String(clusterId).toLowerCase() : '';
        if (!c2 || c2 != c1) continue;
      }
      if (expected.xmrAddress != null) {
        if (!xmrAddress || String(xmrAddress) != String(expected.xmrAddress)) continue;
      }
      if (expected.burnAmount != null) {
        const a1 = parseAtomicAmount(expected.burnAmount);
        const a2 = parseAtomicAmount(burnAmount);
        if (a1 == null || a2 == null || a1 != a2) continue;
      }
    }
    return 'verified';
  }
  return 'missing';
}
export async function verifyPendingBurnsOnChain(node, pending) {
  if (!pending) return false;
  const clusterId = node && node._activeClusterId ? node._activeClusterId : null;
  if (!clusterId) return false;
  if (pending.isBatch && Array.isArray(pending.batchWithdrawals) && pending.batchWithdrawals.length > 0) {
    for (const w of pending.batchWithdrawals) {
      if (!w || !w.txHash) return false;
      const status = await getBurnOnChainStatus(node, w.txHash, {
        user: w.user,
        clusterId,
        xmrAddress: w.xmrAddress,
        burnAmount: w.zxmrAmount,
      });
      if (status !== 'verified') return false;
    }
    return true;
  }
  const txHash = pending.txHash || pending.ethTxHash;
  if (!txHash) return false;
  const status = await getBurnOnChainStatus(node, txHash, {
    user: pending.user,
    clusterId,
    xmrAddress: pending.xmrAddress,
    burnAmount: pending.zxmrAmount,
  });
  return status === 'verified';
}
