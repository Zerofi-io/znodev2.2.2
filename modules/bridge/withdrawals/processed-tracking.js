export function markWithdrawalProcessedInMemory(node, txHash) {
  if (!node) return;
  const key = String(txHash || '').toLowerCase();
  if (!key) return;
  node._processedWithdrawals = node._processedWithdrawals || new Set();
  node._processedWithdrawalsMeta = node._processedWithdrawalsMeta || new Map();
  const prevMeta = node._processedWithdrawalsMeta.get(key) || {};
  const nextMeta = { ...prevMeta, timestamp: Date.now() };
  node._processedWithdrawalsMeta.set(key, nextMeta);
  if (!node._processedWithdrawals.has(key)) {
    node._processedWithdrawals.add(key);
  }
}
export function markWithdrawalCompleted(node, pendingKey, pending, xmrTxHashes) {
  if (!node) return;
  const metaHashes = Array.isArray(xmrTxHashes) ? xmrTxHashes : [];
  const metaTxKey = pending && pending.xmrTxKey ? String(pending.xmrTxKey) : null;
  const metaMoneroSigners = pending && Array.isArray(pending.moneroSigners) ? pending.moneroSigners : null;
  node._processedWithdrawalsMeta = node._processedWithdrawalsMeta || new Map();
  const markOne = (h) => {
    const hk = String(h || '').toLowerCase();
    if (!hk) return;
    const prev = node._processedWithdrawalsMeta.get(hk) || {};
    const base = { ...prev, status: 'completed', xmrTxHashes: metaHashes };
    if (metaMoneroSigners) base.moneroSigners = metaMoneroSigners;
    const next = metaTxKey ? { ...base, xmrTxKey: metaTxKey } : base;
    node._processedWithdrawalsMeta.set(hk, next);
    markWithdrawalProcessedInMemory(node, hk);
  };
  if (pending && pending.isBatch && Array.isArray(pending.ethTxHashes)) {
    for (const h of pending.ethTxHashes) markOne(h);
    return;
  }
  markOne(pendingKey);
}
