const DEFAULT_TTL_MS = Number(process.env.AUTH_TXID_TTL_MS || 86400000);
const DEFAULT_MAX = Number(process.env.AUTH_TXID_MAX || 2000);
function normalizeTxid(value) {
  const v = String(value || '').trim();
  if (!v) return null;
  return v.toLowerCase();
}
function prune(node, now) {
  const map = node._authorizedTxids;
  if (!map || map.size === 0) return;
  const ttl = DEFAULT_TTL_MS > 0 ? DEFAULT_TTL_MS : 0;
  if (ttl > 0) {
    for (const [k, ts] of map) {
      if (typeof ts !== 'number' || now - ts > ttl) {
        map.delete(k);
      }
    }
  }
  const max = DEFAULT_MAX > 0 ? DEFAULT_MAX : 0;
  if (max > 0) {
    while (map.size > max) {
      const first = map.keys().next();
      if (first.done) break;
      map.delete(first.value);
    }
  }
}
export function recordAuthorizedTxids(node, txids) {
  if (!node || !Array.isArray(txids) || txids.length === 0) return;
  node._authorizedTxids = node._authorizedTxids || new Map();
  const now = Date.now();
  prune(node, now);
  for (const t of txids) {
    const txid = normalizeTxid(t);
    if (!txid) continue;
    node._authorizedTxids.set(txid, now);
  }
  prune(node, now);
}
export function isAuthorizedTxid(node, txid) {
  if (!node || !node._authorizedTxids) return false;
  const k = normalizeTxid(txid);
  if (!k) return false;
  const now = Date.now();
  prune(node, now);
  return node._authorizedTxids.has(k);
}
