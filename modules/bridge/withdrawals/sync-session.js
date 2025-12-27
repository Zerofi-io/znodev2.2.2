const MULTISIG_SYNC_SESSION_TTL_MS = Number(process.env.MULTISIG_SYNC_SESSION_TTL_MS || 300000);
export function markRespondedSyncSession(node, sessionKey) {
  if (!node || !sessionKey) return;
  const key = String(sessionKey).toLowerCase();
  const now = Date.now();
  if (!node._respondedSyncSessions || !(node._respondedSyncSessions instanceof Map)) {
    node._respondedSyncSessions = new Map();
  }
  node._respondedSyncSessions.set(key, now);
  if (MULTISIG_SYNC_SESSION_TTL_MS > 0 && node._respondedSyncSessions.size > 0) {
    const cutoff = now - MULTISIG_SYNC_SESSION_TTL_MS;
    for (const [k, ts] of node._respondedSyncSessions) {
      if (typeof ts === 'number' && ts < cutoff) {
        node._respondedSyncSessions.delete(k);
      }
    }
  }
}
export function hasRespondedSyncSession(node, sessionKey) {
  if (!node || !sessionKey) return false;
  const key = String(sessionKey).toLowerCase();
  const store = node._respondedSyncSessions;
  if (!store) return false;
  if (store instanceof Map) {
    return store.has(key);
  }
  try {
    return store.has(key);
  } catch {
    return false;
  }
}
