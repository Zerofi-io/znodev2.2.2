export function pruneBlacklistEntries(blacklist, now, maxEntries = 0) {
  const input = blacklist || {};
  const entries = Object.entries(input).filter(([, expiryTime]) => expiryTime > now);
  if (maxEntries > 0 && entries.length > maxEntries) {
    entries.sort((a, b) => b[1] - a[1]);
    entries.length = maxEntries;
  }
  const pruned = Object.fromEntries(entries);
  return { pruned, active: entries.length };
}
export function computeAdaptiveBlacklistCooldownMs(failureCount, baseCooldownMs) {
  const count = Number.isFinite(failureCount) && failureCount > 0 ? failureCount : 1;
  const base = Number.isFinite(baseCooldownMs) && baseCooldownMs > 0 ? baseCooldownMs : 600000;
  const exponentialFactor = Math.pow(2, Math.min(count - 1, 5));
  return base * exponentialFactor;
}
export function isClusterBlacklisted(blacklist, clusterId, now) {
  if (!blacklist || !clusterId) return false;
  const expiry = blacklist[clusterId];
  return typeof expiry === 'number' && expiry > now;
}
export function getBlacklistRemainingMs(blacklist, clusterId, now) {
  if (!blacklist || !clusterId) return 0;
  const expiry = blacklist[clusterId];
  if (typeof expiry !== 'number' || expiry <= now) return 0;
  return expiry - now;
}
