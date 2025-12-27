export function getCanonicalSignerSet(node) {
  if (!node || !Array.isArray(node._clusterMembers) || node._clusterMembers.length === 0) return [];
  return node._clusterMembers.map((a) => String(a || '').toLowerCase());
}
