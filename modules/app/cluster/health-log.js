import { MoneroHealth } from '../monero/health.js';
export function logClusterHealth(node) {
  try {
    console.log('[health] Cluster state snapshot:');
    console.log(`  Active cluster: ${node._activeClusterId || 'none'}`);
    console.log(`  Active members: ${node._clusterMembers ? node._clusterMembers.length : 0}`);
    console.log(`  Finalized: ${node._clusterFinalized ? 'yes' : 'no'}`);
    const failCount = Object.fromEntries(node._clusterFailures || new Map());
    const failMeta = node._clusterFailMeta || {};
    const blacklist = node._clusterBlacklist || {};
    const now = Date.now();
    const failEntries = Object.entries(failCount);
    if (!failEntries.length) {
      console.log('  Failures: none');
    } else {
      console.log('  Failures:');
      for (const [cid, count] of failEntries) {
        const meta = failMeta[cid] || {};
        const reachable = meta.reachable != null ? meta.reachable : 'unknown';
        const reason = meta.reason || 'unknown';
        console.log(`    ${cid}: ${count} failures (reachable=${reachable}, reason=${reason})`);
      }
    }
    const blEntries = Object.entries(blacklist);
    if (!blEntries.length) {
      console.log('  Blacklist: empty');
    } else {
      console.log('  Blacklist:');
      for (const [cid, until] of blEntries) {
        const remainingMs = until - now;
        const remainingMin = remainingMs > 0 ? Math.round(remainingMs / 60000) : 0;
        console.log(`    ${cid}: ${remainingMin}m remaining`);
      }
    }
    try {
      const health = node.moneroHealth || MoneroHealth.HEALTHY;
      console.log(`  Monero health: ${health}`);
      console.log('  Monero error counts:', JSON.stringify(node.moneroErrorCounts || {}));
    } catch {}
  } catch {}
}
