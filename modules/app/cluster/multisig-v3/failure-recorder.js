import { computeAdaptiveBlacklistCooldownMs } from '../../../cluster/blacklist-utils.js';
export function createClusterFailureRecorder(node, clusterId) {
  return (reason) => {
    node.stateMachine.fail(reason);
    const reachable = node.p2p ? node.p2p.countConnectedPeers() : 0;
    const currentCount = node._clusterFailures.get(clusterId) || 0;
    const newCount = currentCount + 1;
    node._clusterFailures.set(clusterId, newCount);
    node._clusterFailMeta[clusterId] = {
      failures: newCount,
      reachable,
      lastFailureAt: Date.now(),
      reason,
    };
    if (newCount >= 3) {
      if (!node._clusterBlacklist) node._clusterBlacklist = {};
      const baseCooldownMs = Number(process.env.CLUSTER_BLACKLIST_BASE_COOLDOWN_MS || 600000);
      const cooldownMs = computeAdaptiveBlacklistCooldownMs(newCount, baseCooldownMs);
      node._clusterBlacklist[clusterId] = Date.now() + cooldownMs;
      console.log(
        `[WARN] Cluster ${clusterId.slice(0, 10)} blacklisted for ${cooldownMs / 60000} minutes (${newCount} failures, adaptive cooldown)`,
      );
    }
    node._saveClusterBlacklist();
  };
}
