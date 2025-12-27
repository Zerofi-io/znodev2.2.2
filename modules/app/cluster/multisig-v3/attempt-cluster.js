export async function joinMultisigAttemptCluster(node, clusterId, attemptScopeId, canonicalMembers, isCoordinator, recordClusterFailure) {
  const attemptClusterId = `${clusterId}:${attemptScopeId}`;
  try {
    if (node.p2p && typeof node.p2p.joinCluster === 'function') {
      await node.p2p.joinCluster(attemptClusterId, canonicalMembers, isCoordinator);
    }
  } catch (e) {
    console.log('[WARN]  Failed to join multisig attempt cluster:', e.message || String(e));
    recordClusterFailure('attempt_join_failed');
    return false;
  }
  if (node.p2p && typeof node.p2p.clearClusterRounds === 'function') {
    try {
      await node.p2p.clearClusterRounds(clusterId, attemptScopeId, []);
    } catch (e) {
      console.log('[WARN]  Failed to clear multisig attempt round cache:', e.message || String(e));
    }
  }
  return true;
}
