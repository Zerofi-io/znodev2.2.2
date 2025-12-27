export async function cleanupClusterAttempt(clusterId) {
  if (!this.p2p) return;
  this._stopCoordinatorHeartbeat();
  this._stopCoordinatorMonitor();
  const sessionId = this._sessionId || null;
  const sessionClusterId = sessionId ? `${clusterId}:${sessionId}` : null;
  const attemptScopeId =
    this._attemptScopeId ||
    (sessionId && typeof sessionId === 'string' && sessionId.startsWith('msig-')
      ? sessionId.split(':')[0]
      : null);
  const attemptClusterId = attemptScopeId ? `${clusterId}:${attemptScopeId}` : null;
  try {
    if (typeof this.p2p.clearClusterRounds === 'function') {
      await this.p2p.clearClusterRounds(clusterId, null, []);
      if (attemptScopeId) {
        await this.p2p.clearClusterRounds(clusterId, attemptScopeId, []);
      }
      if (sessionId) {
        await this.p2p.clearClusterRounds(clusterId, sessionId, []);
      }
    }
  } catch (e) {
    console.log('[WARN]  P2P clearClusterRounds error during cleanup:', e.message || String(e));
  }
  try {
    if (typeof this.p2p.clearClusterSignatures === 'function') {
      await this.p2p.clearClusterSignatures(clusterId);
      if (attemptClusterId && attemptClusterId !== clusterId && attemptClusterId !== sessionClusterId) {
        await this.p2p.clearClusterSignatures(attemptClusterId);
      }
      if (sessionClusterId) {
        await this.p2p.clearClusterSignatures(sessionClusterId);
      }
    }
  } catch (e) {
    console.log(
      '[WARN]  P2P clearClusterSignatures error during cleanup:',
      e.message || String(e),
    );
  }
  try {
    if (typeof this.p2p.leaveCluster === 'function') {
      await this.p2p.leaveCluster(clusterId);
      if (attemptClusterId && attemptClusterId !== clusterId && attemptClusterId !== sessionClusterId) {
        await this.p2p.leaveCluster(attemptClusterId);
      }
      if (sessionClusterId) {
        await this.p2p.leaveCluster(sessionClusterId);
      }
    }
  } catch (e) {
    console.log('[WARN]  P2P leaveCluster error during cleanup:', e.message || String(e));
  }
  try {
    if (typeof this.p2p.cleanupOldMessages === 'function') {
      this.p2p.cleanupOldMessages();
    }
  } catch (e) {
    console.log('[WARN]  P2P cleanupOldMessages error during cleanup:', e.message || String(e));
  }
  if (this.clusterState && typeof this.clusterState.setPendingR3 === 'function') {
    this.clusterState.setPendingR3(null);
  }
  if (typeof this._sessionId === 'string' && this._sessionId.startsWith('msig-')) {
    this._sessionId = null;
  }
  this._attemptScopeId = null;
}
export default {
  cleanupClusterAttempt,
};
