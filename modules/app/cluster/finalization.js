import metrics from '../../core/metrics.js';
export function onClusterFinalized(clusterId, members, finalAddress) {
  const effectiveClusterId = clusterId || this._activeClusterId;
  if (!effectiveClusterId) {
    return;
  }
  this._sessionId = 'bridge';
  this._stopCoordinatorHeartbeat();
  this._stopCoordinatorMonitor();
  const wasFinalized = this.clusterState ? this.clusterState.finalized : this._clusterFinalized;
  if (this.clusterState && typeof this.clusterState.update === 'function') {
    const updates = { clusterId: effectiveClusterId };
    if (Array.isArray(members) && members.length) {
      updates.members = members;
    }
    if (finalAddress) {
      updates.finalAddress = finalAddress;
    }
    this.clusterState.update(updates);
    if (typeof this.clusterState.setFinalized === 'function') {
      this.clusterState.setFinalized();
    }
  }
  if (!this._clusterFinalizedAt) {
    this._clusterFinalizedAt = Date.now();
  }
  if (!wasFinalized) {
    metrics.incrementCounter('cluster_formations_total');
  }
  metrics.setGauge('cluster_active', 1, { clusterId: effectiveClusterId.slice(0, 8) });
  if (this.p2p && typeof this.p2p.setActiveCluster === 'function') {
    this.p2p.setActiveCluster(effectiveClusterId);
  }
  if (this.p2p && typeof this.p2p.joinCluster === 'function') {
    try {
      const sessionId = this._sessionId;
      const memberList = Array.isArray(members) && members.length ? members : (this._clusterMembers || []);
      if (memberList && memberList.length) {
        this.p2p
          .joinCluster(effectiveClusterId + ':' + sessionId, memberList, false)
          .catch((e) => {
            console.log('[Bridge] Failed to join PBFT cluster for bridge session:', e.message || String(e));
          });
      }
    } catch (e) {
      console.log('[Bridge] Error while initializing bridge PBFT cluster:', e.message || String(e));
    }
  }
  if (typeof this._saveClusterState === 'function') {
    this._saveClusterState();
  }
  if (!wasFinalized) {
    console.log('[OK] Cluster finalized on-chain');
  }
  try {
    this.startDepositMonitor();
  } catch (e) {
    console.log('[Bridge] Failed to start deposit monitor:', e.message || String(e));
  }
  try {
    this.startBridgeAPI();
  } catch (e) {
    console.log('[BridgeAPI] Failed to start API server:', e.message || String(e));
  }
  try {
    this.startWithdrawalMonitor();
  } catch (e) {
    console.log('[Withdrawal] Failed to start withdrawal monitor:', e.message || String(e));
  }
  if (this.stateMachine && this.stateMachine.currentState !== 'ACTIVE') {
    if (!wasFinalized && typeof this.stateMachine.transition === 'function') {
      this.stateMachine.transition('ACTIVE', { clusterId: effectiveClusterId }, 'cluster finalized');
    } else if (wasFinalized && typeof this.stateMachine.resume === 'function') {
      this.stateMachine.resume('ACTIVE', { clusterId: effectiveClusterId }, 'cluster finalized');
    }
  }
}
export default {
  onClusterFinalized,
};
