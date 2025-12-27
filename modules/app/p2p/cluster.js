import metrics from '../../core/metrics.js';
export async function startP2P(node) {
  console.log('[INFO] Starting P2P network...');
  try {
    await node.p2p.start();
    if (node.provider && typeof node.provider.getBlock === 'function' && typeof node.p2p.setTimeOracle === 'function') {
      try {
        const block = await node.provider.getBlock('latest');
        const ts = block && block.timestamp != null ? block.timestamp : null;
        const numTs = typeof ts === 'bigint' ? Number(ts) : Number(ts);
        const chainTimeMs = Number.isFinite(numTs) && numTs > 0 ? Math.floor(numTs * 1000) : 0;
        const rawBlockNumber = block && block.number != null ? block.number : null;
        const blockNumber = typeof rawBlockNumber === 'bigint' ? Number(rawBlockNumber) : rawBlockNumber;
        if (chainTimeMs > 0) {
          await node.p2p.setTimeOracle(chainTimeMs, blockNumber, 'ethers');
        }
      } catch (e) {
        console.log('[WARN]  Time oracle sync failed:', e && e.message ? e.message : String(e));
      }
    }
    if (typeof node.p2p.setHeartbeatDomain === 'function') {
      const stakingAddr = node.staking.target || node.staking.address;
      await node.p2p.setHeartbeatDomain(node.chainId, stakingAddr);
    }
    if (typeof node.p2p.startQueueDiscovery === 'function') {
      const publicIp = (process.env.PUBLIC_IP || '').trim();
      const bridgePort = Number(process.env.BRIDGE_API_PORT) || 3002;
      const apiScheme = process.env.BRIDGE_API_TLS_ENABLED === '1' ? 'https' : 'http';
      const apiBase = publicIp ? `${apiScheme}://${publicIp}:${bridgePort}` : '';
      await node.p2p.startQueueDiscovery(
        node.staking.target || node.staking.address,
        node.chainId,
        apiBase,
      );
    }
    console.log('[OK] P2P network started\n');
    metrics.setGauge('p2p_connected', 1);
    if (typeof node.p2p.startHealthWatchdog === 'function') {
      node.p2p.startHealthWatchdog({
        minPeers: 3,
        criticalDurationMs: 10 * 60 * 1000,
        checkIntervalMs: 20000,
      });
    }
    if (node.p2p._connectedPeers) metrics.setGauge('p2p_peers', node.p2p._connectedPeers);
  } catch (error) {
    console.log('[WARN]  P2P start failed:', error.message);
    console.log(
      '  Cluster formation and multisig coordination are disabled until P2P is available.\n',
    );
    metrics.setGauge('p2p_connected', 0);
    if (node && typeof node._scheduleP2PRecovery === 'function') {
      node._scheduleP2PRecovery('start_failed');
    }
  }
}
export async function ensureBridgeSessionCluster(node) {
  if (!node.p2p || !node.p2p.node) return;
  if (!node._activeClusterId || !Array.isArray(node._clusterMembers) || node._clusterMembers.length === 0) return;
  if (!node._sessionId) node._sessionId = 'bridge';
  const clusterId = node._activeClusterId;
  const members = node._clusterMembers;
  let topicCluster;
  if (typeof node.p2p.topic === 'function') {
    topicCluster = node.p2p.topic(clusterId, node._sessionId);
  } else {
    topicCluster = `${clusterId}:${node._sessionId}`;
  }
  try {
    await node.p2p.joinCluster(topicCluster, members, false);
  } catch {}
}
export async function initClusterP2P(node, clusterId, clusterNodes, isCoordinator = false) {
  if (!node.p2p || !node.p2p.node) {
    console.log('[WARN]  P2P not available, using smart contract');
    return false;
  }
  try {
    console.log('[INFO] Initializing P2P for cluster...');
    await node.p2p.connectToCluster(clusterId, clusterNodes, isCoordinator, node.registry);
    if (node.p2p) {
      node.p2p.round0Responder = async () => {
        if (!node.multisigInfo) {
          await node.prepareMultisig();
        }
        return node.multisigInfo;
      };
    }
    await new Promise((r) => setTimeout(r, 3000));
    console.log('[OK] P2P cluster initialized');
    return true;
  } catch (error) {
    console.log('[WARN]  P2P cluster init failed:', error.message);
    return false;
  }
}
export async function waitForPeerPayloads(
  node,
  clusterId,
  sessionId,
  round,
  members,
  expectedCount,
  timeoutMs,
) {
  const timeout = Number.isFinite(timeoutMs) && timeoutMs > 0 ? timeoutMs : 300000;
  const self = node.wallet && node.wallet.address ? node.wallet.address.toLowerCase() : '';
  const requireAll =
    Array.isArray(members) &&
    members.length > 0 &&
    Number.isFinite(expectedCount) &&
    expectedCount >= members.length - 1;
  let complete = false;
  if (node.p2p && typeof node.p2p.waitForRoundCompletion === 'function') {
    try {
      complete = await node.p2p.waitForRoundCompletion(
        clusterId,
        sessionId,
        round,
        members,
        timeout,
        { requireAll },
      );
    } catch (e) {
      console.log(
        `[WARN]  P2P waitForRoundCompletion error (round ${round}):`,
        e.message || String(e),
      );
    }
  }
  let payloads = [];
  try {
    if (node.p2p && typeof node.p2p.getPeerPayloads === 'function') {
      payloads = await node.p2p.getPeerPayloads(clusterId, sessionId, round, members);
    }
  } catch (e) {
    console.log(`[WARN]  P2P getPeerPayloads error (round ${round}):`, e.message || String(e));
  }
  const count = Array.isArray(payloads) ? payloads.length : 0;
  let missing = [];
  if (node.p2p && typeof node.p2p.getRoundData === 'function' && Array.isArray(members)) {
    try {
      const data = await node.p2p.getRoundData(clusterId, sessionId, round);
      if (data && typeof data === 'object') {
        for (const m of members) {
          const addr = String(m || '').toLowerCase();
          if (!addr || addr === self) continue;
          const v = data[addr];
          if (typeof v !== 'string' || v.length === 0) {
            missing.push(addr);
          }
        }
      }
    } catch (e) {
      console.log(`[WARN]  P2P getRoundData error (round ${round}):`, e.message || String(e));
    }
  }
  const ok = complete && count >= expectedCount;
  if (!ok) {
    if (missing.length) {
      console.log(
        `[P2P] Round ${round} peer payload wait timed out: ${count}/${expectedCount} missing=[${missing.join(', ')}]`,
      );
    } else {
      console.log(`[P2P] Round ${round} peer payload wait timed out: ${count}/${expectedCount}`);
    }
  }
  return { complete: ok, payloads: Array.isArray(payloads) ? payloads : [], missing };
}
export default {
  startP2P,
  ensureBridgeSessionCluster,
  initClusterP2P,
  waitForPeerPayloads,
};
