import { ethers } from 'ethers';
function computeNextEpochWindowStart(nowMs, provider) {
  return new Promise(async (resolve) => {
    const epochRaw = process.env.FORMATION_EPOCH_SECONDS;
    const epochParsed = epochRaw != null ? Number(epochRaw) : NaN;
    const epochSec = Number.isFinite(epochParsed) && epochParsed > 0 ? epochParsed : 15 * 60;
    const windowRaw = process.env.FORMATION_WINDOW_SECONDS;
    const windowParsed = windowRaw != null ? Number(windowRaw) : NaN;
    const windowSec = Number.isFinite(windowParsed) && windowParsed > 0 ? windowParsed : 180;
    let tsSec = Math.floor(nowMs / 1000);
    try {
      if (provider) {
        const latest = await provider.getBlock('latest');
        const t = latest && latest.timestamp != null ? Number(latest.timestamp) : NaN;
        if (Number.isFinite(t) && t > 0) {
          tsSec = t;
        }
      }
    } catch {}
    const epoch = Math.floor(tsSec / epochSec);
    const epochEndSec = (epoch + 1) * epochSec;
    const windowStartSec = epochEndSec - windowSec;
    const nextStartSec =
      tsSec < windowStartSec
        ? windowStartSec
        : epochEndSec + epochSec - windowSec;
    resolve(Math.max(nowMs, Math.floor(nextStartSec * 1000)));
  });
}
function computeAdaptiveBackoff(failureCount, baseMs = 300000, maxMs = 3600000) {
  if (failureCount <= 1) return baseMs;
  const multiplier = Math.pow(2, Math.min(failureCount - 1, 5));
  return Math.min(baseMs * multiplier, maxMs);
}
export function startCoordinatorHeartbeat(node, clusterId, expectedCoordinator) {
  if (node._coordHeartbeatTimer) {
    clearInterval(node._coordHeartbeatTimer);
  }
  const intervalRaw = process.env.COORD_HEARTBEAT_INTERVAL_MS;
  const intervalParsed = intervalRaw != null ? Number(intervalRaw) : NaN;
  let intervalMs = Number.isFinite(intervalParsed) && intervalParsed > 0 ? intervalParsed : 10000;
  if (intervalMs < 1000 || intervalMs > 60000) {
    console.warn(
      `[CoordHB] Invalid COORD_HEARTBEAT_INTERVAL_MS ${intervalMs}ms, clamping to [1000, 60000]`,
    );
    intervalMs = Math.max(1000, Math.min(60000, intervalMs));
  }
  const maxRaw = process.env.COORD_HEARTBEAT_MAX_BROADCASTS;
  const maxParsed = maxRaw != null ? Number(maxRaw) : NaN;
  const maxBroadcasts = Number.isFinite(maxParsed) && maxParsed > 0 ? Math.floor(maxParsed) : 0;
  const maxLabel = maxBroadcasts > 0 ? String(maxBroadcasts) : 'unlimited';
  node._lastCoordHeartbeatSent = Date.now();
  node._coordHeartbeatSuccessCount = 0;
  node._coordHeartbeatErrorCount = 0;
  node._expectedCoordinator = expectedCoordinator ? expectedCoordinator.toLowerCase() : null;
  console.log(
    `[CoordHB] Starting coordinator heartbeat (interval: ${intervalMs / 1000}s, max: ${maxLabel})`,
  );
  const sendHeartbeat = async () => {
    if (node.clusterState && node.clusterState.finalized) {
      stopCoordinatorHeartbeat(node);
      return;
    }
    if (!node.p2p || !node._activeClusterId) {
      return;
    }
    if (maxBroadcasts > 0 && node._coordHeartbeatSuccessCount >= maxBroadcasts) {
      stopCoordinatorHeartbeat(node);
      return;
    }
    try {
      const now = Date.now();
      let coordinatorAddr = null;
      try { coordinatorAddr = ethers.getAddress(node.wallet.address); } catch {}
      const hbHash = ethers.solidityPackedKeccak256(
        ['string', 'bytes32', 'address', 'uint256'],
        ['coord-heartbeat-v1', clusterId || ethers.ZeroHash, coordinatorAddr || ethers.ZeroAddress, BigInt(Math.floor(now))],
      );
      const signature = await node.wallet.signMessage(ethers.getBytes(hbHash));
      // Round 9999 payload changes every heartbeat (timestamp/signature).
      // The P2P host rejects overwriting an existing round payload, so clear it first.
      try {
        await node.p2p.clearClusterRounds(clusterId, null, [9999]);
      } catch {}
      await node.p2p.broadcastRoundData(
        clusterId,
        null,
        9999,
        JSON.stringify({
          version: 2,
          type: 'coord-alive',
          timestamp: now,
          coordinator: coordinatorAddr || node.wallet.address,
          sig: signature,
        }),
      );
      node._lastCoordHeartbeatSent = Date.now();
      node._coordHeartbeatSuccessCount += 1;
    } catch (e) {
      node._coordHeartbeatErrorCount += 1;
      console.log('[CoordHB] Failed to send heartbeat:', e.message || String(e));
    }
  };
  sendHeartbeat();
  node._coordHeartbeatTimer = setInterval(sendHeartbeat, intervalMs);
}
export function stopCoordinatorHeartbeat(node) {
  if (node._coordHeartbeatTimer) {
    clearInterval(node._coordHeartbeatTimer);
    node._coordHeartbeatTimer = null;
  }
  if (node.p2p && node._activeClusterId) {
    try {
      node.p2p.clearClusterRounds(node._activeClusterId, null, [9999]);
    } catch {}
  }
}
export function startCoordinatorMonitor(node, expectedCoordinator) {
  if (node._coordMonitorTimer) {
    clearInterval(node._coordMonitorTimer);
  }
  node._lastCoordHeartbeatReceived = Date.now();
  node._expectedCoordinator = expectedCoordinator ? expectedCoordinator.toLowerCase() : null;
  const timeoutRaw = process.env.COORD_HEARTBEAT_TIMEOUT_MS;
  const timeoutParsed = timeoutRaw != null ? Number(timeoutRaw) : NaN;
  let timeoutMs = Number.isFinite(timeoutParsed) && timeoutParsed > 0 ? timeoutParsed : 30000;
  const intervalRaw = process.env.COORD_HEARTBEAT_INTERVAL_MS;
  const intervalParsed = intervalRaw != null ? Number(intervalRaw) : NaN;
  const intervalMs = Number.isFinite(intervalParsed) && intervalParsed > 0 ? intervalParsed : 10000;
  if (timeoutMs < intervalMs * 3) {
    console.warn(
      `[CoordHB] COORD_HEARTBEAT_TIMEOUT_MS ${timeoutMs}ms < 3x interval, setting to ${intervalMs * 3}ms`,
    );
    timeoutMs = intervalMs * 3;
  }
  const checkIntervalMs = Math.max(1000, Math.floor(timeoutMs / 6));
  console.log(
    `[CoordHB] Monitoring coordinator (timeout: ${timeoutMs / 1000}s, check: ${checkIntervalMs / 1000}s)`,
  );
  node._coordMonitorTimer = setInterval(async () => {
    if (node._coordMonitorFailed) {
      if (node.clusterState && typeof node.clusterState.getCooldownUntil === 'function') {
        const cooldownUntil = node.clusterState.getCooldownUntil();
        if (Date.now() > cooldownUntil) {
          node._coordMonitorFailed = false;
        }
      }
      return;
    }
    try {
      if (node.p2p && node._activeClusterId) {
        const r9999 = await node.p2p.getRoundData(node._activeClusterId, null, 9999);
        for (const payload of Object.values(r9999 || {})) {
          try {
            const data = JSON.parse(payload);
            handleCoordinatorHeartbeat(node, data);
          } catch (e) {
            const prefix = String(payload).substring(0, 50);
            console.warn(
              '[CoordHB] Failed to parse heartbeat:',
              prefix,
              e.message || String(e),
            );
          }
        }
      }
    } catch {}
    const elapsed = Date.now() - node._lastCoordHeartbeatReceived;
    if (elapsed > timeoutMs) {
      console.log(
        `[WARN]  [CoordHB] Coordinator silent for ${Math.round(elapsed / 1000)}s (>${timeoutMs / 1000}s)`,
      );
      try {
        if (node.stateMachine && typeof node.stateMachine.fail === 'function') {
          node.stateMachine.fail('coordinator_silent');
        }
        const now = Date.now();
        let cooldownUntil = now + Number(process.env.CLUSTER_RETRY_COOLDOWN_MS || 300000);
        const useNextWindow = process.env.COOLDOWN_TO_NEXT_EPOCH_WINDOW !== '0';
        if (useNextWindow) {
          cooldownUntil = await computeNextEpochWindowStart(now, node.provider);
        }
        let failureCount = 1;
        if (node.clusterState) {
          if (typeof node.clusterState.getFailureCount === 'function') {
            failureCount = node.clusterState.getFailureCount() + 1;
          }
          if (typeof node.clusterState.recordFailure === 'function') {
            node.clusterState.recordFailure('coordinator_silent');
          }
          const backoffMs = computeAdaptiveBackoff(failureCount);
          cooldownUntil = Math.max(cooldownUntil, now + backoffMs);
          if (typeof node.clusterState.setCooldownUntil === 'function') {
            node.clusterState.setCooldownUntil(cooldownUntil);
          }
        }
        node._coordMonitorFailed = true;
        stopCoordinatorMonitor(node);
        return;
      } catch {}
    }
  }, checkIntervalMs);
}
export function stopCoordinatorMonitor(node) {
  if (node._coordMonitorTimer) {
    clearInterval(node._coordMonitorTimer);
    node._coordMonitorTimer = null;
  }
}
export function handleCoordinatorHeartbeat(node, data) {
  if (!data || data.type !== 'coord-alive') return;
  const now = Date.now();
  const requireSig = process.env.COORD_HEARTBEAT_REQUIRE_SIG !== '0';
  const maxFutureSkewMsRaw = process.env.COORD_HEARTBEAT_MAX_SKEW_MS;
  const maxFutureSkewMsParsed = maxFutureSkewMsRaw != null ? Number(maxFutureSkewMsRaw) : NaN;
  const maxFutureSkewMs = Number.isFinite(maxFutureSkewMsParsed) && maxFutureSkewMsParsed >= 0 ? maxFutureSkewMsParsed : 120000;
  const ts = Number(data.timestamp);
  if (!Number.isFinite(ts) || ts <= 0) return;
  if (ts > now + maxFutureSkewMs) return;
  const expected = node._expectedCoordinator ? String(node._expectedCoordinator).toLowerCase() : null;
  const sig = typeof data.sig === 'string' ? data.sig : null;
  let ok = false;
  if (sig) {
    try {
      const recovered = ethers.verifyMessage(
        ethers.getBytes(
          ethers.solidityPackedKeccak256(
            ['string', 'bytes32', 'address', 'uint256'],
            ['coord-heartbeat-v1', node._activeClusterId || ethers.ZeroHash, ethers.getAddress(data.coordinator), BigInt(Math.floor(ts))],
          ),
        ),
        sig,
      );
      const recLc = recovered.toLowerCase();
      if (!expected || recLc === expected) ok = true;
    } catch {}
  }
  if (!ok && requireSig) return;
  if (!ok && !requireSig) {
    if (!expected) return;
    try {
      const fromPayload = ethers.getAddress(data.coordinator || '');
      if (fromPayload.toLowerCase() !== expected) return;
      ok = true;
    } catch { return; }
  }
  if (!ok) return;
  const bounded = ts > now ? now : ts;
  node._lastCoordHeartbeatReceived = Math.max(node._lastCoordHeartbeatReceived || 0, bounded);
}
export default {
  startCoordinatorHeartbeat,
  stopCoordinatorHeartbeat,
  startCoordinatorMonitor,
  stopCoordinatorMonitor,
  handleCoordinatorHeartbeat,
};
