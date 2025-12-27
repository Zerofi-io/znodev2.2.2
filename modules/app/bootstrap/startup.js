import { createHealthServer } from '../../core/health-server.js';
import metrics from '../../core/metrics.js';
import ConfigValidator from '../../core/config-validator.js';
import { startBridgeAPI } from '../../bridge/http-api.js';
async function validateConfiguration() {
  const validator = new ConfigValidator();
  const result = validator.validate();
  if (result.warnings.length > 0) {
    for (const warning of result.warnings) {
      console.warn(`[Config] Warning: ${warning}`);
    }
  }
  if (!result.valid) {
    for (const error of result.errors) {
      console.error(`[Config] Error: ${error}`);
    }
    throw new Error('Configuration validation failed. Fix the above errors before starting.');
  }
  console.log('[Config] Configuration validation passed');
  return result;
}
async function checkCriticalDependencies(node) {
  const checks = [];
  if (node.provider) {
    try {
      const blockNumber = await Promise.race([
        node.provider.getBlockNumber(),
        new Promise((_, reject) => setTimeout(() => reject(new Error('timeout')), 10000)),
      ]);
      if (typeof blockNumber !== 'number' || blockNumber < 0) {
        checks.push({ name: 'Ethereum RPC', ok: false, error: 'Invalid block number' });
      } else {
        checks.push({ name: 'Ethereum RPC', ok: true, blockNumber });
      }
    } catch (e) {
      checks.push({ name: 'Ethereum RPC', ok: false, error: e.message || String(e) });
    }
  }
  if (node.monero) {
    try {
      const height = await Promise.race([
        node.monero.getHeight(),
        new Promise((_, reject) => setTimeout(() => reject(new Error('timeout')), 10000)),
      ]);
      if (typeof height !== 'number' || height < 0) {
        checks.push({ name: 'Monero RPC', ok: false, error: 'Invalid height' });
      } else {
        checks.push({ name: 'Monero RPC', ok: true, height });
      }
    } catch (e) {
      const msg = e.message || String(e);
      if (/ECONNREFUSED|timeout/i.test(msg)) {
        checks.push({ name: 'Monero RPC', ok: false, error: msg });
      } else {
        checks.push({ name: 'Monero RPC', ok: true, note: 'Wallet may not be open yet' });
      }
    }
  }
  const failed = checks.filter((c) => !c.ok);
  if (failed.length > 0) {
    for (const check of failed) {
      console.error(`[Health] ${check.name}: FAILED - ${check.error}`);
    }
    throw new Error(`Critical dependency check failed: ${failed.map((c) => c.name).join(', ')}`);
  }
  for (const check of checks) {
    if (check.ok) {
      const info = check.blockNumber ? `block=${check.blockNumber}` : check.height ? `height=${check.height}` : check.note || 'OK';
      console.log(`[Health] ${check.name}: ${info}`);
    }
  }
}
export async function runStartupSequence(node) {
  if (!node) throw new Error('runStartupSequence: node is required');
  await validateConfiguration();
  await checkCriticalDependencies(node);
  await node.txManager.loadState();
  await node.txManager.detectAndClearStuckTransactions();
  await node.checkRequirements();
  await node.setupMonero();
  await node.startP2P();
  node.startHeartbeatLoop();
  try {
    node.rpcManager.startHealthWatch(node.monero);
  } catch (e) {
    console.warn('[RPC] Failed to start health watch:', e.message || String(e));
  }
  await node.ensureRegistered();
  const restoredCluster = await node._verifyAndRestoreCluster();
  if (restoredCluster && node.clusterWalletName) {
    try {
      await node.monero.openWallet(node.clusterWalletName, node.moneroPassword);
      console.log('[Cluster] Opened cluster wallet: ' + node.clusterWalletName);
    } catch (e) {
      console.log('[Cluster] Failed to open cluster wallet:', e.message || String(e));
      node._clearClusterState();
    }
  }
  if (node._clusterFinalized) {
    console.log('[Cluster] Resuming active cluster from restored state');
    if (typeof node._onClusterFinalized === 'function') {
      node._onClusterFinalized(node._activeClusterId, node._clusterMembers, node._clusterFinalAddress);
    }
  } else {
    await node.monitorNetwork();
  }
  node.startSlashingLoop();
  node.healthServer = createHealthServer(node, node._healthServerPort);
  node.healthServer.listen(node._healthServerPort, () => {
    console.log(`[Health] Server listening on port ${node._healthServerPort}`);
    console.log('  Endpoints: /health, /metrics, /ready');
  });
  startBridgeAPI(node);
  metrics.setGauge('znode_started', 1);
  metrics.setGauge('znode_chain_id', node.chainId);
}
export default runStartupSequence;
