import fs from 'fs';
import path from 'path';
import { pruneBlacklistEntries } from '../../cluster/blacklist-utils.js';
export function getClusterStatePath(projectRoot) {
  return path.join(projectRoot, '.cluster-state.json');
}
export function getClusterRestorePath(projectRoot) {
  return path.join(projectRoot, '.cluster-restore.json');
}
export async function loadClusterBlacklist(node) {
  if (!node || !node._clusterBlacklistPath) return;
  try {
    const exists = await fs.promises
      .access(node._clusterBlacklistPath)
      .then(() => true)
      .catch(() => false);
    if (!exists) return;
    const data = await fs.promises.readFile(node._clusterBlacklistPath, 'utf8');
    const parsed = JSON.parse(data);
    node._clusterFailures = new Map(Object.entries(parsed.failures || {}));
    node._clusterFailMeta = parsed.meta || {};
    node._clusterBlacklist = parsed.blacklist || {};
    const now = Date.now();
    const { pruned, active } = pruneBlacklistEntries(node._clusterBlacklist, now);
    node._clusterBlacklist = pruned;
    console.log(
      `[Blacklist] Loaded ${node._clusterFailures.size} cluster failure records from disk`,
    );
    if (active > 0) {
      console.log(`[Blacklist] ${active} active blacklisted clusters`);
    }
  } catch (e) {
    console.warn(`[Blacklist] Failed to load from disk: ${e.message}`);
  }
}
export function saveClusterBlacklist(node) {
  if (!node) return;
  if (node._blacklistSavePending) {
    node._blacklistSaveQueued = true;
    return;
  }
  node._blacklistSavePending = true;
  node._blacklistSaveQueued = false;
  setImmediate(() => {
    try {
      const now = Date.now();
      const { pruned: prunedBlacklist } = pruneBlacklistEntries(
        node._clusterBlacklist || {},
        now,
      );
      const MAX_FAILURE_RECORDS = 1000;
      const failureEntries = Array.from((node._clusterFailures || new Map()).entries());
      if (failureEntries.length > MAX_FAILURE_RECORDS) {
        node._clusterFailures = new Map(failureEntries.slice(-MAX_FAILURE_RECORDS));
      }
      const data = {
        failures: Object.fromEntries(node._clusterFailures || []),
        meta: node._clusterFailMeta || {},
        blacklist: prunedBlacklist,
        updated: new Date().toISOString(),
      };
      const tmpPath = node._clusterBlacklistPath + '.tmp';
      fs.writeFileSync(tmpPath, JSON.stringify(data, null, 2), {
        mode: 0o600,
      });
      fs.renameSync(tmpPath, node._clusterBlacklistPath);
    } catch (e) {
      console.warn(`[Blacklist] Failed to save to disk: ${e.message}`);
    } finally {
      node._blacklistSavePending = false;
      if (node._blacklistSaveQueued) {
        saveClusterBlacklist(node);
      }
    }
  });
}
export function saveClusterState(node, projectRoot) {
  if (!node) return;
  try {
    const snapshot = node.clusterState && node.clusterState.state ? node.clusterState.state : null;
    if (!snapshot || !snapshot.clusterId || !snapshot.finalized) {
      return;
    }
    const state = {
      ...snapshot,
      walletName: node.clusterWalletName,
      savedAt: Date.now(),
    };
    const statePath = getClusterStatePath(projectRoot);
    const tmpPath = statePath + '.' + Date.now() + '.tmp';
    fs.writeFileSync(tmpPath, JSON.stringify(state, null, 2), { mode: 0o600 });
    fs.renameSync(tmpPath, statePath);
    console.log('[Cluster] Saved cluster state to disk');
  } catch (e) {
    console.log('[Cluster] Failed to save cluster state:', e.message || String(e));
  }
}
export function saveClusterRestoreSnapshot(node, projectRoot, state) {
  try {
    if (!state || !state.clusterId || !Array.isArray(state.members) || state.members.length === 0 || !state.finalAddress || !state.walletName) {
      return;
    }
    const restorePath = getClusterRestorePath(projectRoot);
    const snapshot = {
      clusterId: state.clusterId,
      members: state.members,
      finalAddress: state.finalAddress,
      walletName: state.walletName,
      savedAt: Date.now(),
    };
    const tmpPath = restorePath + '.' + Date.now() + '.tmp';
    fs.writeFileSync(tmpPath, JSON.stringify(snapshot, null, 2), { mode: 0o600 });
    fs.renameSync(tmpPath, restorePath);
  } catch (e) {
    console.log('[Cluster] Failed to save legacy restore snapshot:', e.message || String(e));
  }
}
export function clearClusterState(node, projectRoot) {
  try {
    if (node && node.clusterState && typeof node.clusterState.reset === 'function') {
      node.clusterState.reset();
    }
    const statePath = getClusterStatePath(projectRoot);
    if (fs.existsSync(statePath)) {
      fs.unlinkSync(statePath);
      console.log('[Cluster] Cleared persisted cluster state');
    }
  } catch (e) {
    console.log('[Cluster] Failed to clear cluster state:', e.message || String(e));
  }
}
