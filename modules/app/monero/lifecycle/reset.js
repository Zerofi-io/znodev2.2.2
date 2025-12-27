import fs from 'fs';
import path from 'path';
import { MoneroHealth, resetMoneroHealth } from '../health.js';
import { getMoneroWalletDir } from '../wallet-dir.js';
import { enableMoneroMultisigExperimentalForWallet } from './multisig-experimental.js';
export async function hardResetMoneroWalletState(node, reason) {
  if (node._clusterFinalAddress) {
    console.log(
      '[MoneroReset] Cluster has a finalized multisig; skipping hard reset to preserve cluster wallet state.',
    );
    return;
  }
  try {
    const statePath = node._getClusterStatePath ? node._getClusterStatePath() : null;
    if (statePath && fs.existsSync(statePath)) {
      console.log(
        '[MoneroReset] Finalized cluster state exists on disk; skipping hard reset to preserve cluster wallet state.',
      );
      return;
    }
    const restorePath = node._getClusterRestorePath ? node._getClusterRestorePath() : null;
    if (restorePath && fs.existsSync(restorePath)) {
      console.log(
        '[MoneroReset] Cluster restore snapshot exists on disk; skipping hard reset to preserve wallet state.',
      );
      return;
    }
  } catch {}
  const tag = (reason || 'unknown').toString().slice(0, 120);
  const walletDir = getMoneroWalletDir();
  const maxResets = Number(process.env.MAX_AUTO_WALLET_RESETS || 1);
  node._walletHardResetCount = node._walletHardResetCount || 0;
  if (node._walletHardResetCount >= maxResets) {
    console.log(
      `[MoneroReset] Skipping hard reset (already performed ${node._walletHardResetCount}/${maxResets} times this process)`,
    );
    return;
  }
  node._walletHardResetCount += 1;
  console.log(`[INFO] [MoneroReset] Performing hard reset of Monero wallet state due to: ${tag}`);
  await node._walletMutex.withLock(async () => {
    try {
      try {
        await node.monero.closeWallet();
      } catch {}
      try {
        const entries = await fs.promises.readdir(walletDir, {
          withFileTypes: true,
        });
        for (const entry of entries) {
          const isFile = typeof entry.isFile === 'function' ? entry.isFile() : entry.isFile;
          if (!isFile) continue;
          const name = entry.name || '';
          if (!name.startsWith('znode_')) continue;
          try {
            await fs.promises.unlink(path.join(walletDir, name));
            console.log(`[MoneroReset] Deleted wallet file: ${name}`);
          } catch (e) {
            console.log(
              `[MoneroReset] Failed to delete wallet file ${name}:`,
              e.message || String(e),
            );
          }
        }
      } catch (e) {
        console.log(
          `[MoneroReset] Warning: could not scan wallet dir ${walletDir}:`,
          e.message || String(e),
        );
      }
      node.baseWalletName =
        process.env.MONERO_WALLET_NAME || `znode_${node.wallet.address.slice(2, 10)}`;
      node.multisigInfo = null;
      if (node.clusterState && typeof node.clusterState.setPendingR3 === 'function') {
        node.clusterState.setPendingR3(null);
      }
      try {
        await node.monero.createWallet(node.baseWalletName, node.moneroPassword);
        console.log(`[MoneroReset] Fresh base wallet created: ${node.baseWalletName}`);
      } catch (e) {
        console.log('[MoneroReset] Failed to create fresh base wallet:', e.message || String(e));
        return;
      }
      try {
        await enableMoneroMultisigExperimentalForWallet(
          node,
          node.baseWalletName,
          `hard reset base wallet ${node.baseWalletName}`,
        );
      } catch (e) {
        console.log(
          '[MoneroReset] Failed to enable multisig experimental mode after hard reset:',
          e.message || String(e),
        );
      }
    } catch (e) {
      console.log('[MoneroReset] Hard reset failed:', e.message || String(e));
    }
  });
  node.currentAttemptWallet = null;
  resetMoneroHealth(node);
  if (node.moneroErrorCounts) {
    node.moneroErrorCounts.kexRoundMismatch = 0;
    node.moneroErrorCounts.kexTimeout = 0;
    node.moneroErrorCounts.rpcFailure = 0;
  }
}
export async function maybeAutoResetMoneroOnStartup(node) {
  if (node._moneroStartupScanDone) return;
  node._moneroStartupScanDone = true;
  const walletDir = getMoneroWalletDir();
  let entries;
  try {
    entries = await fs.promises.readdir(walletDir, { withFileTypes: true });
  } catch (e) {
    if (e && e.code !== 'ENOENT') {
      console.log('[MoneroReset] Startup wallet dir scan failed:', e.message || String(e));
    }
    return;
  }
  const walletBases = new Set();
  for (const entry of entries) {
    const isFile = typeof entry.isFile === 'function' ? entry.isFile() : entry.isFile;
    if (!isFile) continue;
    const name = entry.name || '';
    if (!name.startsWith('znode_')) continue;
    const base = name.endsWith('.keys') ? name.slice(0, -5) : name;
    if (!base) continue;
    walletBases.add(base);
  }
  const important = new Set();
  if (node.baseWalletName) important.add(node.baseWalletName);
  if (node.clusterWalletName) important.add(node.clusterWalletName);
  if (node.currentAttemptWallet) important.add(node.currentAttemptWallet);
  let needsReset = false;
  for (const base of walletBases) {
    const walletPath = path.join(walletDir, base);
    const keysPath = `${walletPath}.keys`;
    const walletExists = fs.existsSync(walletPath);
    const keysExists = fs.existsSync(keysPath);
    if (walletExists && keysExists) {
      continue;
    }
    if (important.has(base)) {
      console.log(
        `[MoneroReset] Startup scan detected incomplete important wallet: ${base} (wallet=${walletExists}, keys=${keysExists})`,
      );
      needsReset = true;
      break;
    }
    console.log(
      `[MoneroReset] Startup scan cleaning incomplete wallet artifacts: ${base} (wallet=${walletExists}, keys=${keysExists})`,
    );
    try {
      await fs.promises.unlink(walletPath);
      console.log(`[MoneroReset] Deleted wallet file: ${base}`);
    } catch (e) {
      if (e && e.code !== 'ENOENT') {
        console.log(
          `[MoneroReset] Failed to delete wallet file ${base}:`,
          e.message || String(e),
        );
      }
    }
    try {
      await fs.promises.unlink(keysPath);
      console.log(`[MoneroReset] Deleted wallet keys file: ${base}.keys`);
    } catch (e) {
      if (e && e.code !== 'ENOENT') {
        console.log(
          `[MoneroReset] Failed to delete wallet keys file ${base}.keys:`,
          e.message || String(e),
        );
      }
    }
  }
  if (!needsReset) return;
  try {
    const statePath = node._getClusterStatePath ? node._getClusterStatePath() : null;
    const restorePath = node._getClusterRestorePath ? node._getClusterRestorePath() : null;
    if ((statePath && fs.existsSync(statePath)) || (restorePath && fs.existsSync(restorePath))) {
      console.log(
        '[MoneroReset] Startup detected wallet issues but cluster state exists; refusing automatic hard reset.',
      );
      node.moneroHealth = MoneroHealth.QUARANTINED;
      return;
    }
  } catch {}
  try {
    await hardResetMoneroWalletState(node, 'startup: detected incomplete important znode wallet files');
  } catch (e) {
    console.log('[MoneroReset] Startup hard reset failed:', e.message || String(e));
  }
}
