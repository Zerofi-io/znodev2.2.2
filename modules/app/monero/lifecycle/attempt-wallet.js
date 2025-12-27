import fs from 'fs';
import path from 'path';
import { MoneroHealth, resetMoneroHealth } from '../health.js';
import { getMoneroWalletDir } from '../wallet-dir.js';
import { enableMoneroMultisigExperimentalForWallet } from './multisig-experimental.js';
import { hardResetMoneroWalletState } from './reset.js';
const MAX_ROTATION_STAT_KEYS = 50;
export function computeAttemptWalletName(node, clusterId, attempt) {
  const shortEth = node.wallet.address.slice(2, 10);
  const clusterPart = (clusterId || '').toString().slice(2, 10) || 'noclust';
  const attemptPart = String(attempt || 1).padStart(2, '0');
  return `znode_att_${shortEth}_${clusterPart}_${attemptPart}`;
}
export async function deleteWalletFiles(_node, walletFile) {
  if (!walletFile) return;
  const walletDir = getMoneroWalletDir();
  const raw = walletFile.endsWith('.keys') ? walletFile.slice(0, -5) : walletFile;
  const base = path.basename(raw);
  if (!base || base !== raw) return;
  const walletPath = path.join(walletDir, base);
  const keysPath = `${walletPath}.keys`;
  try {
    await fs.promises.unlink(walletPath);
    console.log(`[MoneroAttempt] Deleted wallet file: ${base}`);
  } catch (e) {
    if (e && e.code !== 'ENOENT') {
      console.log(
        `[MoneroAttempt] Failed to delete wallet file ${base}:`,
        e.message || String(e),
      );
    }
  }
  try {
    await fs.promises.unlink(keysPath);
    console.log(`[MoneroAttempt] Deleted wallet keys file: ${base}.keys`);
  } catch (e) {
    if (e && e.code !== 'ENOENT') {
      console.log(
        `[MoneroAttempt] Failed to delete wallet keys file ${base}.keys:`,
        e.message || String(e),
      );
    }
  }
}
export function verifyWalletFilesExist(_node, walletFile) {
  if (!walletFile) return false;
  const walletDir = getMoneroWalletDir();
  const raw = walletFile.endsWith('.keys') ? walletFile.slice(0, -5) : walletFile;
  const base = path.basename(raw);
  if (!base || base !== raw) return false;
  const walletPath = path.join(walletDir, base);
  const keysPath = `${walletPath}.keys`;
  const walletExists = fs.existsSync(walletPath);
  const keysExists = fs.existsSync(keysPath);
  if (!walletExists || !keysExists) {
    console.log(
      `[MoneroAttempt] Wallet file validation failed for ${base}: wallet=${walletExists}, keys=${keysExists}`,
    );
    return false;
  }
  return true;
}
export async function preflightMoneroForClusterAttempt(node, clusterId, attempt) {
  if (!node.moneroHealth) {
    node.moneroHealth = MoneroHealth.HEALTHY;
  }
  if (node.moneroHealth === MoneroHealth.QUARANTINED) {
    console.log('[MoneroHealth] Node is QUARANTINED; refusing to start new multisig attempt.');
    return false;
  }
  if (node.moneroHealth === MoneroHealth.NEEDS_GLOBAL_RESET) {
    console.log(
      '[MoneroHealth] Global Monero reset required before new cluster attempt (from previous error).',
    );
    try {
      await hardResetMoneroWalletState(
        node,
        'monero health = NEEDS_GLOBAL_RESET pre-attempt',
      );
    } catch (e) {
      console.log('[MoneroHealth] Hard reset before attempt failed:', e.message || String(e));
      return false;
    }
    resetMoneroHealth(node);
  } else if (node.moneroHealth === MoneroHealth.NEEDS_ATTEMPT_RESET && node.currentAttemptWallet) {
    console.log('[MoneroHealth] Cleaning up prior attempt wallet before new cluster attempt.');
    await deleteWalletFiles(node, node.currentAttemptWallet);
    node.currentAttemptWallet = null;
    resetMoneroHealth(node);
  }
  const attemptWallet = computeAttemptWalletName(node, clusterId, attempt);
  node.currentAttemptWallet = attemptWallet;
  console.log(
    `[MoneroAttempt] Preparing fresh attempt wallet for cluster=${clusterId} attempt=${attempt}: ${attemptWallet}`,
  );
  try {
    try {
      await node.monero.closeWallet();
    } catch {}
    await node.monero.createWallet(attemptWallet, node.moneroPassword);
    console.log(`[MoneroAttempt] Created attempt wallet: ${attemptWallet}`);
  } catch (e) {
    const msg = e && e.message ? e.message : String(e);
    if (/already exists/i.test(msg)) {
      console.log(
        `[MoneroAttempt] Attempt wallet already exists; validating files: ${attemptWallet}`,
      );
      const filesOk = verifyWalletFilesExist(node, attemptWallet);
      if (!filesOk) {
        console.log(
          `[MoneroAttempt] Wallet files incomplete; cleaning up and retrying creation: ${attemptWallet}`,
        );
        await deleteWalletFiles(node, attemptWallet);
        try {
          await node.monero.closeWallet();
        } catch {}
        try {
          await node.monero.createWallet(attemptWallet, node.moneroPassword);
          console.log(
            `[MoneroAttempt] Created attempt wallet after cleanup: ${attemptWallet}`,
          );
        } catch (retryErr) {
          const retryMsg =
            retryErr && retryErr.message ? retryErr.message : String(retryErr);
          console.log('[MoneroAttempt] Failed to create wallet after cleanup:', retryMsg);
          if (/already exists/i.test(retryMsg)) {
            console.log(
              `[MoneroAttempt] Wallet RPC still reports existing attempt wallet after cleanup; proceeding to reuse: ${attemptWallet}`,
            );
          } else {
            throw retryErr;
          }
        }
      } else {
        console.log(`[MoneroAttempt] Wallet files validated; reusing: ${attemptWallet}`);
      }
    } else {
      console.log('[MoneroAttempt] Failed to create attempt wallet:', msg);
      throw e;
    }
  }
  try {
    await node.monero.openWallet(attemptWallet, node.moneroPassword);
    console.log(`[MoneroAttempt] Opened attempt wallet: ${attemptWallet}`);
  } catch (e) {
    const msg = e && e.message ? e.message : String(e);
    console.log('[MoneroAttempt] Failed to open attempt wallet after create:', msg);
    const isFileMissingError = /file not found/i.test(msg) || /key file not found/i.test(msg);
    if (!isFileMissingError) {
      throw e;
    }
    console.log(
      `[MoneroAttempt] Attempt wallet appears corrupted; deleting and recreating: ${attemptWallet}`,
    );
    await deleteWalletFiles(node, attemptWallet);
    try {
      try {
        await node.monero.closeWallet();
      } catch {}
      await node.monero.createWallet(attemptWallet, node.moneroPassword);
      console.log(
        `[MoneroAttempt] Recreated attempt wallet after open failure: ${attemptWallet}`,
      );
      await node.monero.openWallet(attemptWallet, node.moneroPassword);
      console.log(
        `[MoneroAttempt] Opened attempt wallet after recreate: ${attemptWallet}`,
      );
    } catch (retryErr) {
      const retryMsg =
        retryErr && retryErr.message ? retryErr.message : String(retryErr);
      console.log(
        '[MoneroAttempt] Failed to recreate/open attempt wallet after cleanup:',
        retryMsg,
      );
      if (/already exists/i.test(retryMsg)) {
        node.moneroHealth = MoneroHealth.NEEDS_GLOBAL_RESET;
        return false;
      }
      throw retryErr;
    }
  }
  try {
    await enableMoneroMultisigExperimentalForWallet(
      node,
      attemptWallet,
      `cluster attempt wallet ${attemptWallet}`,
    );
  } catch (e) {
    console.log(
      '[MoneroAttempt] Failed to enable multisig experimental mode on attempt wallet:',
      e.message || String(e),
    );
    return false;
  }
  node.multisigInfo = null;
  if (node.clusterState && typeof node.clusterState.setPendingR3 === 'function') {
    node.clusterState.setPendingR3(null);
  }
  return true;
}
export async function postAttemptMoneroCleanup(node, clusterId, _attempt, success) {
  if (success) {
    resetMoneroHealth(node);
    node.currentAttemptWallet = null;
    return;
  }
  if (node.currentAttemptWallet) {
    console.log(
      `[MoneroAttempt] Cleaning up failed attempt wallet for cluster=${clusterId}: ${node.currentAttemptWallet}`,
    );
    try {
      await node.monero.closeWallet();
    } catch {}
    await deleteWalletFiles(node, node.currentAttemptWallet);
    node.currentAttemptWallet = null;
  }
}
function pruneRotationStats(stats) {
  if (!stats || typeof stats !== 'object') return;
  const keys = Object.keys(stats).filter(k => k !== 'total');
  if (keys.length <= MAX_ROTATION_STAT_KEYS) return;
  const sorted = keys.sort((a, b) => (stats[a] || 0) - (stats[b] || 0));
  const toRemove = sorted.slice(0, keys.length - MAX_ROTATION_STAT_KEYS);
  for (const key of toRemove) {
    delete stats[key];
  }
}
export async function rotateBaseWalletAfterKexError(node, reason) {
  const key = (reason || 'unknown').toString().slice(0, 120);
  if (!node._walletRotationStats) {
    node._walletRotationStats = { total: 0 };
  }
  node._walletRotationStats.total = (node._walletRotationStats.total || 0) + 1;
  node._walletRotationStats[key] = (node._walletRotationStats[key] || 0) + 1;
  pruneRotationStats(node._walletRotationStats);
  try {
    console.log(`  [INFO] Rotating base wallet due to Monero KEX error: ${key}`);
    console.log(
      `    Rotation counts so far: total=${node._walletRotationStats.total}, reasonCount=${node._walletRotationStats[key]}`,
    );
    try {
      await node.monero.closeWallet();
    } catch {}
    await new Promise((r) => setTimeout(r, 500));
    const suffix = Math.floor(Date.now() / 1000)
      .toString(36)
      .slice(-4);
    node.baseWalletName = `${node.baseWalletName}_kx${suffix}`;
    node._updateEnv('MONERO_WALLET_NAME', node.baseWalletName);
    await node.monero.createWallet(node.baseWalletName, node.moneroPassword);
    console.log(`  [OK] New base wallet created after kex error: ${node.baseWalletName}`);
    try {
      await enableMoneroMultisigExperimentalForWallet(
        node,
        node.baseWalletName,
        `rotated base wallet ${node.baseWalletName} after kex error`,
      );
    } catch (e2) {
      console.log(
        '  [WARN] Failed to enable multisig experimental mode on rotated base wallet:',
        e2.message || String(e2),
      );
    }
    node.multisigInfo = null;
  } catch (rotateErr) {
    console.log(
      '  [WARN] Wallet rotation after kex error failed:',
      rotateErr.message || String(rotateErr),
    );
  }
}
