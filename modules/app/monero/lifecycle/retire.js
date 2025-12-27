import fs from 'fs';
import os from 'os';
import path from 'path';
import { getMoneroWalletDir } from '../wallet-dir.js';
export async function retireClusterWalletAndBackups(node, reason) {
  node._clearClusterState();
  if (!node._clusterFinalAddress) {
    return;
  }
  const tag = (reason || 'unknown').toString().slice(0, 120);
  const walletDir = getMoneroWalletDir();
  const backupDir =
    process.env.WALLET_BACKUP_DIR || path.join(os.homedir(), '.znode-backup', 'wallets');
  const walletFile = node.clusterWalletName || node.baseWalletName;
  if (!walletFile) {
    console.log('[Cluster] No cluster wallet file recorded; skipping retirement.');
    return;
  }
  console.log(`[INFO] [Cluster] Retiring cluster wallet and backups due to cluster death: ${tag}`);
  await node._walletMutex.withLock(async () => {
    try {
      try {
        if (
          node.monero &&
          node.monero._lastWallet &&
          node.monero._lastWallet.filename === walletFile
        ) {
          await node.monero.closeWallet();
        }
      } catch {}
      try {
        await fs.promises.unlink(path.join(walletDir, walletFile));
        console.log(`[Cluster] Deleted cluster wallet file: ${walletFile}`);
      } catch {}
      try {
        await fs.promises.unlink(path.join(walletDir, `${walletFile}.keys`));
        console.log(`[Cluster] Deleted cluster wallet keys file: ${walletFile}.keys`);
      } catch {}
      try {
        const entries = await fs.promises.readdir(backupDir, {
          withFileTypes: true,
        });
        for (const entry of entries) {
          const isFile = typeof entry.isFile === 'function' ? entry.isFile() : entry.isFile;
          if (!isFile) continue;
          const name = entry.name || '';
          if (!name.startsWith(walletFile)) continue;
          try {
            await fs.promises.unlink(path.join(backupDir, name));
            console.log(`[Cluster] Deleted cluster backup file: ${name}`);
          } catch (e) {
            console.log(
              `[Cluster] Failed to delete cluster backup file ${name}:`,
              e.message || String(e),
            );
          }
        }
      } catch (e) {
        console.log(
          `[Cluster] Warning: could not scan backup dir ${backupDir}:`,
          e.message || String(e),
        );
      }
    } catch (e) {
      console.log('[Cluster] Error while retiring cluster wallet/backups:', e.message || String(e));
    }
  });
}
