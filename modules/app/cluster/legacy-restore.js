import fs from 'fs';
export async function tryRestoreLegacyCluster(clusterId, members, isCoordinator, recordClusterFailure) {
  let snapshot = null;
  try {
    const restorePath = this._getClusterRestorePath();
    if (!fs.existsSync(restorePath)) {
      return false;
    }
    snapshot = JSON.parse(fs.readFileSync(restorePath, 'utf8'));
  } catch (e) {
    console.log('[Cluster] Failed to load legacy restore snapshot:', e.message || String(e));
    return false;
  }
  if (!snapshot || !Array.isArray(snapshot.members) || snapshot.members.length === 0 || !snapshot.finalAddress || !snapshot.walletName) {
    return false;
  }
  const legacyMembersLower = snapshot.members.map((a) => String(a || '').toLowerCase()).sort();
  const currentMembersLower = members.map((a) => String(a || '').toLowerCase()).sort();
  if (legacyMembersLower.length !== currentMembersLower.length) {
    return false;
  }
  for (let i = 0; i < legacyMembersLower.length; i += 1) {
    if (legacyMembersLower[i] !== currentMembersLower[i]) {
      return false;
    }
  }
  try {
    await this.monero.openWallet(snapshot.walletName, this.moneroPassword);
    const addr = await this.monero.getAddress();
    if (!addr || addr !== snapshot.finalAddress) {
      return false;
    }
  } catch (e) {
    console.log('[Cluster] Failed to open legacy multisig wallet:', e.message || String(e));
    return false;
  }
  this.clusterWalletName = snapshot.walletName;
  if (this.clusterState && typeof this.clusterState.update === 'function') {
    this.clusterState.update({
      clusterId,
      members,
      finalAddress: snapshot.finalAddress,
      finalized: false,
    });
  }
  const finalAddr = snapshot.finalAddress;
  const DRY_RUN = process.env.DRY_RUN === '1';
  if (isCoordinator) {
    const jitterMs = Math.floor(Math.random() * 2000);
    if (jitterMs > 0) {
      console.log(`  [INFO] Adding ${jitterMs}ms jitter to reduce coordinator collision`);
      await new Promise((resolve) => setTimeout(resolve, jitterMs));
    }
    const maxRetries = 3;
    let attempt = 0;
    let finalized = false;
    let finalizeNonce = null;
    while (attempt < maxRetries && !finalized) {
      attempt += 1;
      try {
        const alreadyFinalized = await this.isClusterFinalized(clusterId);
        if (alreadyFinalized) {
          console.log('[OK] Cluster already finalized on-chain (by another coordinator)');
          finalized = true;
          this._onClusterFinalized(clusterId, members, finalAddr);
          break;
        }
        console.log(
          `  [INFO] Attempt ${attempt}/${maxRetries}: finalizeCluster([${members.length} members, canonical order], ${finalAddr})`,
        );
        if (DRY_RUN) {
          console.log('  [DRY_RUN] Would send finalizeCluster transaction');
          finalized = true;
        } else {
          const txOptions = { operation: 'finalizeCluster', retryWithSameNonce: true };
          if (finalizeNonce != null) {
            txOptions.nonce = finalizeNonce;
          }
          const tx = await this.txManager.sendTransaction(
            (overrides) => this.registry.finalizeCluster(members, finalAddr, overrides),
            txOptions,
          );
          if (finalizeNonce == null && tx.nonce != null) {
            finalizeNonce = tx.nonce;
          }
          await tx.wait();
          console.log('[OK] Cluster finalized on-chain (v3)');
          finalized = true;
          if (this.p2p) {
            try {
              await this.p2p.broadcastRoundData(
                clusterId,
                null,
                9998,
                JSON.stringify({
                  type: 'cluster-finalized',
                  clusterId,
                  moneroAddress: finalAddr,
                  members,
                  timestamp: Date.now(),
                }),
              );
              console.log('[Coordinator] Broadcast finalization to cluster members');
            } catch (e) {
              console.log('[Coordinator] Failed to broadcast finalization:', e.message);
            }
          }
          this._onClusterFinalized(clusterId, members, finalAddr);
        }
      } catch (e) {
        const msg = e && e.message ? e.message : String(e);
        if (/already finalized|already exists|cluster.*finalized/i.test(msg)) {
          console.log('[OK] Cluster already finalized on-chain (race condition handled)');
          finalized = true;
          this._onClusterFinalized(clusterId, members, finalAddr);
          break;
        }
        if (/nonce.*too low|replacement.*underpriced|already known/i.test(msg)) {
          console.log(
            `  [WARN]  Nonce/replacement error (attempt ${attempt}/${maxRetries}): ${msg}`,
          );
          if (attempt < maxRetries) {
            const backoffMs = 1000 * Math.pow(2, attempt - 1);
            console.log(`  [INFO] Waiting ${backoffMs}ms before retry...`);
            await new Promise((resolve) => setTimeout(resolve, backoffMs));
            continue;
          }
        }
        console.log(
          `  [WARN]  finalizeCluster error (attempt ${attempt}/${maxRetries}): ${msg}`,
        );
        const nowFinalized = await this.isClusterFinalized(clusterId);
        if (nowFinalized) {
          console.log('[OK] Cluster finalized despite error (transaction succeeded)');
          finalized = true;
          this._onClusterFinalized(clusterId, members, finalAddr);
          break;
        }
        if (attempt >= maxRetries) {
          console.log('[ERROR] finalizeCluster() failed after all retries');
          recordClusterFailure('finalize_restore_failed');
          return false;
        }
        const backoffMs = 1000 * Math.pow(2, attempt - 1);
        console.log(`  [INFO] Waiting ${backoffMs}ms before retry...`);
        await new Promise((resolve) => setTimeout(resolve, backoffMs));
      }
    }
    if (!finalized) {
      recordClusterFailure('finalize_restore_failed');
      return false;
    }
    return true;
  }
  console.log('[INFO] Waiting for coordinator to finalize restored cluster on-chain...');
  try {
    await this._waitForOnChainFinalizationForSelf(finalAddr, members);
  } catch (e) {
    console.log(
      '[Cluster] Error while waiting for on-chain finalization:',
      e.message || String(e),
    );
    recordClusterFailure('finalize_restore_wait_error');
    return false;
  }
  return true;
}
export default {
  tryRestoreLegacyCluster,
};
