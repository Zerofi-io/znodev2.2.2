import { DRY_RUN } from '../../runtime.js';

function truncateHex(value, maxLen = 66) {
  if (typeof value !== 'string') return value;
  if (!value.startsWith('0x')) return value;
  if (value.length <= maxLen) return value;
  return `${value.slice(0, maxLen)}…(${value.length} chars)`;
}

function safeEthersError(e) {
  const tx = e && e.transaction && typeof e.transaction === 'object' ? e.transaction : null;
  const info = e && e.info && typeof e.info === 'object' ? e.info : null;
  const rpcErr = info && info.error && typeof info.error === 'object' ? info.error : null;

  const out = {
    name: e?.name,
    code: e?.code,
    action: e?.action,
    shortMessage: e?.shortMessage,
    reason: e?.reason,
    data: truncateHex(e?.data),
  };

  if (tx) {
    out.transaction = {
      from: tx.from,
      to: tx.to,
      dataPrefix: typeof tx.data === 'string' ? tx.data.slice(0, 10) : undefined,
      dataLen: typeof tx.data === 'string' ? tx.data.length : undefined,
    };
  }

  if (rpcErr) {
    out.rpcError = {
      code: rpcErr.code,
      message: rpcErr.message,
      data: truncateHex(rpcErr.data),
    };
  }

  if (info) {
    if (info.status != null) {
      out.httpStatus = info.status;
    }
    if (info.payload && typeof info.payload === 'object') {
      out.rpcMethod = info.payload.method;
    }
  }

  return out;
}

function logEthersError(prefix, e) {
  try {
    console.log(prefix, safeEthersError(e));
  } catch (logErr) {
    try {
      const msg = e && e.message ? e.message : String(e);
      console.log(prefix, msg);
    } catch {}
  }
}

export async function finalizeClusterOnChainOrWait(
  node,
  clusterId,
  canonicalMembers,
  finalAddr,
  memberCount,
  isCoordinator,
  attempt,
) {
  if (isCoordinator) {
    const jitterMs = Math.floor(Math.random() * 2000);
    if (jitterMs > 0) {
      console.log(`  [INFO] Adding ${jitterMs}ms jitter to reduce coordinator collision`);
      await new Promise((resolve) => setTimeout(resolve, jitterMs));
    }
    const maxRetries = 3;
    let finalizeAttempt = 0;
    let finalized = false;
    let finalizeNonce = null;
    while (finalizeAttempt < maxRetries && !finalized) {
      finalizeAttempt++;
      try {
        const alreadyFinalized = await node.isClusterFinalized(clusterId);
        if (alreadyFinalized) {
          console.log('[OK] Cluster already finalized on-chain (by another coordinator)');
          finalized = true;
          node._onClusterFinalized(clusterId, canonicalMembers, finalAddr);
          break;
        }
        console.log(
          `  [INFO] Attempt ${finalizeAttempt}/${maxRetries}: finalizeCluster([${memberCount} members, canonical order], ${finalAddr})`,
        );
        if (DRY_RUN) {
          console.log('  [DRY_RUN] Would send finalizeCluster transaction');
          finalized = true;
        } else {
          const txOptions = { operation: 'finalizeCluster', retryWithSameNonce: true };
          if (finalizeNonce != null) {
            txOptions.nonce = finalizeNonce;
          }
          const tx = await node.txManager.sendTransaction(
            (overrides) => node.registry.finalizeCluster(canonicalMembers, finalAddr, overrides),
            txOptions,
          );
          if (finalizeNonce == null && tx.nonce != null) {
            finalizeNonce = tx.nonce;
          }
          await tx.wait();
          console.log('[OK] Cluster finalized on-chain (v3)');
          finalized = true;
          if (node.p2p) {
            try {
              await node.p2p.broadcastRoundData(
                clusterId,
                null,
                9998,
                JSON.stringify({
                  type: 'cluster-finalized',
                  clusterId: clusterId,
                  moneroAddress: finalAddr,
                  members: canonicalMembers,
                  timestamp: Date.now(),
                }),
              );
              console.log('[Coordinator] Broadcast finalization to cluster members');
            } catch (e) {
              console.log('[Coordinator] Failed to broadcast finalization:', e.message);
            }
          }
          node._onClusterFinalized(clusterId, canonicalMembers, finalAddr);
        }
      } catch (e) {
        const msg = e && e.message ? e.message : String(e);
        if (/already finalized|already exists|cluster.*finalized/i.test(msg)) {
          console.log('[OK] Cluster already finalized on-chain (race condition handled)');
          finalized = true;
          node._onClusterFinalized(clusterId, canonicalMembers, finalAddr);
          break;
        }

        if (/nonce.*too low|replacement.*underpriced|already known/i.test(msg)) {
          console.log(
            `  [WARN]  Nonce/replacement error (attempt ${finalizeAttempt}/${maxRetries}): ${msg}`,
          );
          logEthersError('  [WARN]  Nonce/replacement error details:', e);
          if (finalizeAttempt < maxRetries) {
            const backoffMs = 1000 * Math.pow(2, finalizeAttempt - 1);
            console.log(`  [INFO] Waiting ${backoffMs}ms before retry...`);
            await new Promise((resolve) => setTimeout(resolve, backoffMs));
            continue;
          }
        }

        console.log(
          `  [WARN]  finalizeCluster error (attempt ${finalizeAttempt}/${maxRetries}): ${msg}`,
        );
        logEthersError('  [WARN]  finalizeCluster error details:', e);

        // Extra diagnostics: try a staticCall to see if we can recover a revert reason
        // when estimateGas fails without data.
        try {
          if (node?.registry?.finalizeCluster?.staticCall) {
            await node.registry.finalizeCluster.staticCall(canonicalMembers, finalAddr, {
              from: node?.wallet?.address,
            });
            console.log('  [INFO] finalizeCluster.staticCall succeeded (callable at current chain state)');
          }
        } catch (callErr) {
          const callMsg = callErr && callErr.message ? callErr.message : String(callErr);
          console.log(`  [WARN]  finalizeCluster.staticCall failed: ${callMsg}`);
          logEthersError('  [WARN]  finalizeCluster.staticCall error details:', callErr);
        }

        let nowFinalized = false;
        try {
          nowFinalized = await node.isClusterFinalized(clusterId);
        } catch (finalizedErr) {
          const fmsg = finalizedErr && finalizedErr.message ? finalizedErr.message : String(finalizedErr);
          console.log(`  [WARN]  Failed to re-check cluster finalized state: ${fmsg}`);
          logEthersError('  [WARN]  isClusterFinalized error details:', finalizedErr);
        }

        if (nowFinalized) {
          console.log('[OK] Cluster finalized despite error (transaction succeeded)');
          finalized = true;
          node._onClusterFinalized(clusterId, canonicalMembers, finalAddr);
          break;
        }
        if (finalizeAttempt >= maxRetries) {
          console.log('[ERROR] finalizeCluster() failed after all retries');
          return false;
        }
        const backoffMs = 1000 * Math.pow(2, finalizeAttempt - 1);
        console.log(`  [INFO] Waiting ${backoffMs}ms before retry...`);
        await new Promise((resolve) => setTimeout(resolve, backoffMs));
      }
    }
  } else {
    console.log('[INFO] Waiting for coordinator to finalize cluster on-chain...');
    try {
      await node._waitForOnChainFinalizationForSelf(finalAddr, canonicalMembers);
    } catch (e) {
      console.log('[Cluster] Error while waiting for on-chain finalization:', e.message || String(e));
    }
  }
  return true;
}
