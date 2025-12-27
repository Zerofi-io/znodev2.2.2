import { backupMultisigWallet } from './wallet-backup.js';
export async function finalizeMoneroMultisigAndGetFinalAddress(node, clusterId, canonicalMembers, lastRound, lastPeerPayloads, projectRoot, recordClusterFailure) {
  console.log('[INFO] PBFT Consensus: confirming all nodes ready to finalize...');
  node.stateMachine.transition('FINALIZING_MONERO', {}, 'finalizing monero multisig');
  let finalizeConsensus;
  try {
    finalizeConsensus = await node.p2p.runConsensus(
      clusterId,
      node._sessionId,
      'finalize',
      `ready-${lastRound}`,
      canonicalMembers,
      120000,
      { requireAll: true },
    );
  } catch (e) {
    console.log(`[ERROR] PBFT finalize consensus error: ${e.message || String(e)}`);
    recordClusterFailure('finalize_pbft_timeout');
    return null;
  }
  if (!finalizeConsensus.success) {
    console.log(
      `[ERROR] PBFT finalize consensus failed - missing: ${(finalizeConsensus.missing || []).join(', ')}`,
    );
    recordClusterFailure('finalize_pbft_timeout');
    return null;
  }
  console.log('[OK] PBFT finalize consensus reached');
  console.log('  [INFO] Finalizing multisig with peer keys...');
  let finalizeErr = null;
  try {
    await node.monero.finalizeMultisig(lastPeerPayloads);
  } catch (e1) {
    finalizeErr = e1;
    try {
      await node.monero.finalizeMultisig();
    } catch (e2) {
      finalizeErr = e2;
    }
  }
  const finalizeRetries = Number(process.env.FINALIZE_READY_RETRIES || 10);
  const finalizeDelayMs = Number(process.env.FINALIZE_READY_DELAY_MS || 5000);
  let info;
  let retries = finalizeRetries;
  while (retries > 0) {
    try {
      info = await node.monero.call('is_multisig');
      if (info && info.ready) {
        break;
      }
      if (retries > 1) {
        console.log(
          `  [INFO] Multisig not ready yet, waiting ${finalizeDelayMs}ms before retry (${retries - 1} retries left)...`,
        );
        await new Promise((resolve) => setTimeout(resolve, finalizeDelayMs));
      }
    } catch (e) {
      console.log('  [WARN]  is_multisig check failed:', e.message || String(e));
      if (retries > 1) {
        await new Promise((resolve) => setTimeout(resolve, finalizeDelayMs));
      }
    }
    retries--;
  }
  if (finalizeErr && info && info.ready) {
    const msg = finalizeErr.message || String(finalizeErr);
    const code = finalizeErr.code !== undefined ? ` code=${finalizeErr.code}` : '';
    let data = '';
    if (finalizeErr.data !== undefined) {
      try {
        data = ` data=${JSON.stringify(finalizeErr.data)}`;
      } catch {
        data = ' data=[unstringifiable]';
      }
    }
    console.log(`  [INFO] Finalize multisig non-fatal error (wallet ready):${code}${data} ${msg}`);
  }
  if (!info || !info.ready) {
    if (finalizeErr) {
      const msg = finalizeErr.message || String(finalizeErr);
      const code = finalizeErr.code !== undefined ? ` code=${finalizeErr.code}` : '';
      let data = '';
      if (finalizeErr.data !== undefined) {
        try {
          data = ` data=${JSON.stringify(finalizeErr.data)}`;
        } catch {
          data = ' data=[unstringifiable]';
        }
      }
      console.log(`  [ERROR] finalize_multisig error:${code}${data} ${msg}`);
    }
    console.log('[ERROR] Multisig still not ready after finalize and retries');
    recordClusterFailure('final_multisig_not_ready');
    return null;
  }
  const getAddrResult = await node.monero.call('get_address');
  const finalAddr = getAddrResult.address;
  await backupMultisigWallet(projectRoot);
  console.log(`\n\n[OK] Final multisig address: ${finalAddr}`);
  try {
    const currentWallet =
      node.monero && node.monero._lastWallet && node.monero._lastWallet.filename
        ? node.monero._lastWallet.filename
        : node.baseWalletName;
    node.clusterWalletName = currentWallet;
    console.log('[Cluster] Active cluster wallet file:', node.clusterWalletName || '(unknown)');
  } catch {}
  node._clusterFinalAddress = finalAddr;
  if (node.clusterState && typeof node.clusterState.update === 'function') {
    node.clusterState.update({ finalAddress: finalAddr, finalizationStartAt: Date.now() });
  }
  return finalAddr;
}
