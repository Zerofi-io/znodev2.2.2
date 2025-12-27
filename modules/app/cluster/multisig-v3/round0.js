export async function runRound0PrepareMultisigExchange(node, clusterId, attemptScopeId, canonicalMembers, expectedPeerCount, roundTimeoutPrepare, isCoordinator, recordClusterFailure) {
  let peers = null;
  console.log('\n' + '[INFO] Round 0: exchanging prepare_multisig info via P2P (broadcast)...');
  const maxBroadcastRetries = Number(process.env.ROUND0_BROADCAST_RETRIES || 2);
  let broadcastSuccess = false;
  for (let attempt = 0; attempt <= maxBroadcastRetries; attempt++) {
    try {
      await node.p2p.broadcastRoundData(clusterId, attemptScopeId, 0, node.multisigInfo);
      broadcastSuccess = true;
      break;
    } catch (e) {
      const msg = e.message || String(e);
      if (attempt < maxBroadcastRetries) {
        const delayMs = 1000 * (attempt + 1);
        console.log(`  [WARN] Round 0 broadcast attempt ${attempt + 1} failed: ${msg}; retrying in ${delayMs}ms`);
        await new Promise((r) => setTimeout(r, delayMs));
      } else {
        console.log(`  [ERROR] Round 0 broadcast failed after ${maxBroadcastRetries + 1} attempts: ${msg}`);
      }
    }
  }
  if (!broadcastSuccess) {
    console.log('[ERROR] Round 0: critical broadcast failure; aborting round');
    if (isCoordinator) {
      recordClusterFailure('round0_broadcast_failed');
    }
    return null;
  }
  const r0 = await node._waitForPeerPayloads(
    clusterId,
    attemptScopeId,
    0,
    canonicalMembers,
    expectedPeerCount,
    roundTimeoutPrepare,
  );
  peers = r0.payloads;
  if (!r0.complete) {
    const got = Array.isArray(peers) ? peers.length : 0;
    const missing = Array.isArray(r0.missing) ? r0.missing : [];
    if (missing.length) {
      console.log(
        `[ERROR] Round 0: expected ${expectedPeerCount} peer multisig infos, got ${got} missing=[${missing.join(', ')}]`,
      );
    } else {
      console.log(`[ERROR] Round 0: expected ${expectedPeerCount} peer multisig infos, got ${got}`);
    }
    if (isCoordinator) {
      recordClusterFailure('round0_peers_short');
    }
    return null;
  }
  const consensusHash0 = await node.p2p.computeConsensusHash(
    clusterId,
    attemptScopeId,
    0,
    canonicalMembers,
  );
  console.log(`Round 0 consensus hash: ${consensusHash0.substring(0, 18)}...`);
  console.log('[INFO] PBFT Consensus: confirming Round 0 complete...');
  let round0Consensus;
  try {
    round0Consensus = await node.p2p.runConsensus(
      clusterId,
      attemptScopeId,
      'round-0',
      consensusHash0,
      canonicalMembers,
      roundTimeoutPrepare,
      { requireAll: true },
    );
  } catch (e) {
    console.log(`[ERROR] PBFT Round 0 consensus error: ${e.message || String(e)}`);
    if (isCoordinator) {
      recordClusterFailure('round0_pbft_timeout');
    }
    return null;
  }
  if (!round0Consensus.success) {
    console.log(
      `[ERROR] PBFT Round 0 consensus failed - missing: ${(round0Consensus.missing || []).join(', ')}`,
    );
    if (isCoordinator) {
      recordClusterFailure('round0_pbft_timeout');
    }
    return null;
  }
  console.log('[OK] PBFT Round 0 consensus reached');
  node.stateMachine.transition('ROUND_0', { round: 0 }, 'round 0 consensus complete');
  return peers;
}
