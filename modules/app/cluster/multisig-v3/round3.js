export async function runRound3ConsensusAndEnterKeyExchange(node, clusterId, canonicalMembers, expectedPeerCount, roundTimeoutExchange, recordClusterFailure) {
  console.log('\n[INFO] Round 3: broadcasting key exchange payload via P2P...');
  await node.p2p.broadcastRoundData(clusterId, node._sessionId, 3, node._pendingR3);
  const r3 = await node._waitForPeerPayloads(
    clusterId,
    node._sessionId,
    3,
    canonicalMembers,
    expectedPeerCount,
    roundTimeoutExchange,
  );
  if (!r3.complete) {
    const got = Array.isArray(r3.payloads) ? r3.payloads.length : 0;
    const missing = Array.isArray(r3.missing) ? r3.missing : [];
    if (missing.length) {
      console.log(
        `[ERROR] Round 3: expected ${expectedPeerCount} peer payloads, got ${got} missing=[${missing.join(', ')}]`,
      );
    } else {
      console.log(`[ERROR] Round 3: expected ${expectedPeerCount} peer payloads, got ${got}`);
    }
    recordClusterFailure('round3_timeout');
    return false;
  }
  let consensusHash3 = '';
  try {
    consensusHash3 = await node.p2p.computeConsensusHash(
      clusterId,
      node._sessionId,
      3,
      canonicalMembers,
    );
  } catch (e) {
    console.log(`  [ERROR] Failed to compute Round 3 consensus hash: ${e.message || String(e)}`);
    recordClusterFailure('round3_hash_error');
    return false;
  }
  if (!consensusHash3) {
    console.log('[ERROR] Round 3: empty consensus hash');
    recordClusterFailure('round3_hash_empty');
    return false;
  }
  console.log(`Round 3 consensus hash: ${consensusHash3.substring(0, 18)}...`);
  console.log('[INFO] PBFT Consensus: confirming Round 3 complete...');
  let round3Consensus;
  try {
    round3Consensus = await node.p2p.runConsensus(
      clusterId,
      node._sessionId,
      'round-3',
      consensusHash3,
      canonicalMembers,
      roundTimeoutExchange,
      { requireAll: true },
    );
  } catch (e) {
    console.log(`[ERROR] PBFT Round 3 consensus error: ${e.message || String(e)}`);
    recordClusterFailure('round3_pbft_timeout');
    return false;
  }
  if (!round3Consensus.success) {
    console.log(
      `[ERROR] PBFT Round 3 consensus failed - missing: ${(round3Consensus.missing || []).join(', ')}`,
    );
    recordClusterFailure('round3_pbft_timeout');
    return false;
  }
  console.log('[OK] PBFT Round 3 consensus reached');
  node.stateMachine.transition('ROUND_3', { round: 3 }, 'round 3 consensus complete');
  node.stateMachine.transition(
    'KEY_EXCHANGE',
    { startingRound: 4 },
    'entering key exchange rounds',
  );
  return true;
}
