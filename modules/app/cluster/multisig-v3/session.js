import crypto from 'crypto';
export async function establishSessionForKex(node, clusterId, attemptScopeId, attemptId, canonicalMembers, expectedPeerCount, roundTimeoutPrepare, roundTimeoutExchange, isCoordinator, recordClusterFailure) {
  const sessionSeed = `${clusterId}|${attemptId}|${canonicalMembers
    .map((a) => (a || '').toLowerCase())
    .join(',')}`;
  const sessionShort = crypto
    .createHash('sha256')
    .update(sessionSeed)
    .digest('hex')
    .slice(0, 12);
  console.log(`[Session] Using session ${sessionShort} for attempt ${attemptId}`);
  console.log('[INFO] PBFT Consensus: confirming session across cluster...');
  let sessionConsensus;
  try {
    sessionConsensus = await node.p2p.runConsensus(
      clusterId,
      attemptScopeId,
      `session-${attemptId}`,
      sessionShort,
      canonicalMembers,
      roundTimeoutPrepare,
      { requireAll: true },
    );
  } catch (e) {
    console.log('[ERROR] PBFT session consensus error:', e.message || String(e));
    recordClusterFailure('session_pbft_error');
    return null;
  }
  if (!sessionConsensus.success) {
    console.log(
      '[ERROR] PBFT session consensus failed - missing:',
      (sessionConsensus.missing || []).join(', '),
    );
    recordClusterFailure('session_pbft_failed');
    return null;
  }
  node._sessionId = `${attemptScopeId}:${sessionShort}`;
  try {
    if (node.p2p && typeof node.p2p.joinCluster === 'function') {
      await node.p2p.joinCluster(`${clusterId}:${node._sessionId}`, canonicalMembers, isCoordinator);
    }
  } catch (e) {
    console.log('[ERROR] Failed to join session-scoped topic:', e.message || String(e));
    recordClusterFailure('session_join_failed');
    return null;
  }
  try {
    if (node.p2p && typeof node.p2p.clearClusterRounds === 'function') {
      await node.p2p.clearClusterRounds(clusterId, node._sessionId, []);
    }
  } catch (e) {
    console.log('[WARN]  Failed to clear session round cache:', e.message || String(e));
  }
  console.log(`[Session] Using sessionId ${node._sessionId} for R3+`);
  try {
    await node.p2p.broadcastRoundData(clusterId, node._sessionId, 9701, sessionShort);
  } catch (e) {
    console.log('[WARN]  Session barrier broadcast error:', e.message || String(e));
  }
  const barrier = await node._waitForPeerPayloads(
    clusterId,
    node._sessionId,
    9701,
    canonicalMembers,
    expectedPeerCount,
    roundTimeoutExchange,
  );
  if (!barrier.complete) {
    const got = Array.isArray(barrier.payloads) ? barrier.payloads.length : 0;
    const missing = Array.isArray(barrier.missing) ? barrier.missing : [];
    if (missing.length) {
      console.log(
        `[ERROR] Session barrier: expected ${expectedPeerCount} acks, got ${got} missing=[${missing.join(', ')}]`,
      );
    } else {
      console.log(`[ERROR] Session barrier: expected ${expectedPeerCount} acks, got ${got}`);
    }
    recordClusterFailure('session_barrier_timeout');
    return null;
  }
  console.log('[OK] Session barrier complete');
  return sessionShort;
}
