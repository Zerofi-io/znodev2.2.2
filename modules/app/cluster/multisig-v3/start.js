import { canonicalizeMembers } from '../../../cluster/round-utils.js';
import { joinMultisigAttemptCluster } from './attempt-cluster.js';
import { confirmMoneroConfigConsensus } from './monero-config.js';
import { finalizeMoneroMultisigAndGetFinalAddress } from './finalize-monero.js';
import { finalizeClusterOnChainOrWait } from './onchain-finalize.js';
import { createClusterFailureRecorder } from './failure-recorder.js';
import { runRound0PrepareMultisigExchange } from './round0.js';
import { establishSessionForKex } from './session.js';
import { runRound3ConsensusAndEnterKeyExchange } from './round3.js';
export async function startClusterMultisigV3(clusterId, members, isCoordinator, threshold = 7, attempt = 1, expectedCoordinator = null, projectRoot) {
  const __dirname = projectRoot;
  this.stateMachine.transition(
    'DISCOVERING',
    { clusterId, memberCount: members.length },
    'cluster formation started',
  );
  if (isCoordinator) {
    this._startCoordinatorHeartbeat(clusterId, expectedCoordinator);
  } else {
    this._startCoordinatorMonitor(expectedCoordinator);
  }
  const attemptIdParsed = Number(attempt);
  const attemptId =
    Number.isFinite(attemptIdParsed) && attemptIdParsed > 0 ? Math.floor(attemptIdParsed) : 1;
  const attemptScopeId = `msig-${attemptId}`;
  this._sessionId = null;
  this._attemptScopeId = attemptScopeId;
  const recordClusterFailure = createClusterFailureRecorder(this, clusterId);
  const roundTimeoutPrepare = Number(process.env.ROUND_TIMEOUT_PREPARE_MS || 600000);
  const roundTimeoutExchange = Number(process.env.ROUND_TIMEOUT_EXCHANGE_MS || 180000);
  try {
    const canonicalMembers = canonicalizeMembers(members);
    if (members.some((a, i) => canonicalMembers[i] !== a)) {
      console.log('[Multisig] Canonicalized member order for key exchange rounds.');
    }
    if (!this.p2p || !this.p2p.node) {
      console.log('[WARN] P2P not available - cannot perform multisig coordination');
      return false;
    }
    const restored = await this._tryRestoreLegacyCluster(
      clusterId,
      canonicalMembers,
      isCoordinator,
      recordClusterFailure,
    );
    if (restored) {
      return true;
    }
    console.log(`[Multisig] Using attempt ${attemptId} (${attemptScopeId})`);
    const attemptJoined = await joinMultisigAttemptCluster(
      this,
      clusterId,
      attemptScopeId,
      canonicalMembers,
      isCoordinator,
      recordClusterFailure,
    );
    if (!attemptJoined) {
      return false;
    }
    const moneroConfigOk = await confirmMoneroConfigConsensus(
      this,
      clusterId,
      attemptScopeId,
      canonicalMembers,
      threshold,
      roundTimeoutPrepare,
      recordClusterFailure,
    );
    if (!moneroConfigOk) {
      return false;
    }
    const expectedPeerCount = members.length - 1;
    const peers = await runRound0PrepareMultisigExchange(
      this,
      clusterId,
      attemptScopeId,
      canonicalMembers,
      expectedPeerCount,
      roundTimeoutPrepare,
      isCoordinator,
      recordClusterFailure,
    );
    if (!peers) {
      return false;
    }
    const sessionShort = await establishSessionForKex(
      this,
      clusterId,
      attemptScopeId,
      attemptId,
      canonicalMembers,
      expectedPeerCount,
      roundTimeoutPrepare,
      roundTimeoutExchange,
      isCoordinator,
      recordClusterFailure,
    );
    if (!sessionShort) {
      return false;
    }
    console.log(
      `Using wallet for multisig attempt: ${this.currentAttemptWallet || this.baseWalletName}`,
    );
    this.stateMachine.transition('MAKE_MULTISIG', {}, 'making multisig wallet');
    const res = await this.makeMultisig(peers, threshold);
    try {
      const currentWallet =
        this.monero && this.monero._lastWallet && this.monero._lastWallet.filename
          ? this.monero._lastWallet.filename
          : this.baseWalletName;
      await this._enableMoneroMultisigExperimentalForWallet(
        currentWallet,
        `multisig wallet ${currentWallet}`,
      );
    } catch (e) {
      console.log(
        '  [WARN] Failed to ensure multisig experimental mode after make_multisig:',
        e.message || String(e),
      );
    }
    if (res && (res.multisig_info || res.multisigInfo)) {
      if (this.clusterState && typeof this.clusterState.setPendingR3 === 'function') {
        this.clusterState.setPendingR3(res.multisig_info || res.multisigInfo);
      }
    } else if (this.clusterState && typeof this.clusterState.setPendingR3 === 'function') {
      this.clusterState.setPendingR3('');
    }
    if (!this._pendingR3 || this._pendingR3.length === 0) {
      console.log('[ERROR] No Round 3 payload available after make_multisig');
      return false;
    }
    const round3Ok = await runRound3ConsensusAndEnterKeyExchange(
      this,
      clusterId,
      canonicalMembers,
      expectedPeerCount,
      roundTimeoutExchange,
      recordClusterFailure,
    );
    if (!round3Ok) {
      return false;
    }
    const { success, lastRound, lastPeerPayloads } = await this.runKeyExchangeRounds(
      clusterId,
      canonicalMembers,
      4,
    );
    if (!success) {
      console.log(`[ERROR] Key exchange failed at round ${lastRound}`);
      recordClusterFailure('key_exchange_failed');
      return false;
    }
    const finalAddr = await finalizeMoneroMultisigAndGetFinalAddress(
      this,
      clusterId,
      canonicalMembers,
      lastRound,
      lastPeerPayloads,
      __dirname,
      recordClusterFailure,
    );
    if (!finalAddr) {
      return false;
    }
    const onchainOk = await finalizeClusterOnChainOrWait(
      this,
      clusterId,
      canonicalMembers,
      finalAddr,
      members.length,
      isCoordinator,
      attempt,
    );
    if (!onchainOk) {
      return false;
    }
    return true;
  } catch (e) {
    console.log('[ERROR] Cluster multisig v3 error:', e.message || String(e));
    recordClusterFailure('exception');
    if (isCoordinator && this.p2p && typeof this.p2p.leaveCluster === 'function') {
      try {
        await this.p2p.leaveCluster(clusterId);
        this.p2p.cleanupOldMessages();
      } catch (cleanupErr) {
        console.log('[WARN]  P2P cleanup error:', cleanupErr.message);
      }
    }
    return false;
  }
}
