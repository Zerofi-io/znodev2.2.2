import { fileURLToPath } from 'url';
import { dirname } from 'path';
import path from 'path';
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.resolve(dirname(__filename), '..', '..');
import { DRY_RUN } from './runtime.js';
import { updateEnv } from './env/update.js';
import { maybeApplyMoneroHealthOverrideCommand } from './monero/health-override.js';
import { logClusterHealth as logClusterHealthSnapshot } from './cluster/health-log.js';
import { withRetryForNode } from './cluster/retry.js';
import { onTxManagerPendingChange } from './tx-manager/pending-monitor.js';
import { initZNode } from './znode-init.js';
import { initContracts } from './contracts/init.js';
import { runStartupSequence } from './bootstrap/startup.js';
import { checkRequirements, ensureRegistered } from './bootstrap/onchain.js';
import { stopZNode } from './bootstrap/shutdown.js';
import {
  ensureMoneroErrorCounts,
  noteMoneroKexRoundMismatch,
  noteMoneroKexTimeout,
  noteMoneroRpcFailure,
  resetMoneroHealth,
} from './monero/health.js';
import {
  autoEnableMoneroMultisigExperimental,
  computeAttemptWalletName,
  deleteWalletFiles,
  enableMoneroMultisigExperimentalForWallet,
  hardResetMoneroWalletState,
  maybeAutoResetMoneroOnStartup,
  postAttemptMoneroCleanup,
  preflightMoneroForClusterAttempt,
  retireClusterWalletAndBackups,
  rotateBaseWalletAfterKexError,
  setupMonero,
  verifyWalletFilesExist,
} from './monero/lifecycle.js';
import { makeMultisig, prepareMultisig } from './monero/multisig.js';
import { ensureBridgeSessionCluster, initClusterP2P, startP2P, waitForPeerPayloads } from './p2p/cluster.js';
import { handleCoordinatorHeartbeat, startCoordinatorHeartbeat, startCoordinatorMonitor, stopCoordinatorHeartbeat, stopCoordinatorMonitor } from './cluster/coordinator-heartbeat.js';
import {
  clearClusterState,
  getClusterRestorePath,
  getClusterStatePath,
  loadClusterBlacklist,
  saveClusterBlacklist,
  saveClusterRestoreSnapshot,
  saveClusterState,
} from './cluster/persistence.js';
import { recoverClusterFromChain, verifyAndRestoreCluster, waitForOnChainFinalizationForSelf } from './cluster/restore.js';
import { submitRoundExchangeInfo, waitForOnChainRoundBarrier } from './cluster/round-barrier.js';
import { runKeyExchangeRounds, startClusterMultisigV3 } from './cluster/multisig-v3.js';
import { tryRestoreLegacyCluster } from './cluster/legacy-restore.js';
import { cleanupClusterAttempt } from './cluster/attempt-cleanup.js';
import { onClusterFinalized } from './cluster/finalization.js';
import { startHeartbeatLoop, startHeartbeatProofLoop, startSlashingLoop, monitorNetwork } from '../core/monitoring.js';
import {
  startBridgeAPI,
  stopBridgeAPI,
  startDepositMonitor,
  stopDepositMonitor,
  startWithdrawalMonitor,
  stopWithdrawalMonitor,
  getMintSignature,
  getAllPendingSignatures,
  registerDepositRequest,
  getWithdrawalStatus,
  getAllPendingWithdrawals,
} from '../bridge/api.js';
import { checkSelfStakeHealth, checkEmergencySweep, dissolveStuckFinalizedCluster } from '../core/health.js';
class ZNode {
  constructor() {
    initZNode.call(this, __dirname);
  }
  async _loadClusterBlacklist() {
    return loadClusterBlacklist(this);
  }
  _saveClusterBlacklist() {
    return saveClusterBlacklist(this);
  }
  _getClusterStatePath() {
    return getClusterStatePath(__dirname);
  }
  _getClusterRestorePath() {
    return getClusterRestorePath(__dirname);
  }
  _saveClusterState() {
    return saveClusterState(this, __dirname);
  }
  _saveClusterRestoreSnapshot(state) {
    return saveClusterRestoreSnapshot(this, __dirname, state);
  }
  _clearClusterState() {
    return clearClusterState(this, __dirname);
  }
  async _verifyAndRestoreCluster() {
    return verifyAndRestoreCluster(this);
  }
  async _recoverClusterFromChain() {
    return recoverClusterFromChain(this);
  }
  async _waitForOnChainFinalizationForSelf(expectedMoneroAddress, fallbackMembers) {
    return waitForOnChainFinalizationForSelf(this, expectedMoneroAddress, fallbackMembers);
  }
  _onClusterFinalized(clusterId, members, finalAddress) {
    return onClusterFinalized.call(this, clusterId, members, finalAddress);
  }
  async _withRetry(fn, options = {}) {
    return withRetryForNode(this, fn, options);
  }
  async _submitRoundExchangeInfo(clusterId, round, clusterNodes, payloads) {
    return submitRoundExchangeInfo(this, clusterId, round, clusterNodes, payloads, DRY_RUN);
  }
  async _waitForOnChainRoundBarrier(clusterId, round, clusterNodes, timeoutMs) {
    return waitForOnChainRoundBarrier(this, clusterId, round, clusterNodes, timeoutMs, DRY_RUN);
  }
  async _cleanupClusterAttempt(clusterId) {
    return cleanupClusterAttempt.call(this, clusterId);
  }
  _onTxManagerPendingChange(pendingSize) {
    return onTxManagerPendingChange(this, pendingSize);
  }
  async start() {
    this._p2pRecoveryDisabled = false;
    console.log('\n[ZNode] Starting zNode - Monero Bridge');
    console.log(`Address: ${this.wallet.address}`);
    await this._loadClusterBlacklist();
    await initContracts(this);
    try {
      await runStartupSequence(this);
    } catch (e) {
      console.error('\n[ERROR] Startup failed:', e.message || String(e));
      console.error('Cleaning up...');
      await this.stop();
      throw e;
    }
  }
  async stop() {
    return stopZNode.call(this);
  }
  async checkRequirements() {
    return checkRequirements(this);
  }
  async _checkSelfStakeHealth() {
    return checkSelfStakeHealth(this);
  }
  async setupMonero() {
    return setupMonero(this, __dirname);
  }
  async _enableMoneroMultisigExperimentalForWallet(walletFile, label) {
    return enableMoneroMultisigExperimentalForWallet(this, walletFile, label);
  }
  _resetMoneroHealth() {
    return resetMoneroHealth(this);
  }
  _ensureMoneroErrorCounts() {
    return ensureMoneroErrorCounts(this);
  }
  _noteMoneroKexRoundMismatch(round, msg) {
    return noteMoneroKexRoundMismatch(this, round, msg);
  }
  _noteMoneroRpcFailure(kind, msg) {
    return noteMoneroRpcFailure(this, kind, msg);
  }
  _noteMoneroKexTimeout(round) {
    return noteMoneroKexTimeout(this, round);
  }
  _computeAttemptWalletName(clusterId, attempt) {
    return computeAttemptWalletName(this, clusterId, attempt);
  }
  async _deleteWalletFiles(walletFile) {
    return deleteWalletFiles(this, walletFile);
  }
  _verifyWalletFilesExist(walletFile) {
    return verifyWalletFilesExist(this, walletFile);
  }
  async _preflightMoneroForClusterAttempt(clusterId, attempt) {
    return preflightMoneroForClusterAttempt(this, clusterId, attempt);
  }
  async _postAttemptMoneroCleanup(clusterId, attempt, success) {
    return postAttemptMoneroCleanup(this, clusterId, attempt, success);
  }
  async _rotateBaseWalletAfterKexError(reason) {
    return rotateBaseWalletAfterKexError(this, reason);
  }
  async _hardResetMoneroWalletState(reason) {
    return hardResetMoneroWalletState(this, reason);
  }
  async _maybeAutoResetMoneroOnStartup() {
    return maybeAutoResetMoneroOnStartup(this);
  }
  async _retireClusterWalletAndBackups(reason) {
    return retireClusterWalletAndBackups(this, reason);
  }
  async _autoEnableMoneroMultisigExperimental() {
    return autoEnableMoneroMultisigExperimental(this);
  }
  async startP2P() {
    return startP2P(this);
  }
  async _ensureBridgeSessionCluster() {
    return ensureBridgeSessionCluster(this);
  }
  async initClusterP2P(clusterId, clusterNodes, isCoordinator = false) {
    return initClusterP2P(this, clusterId, clusterNodes, isCoordinator);
  }
  async _waitForPeerPayloads(clusterId, sessionId, round, members, expectedCount, timeoutMs) {
    return waitForPeerPayloads(this, clusterId, sessionId, round, members, expectedCount, timeoutMs);
  }
  async prepareMultisig() {
    return prepareMultisig(this, __dirname);
  }
  async makeMultisig(multisigInfos, threshold) {
    return makeMultisig(this, multisigInfos, threshold);
  }
  _startCoordinatorHeartbeat(clusterId, expectedCoordinator) {
    return startCoordinatorHeartbeat(this, clusterId, expectedCoordinator);
  }
  _stopCoordinatorHeartbeat() {
    return stopCoordinatorHeartbeat(this);
  }
  _startCoordinatorMonitor(expectedCoordinator) {
    return startCoordinatorMonitor(this, expectedCoordinator);
  }
  _stopCoordinatorMonitor() {
    return stopCoordinatorMonitor(this);
  }
  _handleCoordinatorHeartbeat(data) {
    return handleCoordinatorHeartbeat(this, data);
  }
  async _tryRestoreLegacyCluster(clusterId, members, isCoordinator, recordClusterFailure) {
    return tryRestoreLegacyCluster.call(this, clusterId, members, isCoordinator, recordClusterFailure);
  }
  async startClusterMultisigV3(clusterId, members, isCoordinator, threshold = 7, attempt = 1, expectedCoordinator = null) {
    return startClusterMultisigV3.call(
      this,
      clusterId,
      members,
      isCoordinator,
      threshold,
      attempt,
      expectedCoordinator,
      __dirname,
    );
  }
  async runKeyExchangeRounds(clusterId, clusterNodes, startRound = 4) {
    return runKeyExchangeRounds.call(this, clusterId, clusterNodes, startRound);
  }
  async ensureRegistered() {
    return ensureRegistered(this);
  }
  async isClusterFinalized(clusterId) {
    try {
      const [, , finalized] = await this.registry.clusters(clusterId);
      return !!finalized;
    } catch {
      return false;
    }
  }
  startHeartbeatLoop() {
    return startHeartbeatLoop(this, DRY_RUN);
  }
  startHeartbeatProofLoop() {
    return startHeartbeatProofLoop(this, DRY_RUN);
  }
  startSlashingLoop() {
    return startSlashingLoop(this, DRY_RUN);
  }
  async monitorNetwork() {
    return monitorNetwork(this, DRY_RUN);
  }
  startBridgeAPI() {
    return startBridgeAPI(this);
  }
  stopBridgeAPI() {
    return stopBridgeAPI(this);
  }
  async startDepositMonitor() {
    return startDepositMonitor(this);
  }
  stopDepositMonitor() {
    return stopDepositMonitor(this);
  }
  async startWithdrawalMonitor() {
    return startWithdrawalMonitor(this);
  }
  stopWithdrawalMonitor() {
    return stopWithdrawalMonitor(this);
  }
  getMintSignature(xmrTxid) {
    return getMintSignature(this, xmrTxid);
  }
  getAllPendingSignatures() {
    return getAllPendingSignatures(this);
  }
  registerDepositRequest(ethAddress, paymentId, userSignature) {
    return registerDepositRequest(this, ethAddress, paymentId, userSignature);
  }
  getWithdrawalStatus(ethTxHash) {
    return getWithdrawalStatus(this, ethTxHash);
  }
  getAllPendingWithdrawals() {
    return getAllPendingWithdrawals(this);
  }
  async checkEmergencySweep() {
    return checkEmergencySweep(this);
  }
  async dissolveStuckFinalizedCluster(clusterId) {
    return dissolveStuckFinalizedCluster(this, clusterId);
  }
  _updateEnv(key, value) {
    return updateEnv(__dirname, key, value);
  }
  async _maybeApplyMoneroHealthOverrideCommand() {
    return maybeApplyMoneroHealthOverrideCommand(this);
  }
  logClusterHealth() {
    return logClusterHealthSnapshot(this);
  }
}
export default ZNode;
