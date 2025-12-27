import { ethers } from 'ethers';
import fs from 'fs';
import { CircuitBreaker } from './circuit-breaker.js';
import axios from 'axios';
import { parseEnvInt, parseEnvBigInt, parseEnvBoolean } from './env-parser.js';
import { recordAuthorizedTxids } from '../utils/authorized-txids.js';
import { saveBridgeState } from '../bridge/bridge-state.js';
import { withMoneroLock } from '../bridge/monero-lock.js';
import { SWEEP_SYNC_ROUND } from '../bridge/withdrawals/config.js';
import { HEALTH_DEFAULTS, BLACKLIST_THRESHOLD } from './health-constants.js';
import metrics from './metrics.js';
import Logger from './logger.js';
const logger = new Logger('Health');
const PROCESS_ID = `${process.pid}-${Date.now()}`;
let monotonicOffset = 0n;
let lastSystemTime = BigInt(Date.now());
function getMonotonicTime() {
  const currentSystemTime = BigInt(Date.now());
  if (currentSystemTime < lastSystemTime) {
    monotonicOffset += lastSystemTime - currentSystemTime + 1n;
    console.warn(`[Clock] System clock went backwards, adjusting offset by ${monotonicOffset}ms`);
  }
  lastSystemTime = currentSystemTime;
  return Number(currentSystemTime + monotonicOffset);
}
function createSweepLock() {
  return {
    processId: PROCESS_ID,
    startedAt: getMonotonicTime(),
    systemTime: Date.now(),
  };
}
function validateSweepLock(lock, timeoutMs) {
  if (!lock || typeof lock !== 'object') return false;
  if (lock.processId !== PROCESS_ID) {
    console.log(`[Sweep] Lock held by different process: ${lock.processId}`);
    return false;
  }
  const elapsed = getMonotonicTime() - lock.startedAt;
  if (elapsed > timeoutMs) {
    console.log(`[Sweep] Lock expired after ${elapsed}ms (timeout: ${timeoutMs}ms)`);
    return false;
  }
  return true;
}
async function waitForReceipt(tx, node, options = {}) {
  const { confirmations = 1, timeout = 60000, operation = 'transaction' } = options;
  const startTime = Date.now();
  try {
    const receipt = await Promise.race([
      tx.wait(confirmations),
      new Promise((_, reject) => setTimeout(() => reject(new Error('Receipt timeout')), timeout))
    ]);
    if (!receipt) {
      console.log(`[${operation}] No receipt returned`);
      return { success: false, error: 'no_receipt' };
    }
    if (receipt.status !== 1) {
      console.log(`[${operation}] Transaction reverted (status: ${receipt.status})`);
      return { success: false, error: 'reverted', receipt };
    }
    const currentBlock = await node.provider.getBlockNumber();
    const confirmationDepth = currentBlock - receipt.blockNumber;
    if (confirmationDepth < 0) {
      console.log(`[${operation}] Block reorg detected`);
      return { success: false, error: 'reorg', receipt };
    }
    return {
      success: true,
      receipt,
      blockNumber: receipt.blockNumber,
      confirmations: confirmationDepth + 1,
      gasUsed: receipt.gasUsed,
      duration: Date.now() - startTime,
    };
  } catch (e) {
    const msg = e.message || String(e);
    console.log(`[${operation}] Receipt error: ${msg}`);
    return { success: false, error: msg };
  }
}
function isValidAddress(address) {
  if (!address || typeof address !== 'string') return false;
  try {
    return ethers.isAddress(address);
  } catch {
    return false;
  }
}
function sanitizeAddress(address) {
  if (!isValidAddress(address)) return null;
  try {
    return ethers.getAddress(address);
  } catch {
    return null;
  }
}
function sanitizeClusterId(clusterId) {
  if (!clusterId || typeof clusterId !== 'string') return null;
  if (!/^0x[a-fA-F0-9]{64}$/.test(clusterId)) return null;
  return clusterId.toLowerCase();
}
function truncateAddress(address, len = 8) {
  if (!address || typeof address !== 'string') return 'unknown';
  return address.slice(0, len);
}
const stakingCircuitBreaker = new CircuitBreaker('StakingRPC', { failureThreshold: 5, resetTimeout: 60000, halfOpenMaxCalls: 3, onStateChange: (e) => logger.warn('Staking circuit breaker state change', e) });
const registryCircuitBreaker = new CircuitBreaker('RegistryRPC', { failureThreshold: 5, resetTimeout: 60000, halfOpenMaxCalls: 3, onStateChange: (e) => logger.warn('Registry circuit breaker state change', e) });
const DISSOLUTION_STATES = {
  IDLE: 'idle',
  CHECKING_ELIGIBILITY: 'checking_eligibility',
  ATTEMPTING_DISSOLUTION: 'attempting_dissolution',
  WAITING_CONFIRMATION: 'waiting_confirmation',
  COMPLETED: 'completed',
  FAILED: 'failed',
};
const DISSOLUTION_STATE_PATH = process.env.DISSOLUTION_STATE_PATH || null;
class DissolutionStateMachine {
  constructor() {
    this.state = DISSOLUTION_STATES.IDLE;
    this.clusterId = null;
    this.startedAt = 0;
    this.attempts = 0;
    this.lastError = null;
    this.currentAttemptStartedAt = 0;
  }
  canStart(clusterId) {
    if (this.state === DISSOLUTION_STATES.IDLE) return true;
    if (this.state === DISSOLUTION_STATES.FAILED || this.state === DISSOLUTION_STATES.COMPLETED) {
      if (this.clusterId !== clusterId) return true;
    }
    return false;
  }
  start(clusterId) {
    if (!this.canStart(clusterId)) return false;
    this.state = DISSOLUTION_STATES.CHECKING_ELIGIBILITY;
    this.clusterId = clusterId;
    this.startedAt = getMonotonicTime();
    this.attempts = 0;
    this.lastError = null;
    this.currentAttemptStartedAt = 0;
    this._persist();
    return true;
  }
  setAttempting() {
    this.state = DISSOLUTION_STATES.ATTEMPTING_DISSOLUTION;
    this.attempts++;
    this.currentAttemptStartedAt = getMonotonicTime();
    this._persist();
  }
  setWaiting() {
    this.state = DISSOLUTION_STATES.WAITING_CONFIRMATION;
    this._persist();
  }
  complete() {
    this.state = DISSOLUTION_STATES.COMPLETED;
    this._persist();
  }
  fail(error) {
    this.state = DISSOLUTION_STATES.FAILED;
    this.lastError = error;
    this._persist();
  }
  reset() {
    this.state = DISSOLUTION_STATES.IDLE;
    this.clusterId = null;
    this.startedAt = 0;
    this.attempts = 0;
    this.lastError = null;
    this.currentAttemptStartedAt = 0;
    this._persist();
  }
  isCurrentAttemptTimedOut(timeoutMs) {
    if (this.currentAttemptStartedAt <= 0) return false;
    return (getMonotonicTime() - this.currentAttemptStartedAt) > timeoutMs;
  }
  getStatus() {
    return {
      state: this.state,
      clusterId: this.clusterId,
      startedAt: this.startedAt,
      attempts: this.attempts,
      lastError: this.lastError,
      duration: this.startedAt ? getMonotonicTime() - this.startedAt : 0,
      currentAttemptDuration: this.currentAttemptStartedAt ? getMonotonicTime() - this.currentAttemptStartedAt : 0,
    };
  }
  _persist() {
    if (!DISSOLUTION_STATE_PATH) return;
    try {
      const data = {
        state: this.state,
        clusterId: this.clusterId,
        startedAt: this.startedAt,
        attempts: this.attempts,
        lastError: this.lastError,
        currentAttemptStartedAt: this.currentAttemptStartedAt,
        updatedAt: Date.now(),
      };
      const tmpPath = DISSOLUTION_STATE_PATH + '.tmp';
      fs.writeFileSync(tmpPath, JSON.stringify(data, null, 2), { mode: 0o600 });
      fs.renameSync(tmpPath, DISSOLUTION_STATE_PATH);
    } catch {}
  }
  restore() {
    if (!DISSOLUTION_STATE_PATH) return false;
    try {
      if (!fs.existsSync(DISSOLUTION_STATE_PATH)) return false;
      const raw = fs.readFileSync(DISSOLUTION_STATE_PATH, 'utf8');
      const data = JSON.parse(raw);
      if (!data || typeof data !== 'object') return false;
      const maxAge = parseEnvInt('DISSOLUTION_STATE_MAX_AGE_MS', 3600000, 60000, 86400000);
      if (data.updatedAt && (Date.now() - data.updatedAt) > maxAge) {
        console.log('[Dissolve] Persisted state too old, ignoring');
        return false;
      }
      if (data.state === DISSOLUTION_STATES.COMPLETED || data.state === DISSOLUTION_STATES.IDLE) {
        return false;
      }
      this.state = data.state || DISSOLUTION_STATES.IDLE;
      this.clusterId = data.clusterId || null;
      this.startedAt = data.startedAt || 0;
      this.attempts = data.attempts || 0;
      this.lastError = data.lastError || null;
      this.currentAttemptStartedAt = data.currentAttemptStartedAt || 0;
      console.log(`[Dissolve] Restored state: ${this.state} for cluster ${truncateAddress(this.clusterId)}`);
      return true;
    } catch {
      return false;
    }
  }
}
const dissolutionState = new DissolutionStateMachine();
dissolutionState.restore();
function shuffleArrayOptimized(array, seed) {
  if (!seed || !Array.isArray(array) || array.length === 0) return array;
  const baseHash = ethers.keccak256(ethers.toUtf8Bytes(seed));
  const hashCache = new Map();
  for (const item of array) {
    const itemHash = ethers.keccak256(ethers.solidityPacked(['bytes32', 'address'], [baseHash, item]));
    hashCache.set(item, itemHash);
  }
  const sorted = [...array].sort((a, b) => {
    const hashA = hashCache.get(a);
    const hashB = hashCache.get(b);
    return hashA < hashB ? -1 : hashA > hashB ? 1 : 0;
  });
  return sorted;
}
export async function checkSelfStakeHealth(node) {
  const startTime = Date.now();
  metrics.incrementCounter('health_check_runs_total', 1, { check: 'self_stake' });
  try {
    if (node._selfSlashed) {
      metrics.setGauge('node_self_slashed', 1);
      return;
    }
    metrics.setGauge('node_self_slashed', 0);
    if (!node.staking || !node.wallet) return;
    const addr = sanitizeAddress(node.wallet.address);
    if (!addr) return;
    let info;
    try {
      info = await node.staking.getNodeInfo(addr);
    } catch (e) {
      metrics.incrementCounter('health_check_errors_total', 1, { check: 'self_stake', error: 'rpc_failed' });
      console.log('[Slashed] Failed to read self getNodeInfo:', e.message || String(e));
      return;
    }
    let stakedAmt = 0n;
    if (Array.isArray(info) && info.length >= 1) {
      try {
        stakedAmt = BigInt(info[0]);
      } catch {}
    }
    if (stakedAmt === undefined || stakedAmt === null) {
      stakedAmt = 0n;
    }
    const zfiDecimals = parseEnvInt('ZFI_DECIMALS', HEALTH_DEFAULTS.ZFI_DECIMALS, 1, 24);
    const requiredStake = BigInt(ethers.parseUnits(String(HEALTH_DEFAULTS.REQUIRED_STAKE_ZFI), zfiDecimals));
    const stakeRatio = stakedAmt > 0n ? Number((stakedAmt * 100n) / requiredStake) / 100 : 0;
    metrics.setGauge('node_stake_ratio', stakeRatio);
    if (stakedAmt >= requiredStake || stakedAmt === 0n) return;
    metrics.incrementCounter('stake_below_minimum_detected_total', 1);
    console.log(
      '[Slashed] Detected self stake below minimum:',
      'current=', ethers.formatUnits(stakedAmt, zfiDecimals),
      'required=', ethers.formatUnits(requiredStake, zfiDecimals),
    );
    console.log('[Slashed] Node can rejoin cluster after topping up stake to 1M ZFI');
    let isBlacklisted = false;
    try {
      isBlacklisted = await node.staking.isBlacklisted(addr);
    } catch {}
    metrics.setGauge('node_blacklisted', isBlacklisted ? 1 : 0);
    if (isBlacklisted) {
      console.log('[Slashed] Node is BLACKLISTED - retiring cluster wallet');
      metrics.incrementCounter('node_blacklisted_events_total', 1);
      try {
        await node._retireClusterWalletAndBackups('blacklisted');
      } catch (e) {
        console.log('[Slashed] Error retiring cluster wallet:', e.message || String(e));
      }
      try {
        await node._hardResetMoneroWalletState('blacklisted');
      } catch (e) {
        console.log('[Slashed] Error resetting Monero wallet:', e.message || String(e));
      }
      node._selfSlashed = true;
    } else {
      console.log('[Slashed] Keeping cluster wallet and keys - top up stake to rejoin');
    }
  } catch (e) {
    metrics.incrementCounter('health_check_errors_total', 1, { check: 'self_stake', error: 'unexpected' });
    console.log('[Slashed] Unexpected error in _checkSelfStakeHealth:', e.message || String(e));
  } finally {
    const duration = (Date.now() - startTime) / 1000;
    metrics.recordHistogram('health_check_duration_seconds', duration, { check: 'self_stake' });
  }
}
async function verifyRecoveryCluster(node, clusterData) {
  if (!clusterData || !clusterData.id) return false;
  if (!node.registry) return true;
  const clusterId = sanitizeClusterId(clusterData.id);
  if (!clusterId) return false;
  try {
    const isActive = await registryCircuitBreaker.call(
      () => node.registry.isClusterActive(clusterId),
      () => null
    );
    if (isActive === null) return true;
    if (!isActive) {
      console.log(`[Sweep] Recovery cluster ${truncateAddress(clusterId)} is not active`);
      return false;
    }
    const info = await registryCircuitBreaker.call(
      () => node.registry.clusters(clusterId),
      () => null
    );
    if (!info) return true;
    const finalized = Array.isArray(info) && info.length > 2 ? !!info[2] : false;
    if (!finalized) {
      console.log(`[Sweep] Recovery cluster ${truncateAddress(clusterId)} is not finalized`);
      return false;
    }
    return true;
  } catch (e) {
    console.log(`[Sweep] Error verifying recovery cluster: ${e.message}`);
    return true;
  }
}
async function findRecoveryClusterAddress(node) {
  const base = process.env.SWEEP_COORDINATOR_URL;
  if (!base) return null;
  let url = base;
  if (!url.includes('/api/clusters/active')) {
    url = url.endsWith('/') ? url + 'api/clusters/active' : url + '/api/clusters/active';
  }
  const timeoutMs = parseEnvInt('SWEEP_COORDINATOR_TIMEOUT_MS', HEALTH_DEFAULTS.SWEEP_COORDINATOR_TIMEOUT_MS, 1000, 30000);
  let response;
  try {
    response = await axios.get(url, { timeout: timeoutMs });
  } catch (e) {
    console.log('[Sweep] Failed to query sweep coordinator:', e.message || String(e));
    return null;
  }
  const data = response && response.data ? response.data : null;
  if (!data || !Array.isArray(data.clusters) || !node._activeClusterId) return null;
  const pubkeyHex = process.env.SWEEP_COORDINATOR_PUBKEY;
  if (pubkeyHex && typeof pubkeyHex === 'string' && pubkeyHex.length === 64) {
    if (!data.signature || !data.message) {
      console.log('[Sweep] Coordinator response missing signature');
      return null;
    }
    try {
      const messageHash = ethers.keccak256(ethers.toUtf8Bytes(JSON.stringify(data.message)));
      const recoveredAddress = ethers.recoverAddress(messageHash, data.signature);
      const expectedAddress = ethers.computeAddress('0x' + pubkeyHex);
      if (recoveredAddress.toLowerCase() !== expectedAddress.toLowerCase()) {
        console.log('[Sweep] Signature verification failed: address mismatch');
        return null;
      }
    } catch (e) {
      console.log('[Sweep] Signature verification failed:', e.message);
      return null;
    }
  }
  const currentId = sanitizeClusterId(node._activeClusterId);
  if (!currentId) return null;
  let best = null;
  let bestClusterData = null;
  for (const cluster of data.clusters) {
    if (!cluster || typeof cluster.id !== 'string' || !cluster.multisigAddress) continue;
    const clusterId = sanitizeClusterId(cluster.id);
    if (!clusterId || clusterId === currentId) continue;
    const eligible = typeof cluster.eligibleNodes === 'number' ? cluster.eligibleNodes : 0;
    const nodes = typeof cluster.nodeCount === 'number' ? cluster.nodeCount : 0;
    const candidate = { address: cluster.multisigAddress, eligible, nodes };
    if (!best) {
      best = candidate;
      bestClusterData = cluster;
      continue;
    }
    if (candidate.eligible > best.eligible) {
      best = candidate;
      bestClusterData = cluster;
      continue;
    }
    if (candidate.eligible === best.eligible && candidate.nodes > best.nodes) {
      best = candidate;
      bestClusterData = cluster;
    }
  }
  if (!best || !best.address || typeof best.address !== 'string') return null;
  if (bestClusterData) {
    const verified = await verifyRecoveryCluster(node, bestClusterData);
    if (!verified) {
      console.log('[Sweep] Recovery cluster verification failed, trying fallback');
      return null;
    }
  }
  return best.address;
}
function selectSweepInitiator(node, healthyMembers) {
  if (!Array.isArray(healthyMembers) || healthyMembers.length === 0) return null;
  const unique = Array.from(new Set(healthyMembers.map((m) => String(m).toLowerCase()))).sort();
  if (unique.length === 0 || !node._activeClusterId) return null;
  const epochMs = parseEnvInt('SWEEP_INITIATOR_EPOCH_MS', HEALTH_DEFAULTS.SWEEP_INITIATOR_EPOCH_MS, 60000, 86400000);
  const epochNumber = Math.floor(Date.now() / epochMs);
  const seed = String(node._activeClusterId) + ':' + String(epochNumber);
  const hash = ethers.keccak256(ethers.toUtf8Bytes(seed));
  const index = Number(BigInt(hash) % BigInt(unique.length));
  return unique[index];
}
async function runEmergencySweep(node, recoveryAddress) {
  if (!node.monero || !recoveryAddress) return false;
  const lockTimeoutMs = parseEnvInt('SWEEP_LOCK_TIMEOUT_MS', HEALTH_DEFAULTS.SWEEP_LOCK_TIMEOUT_MS, 60000, 3600000);
  if (node._sweepLock) {
    if (validateSweepLock(node._sweepLock, lockTimeoutMs)) {
      return false;
    }
    console.log('[Sweep] Clearing stale lock');
    node._sweepLock = null;
  }
  if (node._withdrawalMultisigLock) {
    console.log('[Sweep] Skipping emergency sweep due to active withdrawal');
    return false;
  }
  if (node._pendingWithdrawals && node._pendingWithdrawals.size > 0) {
    for (const [, data] of node._pendingWithdrawals) {
      if (!data) continue;
      const status = String(data.status || '');
      if (status === 'pending' || status === 'signed' || status === 'submit_failed') {
        console.log('[Sweep] Skipping emergency sweep due to pending withdrawal');
        return false;
      }
    }
  }
  if (node._sweepInProgress) {
    console.log('[Sweep] Skipping emergency sweep due to active sweep');
    return false;
  }
  if (node._moneroSigningInProgress) {
    console.log('[Sweep] Skipping emergency sweep due to active Monero signing');
    return false;
  }
  const lockKey = `emergency_sweep:${PROCESS_ID}:${Date.now()}`;
  node._withdrawalMultisigLock = lockKey;
  node._sweepInProgress = true;
  node._sweepLock = createSweepLock();
  const run = async () => {
    try {
      const validation = await node.monero.call('validate_address', { address: recoveryAddress });
      if (!validation || !validation.valid) {
        console.log('[Sweep] Invalid recovery address');
        return false;
      }
    } catch (e) {
      console.log('[Sweep] Address validation failed:', e.message);
      return false;
    }
    const minBalance = parseEnvBigInt('MIN_SWEEP_BALANCE_ATOMIC', HEALTH_DEFAULTS.MIN_SWEEP_BALANCE_ATOMIC);
    const balanceData = await node.monero.getBalance();
    const balance = balanceData && balanceData.balance ? BigInt(balanceData.balance) : 0n;
    if (balance < minBalance) {
      console.log(`[Sweep] Balance ${balance} below minimum ${minBalance}, skipping`);
      return false;
    }
    console.log(`[Sweep] Initiating emergency Monero sweep to ${truncateAddress(recoveryAddress, 20)}...`);
    const sweep = await node.monero.sweepAll(recoveryAddress, { doNotRelay: true });
    if (!sweep || !sweep.txDataHex) {
      console.log('[Sweep] No sweep transaction data returned');
      return false;
    }
    const signed = await node.monero.signMultisig(sweep.txDataHex);
    const txDataHex = signed && signed.txDataHex ? signed.txDataHex : sweep.txDataHex;
    const hashes = await node.monero.submitMultisig(txDataHex);
    if (Array.isArray(hashes) && hashes.length > 0) {
      recordAuthorizedTxids(node, hashes);
      try { saveBridgeState(node); } catch {}
      if (node.p2p && node._activeClusterId) {
        const sweepSession = `emergency-${Date.now()}`;
        try {
          await node.p2p.broadcastRoundData(
            node._activeClusterId,
            node._sessionId || 'bridge',
            SWEEP_SYNC_ROUND + 3,
            JSON.stringify({ type: 'sweep-txhashes', sweepSession, txHashList: hashes }),
          );
        } catch (e) { console.log('[Sweep] Failed to broadcast txhashes:', e.message || String(e)); }
      }
    }
    node._sweepFailureCount = 0;
    node._sweepBackoffUntil = 0;
    if (Array.isArray(hashes) && hashes.length > 0) {
      console.log(`[Sweep] Emergency sweep submitted: ${hashes.join(', ')}`);
    } else {
      console.log('[Sweep] Emergency sweep submitted with unknown tx hashes');
    }
    return true;
  };
  try {
    return await withMoneroLock(node, { key: lockKey, op: 'emergency_sweep', timeoutMs: lockTimeoutMs }, run);
  } catch (e) {
    console.log('[Sweep] Emergency sweep execution error:', e.message || String(e));
    node._sweepFailureCount = (node._sweepFailureCount || 0) + 1;
    const baseMs = HEALTH_DEFAULTS.BACKOFF_BASE_MS;
    const maxMs = HEALTH_DEFAULTS.BACKOFF_MAX_MS;
    const multiplier = Math.pow(2, Math.min(node._sweepFailureCount - 1, HEALTH_DEFAULTS.BACKOFF_MAX_FAILURES));
    const backoffMs = Math.min(baseMs * multiplier, maxMs);
    node._sweepBackoffUntil = getMonotonicTime() + backoffMs;
    console.log(`[Sweep] Backoff ${backoffMs / 1000}s after ${node._sweepFailureCount} failures`);
    return false;
  } finally {
    node._sweepLock = null;
    node._sweepInProgress = false;
    if (node._withdrawalMultisigLock === lockKey) {
      node._withdrawalMultisigLock = null;
    }
  }
}
async function evaluateClusterHealth(node, clusterThreshold) {
  const zfiDecimals = parseEnvInt('ZFI_DECIMALS', HEALTH_DEFAULTS.ZFI_DECIMALS, 1, 24);
  const requiredStake = BigInt(ethers.parseUnits(String(HEALTH_DEFAULTS.REQUIRED_STAKE_ZFI), zfiDecimals));
  const parallelBatchSize = parseEnvInt('HEALTH_CHECK_PARALLEL_BATCH', 5, 1, 20);
  const healthyMembers = [];
  const slashedMembers = [];
  const blacklistedMembers = [];
  const checkMember = async (member) => {
    const memberAddr = sanitizeAddress(member);
    if (!memberAddr) return null;
    try {
      const isBlacklisted = await stakingCircuitBreaker.call(
        () => node.staking.isBlacklisted(memberAddr),
        () => false
      );
      if (isBlacklisted) {
        return { status: 'blacklisted', address: memberAddr };
      }
      const info = await stakingCircuitBreaker.call(
        () => node.staking.getNodeInfo(memberAddr),
        () => null
      );
      if (!Array.isArray(info) || info.length < 7) return null;
      const stakedAmount = BigInt(info[0]);
      const active = info[3];
      if (!active || stakedAmount < requiredStake) {
        return { status: 'slashed', address: memberAddr };
      }
      return { status: 'healthy', address: memberAddr };
    } catch (e) {
      console.log(`[Sweep] Error checking member ${truncateAddress(memberAddr)}: ${e.message}`);
      return null;
    }
  };
  const members = node._clusterMembers || [];
  for (let i = 0; i < members.length; i += parallelBatchSize) {
    const batch = members.slice(i, i + parallelBatchSize);
    const results = await Promise.all(batch.map(checkMember));
    for (const result of results) {
      if (!result) continue;
      if (result.status === 'healthy') healthyMembers.push(result.address);
      else if (result.status === 'slashed') slashedMembers.push(result.address);
      else if (result.status === 'blacklisted') blacklistedMembers.push(result.address);
    }
  }
  return {
    healthyCount: healthyMembers.length,
    slashedCount: slashedMembers.length,
    blacklistedCount: blacklistedMembers.length,
    healthyMembers,
    slashedMembers,
    blacklistedMembers,
  };
}
export async function isClusterOperational(node) {
  if (!node || !node.staking || !Array.isArray(node._clusterMembers) || node._clusterMembers.length === 0) {
    return { operational: false, reason: 'no_cluster' };
  }
  const cacheMs = parseEnvInt('CLUSTER_OPERATIONAL_CACHE_MS', 30000, 0, 300000);
  const nowMs = Date.now();
  const cached = node._clusterOperationalCache;
  if (cached && typeof cached === 'object' && nowMs - cached.atMs < cacheMs) return cached.value;
  if (node._clusterOperationalInFlight) return node._clusterOperationalInFlight;
  node._clusterOperationalInFlight = (async () => {
    try {
      if (!node.p2p || !node.p2p.node || typeof node.p2p.getHeartbeats !== 'function') {
        return { operational: false, reason: 'p2p_not_ready' };
      }
      const members = node._clusterMembers || [];
      const memberCount = members.length;
      const ttlMs = parseEnvInt('HEARTBEAT_ONLINE_TTL_MS', 120000, 1000, 3600000);
      let heartbeats;
      try {
        heartbeats = await node.p2p.getHeartbeats(ttlMs);
      } catch {
        heartbeats = null;
      }
      if (!heartbeats || typeof heartbeats.has !== 'function') {
        return { operational: false, reason: 'heartbeat_unavailable' };
      }
      const zfiDecimals = parseEnvInt('ZFI_DECIMALS', HEALTH_DEFAULTS.ZFI_DECIMALS, 1, 24);
      const requiredStake = BigInt(ethers.parseUnits(String(HEALTH_DEFAULTS.REQUIRED_STAKE_ZFI), zfiDecimals));
      const parallelBatchSize = parseEnvInt('HEALTH_CHECK_PARALLEL_BATCH', 5, 1, 20);
      const pbftOverride = parseEnvInt('PBFT_QUORUM', 0, 0, memberCount);
      const clusterThreshold = parseEnvInt('CLUSTER_THRESHOLD', 0, 0, memberCount);
      const f = memberCount > 1 ? Math.floor((memberCount - 1) / 3) : 0;
      let quorum;
      if (pbftOverride > 0 && pbftOverride <= memberCount) quorum = pbftOverride;
      else if (clusterThreshold > 0 && clusterThreshold <= memberCount) quorum = clusterThreshold;
      else quorum = 2 * f + 1;
      if (quorum < 1) quorum = 1;
      if (quorum > memberCount) quorum = memberCount;
      const requiredOnline = Math.min(memberCount, Math.max(quorum, HEALTH_DEFAULTS.CLUSTER_THRESHOLD));
      let blacklistedCount = 0;
      let healthyCount = 0;
      const checkMember = async (member) => {
        const memberAddr = sanitizeAddress(member);
        if (!memberAddr) return null;
        const slashingInfo = await stakingCircuitBreaker.call(
          () => node.staking.getSlashingInfo(memberAddr),
          () => null
        );
        if (!Array.isArray(slashingInfo) || slashingInfo.length < 3) return null;
        if (Boolean(slashingInfo[2])) return { status: 'blacklisted' };
        const info = await stakingCircuitBreaker.call(
          () => node.staking.getNodeInfo(memberAddr),
          () => null
        );
        if (!Array.isArray(info) || info.length < 7) return null;
        const stakedAmount = BigInt(info[0]);
        const active = Boolean(info[3]);
        if (!active || stakedAmount < requiredStake) return null;
        return memberAddr.toLowerCase();
      };
      for (let i = 0; i < members.length; i += parallelBatchSize) {
        const batch = members.slice(i, i + parallelBatchSize);
        const results = await Promise.all(batch.map(checkMember));
        for (const result of results) {
          if (!result) continue;
          if (result.status === 'blacklisted') {
            blacklistedCount++;
            continue;
          }
          if (typeof result === 'string' && heartbeats.has(result)) {
            healthyCount++;
          }
        }
      }
      if (blacklistedCount >= BLACKLIST_THRESHOLD) {
        return { operational: false, reason: 'too_many_blacklisted', blacklisted: blacklistedCount };
      }
      const offlineCount = members.length - healthyCount - blacklistedCount;
      if (healthyCount < requiredOnline) {
        return { operational: false, reason: 'too_many_offline', offline: offlineCount, healthy: healthyCount };
      }
      return { operational: true, healthy: healthyCount, blacklisted: blacklistedCount };
    } catch (e) {
      console.log('[ClusterHealth] Error checking operational status:', e.message || String(e));
      return { operational: false, reason: 'check_failed' };
    }
  })();
  try {
    const value = await node._clusterOperationalInFlight;
    node._clusterOperationalCache = { atMs: nowMs, value };
    return value;
  } finally {
    node._clusterOperationalInFlight = null;
  }
}
export async function checkEmergencySweep(node) {
  const startTime = Date.now();
  metrics.incrementCounter('health_check_runs_total', 1, { check: 'emergency_sweep' });
  try {
    const enableSweep = parseEnvBoolean('ENABLE_EMERGENCY_SWEEP', true);
    if (!enableSweep) return;
    if (!node._activeClusterId || !node._clusterMembers || node._clusterMembers.length === 0) return;
    if (node._sweepLock) return;
    const clusterThreshold = parseEnvInt('CLUSTER_THRESHOLD', HEALTH_DEFAULTS.CLUSTER_THRESHOLD, 1, 100);
    const health = await evaluateClusterHealth(node, clusterThreshold);
    const { healthyCount, slashedCount, blacklistedCount, healthyMembers, slashedMembers, blacklistedMembers } = health;
    metrics.setGauge('cluster_members_healthy', healthyCount);
    metrics.setGauge('cluster_members_slashed', slashedCount);
    metrics.setGauge('cluster_members_blacklisted', blacklistedCount);
    metrics.setGauge('cluster_members_total', node._clusterMembers.length);
    const clusterId = sanitizeClusterId(node._activeClusterId);
    if (slashedCount > 0 || blacklistedCount > 0) {
      console.log(`[Cluster] Health: ${healthyCount} healthy, ${slashedCount} slashed, ${blacklistedCount} blacklisted`);
      if (slashedMembers.length > 0) {
        const slashedSample = slashedMembers.slice(0, 3).map(m => truncateAddress(m)).join(', ');
        const slashedMore = slashedMembers.length > 3 ? ` (+${slashedMembers.length - 3} more)` : '';
        console.log(`[Cluster] Slashed: ${slashedSample}${slashedMore}`);
      }
      if (blacklistedMembers.length > 0) {
        const blacklistedSample = blacklistedMembers.slice(0, 3).map(m => truncateAddress(m)).join(', ');
        const blacklistedMore = blacklistedMembers.length > 3 ? ` (+${blacklistedMembers.length - 3} more)` : '';
        console.log(`[Cluster] Blacklisted: ${blacklistedSample}${blacklistedMore}`);
      }
    }
    const signingThreshold = clusterThreshold;
    let shouldSweep = false;
    let shouldDissolve = false;
    const clusterSize = node._clusterMembers.length;
    if (blacklistedCount >= BLACKLIST_THRESHOLD) {
      console.log(`\n[WARN] CLUSTER DISSOLUTION CONDITION DETECTED`);
      console.log(`  Blacklisted members: ${blacklistedCount}/${clusterSize}`);
      console.log(`  Slashed members: ${slashedCount}/${clusterSize}`);
      console.log(`  Healthy members: ${healthyCount}/${clusterSize}`);
      metrics.incrementCounter('cluster_dissolution_conditions_total', 1);
      shouldSweep = true;
      shouldDissolve = true;
    } else if (healthyCount < signingThreshold) {
      console.log(`\n[WARN] BELOW SIGNING THRESHOLD`);
      console.log(`  Healthy: ${healthyCount}, Need: ${signingThreshold}`);
      console.log(`  Slashed members can rejoin after topping up stake`);
      metrics.incrementCounter('cluster_below_threshold_total', 1);
      shouldSweep = true;
    }
    if (slashedCount >= BLACKLIST_THRESHOLD && !shouldDissolve) {
      console.log(`[WARN] ${slashedCount} cluster members are slashed but can rejoin after topping up`);
      console.log(`[INFO] Cluster continues operating with ${healthyCount} healthy members`);
    }
    if (shouldSweep) {
      if (!node._firstHealthCheckAt) {
        node._firstHealthCheckAt = getMonotonicTime();
      }
      const sweepGracePeriodMs = parseEnvInt('SWEEP_GRACE_PERIOD_MS', HEALTH_DEFAULTS.SWEEP_GRACE_PERIOD_MS, 0, 86400000);
      if (getMonotonicTime() - node._firstHealthCheckAt < sweepGracePeriodMs) return;
      if (node._sweepBackoffUntil && getMonotonicTime() < node._sweepBackoffUntil) {
        const remaining = Math.ceil((node._sweepBackoffUntil - getMonotonicTime()) / 1000);
        console.log(`[Sweep] Backing off for ${remaining}s after ${node._sweepFailureCount} failures`);
        return;
      }
      const cooldownMs = parseEnvInt('SWEEP_ATTEMPT_COOLDOWN_MS', HEALTH_DEFAULTS.SWEEP_ATTEMPT_COOLDOWN_MS, 60000, 86400000);
      if (node._lastSweepAttemptAt && getMonotonicTime() - node._lastSweepAttemptAt < cooldownMs) {
        console.log('[Sweep] Recent attempt, skipping');
        return;
      }
      const dynamicRecoveryAddress = await findRecoveryClusterAddress(node);
      const fallbackRecoveryAddress = process.env.SWEEP_FALLBACK_ADDRESS || null;
      const recoveryAddress = dynamicRecoveryAddress || fallbackRecoveryAddress;
      if (recoveryAddress) {
        console.log(`[Sweep] Consider sweeping funds to recovery: ${truncateAddress(recoveryAddress, 20)}...`);
        const initiator = selectSweepInitiator(node, healthyMembers);
        const selfAddress = node.wallet && node.wallet.address ? node.wallet.address.toLowerCase() : null;
        if (initiator && selfAddress && initiator === selfAddress) {
          node._lastSweepAttemptAt = getMonotonicTime();
          metrics.incrementCounter('emergency_sweep_attempts_total', 1);
          const sweepResult = await runEmergencySweep(node, recoveryAddress);
          node._lastSweepSucceeded = sweepResult;
        }
      }
    }
    if (shouldDissolve) {
      if (shouldSweep && node._lastSweepSucceeded === false) {
        console.log("[Dissolve] Waiting for successful sweep before dissolution");
        return;
      }
      if (node._sweepBackoffUntil && getMonotonicTime() < node._sweepBackoffUntil) {
        const remaining = Math.ceil((node._sweepBackoffUntil - getMonotonicTime()) / 1000);
        console.log(`[Sweep] Backing off for ${remaining}s after ${node._sweepFailureCount} failures`);
        return;
      }
      const cooldownMs = parseEnvInt('SWEEP_ATTEMPT_COOLDOWN_MS', HEALTH_DEFAULTS.SWEEP_ATTEMPT_COOLDOWN_MS, 60000, 86400000);
      if (node._lastDissolutionAttemptAt && getMonotonicTime() - node._lastDissolutionAttemptAt < cooldownMs) {
        console.log('[Dissolve] Recent attempt, skipping');
        return;
      }
      node._lastDissolutionAttemptAt = getMonotonicTime();
      metrics.incrementCounter('dissolution_attempts_total', 1);
      await attemptClusterDissolution(node, clusterId, healthyMembers);
      const isActive = await node.registry.isClusterActive(clusterId);
      if (!isActive) {
        console.log('[Cluster] Cluster dissolved on-chain, retiring wallet');
        metrics.incrementCounter('cluster_dissolutions_total', 1);
        try {
          await node._retireClusterWalletAndBackups('cluster_dissolved');
        } catch (e) {
          console.log('[Cluster] Failed to retire wallet:', e.message || String(e));
        }
        try {
          if (typeof node._clearClusterState === 'function') {
            node._clearClusterState();
          }
          if (node.p2p && typeof node.p2p.setActiveCluster === 'function') {
            node.p2p.setActiveCluster(null);
          }
        } catch (stateErr) {
          console.log('[Cluster] Error clearing state:', stateErr.message || String(stateErr));
        }
      }
    }
  } catch (e) {
    metrics.incrementCounter('health_check_errors_total', 1, { check: 'emergency_sweep', error: 'unexpected' });
    console.log(`[Sweep] Emergency sweep check error: ${e.message}`);
  } finally {
    const duration = (Date.now() - startTime) / 1000;
    metrics.recordHistogram('health_check_duration_seconds', duration, { check: 'emergency_sweep' });
  }
}
export async function dissolveStuckFinalizedCluster(node, clusterId) {
  const validClusterId = sanitizeClusterId(clusterId);
  if (!node || !node.registry || !validClusterId) return false;
  try {
    const info = await node.registry.clusters(validClusterId);
    if (!info || !info[0]) return false;
    const finalized = !!info[2];
    if (!finalized) return false;
    let isActive = true;
    try {
      isActive = await node.registry.isClusterActive(validClusterId);
    } catch {}
    if (!isActive) return false;
    let gasLimit = null;
    try {
      gasLimit = await node.registry.dissolveCluster.estimateGas(validClusterId);
    } catch {
      gasLimit = parseEnvBigInt('DISSOLUTION_FALLBACK_GAS_LIMIT', HEALTH_DEFAULTS.DISSOLUTION_FALLBACK_GAS_LIMIT);
      console.log(`[Dissolve] Gas estimation failed, using fallback: ${gasLimit}`);
    }
    const balance = await node.provider.getBalance(node.wallet.address);
    if (balance <= gasLimit * 2n) {
      console.log('[Dissolve] Insufficient gas balance to dissolve stuck cluster');
      return false;
    }
    try {
      const tx = await node.txManager.sendTransaction(
        (overrides) => node.registry.dissolveCluster(validClusterId, overrides),
        { operation: 'dissolveCluster-stuck', retryWithSameNonce: true, urgency: 'high' },
      );
      console.log(`[Dissolve] Sent dissolveCluster for stuck cluster (tx: ${tx.hash})`);
      const receipt = await tx.wait();
      if (receipt && receipt.status === 1) {
        console.log('[Dissolve] Stuck cluster dissolved on-chain');
        return true;
      }
    } catch (e) {
      const msg = e && e.message ? e.message : String(e);
      if (msg.includes('already dissolved')) {
        console.log('[Dissolve] Cluster already dissolved');
        return true;
      }
      console.log(`[Dissolve] Failed to dissolve stuck cluster: ${msg}`);
    }
  } catch (e) {
    const msg = e && e.message ? e.message : String(e);
    console.log(`[Dissolve] Error checking stuck cluster state: ${msg}`);
  }
  return false;
}
async function attemptClusterDissolution(node, clusterId, healthyMembers) {
  const validClusterId = sanitizeClusterId(clusterId);
  const waterfallTimeoutMs = parseEnvInt('DISSOLUTION_WATERFALL_TIMEOUT_MS', HEALTH_DEFAULTS.DISSOLUTION_WATERFALL_TIMEOUT_MS, 30000, 600000);
  const perAttemptTimeoutMs = parseEnvInt('DISSOLUTION_PER_ATTEMPT_TIMEOUT_MS', 30000, 10000, 120000);
  if (!validClusterId || !node.registry) return;
  if (!dissolutionState.canStart(validClusterId)) {
    const status = dissolutionState.getStatus();
    console.log(`[Dissolve] State machine busy: ${status.state} for cluster ${truncateAddress(status.clusterId)}`);
    return;
  }
  if (!dissolutionState.start(validClusterId)) {
    console.log(`[Dissolve] Failed to start dissolution state machine`);
    return;
  }
  console.log(`[Dissolve] Checking on-chain dissolution eligibility...`);
  try {
    const [canDissolveBool, blacklistedCount] = await registryCircuitBreaker.call(
      () => node.registry.canDissolve(validClusterId),
      () => [false, 0]
    );
    if (!canDissolveBool) {
      console.log(`[Dissolve] Cluster not eligible (${blacklistedCount} blacklisted, need ${BLACKLIST_THRESHOLD}+)`);
      dissolutionState.fail('not_eligible');
      return;
    }
    console.log(`[Dissolve] Cluster eligible: ${blacklistedCount} blacklisted members`);
  } catch (e) {
    console.log(`[Dissolve] Error checking eligibility: ${e.message}`);
    dissolutionState.fail(e.message);
    return;
  }
  dissolutionState.setAttempting();
  const shuffled = shuffleArrayOptimized([...healthyMembers], validClusterId);
  const selfAddress = sanitizeAddress(node.wallet?.address);
  if (selfAddress) {
    const selfIdx = shuffled.findIndex((m) => m.toLowerCase() === selfAddress.toLowerCase());
    if (selfIdx > 0) {
      shuffled.splice(selfIdx, 1);
      shuffled.unshift(selfAddress);
    }
  }
  console.log(`[Dissolve] Attempting with ${shuffled.length} healthy nodes...`);
  const startTime = dissolutionState.startedAt;
  for (let i = 0; i < shuffled.length; i++) {
    if (getMonotonicTime() - startTime > waterfallTimeoutMs) {
      console.log(`[Dissolve] Waterfall timeout after ${i} attempts`);
      dissolutionState.fail('timeout');
      break;
    }
    if (dissolutionState.isCurrentAttemptTimedOut(perAttemptTimeoutMs)) {
      console.log(`[Dissolve] Per-attempt timeout, moving to next candidate`);
      dissolutionState.setAttempting();
    }
    const candidate = shuffled[i];
    const isSelf = selfAddress && candidate.toLowerCase() === selfAddress.toLowerCase();
    if (isSelf) {
      dissolutionState.setWaiting();
      const success = await tryDissolveSelf(node, validClusterId);
      if (success) {
        console.log(`[Dissolve] Successfully dissolved cluster on-chain`);
        dissolutionState.complete();
        return;
      }
      console.log(`[Dissolve] Self dissolution failed, trying next...`);
      dissolutionState.setAttempting();
    } else {
      const success = await requestPeerDissolution(node, validClusterId, candidate);
      if (success) {
        console.log(`[Dissolve] Peer ${truncateAddress(candidate)} dissolved cluster`);
        dissolutionState.complete();
        return;
      }
      console.log(`[Dissolve] Peer ${truncateAddress(candidate)} failed, trying next...`);
    }
  }
  dissolutionState.fail('all_attempts_failed');
  console.log(`[Dissolve] All nodes failed. Cluster dissolvable by anyone for reward.`);
}
async function tryDissolveSelf(node, clusterId) {
  const validClusterId = sanitizeClusterId(clusterId);
  if (!validClusterId) return false;
  try {
    let gasLimit = null;
    try {
      gasLimit = await node.registry.dissolveCluster.estimateGas(validClusterId);
    } catch {
      gasLimit = parseEnvBigInt('DISSOLUTION_FALLBACK_GAS_LIMIT', HEALTH_DEFAULTS.DISSOLUTION_FALLBACK_GAS_LIMIT);
      console.log(`[Dissolve] Gas estimation failed, using fallback: ${gasLimit}`);
    }
    const balance = await node.provider.getBalance(node.wallet.address);
    if (balance <= gasLimit * 2n) {
      console.log(`[Dissolve] Insufficient gas balance`);
      return false;
    }
    console.log(`[Dissolve] Submitting dissolution transaction...`);
    const tx = await node.txManager.sendTransaction(
      (overrides) => node.registry.dissolveCluster(validClusterId, overrides),
      { operation: 'dissolveCluster-self', retryWithSameNonce: true, urgency: 'high' },
    );
    const result = await waitForReceipt(tx, node, { operation: 'Dissolve', confirmations: 1, timeout: 120000 });
    if (!result.success) {
      if (result.error === 'reverted') {
        console.log(`[Dissolve] Transaction reverted`);
      }
      return false;
    }
    console.log(`[Dissolve] Confirmed in block ${result.blockNumber} (${result.confirmations} confirmations)`);
    try {
      const isActive = await node.registry.isClusterActive(validClusterId);
      if (isActive) {
        console.log('[Dissolve] WARNING: Cluster still active after dissolution tx');
        return false;
      }
    } catch (e) {
      console.log('[Dissolve] State verification error:', e.message);
    }
    return true;
  } catch (e) {
    const msg = e.message || String(e);
    if (msg.includes('insufficient funds')) {
      console.log(`[Dissolve] Insufficient funds for gas`);
    } else if (msg.includes('already dissolved')) {
      console.log(`[Dissolve] Cluster already dissolved`);
      return true;
    } else {
      console.log(`[Dissolve] Error: ${msg.slice(0, 100)}`);
    }
    return false;
  }
}
async function requestPeerDissolution(node, clusterId, peerAddress) {
  if (!node.p2p || typeof node.p2p.sendDirectMessage !== 'function') return false;
  const validClusterId = sanitizeClusterId(clusterId);
  const validPeerAddress = sanitizeAddress(peerAddress);
  if (!validClusterId || !validPeerAddress) return false;
  try {
    const response = await node.p2p.sendDirectMessage(validPeerAddress, {
      type: 'cluster/dissolve-request',
      clusterId: validClusterId,
      requester: sanitizeAddress(node.wallet?.address),
      timestamp: Date.now(),
    }, 30000);
    return response && response.success === true;
  } catch (e) {
    console.log(`[Dissolve] P2P request failed: ${e.message}`);
    return false;
  }
}
export async function handleDissolutionRequest(node, message) {
  const clusterId = sanitizeClusterId(message?.clusterId);
  if (!clusterId || !node.registry) return { success: false, error: 'invalid_request' };
  const activeCluster = sanitizeClusterId(node._activeClusterId);
  if (activeCluster !== clusterId) return { success: false, error: 'not_in_cluster' };
  const requesterAddr = sanitizeAddress(message?.requester);
  console.log(`[Dissolve] Received request from ${truncateAddress(requesterAddr) || 'unknown'}`);
  const success = await tryDissolveSelf(node, clusterId);
  return { success, error: success ? null : 'dissolution_failed' };
}
let metricsCollectorInterval = null;
export function startMetricsCollector(node, intervalMs = 30000) {
  if (metricsCollectorInterval) return;
  const collectMetrics = () => {
    try {
      const memUsage = process.memoryUsage();
      metrics.setGauge('process_heap_bytes', memUsage.heapUsed);
      metrics.setGauge('process_heap_total_bytes', memUsage.heapTotal);
      metrics.setGauge('process_rss_bytes', memUsage.rss);
      metrics.setGauge('process_external_bytes', memUsage.external);
      metrics.setGauge('process_uptime_seconds', process.uptime());
      if (node) {
        metrics.setGauge('node_has_wallet', node.wallet ? 1 : 0);
        metrics.setGauge('node_in_cluster', node._activeClusterId ? 1 : 0);
        metrics.setGauge('node_cluster_member_count', node._clusterMembers?.length || 0);
        metrics.setGauge('node_sweep_lock_active', node._sweepLock ? 1 : 0);
        metrics.setGauge('node_sweep_failure_count', node._sweepFailureCount || 0);
        if (node._rpcCircuitBreaker) {
          metrics.setGauge('rpc_circuit_breaker_open', node._rpcCircuitBreaker.isOpen() ? 1 : 0);
          metrics.setGauge('rpc_circuit_breaker_failures', node._rpcCircuitBreaker.failures || 0);
        }
        const pendingDeposits = node._pendingDepositRequests?.size || 0;
        const pendingMintSigs = node._pendingMintSignatures?.size || 0;
        const processedDeposits = node._processedDeposits?.size || 0;
        const pendingWithdrawals = node._pendingWithdrawals?.size || 0;
        const processedWithdrawals = node._processedWithdrawals?.size || 0;
        metrics.setGauge('bridge_pending_deposit_requests', pendingDeposits);
        metrics.setGauge('bridge_pending_mint_signatures', pendingMintSigs);
        metrics.setGauge('bridge_processed_deposits', processedDeposits);
        metrics.setGauge('bridge_pending_withdrawals', pendingWithdrawals);
        metrics.setGauge('bridge_processed_withdrawals', processedWithdrawals);
        const maxPendingDeposits = Number(process.env.MAX_PENDING_DEPOSIT_REQUESTS || 1000);
        const maxPendingWithdrawals = Number(process.env.MAX_PENDING_WITHDRAWALS || 5000);
        if (pendingDeposits > maxPendingDeposits * 0.8) {
          console.warn(`[Metrics] High pending deposit requests: ${pendingDeposits}/${maxPendingDeposits}`);
        }
        if (pendingWithdrawals > maxPendingWithdrawals * 0.8) {
          console.warn(`[Metrics] High pending withdrawals: ${pendingWithdrawals}/${maxPendingWithdrawals}`);
        }
      }
    } catch {}
  };
  collectMetrics();
  metricsCollectorInterval = setInterval(collectMetrics, intervalMs);
}
export function stopMetricsCollector() {
  if (metricsCollectorInterval) {
    clearInterval(metricsCollectorInterval);
    metricsCollectorInterval = null;
  }
}
export function updateHealthComponentGauge(component, status) {
  const statusValue = status === 'healthy' ? 1 : status === 'degraded' ? 0.5 : 0;
  metrics.setGauge('health_component_status', statusValue, { component });
}
export async function runFullHealthCheck(node) {
  const startTime = Date.now();
  const results = {
    selfStake: null,
    emergencySweep: null,
    timestamp: Date.now()
  };
  try {
    await checkSelfStakeHealth(node);
    results.selfStake = 'ok';
    updateHealthComponentGauge('self_stake', 'healthy');
  } catch (e) {
    results.selfStake = e.message;
    updateHealthComponentGauge('self_stake', 'unhealthy');
  }
  try {
    await checkEmergencySweep(node);
    results.emergencySweep = 'ok';
    updateHealthComponentGauge('emergency_sweep', 'healthy');
  } catch (e) {
    results.emergencySweep = e.message;
    updateHealthComponentGauge('emergency_sweep', 'unhealthy');
  }
  const duration = (Date.now() - startTime) / 1000;
  metrics.recordHistogram('full_health_check_duration_seconds', duration);
  return results;
}
