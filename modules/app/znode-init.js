import { DRY_RUN, EFFECTIVE_RPC_URL } from './runtime.js';
import { MoneroHealth } from './monero/health.js';
import { runtimeConfig } from '../core/runtime-config.js';
import metrics from '../core/metrics.js';
import CircuitBreaker from '../core/circuit-breaker.js';
import { AsyncMutex, ClusterState, ClusterStateMachine } from '../cluster/cluster-state.js';
import { ethers } from 'ethers';
import MoneroRPC from '../monero/monero-rpc.js';
import RPCManager from '../monero/rpc-manager.js';
import P2PDaemonClient from '../p2p/p2p-daemon-client.js';
import TransactionManager from '../tx-manager.js';
const P2PExchange = P2PDaemonClient;
export function initZNode(projectRoot) {
  const __dirname = projectRoot;
  const chainIdRaw = process.env.CHAIN_ID || process.env.ETH_CHAIN_ID || process.env.NETWORK_CHAIN_ID || process.env.CHAIN || null;
  this.networkName = process.env.CHAIN_NAME || process.env.ETH_NETWORK_NAME || 'unknown';
  this.chainId = null;
  let network = undefined;
  if (chainIdRaw != null) {
    const parsed = Number(chainIdRaw);
    if (Number.isFinite(parsed) && parsed > 0) {
      this.chainId = parsed;
      network = ethers.Network.from({ name: this.networkName, chainId: this.chainId });
    }
  }
  const rpcTimeoutRaw = process.env.ETH_RPC_TIMEOUT_MS || process.env.RPC_TIMEOUT_MS;
  const rpcTimeoutParsed = rpcTimeoutRaw != null ? Number(rpcTimeoutRaw) : NaN;
  const rpcTimeoutMs = Number.isFinite(rpcTimeoutParsed) && rpcTimeoutParsed > 0 ? Math.floor(rpcTimeoutParsed) : 30000;
  const rpcRequest = new ethers.FetchRequest(EFFECTIVE_RPC_URL);
  rpcRequest.timeout = rpcTimeoutMs;
  if (process.env.RPC_API_KEY) rpcRequest.setHeader("X-API-Key", process.env.RPC_API_KEY);
  this.provider = new ethers.JsonRpcProvider(rpcRequest, network, network ? { staticNetwork: network } : undefined);
  this.wallet = new ethers.Wallet(process.env.PRIVATE_KEY, this.provider);
  const stuckIntervalRaw = process.env.TX_MANAGER_STUCK_SCAN_INTERVAL_MS;
  const stuckIntervalParsed = stuckIntervalRaw != null ? Number(stuckIntervalRaw) : NaN;
  this._txManagerStuckIntervalMs =
    Number.isFinite(stuckIntervalParsed) && stuckIntervalParsed > 0 ? stuckIntervalParsed : 30000;
  this.txManager = new TransactionManager(this.wallet, this.provider, {
    dryRun: DRY_RUN,
    onPendingChange: (pendingSize) => this._onTxManagerPendingChange(pendingSize),
  });
  this._orchestrationMutex = new AsyncMutex();
  this.clusterState = new ClusterState();
  this.stateMachine = new ClusterStateMachine();
  this.stateMachine.on('transition', ({ from, to, data }) => {
    metrics.onStateTransition(from, to, data);
  });
  this.clusterState.on('stateChange', ({ newState }) => {
    this._activeClusterId = newState.clusterId || null;
    this._clusterMembers = newState.members || null;
    this._clusterFinalAddress = newState.finalAddress || null;
    this._clusterFinalized = !!newState.finalized;
    this._pendingR3 = newState.pendingR3 || null;
    this._clusterFinalizationStartAt = newState.finalizationStartAt || null;
    this._clusterFailoverAttempted = !!newState.failoverAttempted;
    this._clusterCooldownUntil = newState.cooldownUntil || null;
    this._lastClusterFailureAt = newState.lastFailureAt || null;
    this._lastClusterFailureReason = newState.lastFailureReason || null;
  });
  this._clusterBlacklistPath = __dirname + '/.cluster-blacklist.json';
  this._downtimeSeenPath = __dirname + '/.downtime-seen.json';
  this._clusterFailures = new Map();
  this._clusterFailMeta = {};
  this._clusterBlacklist = {};
  this._blacklistSavePending = false;
  this._blacklistSaveQueued = false;
  this._downtimeSeenSavePending = false;
  this._downtimeSeenSaveQueued = false;
  this._downtimeSeen = null;
  this._clusterCooldownUntil = null;
  this._lastClusterFailureAt = null;
  this._lastClusterFailureReason = null;
  this.moneroPassword = process.env.MONERO_WALLET_PASSWORD;
  this._chainIdVerified = false;
  this.registry = null;
  this.bridge = null;
  this._processedDeposits = new Set();
  this._pendingMintSignatures = new Map();
  this._depositMonitorRunning = false;
  this.staking = null;
  this.zfi = null;
  this.exchangeCoordinator = null;
  this.monero = new MoneroRPC({
    url:
      process.env.MONERO_RPC_URL || process.env.MONERO_WALLET_RPC_URL || 'http://127.0.0.1:18083',
  });
  this.rpcManager = new RPCManager({ url: this.monero.url });
  this._rpcRestartPromise = null;
  this._rpcRestartAttempts = 0;
  this._rpcLastRestartTime = 0;
  this._rpcRestartWindow = [];
  this._rpcCircuitOpen = false;
  this._rpcCircuitOpenUntil = 0;
  this._rpcCircuitHalfOpen = false;
  this._walletRotationStats = { total: 0 };
  this.moneroHealth = MoneroHealth.HEALTHY;
  this.moneroErrorCounts = {
    kexRoundMismatch: 0,
    kexTimeout: 0,
    rpcFailure: 0,
  };
  this.currentAttemptWallet = null;
  this._walletMutex = new AsyncMutex();
  this._moneroStartupScanDone = false;
  this._moneroRawCall = this.monero.call.bind(this.monero);
  this.monero.call = async (method, params = {}, timeout) => {
    return this._walletMutex.withLock(async () => {
      const cbConfig = runtimeConfig.getRpcCircuitBreakerConfig(); const waitOnCall = cbConfig.waitOnCall;
      const now = Date.now();
      if (this._rpcCircuitOpen && now < this._rpcCircuitOpenUntil) {
        if (waitOnCall) {
          const remaining = Math.ceil((this._rpcCircuitOpenUntil - now) / 1000);
          console.log(
            `[RPC] Circuit breaker open, wait-through mode: waiting ${remaining}s before retry...`,
          );
          await new Promise((resolve) => setTimeout(resolve, this._rpcCircuitOpenUntil - now));
          this._rpcCircuitOpen = false;
          this._rpcCircuitHalfOpen = true;
          console.log('[RPC] Circuit breaker entering half-open state after wait-through...');
        } else {
          const remaining = Math.ceil((this._rpcCircuitOpenUntil - now) / 1000);
          throw new Error(
            `RPC circuit breaker open: cooldown active for ${remaining}s. Manual intervention may be required.`,
          );
        }
      } else if (this._rpcCircuitOpen && now >= this._rpcCircuitOpenUntil) {
        console.log('[RPC] Circuit breaker entering half-open state, testing recovery...');
        this._rpcCircuitHalfOpen = true;
        this._rpcCircuitOpen = false;
      metrics.setGauge('rpc_circuit_breaker_open', 0);
      }
      try {
        const result = await this._moneroRawCall(method, params, timeout);
        if (this._rpcCircuitHalfOpen) {
          console.log('[RPC] Circuit breaker closed: recovery successful');
          this._rpcCircuitHalfOpen = false;
          this._rpcRestartWindow = [];
          this._rpcRestartAttempts = 0;
        }
        return result;
      } catch (e) {
        const msg = ((e && e.code) || (e && e.message) || '').toString();
        if (/ECONNREFUSED|ETIMEDOUT|ECONNRESET|EHOSTUNREACH|ENETUNREACH|timeout/i.test(msg)) {
          if (this._rpcCircuitHalfOpen) {
            console.error('[RPC] Circuit breaker re-opening: recovery failed');
            this._rpcCircuitOpen = true;
            metrics.setGauge('rpc_circuit_breaker_open', 1);
            this._rpcCircuitHalfOpen = false;
            const cooldownMs = cbConfig.cooldownMs;
            this._rpcCircuitOpenUntil = Date.now() + cooldownMs;
            throw new Error(
              `RPC recovery failed in half-open state. Circuit breaker re-opened for ${cooldownMs / 1000}s.`,
            );
          }
          if (this._rpcRestartPromise) {
            console.log('[RPC] Restart already in progress, waiting...');
            await this._rpcRestartPromise;
          } else {
            const now = Date.now();
            const windowMs = cbConfig.windowMs;
            const maxFailures = cbConfig.maxFailures;
            const cooldownMs = cbConfig.cooldownMs;
            this._rpcRestartWindow = this._rpcRestartWindow.filter((t) => t > now - windowMs);
            if (this._rpcRestartWindow.length >= maxFailures) {
              this._rpcCircuitOpen = true;
              this._rpcCircuitOpenUntil = now + cooldownMs;
              console.error(
                `[RPC] Circuit breaker OPEN: ${maxFailures} failures in ${windowMs / 1000}s. Cooldown for ${cooldownMs / 1000}s. Will test recovery after cooldown.`,
              );
              throw new Error(
                `RPC restart limit exceeded: ${maxFailures} failures in ${windowMs / 1000}s. Circuit breaker open for ${cooldownMs / 1000}s.`,
              );
            }
            this._rpcRestartWindow.push(now);
            const backoffMs = Math.min(1000 * Math.pow(2, this._rpcRestartAttempts), 30000);
            const elapsed = now - this._rpcLastRestartTime;
            if (elapsed < backoffMs) {
              const remaining = backoffMs - elapsed;
              console.log(`[RPC] Backoff active: waiting ${remaining}ms before retry`);
              await new Promise((resolve) => setTimeout(resolve, remaining));
            }
            this._rpcRestartPromise = this.rpcManager
              .restart(this.monero, this.monero._lastWallet)
              .then((result) => {
                this._rpcRestartAttempts = 0;
                this._rpcLastRestartTime = Date.now();
                const status = result.rpcReady ? 'ready' : (result.scriptRan ? 'script-ok' : 'unknown');
                console.log(`[RPC] Restart ${status} (${this._rpcRestartWindow.length} failures in window)`);
              })
              .catch((err) => {
                this._rpcRestartAttempts++;
                this._rpcLastRestartTime = Date.now();
                console.error(`[RPC] Restart failed: ${err.message || err}`);
                throw new Error(`RPC restart failed: ${err.message || err}`);
              })
              .finally(() => {
                this._rpcRestartPromise = null;
              });
            await this._rpcRestartPromise;
          }
          return await this._moneroRawCall(method, params, timeout);
        }
        throw e;
      }
    });
  };
  this.baseWalletName =
    process.env.MONERO_WALLET_NAME || `znode_${this.wallet.address.slice(2, 10)}`;
  this.clusterWalletName = null;
  this.multisigInfo = null;
  this.clusterId = null;
  this._selfSlashed = false;
  this.p2p = new P2PExchange({
    ethereumAddress: this.wallet.address,
    privateKey: this.wallet.privateKey,
    listenAddr: process.env.P2P_LISTEN_ADDR || '/ip4/0.0.0.0/tcp/9000',
    bootstrapPeers: process.env.P2P_BOOTSTRAP_PEERS || '',
  });
  this.getRoundStatus = async (clusterId, round) => {
    try {
      return await this.exchangeCoordinator.getExchangeRoundStatus(clusterId, round);
    } catch (e) {
      try {
        return await this.exchangeCoordinator.getExchangeRoundStatus(round);
      } catch {
        throw e;
      }
    }
  };
  this.healthServer = null;
  this._healthServerPort = parseInt(process.env.HEALTH_PORT, 10) || 3003;
  this.ethRpcCircuitBreaker = new CircuitBreaker('EthRPC', runtimeConfig.getCircuitBreakerConfig({ timeout: 30000, resetTimeout: 60000, onStateChange: (e) => console.log('[EthRPC] Circuit breaker:', e.to, e.reason) }));
  this.moneroRpcCircuitBreaker = new CircuitBreaker('MoneroRPC', runtimeConfig.getCircuitBreakerConfig({ failureThreshold: 3, timeout: 60000, resetTimeout: 30000, onStateChange: (e) => console.log('[MoneroRPC] Circuit breaker:', e.to, e.reason) }));
  this._timers = new Map();
  this.registerTimer = (name, timerId) => {
    const existing = this._timers.get(name);
    if (existing != null) {
      clearTimeout(existing);
      clearInterval(existing);
    }
    this._timers.set(name, timerId);
    return timerId;
  };
  this.clearTimer = (name) => {
    const timerId = this._timers.get(name);
    if (timerId != null) {
      clearTimeout(timerId);
      clearInterval(timerId);
      this._timers.delete(name);
    }
  };
  this.clearAllTimers = () => {
    for (const [, timerId] of this._timers) {
      clearTimeout(timerId);
      clearInterval(timerId);
    }
    this._timers.clear();
  };
  this._p2pRecoveryInFlight = false;
  this._p2pRecoveryAttempts = 0;
  this._p2pRecoveryDisabled = false;
  this._scheduleP2PRecovery = (reason) => {
    if (this._p2pRecoveryDisabled) return;
    if (!this.p2p || this._p2pRecoveryInFlight) return;
    if (this._timers && this._timers.has('p2p_recovery')) return;
    const attempt = (this._p2pRecoveryAttempts || 0) + 1;
    const base = 1000;
    const max = 60000;
    const delay = Math.min(max, base * Math.pow(2, Math.min(attempt - 1, 6)));
    const jitter = Math.floor(Math.random() * 1000);
    const waitMs = delay + jitter;
    this.registerTimer('p2p_recovery', setTimeout(() => {
      this.clearTimer('p2p_recovery');
      this._recoverP2P(reason).catch((e) => {
        console.log('[P2P] Recovery error:', e.message || String(e));
      });
    }, waitMs));
  };
  this._recoverP2P = async (reason) => {
    if (this._p2pRecoveryDisabled || this._p2pRecoveryInFlight) return false;
    if (!this.p2p) return false;
    this._p2pRecoveryInFlight = true;
    this._p2pRecoveryAttempts = (this._p2pRecoveryAttempts || 0) + 1;
    try {
      metrics.setGauge('p2p_connected', 0);
      try {
        await this.p2p.stop();
      } catch {}
      try {
        await this.p2p.start();
      } catch (e) {
        console.log('[P2P] Start failed during recovery:', e.message || String(e));
        return false;
      }
      if (typeof this.p2p.startHealthWatchdog === 'function') {
        try {
          this.p2p.startHealthWatchdog({ minPeers: 3, criticalDurationMs: 10 * 60 * 1000, checkIntervalMs: 20000 });
        } catch {}
      }
      const stakingAddr = this.staking && (this.staking.target || this.staking.address) ? (this.staking.target || this.staking.address) : null;
      if (stakingAddr && this.chainId) {
        try {
          await this.p2p.setHeartbeatDomain(this.chainId, stakingAddr);
        } catch {}
        try {
          const publicIp = (process.env.PUBLIC_IP || '').trim();
          const bridgePort = Number(process.env.BRIDGE_API_PORT) || 3002;
          const apiBase = publicIp ? `http://${publicIp}:${bridgePort}` : '';
          await this.p2p.startQueueDiscovery(stakingAddr, this.chainId, apiBase);
        } catch {}
      }
      const clusterId = this._activeClusterId;
      const members = this._clusterMembers;
      if (clusterId && Array.isArray(members) && members.length > 0) {
        const sessionId = this._sessionId || 'bridge';
        try {
          await this.p2p.joinCluster(`${clusterId}:${sessionId}`, members, false);
        } catch {}
      }
      metrics.setGauge('p2p_connected', this.p2p.connected ? 1 : 0);
      if (this.p2p.connected) {
        this._p2pRecoveryAttempts = 0;
        return true;
      }
      return false;
    } finally {
      this._p2pRecoveryInFlight = false;
      if (this.p2p && !this.p2p.connected && !this._p2pRecoveryDisabled) {
        this._scheduleP2PRecovery(reason || 'recovery_failed');
      }
    }
  };
  if (this.p2p && typeof this.p2p.on === 'function') {
    this.p2p.on('disconnect', () => this._scheduleP2PRecovery('disconnect'));
    this.p2p.on('daemon-exit', () => this._scheduleP2PRecovery('daemon-exit'));
  }
}
export default initZNode;
