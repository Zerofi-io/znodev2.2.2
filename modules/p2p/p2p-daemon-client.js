import net from 'net';
import { P2P_MESSAGE_LIMITS } from '../constants.js';
import { spawn } from 'child_process';
import path from 'path';
import fs from 'fs';
import os from 'os';
import { fileURLToPath } from 'url';
import { dirname } from 'path';
import { EventEmitter } from 'events';
import { Buffer } from 'buffer';
const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
export default class P2PDaemonClient extends EventEmitter {
  constructor(options = {}) {
    super();
    this.on('error', (err) => {
      console.error('[P2P-Client] Client error:', err.message || String(err));
    });
    this.socketPath = options.socketPath || '/tmp/znode-p2p.sock';
    this.daemonPath = options.daemonPath || path.join(__dirname, '..', '..', 'p2p-daemon', 'p2p-daemon');
    this.privateKey = options.privateKey;
    this.ethereumAddress = options.ethereumAddress;
    this.listenAddr = options.listenAddr || '/ip4/0.0.0.0/tcp/9000';
    this.bootstrapPeers = options.bootstrapPeers || '';
    this.process = null;
    this.requestId = 0;
    this.pendingRequests = new Map();
    this.connected = false;
    this.reconnectAttempts = 0;
    this.maxReconnectAttempts = 5;
    this.socket = null;
    this.buffer = '';
    this._peerId = '';
    this._connectedPeers = 0;
    this.maxBufferSize = P2P_MESSAGE_LIMITS.MAX_BUFFER_SIZE;
    this.isDisconnecting = false;
    this._features = null;
    this._keyFilePath = null;
    this._watchdogInFlight = false;
  }
  failPendingRequests(error) {
    if (!this.pendingRequests || this.pendingRequests.size === 0) {
      return;
    }
    const err = error instanceof Error ? error : new Error(error || 'P2P daemon connection lost');
    for (const { reject, timeout } of this.pendingRequests.values()) {
      if (timeout) {
        clearTimeout(timeout);
      }
      try {
        reject(err);
      } catch {}
    }
    this.pendingRequests.clear();
  }
  async start() {
    this.isDisconnecting = false;
    try {
      if (!fs.existsSync(this.daemonPath)) {
        throw new Error(
          `p2p-daemon binary not found at ${this.daemonPath}. Run 'cd p2p-daemon && go build' first.`,
        );
      }
      if (fs.existsSync(this.socketPath)) {
        fs.unlinkSync(this.socketPath);
      }
      if (this._keyFilePath) {
        try { fs.unlinkSync(this._keyFilePath); } catch {}
        this._keyFilePath = null;
      }
      const key = String(this.privateKey || '').trim();
      if (!key) {
        throw new Error('p2p-daemon privateKey is required');
      }
      const keyFilePath = path.join(os.tmpdir(), `znode-p2p-key.${process.pid}.${Date.now()}`);
      fs.writeFileSync(keyFilePath, key, { mode: 0o600 });
      this._keyFilePath = keyFilePath;
      const args = [
        '--socket',
        this.socketPath,
        '--key-file',
        keyFilePath,
        '--eth-address',
        this.ethereumAddress,
        '--listen',
        this.listenAddr,
      ];
      if (this.bootstrapPeers) {
        args.push('--bootstrap', this.bootstrapPeers);
      }
      console.log('[P2P-Client] Starting Go p2p-daemon...');
      const env = { ...process.env, HOME: process.env.HOME || os.homedir() };
      this.process = spawn(this.daemonPath, args, {
        stdio: ['ignore', 'pipe', 'pipe'],
        env,
      });
      this.process.stdout.on('data', (data) => {
        const lines = data
          .toString()
          .split('\n')
          .filter((l) => l.trim());
        for (const line of lines) {
          console.log(`[p2p-daemon] ${line}`);
        }
      });
      this.process.stderr.on('data', (data) => {
        const lines = data
          .toString()
          .split('\n')
          .filter((l) => l.trim());
        for (const line of lines) {
          console.log(`[p2p-daemon] ${line}`);
        }
      });
      this.process.on('exit', (code, signal) => {
        console.log(`[P2P-Client] Daemon exited with code ${code}, signal ${signal}`);
        this.connected = false;
        if (this._keyFilePath) {
          try { fs.unlinkSync(this._keyFilePath); } catch {}
          this._keyFilePath = null;
        }
        this.emit('daemon-exit', { code, signal });
      });
      await this.waitForSocket(10000);
      await this.connect();
      const status = await this.status();
      const allowUnverified = process.env.ALLOW_UNVERIFIED_P2P_DAEMON === '1';
      let features;
      try {
        features = await this.features();
      } catch (e) {
        const msg = e && e.message ? e.message : String(e);
        if (allowUnverified) {
          console.log('[WARN]  P2P daemon feature check failed; continuing due to ALLOW_UNVERIFIED_P2P_DAEMON=1:', msg);
        } else {
          throw new Error(`p2p-daemon compatibility check failed: ${msg}`);
        }
      }
      if (features && features.requireAll !== true) {
        if (allowUnverified) {
          console.log(
            '[WARN]  p2p-daemon does not advertise requireAll support; continuing due to ALLOW_UNVERIFIED_P2P_DAEMON=1',
          );
        } else {
          throw new Error('p2p-daemon missing requireAll support; update the p2p-daemon binary');
        }
      }
      const expectedAlgo = 'abi-packed-address-array-v1';
      if (features && features.clusterIdAlgo !== expectedAlgo) {
        const got = features && features.clusterIdAlgo ? String(features.clusterIdAlgo) : '';
        if (allowUnverified) {
          console.log(`[WARN]  p2p-daemon clusterIdAlgo mismatch (${got || 'missing'}); continuing due to ALLOW_UNVERIFIED_P2P_DAEMON=1`);
        } else {
          throw new Error(`p2p-daemon clusterIdAlgo mismatch (${got || 'missing'}); update the p2p-daemon binary`);
        }
      }
      console.log(`[P2P-Client] Connected to daemon. PeerID: ${status.peerId}`);
      return status;
    } catch (e) {
      try {
        await this.stop();
      } catch {}
      throw e;
    }
  }
  async waitForSocket(timeoutMs = 10000) {
    const start = Date.now();
    while (Date.now() - start < timeoutMs) {
      if (fs.existsSync(this.socketPath)) {
        await new Promise((r) => setTimeout(r, 100));
        return;
      }
      await new Promise((r) => setTimeout(r, 100));
    }
    throw new Error(`Timeout waiting for daemon socket at ${this.socketPath}`);
  }
  async connect() {
    return new Promise((resolve, reject) => {
      this.socket = net.createConnection(this.socketPath);
      this.socket.on('connect', () => {
        this.connected = true;
        this.reconnectAttempts = 0;
        resolve();
      });
      this.socket.on('data', (data) => {
        this.handleData(data);
      });
      this.socket.on('error', (err) => {
        if (!this.connected) {
          reject(err);
          return;
        }
        this.connected = false;
        console.error('[P2P-Client] Socket error:', err.message);
        this.failPendingRequests(err);
        this.emit('error', err);
      });
      this.socket.on('close', () => {
        this.connected = false;
        this.failPendingRequests(new Error('P2P daemon socket closed'));
        this.emit('disconnect');
      });
    });
  }
  handleData(data) {
    this.buffer += data.toString();
    if (this.buffer.length > this.maxBufferSize) {
      console.error(`[P2P-Client] Buffer overflow (${this.buffer.length} bytes), disconnecting`);
      if (this.socket) {
        this.socket.destroy();
      }
      this.buffer = '';
      return;
    }
    let newlineIndex;
    while ((newlineIndex = this.buffer.indexOf('\n')) !== -1) {
      const line = this.buffer.slice(0, newlineIndex);
      this.buffer = this.buffer.slice(newlineIndex + 1);
      if (line.trim()) {
        try {
          const response = JSON.parse(line);
          this.handleResponse(response);
        } catch {
          console.error('[P2P-Client] Invalid JSON response:', line);
        }
      }
    }
  }
  handleResponse(response) {
    const { id, result, error } = response;
    const pending = this.pendingRequests.get(id);
    if (!pending) {
      console.warn('[P2P-Client] Received response for unknown request:', id);
      return;
    }
    this.pendingRequests.delete(id);
    clearTimeout(pending.timeout);
    if (error) {
      const errMsg = error.message || error.code || 'Unknown P2P error';
      pending.reject(new P2PError(errMsg, error.code, error.data));
    } else {
      pending.resolve(result);
    }
  }
  async call(method, params, timeoutMs = 300000) {
    if (!this.connected) {
    if (this.isDisconnecting) {
      throw new Error('P2P client is disconnecting');
    }
      throw new Error('Not connected to p2p-daemon');
    }
    const id = ++this.requestId;
    const request = {
      jsonrpc: '2.0',
      method,
      params,
      id,
    };
    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        this.pendingRequests.delete(id);
        reject(new Error(`RPC call ${method} timed out after ${timeoutMs}ms`));
      }, timeoutMs);
      this.pendingRequests.set(id, { resolve, reject, timeout });
      this.socket.write(JSON.stringify(request) + '\n', (err) => {
        if (err) {
          this.pendingRequests.delete(id);
          clearTimeout(timeout);
          reject(err);
        }
      });
    });
  }
  async stop() {
    this.isDisconnecting = true;
    this.stopHealthWatchdog();
    this.failPendingRequests(new Error('P2P client stopping'));
    if (this.socket) {
      this.socket.end();
      this.socket = null;
    }
    if (this.process) {
      this.process.kill('SIGTERM');
      await new Promise((resolve) => {
        const timeout = setTimeout(() => {
          this.process.kill('SIGKILL');
          resolve();
        }, 5000);
        this.process.on('exit', () => {
          clearTimeout(timeout);
          resolve();
        });
      });
      this.process = null;
    }
    this.connected = false;
    this.failPendingRequests(new Error('P2P client stopped'));
    if (this._keyFilePath) {
      try { fs.unlinkSync(this._keyFilePath); } catch {}
      this._keyFilePath = null;
    }
    console.log('[P2P-Client] Stopped');
  }
  async status() {
    const status = await this.call('P2P.Status', {});
    if (status && typeof status.peerId === 'string') {
      this._peerId = status.peerId;
    }
    if (status && typeof status.connectedPeers === 'number') {
      this._connectedPeers = status.connectedPeers;
    }
    return status;
  }
  async features() {
    const result = await this.call('P2P.Features', {});
    this._features = result;
    return result;
  }
  async setTimeOracle(chainTimeMs, blockNumber = null, source = '') {
    if (chainTimeMs == null) {
      throw new Error('chainTimeMs is required');
    }
    const params = { chainTimeMs };
    if (blockNumber !== null && blockNumber !== undefined) {
      params.blockNumber = blockNumber;
    }
    if (source) {
      params.source = source;
    }
    return this.call('P2P.SetTimeOracle', params, 10000);
  }
  async resetDiscovery() {
    return this.call('P2P.ResetDiscovery', {});
  }
  async forceBootstrap() {
    return this.call('P2P.ForceBootstrap', {});
  }
  async debugDialState() {
    return this.call('P2P.DebugDialState', {});
  }
  startHealthWatchdog(options = {}) {
    const minPeers = options.minPeers || 3;
    const criticalDurationMs = options.criticalDurationMs || 10 * 60 * 1000;
    const checkIntervalMs = options.checkIntervalMs || 30000;
    if (this._watchdogInterval) {
      clearInterval(this._watchdogInterval);
    }
    this._watchdogLowPeersSince = null;
    this._watchdogInFlight = false;
    console.log(`[P2P-Watchdog] Starting health watchdog (minPeers=${minPeers}, criticalDuration=${criticalDurationMs}ms)`);
    this._watchdogInterval = setInterval(async () => {
      if (this._watchdogInFlight) return;
      this._watchdogInFlight = true;
      try {
        const status = await this.status();
        const peerCount = status?.connectedPeers || 0;
        if (peerCount < minPeers) {
          if (!this._watchdogLowPeersSince) {
            this._watchdogLowPeersSince = Date.now();
            console.log(`[P2P-Watchdog] WARNING: Peer count ${peerCount} below minimum ${minPeers}`);
          }
          const duration = Date.now() - this._watchdogLowPeersSince;
          if (duration > criticalDurationMs) {
            console.error(`[P2P-Watchdog] CRITICAL: Peer count ${peerCount} below threshold for ${Math.round(duration/1000)}s, triggering recovery`);
            await this.resetDiscovery();
            await this.forceBootstrap();
            this._watchdogLowPeersSince = null;
          }
        } else {
          if (this._watchdogLowPeersSince) {
            console.log(`[P2P-Watchdog] Peer count recovered to ${peerCount}`);
          }
          this._watchdogLowPeersSince = null;
        }
      } catch (err) {
        console.error(`[P2P-Watchdog] Health check failed: ${err.message}`);
      } finally {
        this._watchdogInFlight = false;
      }
    }, checkIntervalMs);
  }
  stopHealthWatchdog() {
    if (this._watchdogInterval) {
      clearInterval(this._watchdogInterval);
      this._watchdogInterval = null;
    }
    this._watchdogLowPeersSince = null;
    this._watchdogInFlight = false;
    console.log('[P2P-Watchdog] Stopped');
  }
  peerID() {
    return this._peerId || '';
  }
  async joinCluster(clusterId, members, isCoordinator) {
    const memberList = members.map((addr) => ({
      address: addr,
      peerId: '',
    }));
    return this.call('P2P.JoinCluster', {
      clusterId,
      members: memberList,
      isCoordinator,
    });
  }
  async leaveCluster(clusterId) {
    return this.call('P2P.LeaveCluster', { clusterId });
  }
  async broadcastRoundData(clusterId, sessionId, round, payload) {
    const _topic = this.topic(clusterId, sessionId);
    const payloadStr = this._normalizeRoundPayload(payload);
    return this.call('P2P.BroadcastRoundData', {
      clusterId: _topic,
      round,
      payload: payloadStr,
    });
  }
  async getRoundData(clusterId, sessionId, round) {
    const _topic = this.topic(clusterId, sessionId);
    const result = await this.call('P2P.GetRoundData', {
      clusterId: _topic,
      round,
    });
    return result && result.data ? result.data : {};
  }
  async waitForRoundCompletion(clusterId, sessionId, round, members, timeoutMs = 300000, opts = null) {
    const _topic = this.topic(clusterId, sessionId);
    try {
      const result = await this.call(
        'P2P.WaitForRoundCompletion',
        {
          clusterId: _topic,
          round,
          members,
          timeoutMs,
          requireAll: !!(opts && opts.requireAll),
        },
        timeoutMs + 5000,
      );
      return !!(result && result.complete);
    } catch (e) {
      if (e && e.code === -32000 && e.data && typeof e.data === 'object') {
        const receivedCount = Array.isArray(e.data.received) ? e.data.received.length : 0;
        const missing = Array.isArray(e.data.missing) ? e.data.missing : [];
        const total = Array.isArray(members) ? members.length : 0;
        console.log(
          `[P2P-Client] Round ${round} timeout: ${receivedCount}/${total} received; ` +
            `missing: ${missing.join(', ')}`,
        );
        return false;
      }
      throw e;
    }
  }
  async getPeerPayloads(clusterId, sessionId, round, members) {
    const _topic = this.topic(clusterId, sessionId);
    const result = await this.call('P2P.GetPeerPayloads', {
      clusterId: _topic,
      round,
      members,
    });
    const sorted = (result.payloads || []).sort((a, b) =>
      a.address.toLowerCase().localeCompare(b.address.toLowerCase()),
    );
    return sorted.map((p) => p.payload);
  }
  async computeConsensusHash(clusterId, sessionId, round, members) {
    const _topic = this.topic(clusterId, sessionId);
    const result = await this.call('P2P.ComputeConsensusHash', {
      clusterId: _topic,
      round,
      members,
    });
    return result.hash;
  }
  async runConsensus(clusterId, sessionId, phase, data, members, timeoutMs, opts) {
    const _topic = this.topic(clusterId, sessionId);
    const effectiveTimeout =
      timeoutMs != null ? timeoutMs : Number(process.env.PBFT_RPC_TIMEOUT_MS || 300000);
    const result = await this.call(
      'P2P.RunConsensus',
      {
        clusterId: _topic,
        phase,
        data: data || '',
        members,
        requireAll: !!(opts && opts.requireAll),
      },
      effectiveTimeout,
    );
    return result;
  }
  async getPBFTDebugState(clusterId, phase) {
    const result = await this.call('P2P.PBFTDebug', {
      clusterId,
      phase,
    });
    return result;
  }
  async broadcastRoundSignature(clusterId, sessionId, round, dataHash, signature) {
    const sig = signature || dataHash;
    const hash = dataHash || sig;
    return this.call('P2P.BroadcastSignature', {
      clusterId,
      round,
      dataHash: hash,
      signature: sig,
    });
  }
  async waitForSignatureBarrier(clusterId, sessionId, round, members, timeoutMs = 300000) {
    const result = await this.call(
      'P2P.WaitForSignatureBarrier',
      {
        clusterId,
        round,
        members,
        timeoutMs,
      },
      timeoutMs + 5000,
    );
    return {
      complete: result.complete,
      consensusHash: result.consensusHash,
      mismatched: result.mismatched || [],
      missing: result.missing || [],
    };
  }
  async broadcastIdentity(clusterId, publicKey) {
    return this.call('P2P.BroadcastIdentity', {
      clusterId,
      publicKey,
    });
  }
  async waitForIdentities(clusterId, members, timeoutMs = 60000) {
    return this.call(
      'P2P.WaitForIdentities',
      {
        clusterId,
        members,
        timeoutMs,
      },
      timeoutMs + 5000,
    );
  }
  async getPeerBindings(clusterId) {
    const result = await this.call('P2P.GetPeerBindings', { clusterId });
    const bindings = {};
    for (const b of result.bindings || []) {
      bindings[b.address] = b.peerId;
    }
    return bindings;
  }
  async clearPeerBindings(clusterId) {
    return this.call('P2P.ClearPeerBindings', { clusterId });
  }
  async getPeerScores(clusterId) {
    const result = await this.call('P2P.GetPeerScores', { clusterId });
    const scores = {};
    for (const s of result.scores || []) {
      scores[s.address] = s.score;
    }
    return scores;
  }
  async reportPeerFailure(clusterId, address, reason) {
    return this.call('P2P.ReportPeerFailure', {
      clusterId,
      address,
      reason,
    });
  }
  async connectToCluster(clusterId, clusterNodes, isCoordinator, registry) {
    await this.joinCluster(clusterId, clusterNodes, isCoordinator);
    await this.broadcastIdentity(clusterId, '');
    return true;
  }
  cleanupOldMessages() {}
  countConnectedPeers() {
    return this._connectedPeers || 0;
  }
  get node() {
    return this.connected ? { status: 'started' } : null;
  }
  async broadcastHeartbeat() {
    await this.call('P2P.BroadcastHeartbeat', {});
  }
  async startQueueDiscovery(stakingAddress, chainId = 1, apiBase = '') {
    await this.call('P2P.StartQueueDiscovery', {
      stakingAddress,
      chainId,
      intervalMs: 30000,
      apiBase,
    });
  }
  async setHeartbeatDomain(chainId, stakingAddress) {
    await this.call('P2P.SetHeartbeatDomain', {
      chainId: chainId,
      stakingAddress: stakingAddress,
    });
  }
  async startHeartbeat(intervalMs = 30000) {
    await this.call('P2P.StartHeartbeat', { intervalMs });
  }
  async stopHeartbeat() {
    await this.call('P2P.StopHeartbeat', {});
  }
  async getRecentPeers(ttlMs = 300000) {
    const result = await this.call('P2P.GetRecentPeers', { ttlMs });
    return result?.peers || [];
  }
  async clearClusterRounds(clusterId, sessionId, rounds) {
    const _topic = this.topic(clusterId, sessionId);
    return this.call('P2P.ClearClusterRounds', { clusterId: _topic, rounds });
  }
  async clearClusterSignatures(clusterId, rounds = []) {
    return this.call('P2P.ClearClusterSignatures', {
      clusterId,
      rounds,
    });
  }
  async waitForIdentityBarrier(clusterId, members, timeoutMs = 60000) {
    return this.call(
      'P2P.WaitForIdentityBarrier',
      {
        clusterId,
        members,
        timeoutMs,
      },
      timeoutMs + 5000,
    );
  }
  async getLastHeartbeat(address) {
    const result = await this.call('P2P.GetLastHeartbeatWithProof', { address });
    const ts = result && typeof result.timestampMs !== 'undefined' ? Number(result.timestampMs) : 0;
    const sig = result && typeof result.signature === 'string' ? result.signature : '';
    if (!Number.isFinite(ts) || ts <= 0 || !sig) return null;
    return { timestampMs: ts, signature: sig };
  }
  async getHeartbeats(ttlMs = 300000) {
    const cacheKey = `heartbeats_${ttlMs}`;
    const cacheTtlMs = Math.min(ttlMs, Number(process.env.HEARTBEAT_CACHE_TTL_MS || 5000));
    const now = Date.now();
    if (!this._heartbeatCache) {
      this._heartbeatCache = new Map();
    }
    const cached = this._heartbeatCache.get(cacheKey);
    if (cached && now - cached.fetchedAt < cacheTtlMs) {
      return cached.data;
    }
    const result = await this.call('P2P.GetHeartbeats', { ttlMs });
    const map = new Map();
    for (const hb of result?.heartbeats || []) {
      if (!hb || !hb.address) continue;
      map.set(String(hb.address).toLowerCase(), { timestamp: hb.timestamp, apiBase: hb.apiBase || '' });
    }
    this._heartbeatCache.set(cacheKey, { data: map, fetchedAt: now });
    if (this._heartbeatCache.size > 10) {
      for (const [k, v] of this._heartbeatCache) {
        if (now - v.fetchedAt > cacheTtlMs * 10) {
          this._heartbeatCache.delete(k);
        }
      }
    }
    return map;
  }
  async getQueuePeers(ttlMs = 300000) {
    const result = await this.call('P2P.GetRecentPeers', { ttlMs });
    return result?.peers || [];
  }
  setActiveCluster(clusterId) {
    this._activeCluster = clusterId;
  }
  get activeCluster() {
    return this._activeCluster || null;
  }
  get roundData() {
    if (!this._roundData) {
      this._roundData = new Map();
    }
    return this._roundData;
  }
  set round0Responder(fn) {
    this._round0Responder = fn;
  }
  get round0Responder() {
    return this._round0Responder || null;
  }
  topic(clusterId, sessionId) {
    return sessionId ? `${clusterId}:${sessionId}` : clusterId;
  }
  async broadcastPreSelection(blockNumber, epochSeed, candidates) {
    const result = await this.call(
      'P2P.BroadcastPreSelection',
      { blockNumber, epochSeed, candidates },
      30000,
    );
    return result;
  }
  _normalizeRoundPayload(payload) {
    let str;
    if (typeof payload === 'string') {
      str = payload;
    } else if (payload == null) {
      str = '';
    } else if (Buffer.isBuffer(payload)) {
      str = payload.toString('hex');
    } else if (payload instanceof Uint8Array) {
      str = Buffer.from(payload).toString('hex');
    } else if (typeof payload === 'number' || typeof payload === 'boolean') {
      str = JSON.stringify(payload);
    } else if (typeof payload === 'object') {
      try {
        str = JSON.stringify(payload);
      } catch (e) {
        throw new Error(`broadcastRoundData payload is not JSON-serializable: ${e.message || String(e)}`);
      }
      if (typeof str !== "string") {
        throw new Error('broadcastRoundData payload JSON.stringify returned non-string');
      }
    } else {
      throw new Error(`broadcastRoundData payload must be a string/Buffer/Uint8Array/JSON value, got ${typeof payload}`);
    }
    if (str.length > 2000000) {
      throw new Error(`broadcastRoundData payload too large (${str.length} chars)`);
    }
    if (str.includes("\n")) {
      throw new Error('broadcastRoundData payload contains newline');
    }
    if (str.includes("|")) {
      throw new Error('broadcastRoundData payload contains |');
    }
    return str;
  }
  async votePreSelection(proposalId, approved, localCandidates = []) {
    const result = await this.call(
      'P2P.VotePreSelection',
      { proposalId, approved, localCandidates },
      10000,
    );
    return result;
  }
  async waitPreSelection(proposalId, expectedVoters, timeoutMs = 60000) {
    const result = await this.call(
      'P2P.WaitPreSelection',
      { proposalId, expectedVoters, timeoutMs },
      timeoutMs + 5000,
    );
    return result;
  }
  async getPreSelectionProposal(proposalId = null) {
    const result = await this.call(
      'P2P.GetPreSelectionProposal',
      { proposalId },
      10000,
    );
    return result;
  }
  calculateReconnectDelay() {
    if (this.reconnectAttempts === 0) return 1000;
    const baseDelay = 1000;
    const maxDelay = 60000;
    const delay = Math.min(baseDelay * Math.pow(2, this.reconnectAttempts - 1), maxDelay);
    const jitter = Math.random() * 1000;
    return delay + jitter;
  }
  async reconnectWithBackoff() {
    if (this.reconnectAttempts >= this.maxReconnectAttempts) {
      console.error('[P2P-Client] Max reconnect attempts reached');
      return false;
    }
    const delay = this.calculateReconnectDelay();
    console.log(`[P2P-Client] Reconnecting in ${Math.round(delay)}ms (attempt ${this.reconnectAttempts + 1}/${this.maxReconnectAttempts})`);
    await new Promise((resolve) => setTimeout(resolve, delay));
    this.reconnectAttempts++;
    try {
      await this.connect();
      this.reconnectAttempts = 0;
      return true;
    } catch (err) {
      console.error('[P2P-Client] Reconnect failed:', err.message);
      return this.reconnectWithBackoff();
    }
  }
}
class P2PError extends Error {
  constructor(message, code, data) {
    super(message || 'P2P Error');
    this.name = 'P2PError';
    this.code = code;
    this.data = data;
  }
  toString() {
    let str = `${this.name}: ${this.message}`;
    if (this.code) str += ` (code: ${this.code})`;
    return str;
  }
  toJSON() {
    return {
      name: this.name,
      message: this.message,
      code: this.code,
      data: this.data,
    };
  }
}
