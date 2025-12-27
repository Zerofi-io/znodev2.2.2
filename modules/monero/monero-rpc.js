import fs from 'fs';
import path from 'path';
import axios from 'axios';
import http from 'http';
import https from 'https';
import crypto from 'crypto';
import { DigestAuth } from './digest-auth.js';
import { runtimeConfig } from '../core/runtime-config.js';
import { withRetry } from '../core/retry.js';
const CACHEABLE_METHODS = new Set(['get_height', 'get_balance', 'is_multisig', 'get_version']);
const CACHE_TTL_MS = Number(process.env.MONERO_RPC_CACHE_TTL_MS || 2000);
class RpcResponseCache {
  constructor() {
    this.cache = new Map();
  }
  getKey(method, params) {
    const paramStr = params && Object.keys(params).length > 0
      ? JSON.stringify(params, Object.keys(params).sort())
      : '';
    return `${method}:${paramStr}`;
  }
  get(method, params) {
    if (!CACHEABLE_METHODS.has(method)) return null;
    const key = this.getKey(method, params);
    const entry = this.cache.get(key);
    if (!entry) return null;
    if (Date.now() - entry.timestamp > CACHE_TTL_MS) {
      this.cache.delete(key);
      return null;
    }
    return entry.data;
  }
  set(method, params, data) {
    if (!CACHEABLE_METHODS.has(method)) return;
    const key = this.getKey(method, params);
    this.cache.set(key, { data, timestamp: Date.now() });
    if (this.cache.size > 100) {
      const oldest = this.cache.keys().next().value;
      this.cache.delete(oldest);
    }
  }
  invalidate() {
    this.cache.clear();
  }
}
const BIGINT_FIELDS = [
  'amount', 'fee', 'balance', 'unlocked_balance', 'total_amount',
  'total_fee', 'total_received', 'total_sent', 'per_subaddress'
];
const BIGINT_PATTERN = new RegExp(
  `"(${BIGINT_FIELDS.join('|')})"\\s*:\\s*(-?\\d{15,})`,
  'g'
);
export function parseMoneroRpcJson(data) {
  if (typeof data !== 'string') return data;
  const s = data.trim();
  if (!s) return null;
  const patched = s.replace(BIGINT_PATTERN, '"$1":"$2"');
  return JSON.parse(patched);
}
function getBridgeDataDir() {
  const homeDir = process.env.HOME || process.cwd();
  const dataDir = process.env.BRIDGE_DATA_DIR || path.join(homeDir, '.znode-bridge');
  try { fs.mkdirSync(dataDir, { recursive: true, mode: 0o700 }); } catch {}
  return dataDir;
}
function getWalletRpcAuthPath() {
  return path.join(getBridgeDataDir(), 'monero_wallet_rpc_auth.json');
}
function readWalletRpcAuth() {
  try {
    const raw = fs.readFileSync(getWalletRpcAuthPath(), 'utf8');
    const data = JSON.parse(raw);
    if (!data || typeof data !== 'object') return null;
    const user = typeof data.user === 'string' ? data.user : '';
    const password = typeof data.password === 'string' ? data.password : '';
    if (!user || !password) return null;
    return { user, password };
  } catch {
    return null;
  }
}
function writeWalletRpcAuth(auth) {
  const filePath = getWalletRpcAuthPath();
  const dir = path.dirname(filePath);
  try { fs.mkdirSync(dir, { recursive: true, mode: 0o700 }); } catch {}
  const json = JSON.stringify(auth);
  try {
    const fd = fs.openSync(filePath, 'wx', 0o600);
    try {
      fs.writeFileSync(fd, json);
      fs.fsyncSync(fd);
    } finally {
      try { fs.closeSync(fd); } catch {}
    }
    return true;
  } catch (e) {
    if (e && e.code === 'EEXIST') return false;
    throw e;
  }
}
function loadOrCreateWalletRpcAuth() {
  const existing = readWalletRpcAuth();
  if (existing) return existing;
  const user = 'znode_rpc';
  const password = crypto.randomBytes(32).toString('hex');
  const created = { user, password, createdAt: Date.now() };
  try {
    const createdOk = writeWalletRpcAuth(created);
    if (createdOk) return { user, password };
  } catch {}
  const again = readWalletRpcAuth();
  if (again) return again;
  return { user, password };
}
class MoneroRPC {
  constructor(config = {}) {
    this._lastWallet = null;
    this._requestId = 0;
    this._destroyed = false;
    this.url = config.url || process.env.MONERO_RPC_URL || process.env.MONERO_WALLET_RPC_URL || 'http://127.0.0.1:18083';
    this.user = config.user || process.env.MONERO_WALLET_RPC_USER;
    this.password = config.password || process.env.MONERO_WALLET_RPC_PASSWORD;
    this._logger = config.logger || null;
    if (!this.user || !this.password) {
      const auth = loadOrCreateWalletRpcAuth();
      if (auth && auth.user && auth.password) {
        this.user = auth.user;
        this.password = auth.password;
      }
    }
    this._digestAuth = this.user && this.password ? new DigestAuth(this.user, this.password) : null;
    this.httpAgent = new http.Agent({ keepAlive: true, maxSockets: 20, keepAliveMsecs: 30000 });
    this.httpsAgent = new https.Agent({ keepAlive: true, maxSockets: 20, keepAliveMsecs: 30000 });
    this._rpcQueue = Promise.resolve();
    this._responseCache = new RpcResponseCache();
    this._rpcQueueSize = 0;
    this._rpcQueueMax = Math.floor(Number(process.env.MONERO_RPC_MAX_QUEUE || 1000));
    this._queueWaiters = [];
    this._metrics = { totalRequests: 0, successfulRequests: 0, failedRequests: 0, activeConnections: 0, queueSize: 0 };
  }
  _log(level, msg, meta = {}) {
    if (!this._logger) return;
    if (typeof this._logger === 'function') {
      this._logger(level, msg, meta);
    } else if (this._logger[level]) {
      this._logger[level]({ msg, ...meta });
    }
  }
  _getTimeout(operation) {
    const config = runtimeConfig.get('moneroRpc') || {};
    const defaults = { default: 180000, wallet: 180000, transfer: 60000, query: 30000 };
    return config[`${operation}Timeout`] || defaults[operation] || defaults.default;
  }
  _nextRequestId() {
    this._requestId = (this._requestId + 1) % 1000000;
    return String(this._requestId);
  }
  getMetrics() {
    return { ...this._metrics, queueSize: this._rpcQueueSize, agentSockets: this._getAgentStats() };
  }
  _getAgentStats() {
    const httpSockets = this.httpAgent ? Object.keys(this.httpAgent.sockets || {}).reduce((sum, k) => sum + (this.httpAgent.sockets[k]?.length || 0), 0) : 0;
    const httpFreeSockets = this.httpAgent ? Object.keys(this.httpAgent.freeSockets || {}).reduce((sum, k) => sum + (this.httpAgent.freeSockets[k]?.length || 0), 0) : 0;
    const httpsSockets = this.httpsAgent ? Object.keys(this.httpsAgent.sockets || {}).reduce((sum, k) => sum + (this.httpsAgent.sockets[k]?.length || 0), 0) : 0;
    const httpsFreeSockets = this.httpsAgent ? Object.keys(this.httpsAgent.freeSockets || {}).reduce((sum, k) => sum + (this.httpsAgent.freeSockets[k]?.length || 0), 0) : 0;
    return { http: { active: httpSockets, free: httpFreeSockets }, https: { active: httpsSockets, free: httpsFreeSockets } };
  }
  resetQueue() {
    this._rpcQueue = Promise.resolve();
    this._responseCache = new RpcResponseCache();
    this._rpcQueueSize = 0;
    if (this._queueWaiters && this._queueWaiters.length) {
      const waiters = this._queueWaiters;
      this._queueWaiters = [];
      for (const wake of waiters) wake();
    }
  }
  destroy() {
    this._destroyed = true;
    this.resetQueue();
    if (this.httpAgent) { this.httpAgent.destroy(); this.httpAgent = null; }
    if (this.httpsAgent) { this.httpsAgent.destroy(); this.httpsAgent = null; }
    if (this._digestAuth) { this._digestAuth.invalidateCache(); }
  }
  async _queueRPC(fn) {
    if (this._destroyed) throw new Error('MoneroRPC instance has been destroyed');
    const max = this._rpcQueueMax;
    while (Number.isFinite(max) && max > 0 && this._rpcQueueSize >= max) {
      await new Promise((resolve) => this._queueWaiters.push(resolve));
      if (this._destroyed) throw new Error('MoneroRPC instance has been destroyed');
    }
    this._rpcQueueSize += 1;
    this._metrics.queueSize = this._rpcQueueSize;
    const run = async () => {
      this._metrics.activeConnections++;
      try { return await fn(); }
      finally {
        this._rpcQueueSize -= 1;
        this._metrics.queueSize = this._rpcQueueSize;
        this._metrics.activeConnections--;
        if (Number.isFinite(max) && max > 0 && this._queueWaiters.length && this._rpcQueueSize < max) {
          const wake = this._queueWaiters.shift();
          if (wake) wake();
        }
      }
    };
    const p = this._rpcQueue.then(run, run);
    this._rpcQueue = p.catch(() => {});
    return p;
  }
  async call(method, params = {}, timeout = null) {
    const cached = this._responseCache.get(method, params);
    if (cached !== null) {
      return cached;
    }
    const result = await this._queueRPC(() => this._callUnlocked(method, params, timeout));
    this._responseCache.set(method, params, result);
    return result;
  }
  invalidateCache() {
    if (this._responseCache) {
      this._responseCache.invalidate();
    }
  }
  async _callUnlocked(method, params = {}, timeout = null) {
    const effectiveTimeout = timeout || this._getTimeout('default');
    const baseUrl = this.url.endsWith('/') ? this.url.slice(0, -1) : this.url;
    const requestUrl = `${baseUrl}/json_rpc`;
    const urlObj = new URL(requestUrl);
    const uri = urlObj.pathname + (urlObj.search || '');
    const requestId = this._nextRequestId();
    const body = { jsonrpc: '2.0', id: requestId, method, params };
    const axiosConfig = {
      timeout: effectiveTimeout,
      httpAgent: this.httpAgent,
      httpsAgent: this.httpsAgent,
      responseType: 'text',
      transformResponse: [(data) => data],
    };
    this._metrics.totalRequests++;
    this._log('debug', 'RPC request', { method, requestId, params: Object.keys(params) });
    try {
      let response;
      if (this._digestAuth) {
        response = await this._postWithAuth(requestUrl, uri, body, axiosConfig);
      } else {
        response = await axios.post(requestUrl, body, { ...axiosConfig, headers: { 'Content-Type': 'application/json' } });
      }
      if (response.status === 401) throw new Error('HTTP 401: Unauthorized');
      if (response.status < 200 || response.status >= 300) {
        throw new Error(`HTTP ${response.status}: ${response.statusText || 'Unknown error'}`);
      }
      const data = parseMoneroRpcJson(response.data);
      if (!data || typeof data !== 'object') throw new Error('Invalid RPC response');
      if (data.error) {
        const rpcError = data.error || {};
        const msg = rpcError.message || '';
        const err = new Error(`RPC Error: ${msg}`);
        if (rpcError.code !== undefined) err.code = rpcError.code;
        if (rpcError.data !== undefined) err.data = rpcError.data;
        throw err;
      }
      this._metrics.successfulRequests++;
      this._log('debug', 'RPC success', { method, requestId });
      return data.result;
    } catch (error) {
      this._metrics.failedRequests++;
      this._log('warn', 'RPC failed', { method, requestId, error: error.message });
      if (error && error.response) {
        const err = new Error(`HTTP ${error.response.status}: ${error.response.statusText || 'Unknown error'}`);
        err.status = error.response.status;
        if (error.response.data !== undefined) err.data = error.response.data;
        throw err;
      }
      throw error;
    }
  }
  async _postWithAuth(url, uri, body, axiosConfig) {
    if (this._digestAuth.hasCachedAuth()) {
      const authHeader = this._digestAuth.getAuthHeader('POST', uri);
      if (authHeader) {
        try {
          const response = await axios.post(url, body, {
            ...axiosConfig,
            headers: { 'Content-Type': 'application/json', Authorization: authHeader },
            validateStatus: (status) => status < 500,
          });
          if (response.status !== 401) return response;
          this._digestAuth.invalidateCache();
        } catch {}
      }
    }
    const initialResponse = await axios.post(url, body, {
      ...axiosConfig,
      headers: { 'Content-Type': 'application/json' },
      validateStatus: (status) => status === 401 || (status >= 200 && status < 300),
    });
    if (initialResponse.status !== 401) return initialResponse;
    const wwwAuth = initialResponse.headers['www-authenticate'] || initialResponse.headers['WWW-Authenticate'];
    if (!wwwAuth) throw new Error('HTTP 401: Unauthorized (no WWW-Authenticate header)');
    const authResult = this._digestAuth.processChallenge(wwwAuth);
    if (!authResult) throw new Error(`HTTP 401: Unsupported authentication scheme`);
    let authHeader;
    if (authResult.scheme === 'basic') {
      authHeader = authResult.header;
    } else {
      authHeader = this._digestAuth.getAuthHeader('POST', uri);
    }
    const response = await axios.post(url, body, {
      ...axiosConfig,
      headers: { 'Content-Type': 'application/json', Authorization: authHeader },
      validateStatus: (status) => status < 500,
    });
    return response;
  }
  async getAddress() {
    const result = await this.call('get_address');
    if (!result || typeof result.address !== 'string' || !result.address) {
      throw new Error('Invalid get_address response: missing or invalid address field');
    }
    return result.address;
  }
  async makeIntegratedAddress(paymentId, standardAddress = null) {
    const params = {};
    if (paymentId) params.payment_id = paymentId;
    if (standardAddress) params.standard_address = standardAddress;
    const result = await this.call('make_integrated_address', params);
    if (!result || !result.integrated_address) throw new Error('Failed to create integrated address');
    return result.integrated_address;
  }
  async createWallet(filename, password = '') {
    const result = await this.call('create_wallet', { filename, password, language: 'English' }, this._getTimeout('wallet'));
    this._lastWallet = { filename, password };
    return result;
  }
  async closeWallet() {
    const result = await this.call('close_wallet', {});
    return result;
  }
  async openWallet(filename, password = '') {
    const result = await this.call('open_wallet', { filename, password }, this._getTimeout('wallet'));
    this._lastWallet = { filename, password };
    return result;
  }
  async getBalance({ fresh } = {}) {
    if (fresh) this.invalidateCache();
    const result = await this.call('get_balance');
    return { balance: result.balance, unlockedBalance: result.unlocked_balance, multisigImportNeeded: !!result.multisig_import_needed };
  }
  async prepareMultisig() {
    const retryResult = await withRetry(
      async () => {
        const result = await this.call('prepare_multisig', {}, this._getTimeout('wallet'));
        return result.multisig_info;
      },
      { maxAttempts: 3, baseDelayMs: 5000, maxDelayMs: 15000, label: 'prepareMultisig', logger: this._logger }
    );
    if (!retryResult.success) {
      throw new Error(`Failed to prepare multisig after retries: ${retryResult.error?.message || 'Unknown error'}`);
    }
    return retryResult.value;
  }
  async isMultisig() {
    const result = await this.call('is_multisig');
    if (!result || typeof result.multisig !== 'boolean') {
      throw new Error('Invalid is_multisig response: missing or invalid multisig field');
    }
    return result;
  }
  async makeMultisig(multisigInfo, threshold = 7, password) {
    const params = { multisig_info: multisigInfo, threshold };
    if (password) params.password = password;
    const result = await this.call('make_multisig', params, this._getTimeout('wallet'));
    if (!result || typeof result.address !== 'string' || !result.address) {
      throw new Error('Invalid make_multisig response: missing or invalid address field');
    }
    if (typeof result.multisig_info !== 'string') {
      throw new Error('Invalid make_multisig response: missing or invalid multisig_info field');
    }
    return { address: result.address, multisigInfo: result.multisig_info };
  }
  async exchangeMultisigKeys(multisigInfo, password) {
    const params = { multisig_info: multisigInfo };
    if (password) params.password = password;
    const result = await this.call('exchange_multisig_keys', params, this._getTimeout('wallet'));
    if (!result || typeof result !== 'object') {
      throw new Error(`Invalid exchange_multisig_keys response: ${typeof result}`);
    }
    return result;
  }
  async exportMultisigInfo() {
    const result = await this.call('export_multisig_info');
    return result.info;
  }
  async importMultisigInfo(info) {
    this.invalidateCache();
    const infoArray = Array.isArray(info) ? info : [info];
    try {
      const result = await this.call('import_multisig_info', { info: infoArray });
      return result.n_outputs;
    } finally {
      this.invalidateCache();
    }
  }
  async signMultisig(txDataHex) {
    try {
      const result = await this.call('sign_multisig', { tx_data_hex: txDataHex });
      return { txDataHex: result.tx_data_hex, txHashList: result.tx_hash_list };
    } finally {
      this.invalidateCache();
    }
  }
  async submitMultisig(txDataHex) {
    try {
      const result = await this.call('submit_multisig', { tx_data_hex: txDataHex });
      return result.tx_hash_list;
    } finally {
      this.invalidateCache();
    }
  }
  _normalizeDestinations(destinations) {
    if (!Array.isArray(destinations)) return [];
    return destinations.map((d) => {
      const dest = { ...d };
      const rawAmount = dest.amount;
      if (typeof rawAmount === 'bigint') {
        if (rawAmount < 0n) throw new Error('Monero transfer amount cannot be negative');
        if (rawAmount > BigInt(Number.MAX_SAFE_INTEGER)) {
          throw new Error('Monero transfer amount too large for safe JSON numeric representation');
        }
        dest.amount = Number(rawAmount);
      } else if (typeof rawAmount === 'string') {
        const bi = BigInt(rawAmount);
        if (bi < 0n) throw new Error('Monero transfer amount cannot be negative');
        if (bi > BigInt(Number.MAX_SAFE_INTEGER)) {
          throw new Error('Monero transfer amount too large for safe JSON numeric representation');
        }
        dest.amount = Number(bi);
      }
      return dest;
    });
  }
  async _callWithMixinFallback(method, params, ringSize, timeout) {
    const paramsWithRing = { ...params, ring_size: ringSize };
    try {
      return await this.call(method, paramsWithRing, timeout);
    } catch (error) {
      const errorMsg = error.message || '';
      const errorCode = error.code || '';
      const needsFallback = errorMsg.includes('ring_size') || errorMsg.includes('invalid') ||
        errorCode === -32602 || errorMsg.includes('Unknown parameter');
      if (!needsFallback) throw error;
      const paramsWithMixin = { ...params, mixin: ringSize };
      return await this.call(method, paramsWithMixin, timeout);
    }
  }
  async transfer(destinations, mixinOrRingSize = 16) {
    const safeDestinations = this._normalizeDestinations(destinations);
    const params = { destinations: safeDestinations, get_tx_key: true, do_not_relay: true };
    try {
      const result = await this._callWithMixinFallback('transfer', params, mixinOrRingSize, this._getTimeout('transfer'));
      const txKey = result && typeof result.tx_key === 'string' && result.tx_key ? result.tx_key : null;
      return { txDataHex: result.multisig_txset, txHash: result.tx_hash, txKey };
    } finally {
      this.invalidateCache();
    }
  }
  async sweepAll(address, options = {}) {
    const params = {
      address,
      account_index: options.accountIndex || 0,
      priority: options.priority || 0,
      get_tx_key: true,
      do_not_relay: options.doNotRelay !== undefined ? options.doNotRelay : true,
    };
    if (options.subaddrIndices) params.subaddr_indices = options.subaddrIndices;
    const ringSize = options.ringSize || 16;
    try {
      const result = await this._callWithMixinFallback('sweep_all', params, ringSize, this._getTimeout('wallet'));
      return {
        txDataHex: result.multisig_txset || result.tx_hash_list,
        txHashList: result.tx_hash_list,
        amountList: result.amount_list,
        feeList: result.fee_list,
      };
    } finally {
      this.invalidateCache();
    }
  }
  async getIncomingTransfers(transferType = 'available', options = {}) {
    const params = {
      transfer_type: transferType,
      account_index: options.accountIndex || 0,
      verbose: true,
    };
    if (options.subaddrIndices) params.subaddr_indices = options.subaddrIndices;
    const result = await this.call('incoming_transfers', params, this._getTimeout('query'));
    if (!result || !Array.isArray(result.transfers)) return [];
    return result.transfers.map((t) => ({
      amount: t.amount,
      spent: !!t.spent,
      globalIndex: t.global_index,
      txHash: t.tx_hash,
      subaddrIndex: Number.isFinite(t.subaddr_index) ? t.subaddr_index : Number.isFinite(t.subaddr_index?.minor) ? t.subaddr_index.minor : 0,
      keyImage: t.key_image,
      unlocked: !!t.unlocked,
      blockHeight: t.block_height || 0,
    }));
  }
  async refresh() {
    return this.call('refresh');
  }
  async queryKey(keyType) {
    const result = await this.call('query_key', { key_type: keyType });
    return result.key;
  }
  async finalizeMultisig(exchangedKeys) {
    const hasKeys = exchangedKeys != null && !(Array.isArray(exchangedKeys) && exchangedKeys.length === 0);
    const params = hasKeys ? { multisig_info: exchangedKeys } : {};
    try {
      return await this.call('finalize_multisig', params, this._getTimeout('wallet'));
    } catch (error) {
      const msg = (error && error.message) || '';
      if (hasKeys && /Unknown parameter|invalid parameter|Too many arguments|too many params/i.test(msg)) {
        return this.call('finalize_multisig', {}, this._getTimeout('wallet'));
      }
      if (hasKeys && /^RPC Error: ?$/i.test(msg)) {
        return this.call('finalize_multisig', {}, this._getTimeout('wallet'));
      }
      if (/already.*final|already.*multisig/i.test(msg)) return {};
      throw error;
    }
  }
  async getHeight() {
    const result = await this.call('get_height');
    if (!result || typeof result.height !== 'number') {
      throw new Error('Invalid get_height response: missing or invalid height field');
    }
    return result.height;
  }
  async getTransfers(options = {}) {
    const params = {
      in: options.in !== false,
      out: options.out || false,
      pending: options.pending || false,
      pool: options.pool || false,
      failed: options.failed || false,
      filter_by_height: options.minHeight ? true : false,
      min_height: options.minHeight || 0,
      account_index: options.accountIndex || 0,
    };
    const result = await this.call('get_transfers', params, this._getTimeout('transfer'));
    const transfers = [];
    const processTransfers = (txList, type) => {
      if (!Array.isArray(txList)) return;
      for (const tx of txList) {
        transfers.push({
          txid: tx.txid,
          amount: tx.amount,
          confirmations: tx.confirmations || 0,
          blockHeight: tx.height || 0,
          timestamp: tx.timestamp || 0,
          paymentId: tx.payment_id || '',
          address: tx.address || '',
          subaddrIndex: tx.subaddr_index ? tx.subaddr_index.minor : 0,
          type,
          unlocked: tx.unlocked || false,
        });
      }
    };
    processTransfers(result.in, 'in');
    processTransfers(result.pending, 'pending');
    processTransfers(result.pool, 'pool');
    return transfers;
  }
  async getConfirmedDeposits(minConfirmations = 10, options = {}) {
    const minHeight = Number.isFinite(options.minHeight) ? Math.floor(options.minHeight) : 0;
    const transfers = await this.getTransfers({ in: true, pending: false, pool: false, minHeight });
    return transfers.filter((tx) => tx.confirmations >= minConfirmations && tx.type === 'in');
  }
  async getTransferByTxid(txid) {
    try {
      const result = await this.call('get_transfer_by_txid', { txid }, this._getTimeout('query'));
      if (result && result.transfer) {
        const tx = result.transfer;
        return {
          txid: tx.txid,
          amount: tx.amount,
          confirmations: tx.confirmations || 0,
          blockHeight: tx.height || 0,
          timestamp: tx.timestamp || 0,
          paymentId: tx.payment_id || '',
          type: tx.type || 'unknown',
          unlocked: tx.unlocked || false,
        };
      }
      return null;
    } catch (error) {
      const msg = error && error.message ? error.message : '';
      const code = error && error.code;
      if (code === -8 || /not found|no.*transfer|unknown/i.test(msg)) {
        return null;
      }
      const err = new Error(`Failed to get transfer: ${msg}`);
      err.code = code;
      err.isNetworkError = /ECONNREFUSED|ETIMEDOUT|ECONNRESET|timeout/i.test(msg);
      throw err;
    }
  }
}
export default MoneroRPC;
