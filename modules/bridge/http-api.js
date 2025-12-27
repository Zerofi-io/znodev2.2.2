import fs from 'fs';
import path from 'path';
import crypto from 'crypto';
import { Buffer } from 'buffer';
import http from 'http';
import https from 'https';
import { ethers } from 'ethers';
import metrics from '../core/metrics.js';
import consoleLogger from '../core/console-shim.js';
import { saveBridgeState } from './bridge-state.js';
import { registerDepositRequest, getDepositRequestUserMessageHash, getMintSignature, getAllPendingSignatures, getBatchClaimData, getBatchInfo, getAllBatches, getDepositStatus, getDepositQueueStatus, checkDepositQueueAvailability } from './deposits.js';
import { getWithdrawalStatus, getAllPendingWithdrawals } from './withdrawals.js';
import { getTxset, recordSignStepResult } from './withdrawals/txset-store.js';
import LRUCache from '../core/lru-cache.js';
const MoneroHealth = {
  HEALTHY: 'HEALTHY',
  NEEDS_ATTEMPT_RESET: 'NEEDS_ATTEMPT_RESET',
  NEEDS_GLOBAL_RESET: 'NEEDS_GLOBAL_RESET',
  QUARANTINED: 'QUARANTINED',
};
function isBridgeEnabled() {
  const v = process.env.BRIDGE_ENABLED;
  return v === undefined || v === '1';
}
function isBridgeApiEnabled() {
  const v = process.env.BRIDGE_API_ENABLED;
  return v === undefined || v === '1';
}
function getBridgeDataDir() {
  const homeDir = process.env.HOME || process.cwd();
  const dataDir = process.env.BRIDGE_DATA_DIR || path.join(homeDir, '.znode-bridge');
  try { fs.mkdirSync(dataDir, { recursive: true, mode: 0o700 }); } catch {}
  return dataDir;
}
function getBridgeApiAuthPath() {
  return path.join(getBridgeDataDir(), 'bridge_api_auth.json');
}
function readBridgeApiAuthToken() {
  try {
    const raw = fs.readFileSync(getBridgeApiAuthPath(), 'utf8');
    const data = JSON.parse(raw);
    const token = data && typeof data === 'object' ? data.token : null;
    return typeof token === 'string' && token ? token : null;
  } catch {
    return null;
  }
}
function writeBridgeApiAuthToken(token) {
  const filePath = getBridgeApiAuthPath();
  const dir = path.dirname(filePath);
  try { fs.mkdirSync(dir, { recursive: true, mode: 0o700 }); } catch {}
  const json = JSON.stringify({ token, createdAt: Date.now() });
  let fd;
  try {
    fd = fs.openSync(filePath, 'wx', 0o600);
    fs.writeFileSync(fd, json);
    fs.fsyncSync(fd);
    return true;
  } catch (e) {
    if (e && e.code === 'EEXIST') return false;
    throw e;
  } finally {
    if (fd !== undefined) {
      try { fs.closeSync(fd); } catch {}
    }
  }
}
function loadOrCreateBridgeApiAuthToken() {
  const existing = readBridgeApiAuthToken();
  if (existing) return existing;
  const token = crypto.randomBytes(32).toString('hex');
  try {
    const created = writeBridgeApiAuthToken(token);
    if (created) return token;
  } catch {}
  const again = readBridgeApiAuthToken();
  return again || token;
}
function isProtectedRoute(pathname) {
  if (pathname === '/health') return true;
  if (pathname === '/healthz') return true;
  if (pathname === '/metrics') return true;
  if (pathname === '/cluster-status') return true;
  if (pathname === '/bridge/info') return true;
  if (pathname === '/bridge/reserves') return true;
  if (pathname === '/bridge/cluster/members') return true;
  if (pathname === '/bridge/signatures') return true;
  if (pathname === '/bridge/withdrawals') return true;
  if (pathname === '/bridge/burn/ready-signature') return true;
  if (pathname === '/bridge/batches') return true;
  if (pathname === '/bridge/deposit/queue-status') return true;
  if (pathname === '/bridge/txs') return true;
  if (pathname.startsWith('/bridge/batch/')) return true;
  if (pathname.startsWith('/bridge/claim/')) return true;
  if (pathname.startsWith('/bridge/withdrawal/status/')) return true;
  if (pathname.startsWith('/bridge/withdrawal/txset/')) return true;
  if (pathname === '/bridge/withdrawal/sign-step') return true;
  return false;
}
function sanitizeIntParam(value, defaultVal, minVal = 0, maxVal = Number.MAX_SAFE_INTEGER) {
  if (value === null || value === undefined || value === '') return defaultVal;
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || !Number.isSafeInteger(parsed)) return null;
  if (parsed < minVal || parsed > maxVal) return null;
  return parsed;
}
function sanitizeBytes32(value) {
  if (!value || typeof value !== 'string') return null;
  if (!/^0x[0-9a-fA-F]{64}$/.test(value)) return null;
  return value.toLowerCase();
}
const BRIDGE_TX_FEED_QUERY_CHUNK_BLOCKS = 500;
const BRIDGE_TX_FEED_CACHE_TTL_MS = 1000;
const BRIDGE_TX_FEED_INFLIGHT_TTL_MS = 15000;
const BRIDGE_TX_FEED_CACHE_SIZE = 200;
const BRIDGE_TX_FEED_QUERY_RETRIES = 3;
const BRIDGE_TX_FEED_BACKOFF_BASE_MS = 200;
const sleepMs = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
function getErrorMessage(e) {
  return e && e.message ? e.message : String(e);
}
function getErrorCode(e) {
  return (e && (e.code || (e.error && e.error.code))) || null;
}
function isTransientProviderError(e) {
  const msg = getErrorMessage(e);
  const code = getErrorCode(e);
  return code === -32000 || msg.includes('could not coalesce error');
}
function isRateLimitProviderError(e) {
  const msg = getErrorMessage(e).toLowerCase();
  const inner = String(e && e.error && e.error.message ? e.error.message : '').toLowerCase();
  const status =
    (e && (e.status || e.statusCode || (e.response && (e.response.status || e.response.statusCode)))) || null;
  const code = getErrorCode(e);
  if (status === 429) return true;
  if (code === -32005) return true;
  if (msg.includes('rate limit') || msg.includes('too many requests')) return true;
  if (inner.includes('rate limit') || inner.includes('too many requests')) return true;
  return false;
}
async function withBackoff(fn) {
  for (let i = 0; ; i++) {
    try {
      return await fn();
    } catch (e) {
      if (i >= BRIDGE_TX_FEED_QUERY_RETRIES || !isRateLimitProviderError(e)) throw e;
      const delay = BRIDGE_TX_FEED_BACKOFF_BASE_MS * 2 ** i + Math.floor(Math.random() * 100);
      await sleepMs(delay);
    }
  }
}
function getBridgeTxFeedCache(node) {
  if (!node._bridgeTxFeedCache) node._bridgeTxFeedCache = new LRUCache(BRIDGE_TX_FEED_CACHE_SIZE);
  return node._bridgeTxFeedCache;
}
const BURN_READY_SIGNATURE_AUTH = process.env.BURN_READY_SIGNATURE_AUTH === '1';
function isLoopbackHost(host) {
  const h = String(host || '').trim().toLowerCase();
  if (!h || h === 'localhost') return true;
  if (h === '127.0.0.1' || h === '::1' || h === '0:0:0:0:0:0:0:1') return true;
  return false;
}
function extractBearerToken(req) {
  const raw = req && req.headers ? req.headers.authorization : null;
  if (typeof raw !== 'string') return null;
  const m = raw.match(/^\s*bearer\s+(.+?)\s*$/i);
  return m ? m[1] : null;
}
function constantTimeCompare(a, b) {
  if (typeof a !== 'string' || typeof b !== 'string') return false;
  const bufA = Buffer.from(a);
  const bufB = Buffer.from(b);
  if (bufA.length !== bufB.length) return false;
  return crypto.timingSafeEqual(bufA, bufB);
}
function applySecurityHeaders(res) {
  res.setHeader('X-Content-Type-Options', 'nosniff');
  res.setHeader('X-Frame-Options', 'DENY');
  res.setHeader('X-XSS-Protection', '1; mode=block');
  res.setHeader('Cache-Control', 'no-store');
}
function applyCORSHeaders(req, res) {
  applySecurityHeaders(res);
  const configured = process.env.BRIDGE_API_CORS_ORIGIN;
  if (configured) {
    res.setHeader('Access-Control-Allow-Origin', configured);
    res.setHeader('Vary', 'Origin');
  } else {
    const origin = req && req.headers ? req.headers.origin : null;
    if (typeof origin === 'string' && origin) {
      let ok = false;
      try {
        const u = new URL(origin);
        const host = u.hostname ? u.hostname.toLowerCase() : '';
        ok = host === 'localhost' || host === '127.0.0.1' || host === '::1';
        if (ok && (u.protocol === 'http:' || u.protocol === 'https:')) {
          res.setHeader('Access-Control-Allow-Origin', origin);
          res.setHeader('Vary', 'Origin');
        }
      } catch {
        ok = false;
      }
    }
  }
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  res.setHeader('Access-Control-Max-Age', '600');
}
export function startBridgeAPI(node) {
  const apiEnabled = isBridgeApiEnabled();
  if (!apiEnabled) {
    console.log('[BridgeAPI] API server disabled by configuration (BRIDGE_API_ENABLED=0)');
    return;
  }
  const port = Number(process.env.BRIDGE_API_PORT || 3002);
  const bindHost = process.env.BRIDGE_API_BIND_IP || '0.0.0.0';
  const tlsEnabled = process.env.BRIDGE_API_TLS_ENABLED === '1';
  const apiProtoLabel = tlsEnabled ? 'HTTPS' : 'HTTP';
  if (node._apiServer && node._apiServer.listening) {
    try {
      const addr = node._apiServer.address();
      if (addr && typeof addr === 'object') console.log(`[BridgeAPI] ${apiProtoLabel} API server already listening on ${addr.address}:${addr.port}`);
      else console.log(`[BridgeAPI] ${apiProtoLabel} API server already listening`);
    } catch {
      console.log(`[BridgeAPI] ${apiProtoLabel} API server already listening`);
    }
    return;
  }
  if (node._apiServer) {
    try { node._apiServer.close(); } catch {}
    node._apiServer = null;
  }
  const requireAuthAll = process.env.BRIDGE_API_REQUIRE_AUTH === '1';
  const protectSensitiveRoutes = !isLoopbackHost(bindHost);
  let authToken = String(process.env.BRIDGE_API_AUTH_TOKEN || '');
  const needsToken = requireAuthAll || protectSensitiveRoutes || BURN_READY_SIGNATURE_AUTH;
  if (needsToken && !authToken) {
    authToken = loadOrCreateBridgeApiAuthToken();
    console.log(`[BridgeAPI] Auth token stored at ${getBridgeApiAuthPath()}`);
  }
  node._apiRateLimits = new LRUCache(1000);
  const rateLimitWindowMs = Number(process.env.API_RATE_LIMIT_WINDOW_MS || 60000);
  const rateLimitMaxRequests = Number(process.env.API_RATE_LIMIT_MAX_REQUESTS || 60);
  const maxBodyBytes = (() => {
    const raw = process.env.BRIDGE_API_MAX_BODY_BYTES;
    const parsed = raw != null ? Number(raw) : NaN;
    return Number.isFinite(parsed) && parsed > 0 ? Math.floor(parsed) : 32768;
  })();
  const keepAliveTimeoutMs = (() => {
    const raw = process.env.BRIDGE_API_KEEP_ALIVE_TIMEOUT_MS;
    const parsed = raw != null ? Number(raw) : NaN;
    return Number.isFinite(parsed) && parsed > 0 ? Math.floor(parsed) : 5000;
  })();
  const headersTimeoutMs = (() => {
    const raw = process.env.BRIDGE_API_HEADERS_TIMEOUT_MS;
    const parsed = raw != null ? Number(raw) : NaN;
    const v = Number.isFinite(parsed) && parsed > 0 ? Math.floor(parsed) : 15000;
    return v > keepAliveTimeoutMs ? v : keepAliveTimeoutMs + 1000;
  })();
  const requestTimeoutMs = (() => {
    const raw = process.env.BRIDGE_API_REQUEST_TIMEOUT_MS;
    const parsed = raw != null ? Number(raw) : NaN;
    return Number.isFinite(parsed) && parsed > 0 ? Math.floor(parsed) : 60000;
  })();
  const logAllRequests = process.env.BRIDGE_API_LOG_REQUESTS === '1';
  const logRequest = (clientIP, method, pathname, status, latencyMs) => {
    const m = String(method || '');
    const isRead = m === 'GET' || m === 'HEAD';
    if (!logAllRequests && status < 400 && isRead) return;
    console.log(`[BridgeAPI] ${clientIP} ${method} ${pathname} ${status} ${latencyMs}ms`);
  };
  let tlsOptions = null;
  if (tlsEnabled) {
    const keyPath = String(process.env.BRIDGE_API_TLS_KEY_PATH || '').trim();
    const certPath = String(process.env.BRIDGE_API_TLS_CERT_PATH || '').trim();
    const caPath = String(process.env.BRIDGE_API_TLS_CA_PATH || '').trim();
    if (!keyPath || !certPath || !caPath) {
      console.log('[BridgeAPI] TLS enabled but missing BRIDGE_API_TLS_KEY_PATH/BRIDGE_API_TLS_CERT_PATH/BRIDGE_API_TLS_CA_PATH');
      return;
    }
    try {
      tlsOptions = {
        key: fs.readFileSync(keyPath),
        cert: fs.readFileSync(certPath),
        ca: fs.readFileSync(caPath),
        requestCert: true,
        rejectUnauthorized: true,
        minVersion: 'TLSv1.2',
      };
    } catch (e) {
      console.log('[BridgeAPI] Failed to load TLS config:', e && e.message ? e.message : String(e));
      return;
    }
  }
  const requestHandler = (req, res) => {
    const requestStart = Date.now();
    applyCORSHeaders(req, res);
    if (req.method === 'OPTIONS') {
      res.writeHead(200);
      res.end();
      return;
    }
    const clientIP = req.socket?.remoteAddress || req.connection?.remoteAddress || 'unknown';
    const now = Date.now();
    let rateInfo = node._apiRateLimits.get(clientIP);
    if (!rateInfo || now - rateInfo.windowStart > rateLimitWindowMs) {
      rateInfo = { windowStart: now, count: 0 };
    }
    rateInfo.count += 1;
    node._apiRateLimits.set(clientIP, rateInfo);

    if (rateInfo.count > rateLimitMaxRequests) {
      res.writeHead(429, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Too many requests', retryAfter: Math.ceil((rateInfo.windowStart + rateLimitWindowMs - now) / 1000) }));
      logRequest(clientIP, req.method, req.url || '/', 429, Date.now() - requestStart);
      return;
    }
    let url;
    try {
      url = new URL(req.url || '/', `http://${req.headers.host || 'localhost'}`);
    } catch {
      jsonResponse(res, 400, { error: 'Invalid request URL' });
      logRequest(clientIP, req.method, req.url || '/', 400, Date.now() - requestStart);
      return;
    }
    const pathname = url.pathname;
    const needsAuth = requireAuthAll || (protectSensitiveRoutes && isProtectedRoute(pathname));
    const mtlsOk = tlsEnabled && req.socket && req.socket.authorized === true;
    if (needsAuth && !mtlsOk) {
      const token = extractBearerToken(req);
      if (!token || !constantTimeCompare(token, authToken)) {
        jsonResponse(res, 401, { error: 'Unauthorized' });
        logRequest(clientIP, req.method, pathname, 401, Date.now() - requestStart);
        return;
      }
    }
    handleAPIRequest(node, req, res, pathname, url.searchParams, authToken, rateLimitWindowMs, maxBodyBytes, clientIP, requestStart, logRequest);
  };
  node._apiServer = tlsEnabled ? https.createServer(tlsOptions, requestHandler) : http.createServer(requestHandler);
  node._apiServer.keepAliveTimeout = keepAliveTimeoutMs;
  node._apiServer.headersTimeout = headersTimeoutMs;
  node._apiServer.requestTimeout = requestTimeoutMs;
  node._apiServer.on('error', (e) => {
    console.log('[BridgeAPI] Server error:', e.message || String(e));
    if (e && e.code === 'EADDRINUSE') {
      try { node._apiServer.close(); } catch {}
      node._apiServer = null;
    }
  });
  node._apiServer.listen(port, bindHost, () => {
    console.log(`[BridgeAPI] ${apiProtoLabel} API server listening on ${bindHost}:${port}`);
  });
  if (!node._apiRateLimitCleanupTimer) {
    node._apiRateLimitCleanupTimer = setInterval(() => {
      if (!node._apiRateLimits) return;
      const cutoff = Date.now() - rateLimitWindowMs;
      for (const [ip, info] of node._apiRateLimits) {
        if (info.windowStart < cutoff) node._apiRateLimits.delete(ip);
      }
    }, 60000);
  }
}
export function stopBridgeAPI(node) {
  if (node._apiRateLimitCleanupTimer) {
    clearInterval(node._apiRateLimitCleanupTimer);
    node._apiRateLimitCleanupTimer = null;
  }
  if (node._apiServer) {
    node._apiServer.close();
    node._apiServer = null;
    console.log('[BridgeAPI] API server stopped');
  }
}
function jsonResponse(res, status, data) {
  if (res.headersSent) {
    try { res.end(); } catch {}
    return;
  }
  let body;
  try {
    body = JSON.stringify(data, (key, value) => (typeof value === 'bigint' ? value.toString() : value));
  } catch (e) {
    console.log('[BridgeAPI] Failed to serialize JSON response:', e.message || String(e));
    status = 500;
    body = JSON.stringify({ error: 'Internal server error' });
  }
  res.writeHead(status, { 'Content-Type': 'application/json' });
  res.end(body);
}
function isBridgeReadyForDeposit(node) {
  const moneroHealth = node.moneroHealth || MoneroHealth.HEALTHY;
  if (moneroHealth !== MoneroHealth.HEALTHY) {
    return { ok: false, reason: 'Monero backend not healthy' };
  }
  if (!node._clusterFinalized || !node._clusterFinalAddress) {
    return { ok: false, reason: 'Cluster not finalized' };
  }
  if (!node.p2p || !node.p2p.node) {
    return { ok: false, reason: 'P2P not connected' };
  }
  const members = Array.isArray(node._clusterMembers) ? node._clusterMembers : [];
  const self = node.wallet && node.wallet.address ? node.wallet.address.toLowerCase() : null;
  const inCluster = self && members.some((m) => m && m.toLowerCase() === self);
  if (!inCluster) {
    return { ok: false, reason: 'Node not member of active cluster' };
  }
  return { ok: true };
}
function readRequestBody(req, maxBytes) {
  const limitParsed = maxBytes != null ? Number(maxBytes) : NaN;
  const limit = Number.isFinite(limitParsed) && limitParsed > 0 ? Math.floor(limitParsed) : 0;
  return new Promise((resolve, reject) => {
    const chunks = [];
    let size = 0;
    const onEnd = () => {
      let raw = '';
      try {
        raw = chunks.length > 0 ? Buffer.concat(chunks).toString('utf8') : '';
      } catch {
        raw = '';
      }
      if (!raw) {
        resolve({});
        return;
      }
      try {
        resolve(JSON.parse(raw));
      } catch {
        resolve({});
      }
    };
    const onData = (chunk) => {
      if (limit > 0) {
        size += chunk.length;
        if (size > limit) {
          try { req.pause(); } catch {}
          req.removeListener('data', onData);
          req.removeListener('end', onEnd);
          const err = new Error('Body too large');
          err.code = 'BODY_TOO_LARGE';
          reject(err);
          return;
        }
      }
      if (chunk) chunks.push(chunk);
    };
    req.on('data', onData);
    req.on('end', onEnd);
    req.on('error', reject);
  });
}
async function handleAPIRequest(node, req, res, pathname, params, authToken, rateLimitWindowMs, maxBodyBytes, clientIP, requestStart, logRequest) {
  let responseStatus = 200;
  const respond = (status, data) => {
    responseStatus = status;
    return jsonResponse(res, status, data);
  };
  try {
    if (pathname === '/healthz' && req.method === 'GET') {
      const moneroHealth = node.moneroHealth || MoneroHealth.HEALTHY;
      const circuit = { open: !!node._rpcCircuitOpen, halfOpen: !!node._rpcCircuitHalfOpen, openUntil: node._rpcCircuitOpenUntil || 0 };
      const p2pStatus = { connected: !!(node.p2p && node.p2p.node), connectedPeers: node.p2p && typeof node.p2p._connectedPeers === 'number' ? node.p2p._connectedPeers : 0 };
      const cluster = { activeClusterId: node._activeClusterId || null, clusterFinalized: !!node._clusterFinalized, clusterMembers: node._clusterMembers ? node._clusterMembers.length : 0 };
      let status = 'ok';
      if (circuit.open || moneroHealth === MoneroHealth.QUARANTINED) status = 'error';
      else if (moneroHealth !== MoneroHealth.HEALTHY || !p2pStatus.connected) status = 'degraded';
      return respond( 200, { status, monero: { health: moneroHealth, errors: node.moneroErrorCounts || {} }, rpcCircuitBreaker: circuit, p2p: p2pStatus, cluster });
    }
    if (pathname === '/metrics' && req.method === 'GET') {
      const clusterMetrics = metrics.toJSON();
      const loggerMetrics = consoleLogger ? consoleLogger.getMetrics() : null;
      return respond( 200, { metrics: clusterMetrics, logger: loggerMetrics });
    }
    if (pathname === '/cluster-status' && req.method === 'GET') {
      const status = await getClusterStatus(node);
      return respond( 200, status);
    }
    if (pathname === '/health' && req.method === 'GET') {
      return respond( 200, { status: 'ok', clusterFinalized: node._clusterFinalized, multisigAddress: node._clusterFinalAddress || null, bridgeEnabled: isBridgeEnabled() });
    }
    if (pathname === '/bridge/info' && req.method === 'GET') {
      return respond( 200, { multisigAddress: node._clusterFinalAddress || null, clusterMembers: node._clusterMembers ? node._clusterMembers.length : 0, clusterFinalized: node._clusterFinalized, bridgeContract: node.bridge ? node.bridge.target || node.bridge.address : null, clusterId: node._activeClusterId || null, minConfirmations: Number(process.env.MIN_DEPOSIT_CONFIRMATIONS || 2) });
    }
    if (pathname === '/bridge/reserves' && req.method === 'GET') {
      const moneroHealth = node.moneroHealth || MoneroHealth.HEALTHY;
      if (!node.monero || moneroHealth === MoneroHealth.QUARANTINED) {
        return respond( 503, { error: 'Monero backend not healthy', moneroHealth });
      }
      const clusterId = node._activeClusterId || null;
      const multisigAddress = node._clusterFinalAddress || null;
      let walletAddress = null;
      try {
        walletAddress = await node.monero.getAddress();
      } catch {}
      try {
        try { await node.monero.refresh(); } catch {}
        const bal = await node.monero.getBalance();
        const balanceRaw = bal && bal.balance != null ? bal.balance : 0;
        const unlockedRaw = bal && bal.unlockedBalance != null ? bal.unlockedBalance : 0;
        const balanceAtomic = typeof balanceRaw === 'bigint' ? balanceRaw : BigInt(String(balanceRaw));
        const unlockedAtomic = typeof unlockedRaw === 'bigint' ? unlockedRaw : BigInt(String(unlockedRaw));
        const denom = 1000000000000n;
        const toDecimal = (v) => {
          const sign = v < 0n ? '-' : '';
          const abs = v < 0n ? -v : v;
          const intPart = abs / denom;
          const fracPart = abs % denom;
          if (fracPart === 0n) return sign + intPart.toString() + '.0';
          let fracStr = fracPart.toString().padStart(12, '0');
          fracStr = fracStr.replace(/0+$/, '');
          return sign + intPart.toString() + '.' + fracStr;
        };
        const addressVerified = !!(walletAddress && multisigAddress && walletAddress === multisigAddress);
        return respond( 200, {
          clusterId,
          multisigAddress,
          walletAddress,
          addressVerified,
          balanceAtomic: balanceAtomic.toString(),
          unlockedBalanceAtomic: unlockedAtomic.toString(),
          balance: toDecimal(balanceAtomic),
          unlockedBalance: toDecimal(unlockedAtomic),
          checkedAt: Date.now(),
        });
      } catch (e) {
        return respond( 503, { error: 'Failed to query Monero balance', message: e.message || String(e) });
      }
    }
    if (pathname === '/bridge/cluster/members' && req.method === 'GET') {
      const members = node._clusterMembers || [];
      return respond( 200, { members });
    }
    if (pathname === '/bridge/deposit/request' && req.method === 'POST') {
      let body;
      try {
        body = await readRequestBody(req, maxBodyBytes);
      } catch (e) {
        if (e && e.code === 'BODY_TOO_LARGE') {
          return respond( 413, { error: 'Payload too large' });
        }
        throw e;
      }
      const { ethAddress, paymentId, userSignature } = body;
      let normalizedAddress;
      try {
        normalizedAddress = ethAddress ? ethers.getAddress(ethAddress.toLowerCase()) : null;
      } catch {
        normalizedAddress = null;
      }
      if (!normalizedAddress) {
        return respond( 400, { error: 'Invalid ethAddress' });
      }
      const ready = isBridgeReadyForDeposit(node);
      if (!ready.ok) {
        return respond( 503, { error: 'Bridge not ready', reason: ready.reason });
      }
      const queueCheck = checkDepositQueueAvailability(node);
      if (!queueCheck.available) {
        const retrySeconds = queueCheck.retryAfterSeconds || 300;
        return respond( 503, { error: 'Deposit queue full', message: `Try again in ${retrySeconds} seconds`, retryAfterSeconds: retrySeconds });
      }
      const id = typeof paymentId === 'string' && paymentId ? paymentId : crypto.randomBytes(8).toString('hex');
      if (!/^[0-9a-f]{16}$/.test(id)) {
        return respond( 400, { error: 'Invalid paymentId', paymentId: id });
      }
      const messageHash = getDepositRequestUserMessageHash(id, normalizedAddress);
      if (!userSignature || typeof userSignature !== 'string') {
        return respond( 400, { error: 'userSignature required', paymentId: id, messageHash });
      }
      try {
        const result = await registerDepositRequest(node, normalizedAddress, id, userSignature);
        return respond( 200, result);
      } catch (e) {
        const msg = e && e.message ? e.message : String(e);
        if (e && e.code === 'WRONG_CLUSTER') {
          return respond( 409, {
            error: msg,
            paymentId: id,
            messageHash,
            selectedClusterId: e.selectedClusterId || null,
            activeClusterId: e.activeClusterId || null,
          });
        }
        const status = msg.includes('Invalid') || msg.includes('required') ? 400 : 503;
        return respond( status, { error: msg, paymentId: id, messageHash });
      }
    }
    if (pathname.startsWith('/bridge/deposit/status/') && req.method === 'GET') {
      const txid = pathname.split('/').pop();
      if (!txid || txid.length < 10) {
        return respond( 400, { error: 'Invalid txid' });
      }
      let usedOnChain = false;
      let depositId = null;
      try {
        depositId = ethers.keccak256(ethers.toUtf8Bytes(txid));
      } catch {}
      if (node.bridge && typeof node.bridge.usedDepositIds === 'function' && depositId) {
        try {
          usedOnChain = await node.bridge.usedDepositIds(depositId);
        } catch (e) {
          console.log('[BridgeAPI] Failed to check usedDepositIds for deposit status:', e.message || String(e));
        }
      }
      const signature = getMintSignature(node, txid);
      if (usedOnChain) {
        if (node._pendingMintSignatures) node._pendingMintSignatures.delete(txid);
        if (!node._processedDeposits) node._processedDeposits = new Set();
        node._processedDeposits.add(txid);
        try { saveBridgeState(node); } catch {}
        return respond( 200, { status: 'processed', message: 'Deposit already minted on-chain', depositId });
      }
      if (signature) return respond( 200, { status: 'ready', ...signature });
      if (node._processedDeposits && node._processedDeposits.has(txid)) {
        return respond( 200, { status: 'processed', message: 'Deposit processed but no signature available (may have failed consensus)' });
      }
      return respond( 200, { status: 'pending', message: 'Deposit not yet detected or confirmed' });
    }
    if (pathname === '/bridge/signatures' && req.method === 'GET') {
      let signatures = getAllPendingSignatures(node);
      if (node.bridge && typeof node.bridge.usedDepositIds === 'function') {
        const filtered = [];
        let stateChanged = false;
        for (const sig of signatures) {
          let used = false;
          try {
            let depId = sig.depositId;
            if (!depId && sig.xmrTxid) {
              depId = ethers.keccak256(ethers.toUtf8Bytes(sig.xmrTxid));
            }
            if (depId) {
              used = await node.bridge.usedDepositIds(depId);
            }
          } catch (e) {
            console.log('[BridgeAPI] Failed to check usedDepositIds for signature:', e.message || String(e));
          }
          if (used) {
            if (node._pendingMintSignatures) node._pendingMintSignatures.delete(sig.xmrTxid);
            if (!node._processedDeposits) node._processedDeposits = new Set();
            node._processedDeposits.add(sig.xmrTxid);
            stateChanged = true;
          } else {
            filtered.push(sig);
          }
        }
        if (stateChanged) {
          try { saveBridgeState(node); } catch {}
        }
        signatures = filtered;
      }
      return respond( 200, { signatures });
    }
    if (pathname === '/bridge/burn/ready-signature' && req.method === 'GET') {
      if (BURN_READY_SIGNATURE_AUTH) {
        const expected = typeof authToken === 'string' ? authToken : '';
        const token = extractBearerToken(req);
        if (!token || !expected || !constantTimeCompare(token, expected)) {
          return respond(401, { error: 'Burn ready signature auth required' });
        }
      }
      const clusterId = sanitizeBytes32(params.get('clusterId') || node._activeClusterId);
      if (!clusterId) {
        return respond(400, { error: 'Invalid clusterId' });
      }
      const bridgeAddrRaw = node.bridge ? node.bridge.target || node.bridge.address : null;
      let bridgeAddr;
      try {
        bridgeAddr = ethers.getAddress(String(bridgeAddrRaw || ''));
      } catch {
        bridgeAddr = null;
      }
      if (!bridgeAddr) {
        return respond( 503, { error: 'Bridge not configured' });
      }
      if (!node.wallet || typeof node.wallet.signMessage !== 'function') {
        return respond( 503, { error: 'Wallet not available' });
      }
      const chainIdNum = Number(node.chainId || 0);
      if (!Number.isFinite(chainIdNum) || chainIdNum <= 0) {
        return respond( 503, { error: 'Chain ID not available' });
      }
      const nowMs = Date.now();
      let nowSec = Math.floor(nowMs / 1000);
      try {
        const cachedBlock = node._latestBlockCache;
        if (cachedBlock && cachedBlock.block && typeof cachedBlock.fetchedAtMs === 'number' && (nowMs - cachedBlock.fetchedAtMs) < 1500) {
          const ts = cachedBlock.block.timestamp;
          if (typeof ts === 'number' && Number.isFinite(ts) && ts > 0) nowSec = ts;
        } else if (node.provider && typeof node.provider.getBlock === 'function') {
          const block = await node.provider.getBlock('latest');
          if (block) {
            let ts = null;
            if (typeof block.timestamp === 'number') ts = block.timestamp;
            else if (typeof block.timestamp === 'bigint') ts = Number(block.timestamp);
            if (ts && Number.isFinite(ts) && ts > 0) {
              nowSec = Math.floor(ts);
              const num = typeof block.number === 'number' && Number.isFinite(block.number) ? block.number : null;
              node._latestBlockCache = { fetchedAtMs: nowMs, block: { number: num, timestamp: nowSec } };
            }
          }
        }
      } catch {}
      let maxAgeSec = null;
      let maxFutureSec = null;
      const cached = node._burnReadyTimingCache;
      if (cached && typeof cached.fetchedAtMs === 'number' && (nowMs - cached.fetchedAtMs) < 10000) {
        maxAgeSec = cached.maxAgeSec;
        maxFutureSec = cached.maxFutureSec;
      } else {
        try {
          if (node.bridge && typeof node.bridge.burnHeartbeatMaxAgeSec === 'function') {
            const v = await node.bridge.burnHeartbeatMaxAgeSec();
            maxAgeSec = typeof v === 'bigint' ? Number(v) : Number(v);
          }
        } catch {}
        try {
          if (node.bridge && typeof node.bridge.BURN_READY_MAX_FUTURE_SEC === 'function') {
            const v = await node.bridge.BURN_READY_MAX_FUTURE_SEC();
            maxFutureSec = typeof v === 'bigint' ? Number(v) : Number(v);
          }
        } catch {}
        maxAgeSec = Number.isFinite(maxAgeSec) && maxAgeSec >= 0 ? Math.floor(maxAgeSec) : 60;
        maxFutureSec = Number.isFinite(maxFutureSec) && maxFutureSec >= 0 ? Math.floor(maxFutureSec) : 120;
        node._burnReadyTimingCache = { fetchedAtMs: nowMs, maxAgeSec, maxFutureSec };
      }
      const timestampRaw = params.get('timestamp');
      let timestampSec = nowSec;
      if (timestampRaw != null && timestampRaw !== '') {
        const parsed = sanitizeIntParam(timestampRaw, null, 1);
        if (parsed == null) {
          return respond(400, { error: 'Invalid timestamp' });
        }
        timestampSec = parsed;
      }
      if (maxFutureSec > 0 && timestampSec > nowSec + maxFutureSec) {
        return respond( 400, { error: 'Timestamp too far' });
      }
      if (maxAgeSec > 0 && nowSec > timestampSec + maxAgeSec) {
        return respond( 400, { error: 'Timestamp too old' });
      }
      const messageHash = ethers.solidityPackedKeccak256(
        ['string', 'address', 'uint256', 'bytes32', 'uint256'],
        ['burn-ready-v1', bridgeAddr, BigInt(chainIdNum), clusterId, BigInt(timestampSec)],
      );
      let signature;
      try {
        signature = await node.wallet.signMessage(ethers.getBytes(messageHash));
      } catch (e) {
        return respond( 500, { error: 'Signature failed', message: e.message || String(e) });
      }
      return respond( 200, {
        clusterId,
        timestamp: timestampSec,
        signer: node.wallet.address,
        messageHash,
        signature,
      });
    }
    if (pathname === '/bridge/batches' && req.method === 'GET') {
      const batches = getAllBatches(node);
      return respond( 200, { batches });
    }
    if (pathname.startsWith('/bridge/batch/') && req.method === 'GET') {
      const merkleRoot = sanitizeBytes32(pathname.slice(14));
      if (!merkleRoot) {
        return respond(400, { error: 'Invalid merkle root' });
      }
      const batch = getBatchInfo(node, merkleRoot);
      if (!batch) {
        return respond( 404, { error: 'Batch not found' });
      }
      return respond( 200, batch);
    }
    if (pathname.startsWith('/bridge/claim/') && req.method === 'GET') {
      const depositId = sanitizeBytes32(pathname.slice(14));
      if (!depositId) {
        return respond(400, { error: 'Invalid deposit ID' });
      }
      const claimData = getBatchClaimData(node, depositId);
      if (!claimData) {
        return respond( 404, { error: 'Claim data not found (deposit may not be batched yet or use legacy mint)' });
      }
      return respond( 200, claimData);
    }
    if (pathname === '/bridge/deposit/queue-status' && req.method === 'GET') {
      const status = getDepositQueueStatus(node);
      return respond( 200, status);
    }
    if (pathname.startsWith('/bridge/deposit-status/') && req.method === 'GET') {
      const xmrTxid = pathname.slice(23).trim().toLowerCase();
      if (!xmrTxid || xmrTxid.length != 64 || !/^[0-9a-f]+$/.test(xmrTxid)) {
        return respond( 400, { error: 'Invalid XMR txid' });
      }
      const status = getDepositStatus(node, xmrTxid);
      return respond( 200, status);
    }
    if (pathname === '/bridge/txs' && req.method === 'GET') {
      if (!node.bridge || !node.provider) {
        return respond( 503, { error: 'Bridge contract or provider not configured' });
      }
      const limitParam = params && typeof params.get === 'function' ? params.get('limit') : null;
      const blocksParam = params && typeof params.get === 'function' ? params.get('blocks') : null;
      const envLimitRaw = process.env.BRIDGE_TX_FEED_LIMIT;
      const envLookbackRaw = process.env.BRIDGE_TX_FEED_BLOCKS;
      const limit =
        limitParam != null && limitParam !== ''
          ? sanitizeIntParam(limitParam, null, 1, 100)
          : sanitizeIntParam(envLimitRaw, 10, 1, 1000);
      if (limit == null) {
        return respond(400, { error: 'Invalid limit' });
      }
      const lookback =
        blocksParam != null && blocksParam !== ''
          ? sanitizeIntParam(blocksParam, null, 1, 50000)
          : sanitizeIntParam(envLookbackRaw, 20000, 1, 500000);
      if (lookback == null) {
        return respond(400, { error: 'Invalid blocks' });
      }
      try {
        const clusterIdFilter = node._activeClusterId || null;
        const cache = getBridgeTxFeedCache(node);
        const cacheKey = `bridge:txs:${clusterIdFilter || 'all'}:${lookback}:${limit}`;
        const now = Date.now();
        const cached = cache.get(cacheKey);
        if (cached && cached.value && now - cached.ts <= BRIDGE_TX_FEED_CACHE_TTL_MS) {
          return respond( 200, cached.value );
        }
        if (cached && cached.promise && now - cached.ts <= BRIDGE_TX_FEED_INFLIGHT_TTL_MS) {
          try {
            const value = await cached.promise;
            return respond( 200, value );
          } catch {
            cache.delete(cacheKey);
          }
        }
        const promise = (async () => {
          const currentBlock = await node.provider.getBlockNumber();
          const fromBlock = Math.max(0, currentBlock - lookback);
          const txs = [];
          const mintFilter = clusterIdFilter
            ? node.bridge.filters.TokensMinted(null, clusterIdFilter)
            : node.bridge.filters.TokensMinted();
          const burnFilter = clusterIdFilter
            ? node.bridge.filters.TokensBurned(null, clusterIdFilter)
            : node.bridge.filters.TokensBurned();
          let stop = false;
          for (
            let endBlock = currentBlock;
            endBlock >= fromBlock && txs.length < limit && !stop;
            endBlock -= BRIDGE_TX_FEED_QUERY_CHUNK_BLOCKS
          ) {
            const startBlock = Math.max(fromBlock, endBlock - BRIDGE_TX_FEED_QUERY_CHUNK_BLOCKS + 1);
            try {
              const events = await withBackoff(() => node.bridge.queryFilter(mintFilter, startBlock, endBlock));
              for (const ev of events) {
                const args = ev.args || [];
                const user = args[0] ?? args.user ?? null;
                const clusterId = args[1] ?? args.clusterId ?? null;
                const userAmount = args[3] ?? args.userAmount ?? args.amount ?? null;
                const fee = args[4] ?? args.fee ?? null;
                txs.push({
                  type: 'MINT',
                  direction: 'XMR->zXMR',
                  txHash: ev.transactionHash,
                  blockNumber: ev.blockNumber,
                  logIndex: ev.logIndex,
                  user,
                  clusterId,
                  amount: userAmount ? userAmount.toString() : '0',
                  fee: fee ? fee.toString() : '0',
                });
              }
            } catch (e) {
              const msg = getErrorMessage(e);
              const transient = isTransientProviderError(e) || isRateLimitProviderError(e);
              if (!transient) {
                console.log('[BridgeAPI] Failed to query TokensMinted events:', msg);
              }
              if (transient) stop = true;
            }
            try {
              const events = await withBackoff(() => node.bridge.queryFilter(burnFilter, startBlock, endBlock));
              for (const ev of events) {
                const args = ev.args || [];
                const user = args[0] ?? args.user ?? null;
                const clusterId = args[1] ?? args.clusterId ?? null;
                const xmrAddress = args[2] ?? args.xmrAddress ?? null;
                const burnAmount = args[3] ?? args.burnAmount ?? args.amount ?? null;
                const fee = args[4] ?? args.fee ?? null;
                txs.push({
                  type: 'BURN',
                  direction: 'zXMR->XMR',
                  txHash: ev.transactionHash,
                  blockNumber: ev.blockNumber,
                  logIndex: ev.logIndex,
                  user,
                  clusterId,
                  xmrAddress,
                  amount: burnAmount ? burnAmount.toString() : '0',
                  fee: fee ? fee.toString() : '0',
                });
              }
            } catch (e) {
              const msg = getErrorMessage(e);
              const transient = isTransientProviderError(e) || isRateLimitProviderError(e);
              if (!transient) {
                console.log('[BridgeAPI] Failed to query TokensBurned events:', msg);
              }
              if (transient) stop = true;
            }
          }
          txs.sort((a, b) => {
            if (a.blockNumber !== b.blockNumber) return b.blockNumber - a.blockNumber;
            if (a.logIndex != null && b.logIndex != null) return b.logIndex - a.logIndex;
            return 0;
          });
          const out = txs.slice(0, limit);
          const uniqueBlocks = Array.from(
            new Set(out.map((t) => (typeof t.blockNumber === 'number' ? t.blockNumber : null)).filter((n) => n != null)),
          );
          const blockTimestamps = {};
          for (const bn of uniqueBlocks) {
            try {
              const block = await withBackoff(() => node.provider.getBlock(bn));
              if (block && typeof block.timestamp === 'number') {
                blockTimestamps[bn] = block.timestamp;
              }
            } catch (e) {
              const msg = getErrorMessage(e);
              if (isTransientProviderError(e) || isRateLimitProviderError(e)) {
                continue;
              }
              console.log('[BridgeAPI] Failed to fetch block for tx feed (bn=' + bn + '):', msg);
            }
          }
          for (const tx of out) {
            if (tx.blockNumber != null && Object.prototype.hasOwnProperty.call(blockTimestamps, tx.blockNumber)) {
              tx.timestamp = blockTimestamps[tx.blockNumber];
            }
          }
          return { transactions: out };
        })();
        cache.set(cacheKey, { ts: now, promise });
        let value;
        try {
          value = await promise;
        } catch (e) {
          cache.delete(cacheKey);
          throw e;
        }
        cache.set(cacheKey, { ts: Date.now(), value });
        return respond( 200, value );
      } catch (e) {
        console.log('[BridgeAPI] Failed to build tx feed:', getErrorMessage(e));
        return respond( 503, { error: 'Failed to load bridge transactions' });
      }
    }
    if (pathname.startsWith('/bridge/withdrawal/txset/') && req.method === 'GET') {
      const txsetHash = sanitizeBytes32(pathname.split('/').pop());
      if (!txsetHash) {
        return respond( 400, { error: 'Invalid txsetHash' });
      }
      const txDataHex = getTxset(node, txsetHash);
      if (!txDataHex) {
        return respond( 404, { error: 'txset_not_found' });
      }
      return respond( 200, { status: 'found', txsetHash, txDataHex });
    }
    if (pathname === '/bridge/withdrawal/sign-step' && req.method === 'POST') {
      const signStepMaxBodyBytes = (() => {
        const raw = process.env.BRIDGE_API_SIGN_STEP_MAX_BODY_BYTES;
        const parsed = raw != null ? Number(raw) : NaN;
        return Number.isFinite(parsed) && parsed > 0 ? Math.floor(parsed) : 26214400;
      })();
      let body;
      try {
        body = await readRequestBody(req, signStepMaxBodyBytes);
      } catch (e) {
        if (e && e.code === 'BODY_TOO_LARGE') {
          return respond( 413, { error: 'Payload too large' });
        }
        throw e;
      }
      const withdrawalKey =
        body && typeof body === 'object' && typeof (body.withdrawalTxHash || body.withdrawalKey || body.txHash) === 'string'
          ? String(body.withdrawalTxHash || body.withdrawalKey || body.txHash).toLowerCase()
          : '';
      if (!withdrawalKey || withdrawalKey.length > 200 || !/^[0-9a-z:]+$/.test(withdrawalKey)) {
        return respond( 400, { error: 'Invalid withdrawalKey' });
      }
      const stepIndex = sanitizeIntParam(body ? body.stepIndex : null, null, 0, 1000000);
      if (stepIndex === null) {
        return respond( 400, { error: 'Invalid stepIndex' });
      }
      const signer = body && typeof body.signer === 'string' ? body.signer.toLowerCase() : '';
      if (!/^0x[0-9a-f]{40}$/.test(signer)) {
        return respond( 400, { error: 'Invalid signer' });
      }
      const txsetHashIn = sanitizeBytes32(body ? body.txsetHashIn || body.txsetHash : null);
      if (!txsetHashIn) {
        return respond( 400, { error: 'Invalid txsetHashIn' });
      }
      const stale = !!(body && body.stale === true);
      const busy = !!(!stale && body && body.busy === true);
      const txDataHexOut =
        !stale && !busy && body && typeof body.txDataHexOut === 'string'
          ? body.txDataHexOut
          : !stale && !busy && body && typeof body.txDataHex === 'string'
            ? body.txDataHex
            : '';
      if (!stale && !busy && !txDataHexOut) {
        return respond( 400, { error: 'Missing txDataHexOut' });
      }
      const result = recordSignStepResult(node, {
        withdrawalKey,
        stepIndex,
        signer,
        txsetHashIn,
        txDataHexOut,
        stale,
        busy,
      });
      if (!result || !result.ok) {
        const reason = result && result.reason ? result.reason : 'sign_step_rejected';
        const status = reason === 'no_expected_sign_step' || reason === 'txset_hash_in_mismatch' ? 409 : 400;
        return respond( status, { ok: false, error: reason } );
      }
      return respond( 200, result );
    }
    if (pathname.startsWith('/bridge/withdrawal/status/') && req.method === 'GET') {
      const txHash = pathname.split('/').pop();
      if (!txHash || txHash.length < 10) {
        return respond( 400, { error: 'Invalid txHash' });
      }
      const status = getWithdrawalStatus(node, txHash);
      if (status) {
        const { status: internalStatus, ...rest } = status;
        return respond( 200, { status: 'found', internalStatus: internalStatus || null, ...rest });
      }
      return respond( 200, { status: 'not_found', message: 'Withdrawal not yet detected or processed' });
    }
    if (pathname === '/bridge/withdrawals' && req.method === 'GET') {
      const withdrawals = getAllPendingWithdrawals(node);
      return respond( 200, { withdrawals });
    }
    return respond(404, { error: 'Not found' });
  } catch (e) {
    console.log('[BridgeAPI] Request error:', e.message || String(e));
    if (res.headersSent) {
      try { res.end(); } catch {}
      logRequest(clientIP, req.method, pathname, responseStatus, Date.now() - requestStart);
      return;
    }
    responseStatus = 500;
    jsonResponse(res, 500, { error: 'Internal server error' });
  } finally {
    if (logRequest) logRequest(clientIP, req.method, pathname, responseStatus, Date.now() - requestStart);
  }
}
async function getClusterStatus(node) {
  const now = Date.now();
  const moneroHealth = node.moneroHealth || MoneroHealth.HEALTHY;
  const stateSummary = node.stateMachine && typeof node.stateMachine.getSummary === 'function' ? node.stateMachine.getSummary() : null;
  const clusterState = node.clusterState && node.clusterState.state ? node.clusterState.state : null;
  const clusterId = (clusterState && clusterState.clusterId) || node._activeClusterId || null;
  const finalized = clusterState && typeof clusterState.finalized === 'boolean' ? clusterState.finalized : !!node._clusterFinalized;
  const finalAddress = (clusterState && clusterState.finalAddress) || node._clusterFinalAddress || null;
  const selfAddr = node.wallet && node.wallet.address ? node.wallet.address.toLowerCase() : null;
  const membersRaw = clusterState && Array.isArray(clusterState.members) ? clusterState.members : node._clusterMembers || [];
  let members = [];
  const hbRaw = process.env.HEARTBEAT_INTERVAL;
  const hbParsed = hbRaw != null ? Number(hbRaw) : NaN;
  const hbIntervalSec = Number.isFinite(hbParsed) && hbParsed > 0 ? hbParsed : 30;
  const staleThresholdSec = hbIntervalSec * 5;
  let heartbeatMap = new Map();
  const heartbeatTtlMs = staleThresholdSec * 1000;
  if (node.p2p && typeof node.p2p.getHeartbeats === 'function') {
    try {
      heartbeatMap = await node.p2p.getHeartbeats(heartbeatTtlMs);
    } catch {}
  }
  members = membersRaw.map((addr) => {
    const addrLower = String(addr).toLowerCase();
    const hbData = heartbeatMap.get(addrLower);
    const lastHbTs = hbData?.timestamp;
    const apiBase = hbData?.apiBase || '';
    const isSelf = selfAddr ? addrLower === selfAddr : false;
    let lastHeartbeatAgoSec = null;
    let status = 'offline';
    if (isSelf) {
      status = 'healthy';
    } else if (typeof lastHbTs === 'number' && lastHbTs > 0) {
      lastHeartbeatAgoSec = Math.floor((now - lastHbTs) / 1000);
      status = lastHeartbeatAgoSec <= staleThresholdSec ? 'healthy' : 'offline';
    }
    return {
      address: addr,
      isSelf,
      apiBase,
      lastHeartbeatAgoSec,
      status,
      inCluster: true,
    };
  });
  const cooldownUntil = (clusterState && clusterState.cooldownUntil) || node._clusterCooldownUntil || null;
  const cooldownRemainingSec = cooldownUntil && cooldownUntil > now ? Math.floor((cooldownUntil - now) / 1000) : 0;
  const lastFailureAt = (clusterState && clusterState.lastFailureAt) || node._lastClusterFailureAt || null;
  const lastFailureReason = (clusterState && clusterState.lastFailureReason) || node._lastClusterFailureReason || null;
  const p2pConnected = !!(node.p2p && node.p2p.node);
  const p2pConnectedPeers = node.p2p && typeof node.p2p._connectedPeers === 'number' ? node.p2p._connectedPeers : 0;
  const lastHeartbeatAgoSec = node._lastHeartbeatAt && node._lastHeartbeatAt > 0 ? Math.floor((now - node._lastHeartbeatAt) / 1000) : null;
  const allMembersHealthy = members.length > 0 && members.every((m) => m.status === 'healthy');
  const currentState = stateSummary && stateSummary.state ? stateSummary.state : node.stateMachine && node.stateMachine.currentState ? node.stateMachine.currentState : null;
  const eligibleForBridging = !!clusterId && finalized && currentState === 'ACTIVE' && moneroHealth === MoneroHealth.HEALTHY && p2pConnected && allMembersHealthy && isBridgeEnabled();
  const syncMinVisibility = Number(process.env.MIN_P2P_VISIBILITY || members.length || 0);
  const jitterRawEnv = process.env.NON_COORDINATOR_JITTER_MS;
  const jitterParsedEnv = jitterRawEnv != null ? Number(jitterRawEnv) : NaN;
  const nonCoordinatorJitterMs = Number.isFinite(jitterParsedEnv) && jitterParsedEnv >= 0 ? jitterParsedEnv : 5000;
  const warmupRawEnv = process.env.P2P_WARMUP_MS;
  const warmupParsedEnv = warmupRawEnv != null ? Number(warmupRawEnv) : NaN;
  const p2pWarmupMs = Number.isFinite(warmupParsedEnv) && warmupParsedEnv >= 0 ? warmupParsedEnv : 30000;
  const readyRawEnv = process.env.READY_BARRIER_TIMEOUT_MS;
  const readyParsedEnv = readyRawEnv != null ? Number(readyRawEnv) : NaN;
  const readyBarrierTimeoutMs = Number.isFinite(readyParsedEnv) && readyParsedEnv > 0 ? readyParsedEnv : 300000;
  const attemptsRawEnv = process.env.LIVENESS_ATTEMPTS;
  const attemptsParsedEnv = attemptsRawEnv != null ? Number(attemptsRawEnv) : NaN;
  const livenessAttempts = Number.isFinite(attemptsParsedEnv) && attemptsParsedEnv > 0 ? Math.floor(attemptsParsedEnv) : 3;
  const intervalRawEnv = process.env.LIVENESS_ATTEMPT_INTERVAL_MS;
  const intervalParsedEnv = intervalRawEnv != null ? Number(intervalRawEnv) : NaN;
  const livenessAttemptIntervalMs = Number.isFinite(intervalParsedEnv) && intervalParsedEnv >= 0 ? intervalParsedEnv : 30000;
  return {
    clusterState: currentState,
    timeInStateMs: stateSummary && typeof stateSummary.timeInState === 'number' ? stateSummary.timeInState : node.stateMachine && typeof node.stateMachine.timeInState === 'number' ? node.stateMachine.timeInState : null,
    eligibleForBridging,
    syncConfig: { minP2PVisibility: syncMinVisibility, nonCoordinatorJitterMs, p2pWarmupMs, readyBarrierTimeoutMs, livenessAttempts, livenessAttemptIntervalMs },
    cluster: { id: clusterId, finalized, finalAddress, members, size: members.length, coordinator: clusterState && clusterState.coordinator ? clusterState.coordinator : null, coordinatorIndex: clusterState && typeof clusterState.coordinatorIndex === 'number' ? clusterState.coordinatorIndex : null, cooldownUntil, cooldownRemainingSec, lastFailureAt, lastFailureReason },
    monero: { health: moneroHealth, errors: node.moneroErrorCounts || {} },
    p2p: { connected: p2pConnected, connectedPeers: p2pConnectedPeers, lastHeartbeatAgoSec },
  };
}
