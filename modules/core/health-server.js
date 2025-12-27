import http from 'http';
import net from 'net';
import { Buffer } from 'buffer';
import metrics from './metrics.js';
const REQUEST_TIMEOUT_MS = Number(process.env.HEALTH_REQUEST_TIMEOUT_MS) || 5000;
const HEALTH_PORT_RETRY_ATTEMPTS = Number(process.env.HEALTH_PORT_RETRY_ATTEMPTS) || 3;
const HEALTH_PORT_RETRY_DELAY_MS = Number(process.env.HEALTH_PORT_RETRY_DELAY_MS) || 1000;
const ENABLE_CORS = process.env.HEALTH_ENABLE_CORS !== '0';
const RATE_LIMIT_WINDOW_MS = Number(process.env.HEALTH_RATE_LIMIT_WINDOW_MS) || 60000;
const RATE_LIMIT_MAX_REQUESTS = Number(process.env.HEALTH_RATE_LIMIT_MAX_REQUESTS) || 100;
const GRACEFUL_SHUTDOWN_TIMEOUT_MS = Number(process.env.HEALTH_SHUTDOWN_TIMEOUT_MS) || 10000;
const ENABLE_REQUEST_LOGGING = process.env.HEALTH_ENABLE_REQUEST_LOGGING !== '0';
const LOG_SUCCESS_REQUESTS = process.env.HEALTH_LOG_SUCCESS_REQUESTS === '1';
const INFO_DISCLOSURE_LEVEL = process.env.HEALTH_INFO_DISCLOSURE || 'full';
const API_KEY = process.env.HEALTH_API_KEY || null;
class RateLimiter {
  constructor() {
    this.clients = new Map();
    this.cleanupInterval = setInterval(() => this.cleanup(), 60000);
  }
  check(ip) {
    const now = Date.now();
    if (!this.clients.has(ip)) {
      this.clients.set(ip, { count: 1, windowStart: now });
      return true;
    }
    const client = this.clients.get(ip);
    if (now - client.windowStart > RATE_LIMIT_WINDOW_MS) {
      client.count = 1;
      client.windowStart = now;
      return true;
    }
    if (client.count >= RATE_LIMIT_MAX_REQUESTS) {
      return false;
    }
    client.count++;
    return true;
  }
  cleanup() {
    const now = Date.now();
    for (const [ip, client] of this.clients.entries()) {
      if (now - client.windowStart > RATE_LIMIT_WINDOW_MS * 2) {
        this.clients.delete(ip);
      }
    }
  }
  destroy() {
    if (this.cleanupInterval) {
      clearInterval(this.cleanupInterval);
    }
    this.clients.clear();
  }
}
function safeStringify(obj, pretty = false) {
  const seen = new WeakSet();
  const replacer = (key, value) => {
    if (value === undefined) return null;
    if (typeof value === 'bigint') return value.toString();
    if (typeof value === 'object' && value !== null) {
      if (seen.has(value)) return '[Circular]';
      seen.add(value);
    }
    return value;
  };
  try {
    return JSON.stringify(obj, replacer, pretty ? 2 : 0);
  } catch (e) {
    const msg = e && e.message ? e.message : String(e);
    return JSON.stringify({ error: 'serialization_failed', message: msg });
  }
}
function sanitizeForLog(str) {
  if (!str) return '';
  return String(str).replace(/[\x00-\x1F\x7F]/g, '').slice(0, 200);
}
function getClientIp(req) {
  return req.headers['x-forwarded-for']?.split(',')[0]?.trim() ||
         req.headers['x-real-ip'] ||
         req.socket.remoteAddress ||
         'unknown';
}
function isPortAvailable(port) {
  return new Promise((resolve) => {
    const server = net.createServer();
    server.once('error', () => resolve(false));
    server.once('listening', () => {
      server.close();
      resolve(true);
    });
    server.listen(port, '0.0.0.0');
  });
}
async function waitForPortWithRetry(port, attempts) {
  for (let i = 0; i < attempts; i++) {
    const available = await isPortAvailable(port);
    if (available) return true;
    if (i < attempts - 1) {
      await new Promise(r => setTimeout(r, HEALTH_PORT_RETRY_DELAY_MS));
    }
  }
  return false;
}
function checkAuthentication(req) {
  if (!API_KEY) return true;
  const authHeader = req.headers['authorization'];
  const apiKeyHeader = req.headers['x-api-key'];
  if (authHeader && authHeader.startsWith('Bearer ')) {
    return authHeader.slice(7) === API_KEY;
  }
  if (apiKeyHeader) {
    return apiKeyHeader === API_KEY;
  }
  return false;
}
async function getHealthData(node, depth = 'shallow') {
  const basic = {
    status: 'ok',
    timestamp: new Date().toISOString(),
    uptime: Math.floor(process.uptime()),
  };
  if (INFO_DISCLOSURE_LEVEL === 'none') {
    return basic;
  }
  if (depth === 'shallow') {
    return {
      ...basic,
      ready: node.p2p && node.p2p.connected && !node._rpcCircuitOpen,
    };
  }
  const health = { ...basic };
  if (INFO_DISCLOSURE_LEVEL === 'minimal') {
    health.cluster = { active: !!node._activeClusterId };
    health.p2p = { connected: node.p2p ? node.p2p.connected : false };
    health.monero = { health: node.moneroHealth || 'unknown' };
  } else {
    health.cluster = {
      active: node._activeClusterId || null,
      finalized: node._clusterFinalized || false,
      members: node._clusterMembers ? node._clusterMembers.length : 0,
      multisigAddress: node._clusterFinalAddress || null,
    };
    health.p2p = {
      connected: node.p2p ? node.p2p.connected : false,
      peers: node.p2p ? node.p2p._connectedPeers : 0,
    };
    health.monero = {
      health: node.moneroHealth || 'unknown',
      rpcCircuitOpen: node._rpcCircuitOpen || false,
    };
    if (node._clusterFinalized && node.monero && typeof node.monero.queryKey === 'function') {
      const now = Date.now();
      const cached = node._viewKeyCache;
      if (!cached || !cached.ts || now - cached.ts > 600000) {
        try {
          const viewKey = await Promise.race([
            node.monero.queryKey('view_key'),
            new Promise((_, reject) => setTimeout(() => reject(new Error('timeout')), 2000))
          ]);
          if (typeof viewKey === 'string' && viewKey) {
            node._viewKeyCache = { ts: now, value: viewKey };
          }
        } catch {}
      }
      if (node._viewKeyCache && typeof node._viewKeyCache.value === 'string') {
        health.monero.viewKey = node._viewKeyCache.value;
      }
    }
  }
  if (depth === 'full') {
    try {
      if (node.provider) {
        const blockNumber = await Promise.race([
          node.provider.getBlockNumber(),
          new Promise((_, reject) => setTimeout(() => reject(new Error('timeout')), 2000))
        ]);
        health.ethereum = { connected: true, blockNumber };
      } else {
        health.ethereum = { connected: false };
      }
    } catch (e) {
      health.ethereum = { connected: false, error: e.message };
    }
    if (node.monero && typeof node.monero.getBalance === 'function') {
      try {
        await Promise.race([
          node.monero.getBalance(),
          new Promise((_, reject) => setTimeout(() => reject(new Error('timeout')), 2000))
        ]);
        health.monero.rpcConnected = true;
      } catch (e) {
        health.monero.rpcConnected = false;
        health.monero.rpcError = e.message;
      }
    }
  }
  return health;
}
function isReady(node) {
  if (!node.p2p || !node.p2p.connected) return false;
  if (node._rpcCircuitOpen) return false;
  if (!node.provider) return false;
  if (node._selfSlashed) return false;
  return true;
}
function createHealthServer(node, port = 3003) {
  const rateLimiter = new RateLimiter();
  const connections = new Set();
  let isShuttingDown = false;
  metrics.setMetadata('http_requests_total', 'Total HTTP requests to health server', 'counter');
  metrics.setMetadata('http_request_duration_seconds', 'HTTP request duration', 'histogram');
  metrics.setMetadata('http_errors_total', 'Total HTTP errors', 'counter');
  const server = http.createServer(async (req, res) => {
    const startTime = Date.now();
    const clientIp = getClientIp(req);
    const timeout = setTimeout(() => {
      if (!res.headersSent) {
        res.writeHead(408, { 'Content-Type': 'text/plain' });
        res.end('Request Timeout');
        metrics.incrementCounter('http_errors_total', 1, { type: 'timeout' });
      }
    }, REQUEST_TIMEOUT_MS);
    res.on('finish', () => clearTimeout(timeout));
    if (isShuttingDown) {
      res.writeHead(503, { 'Content-Type': 'text/plain' });
      res.end('Server shutting down');
      return;
    }
    if (!rateLimiter.check(clientIp)) {
      res.writeHead(429, { 'Content-Type': 'text/plain' });
      res.end('Rate limit exceeded');
      metrics.incrementCounter('http_errors_total', 1, { type: 'rate_limit' });
      if (ENABLE_REQUEST_LOGGING) {
        console.log(`[Health] 429 ${sanitizeForLog(req.method)} ${sanitizeForLog(req.url)} - ${clientIp} - rate limited`);
      }
      return;
    }
    if (!checkAuthentication(req)) {
      res.writeHead(401, { 'Content-Type': 'text/plain' });
      res.end('Unauthorized');
      metrics.incrementCounter('http_errors_total', 1, { type: 'unauthorized' });
      if (ENABLE_REQUEST_LOGGING) {
        console.log(`[Health] 401 ${sanitizeForLog(req.method)} ${sanitizeForLog(req.url)} - ${clientIp}`);
      }
      return;
    }
    if (req.method !== 'GET' && req.method !== 'HEAD') {
      res.writeHead(405, { 'Content-Type': 'text/plain', 'Allow': 'GET, HEAD' });
      res.end('Method Not Allowed');
      metrics.incrementCounter('http_errors_total', 1, { type: 'method_not_allowed' });
      if (ENABLE_REQUEST_LOGGING) {
        console.log(`[Health] 405 ${sanitizeForLog(req.method)} ${sanitizeForLog(req.url)} - ${clientIp}`);
      }
      return;
    }
    const corsHeaders = ENABLE_CORS ? {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET, HEAD',
      'Access-Control-Allow-Headers': 'Content-Type',
    } : {};
    if (req.url === '/health' || req.url === '/healthz') {
      try {
        const url = new URL(req.url, `http://${req.headers.host}`);
        const depth = url.searchParams.get('depth') || 'deep';
        const pretty = url.searchParams.get('pretty') === '1';
        const healthData = await getHealthData(node, depth);
        const body = safeStringify(healthData, pretty);
        const headers = {
          'Content-Type': 'application/json',
          'Content-Length': Buffer.byteLength(body),
          ...corsHeaders,
        };
        res.writeHead(200, headers);
        res.end(body);
        metrics.incrementCounter('http_requests_total', 1, { endpoint: 'health', status: '200' });
        if (ENABLE_REQUEST_LOGGING && LOG_SUCCESS_REQUESTS) {
          console.log(`[Health] 200 ${req.method} ${sanitizeForLog(req.url)} - ${clientIp} - ${Date.now() - startTime}ms`);
        }
      } catch (e) {
        const body = safeStringify({ status: 'error', error: e.message });
        res.writeHead(500, {
          'Content-Type': 'application/json',
          'Content-Length': Buffer.byteLength(body),
          ...corsHeaders,
        });
        res.end(body);
        metrics.incrementCounter('http_errors_total', 1, { type: 'internal_error', endpoint: 'health' });
      }
    } else if (req.url === '/metrics') {
      try {
        const body = metrics.toPrometheus();
        const headers = {
          'Content-Type': 'text/plain; version=0.0.4',
          'Content-Length': Buffer.byteLength(body),
          ...corsHeaders,
        };
        res.writeHead(200, headers);
        res.end(body);
        metrics.incrementCounter('http_requests_total', 1, { endpoint: 'metrics', status: '200' });
        if (ENABLE_REQUEST_LOGGING && LOG_SUCCESS_REQUESTS) {
          console.log(`[Health] 200 ${req.method} /metrics - ${clientIp} - ${Date.now() - startTime}ms`);
        }
      } catch {
        res.writeHead(500, { 'Content-Type': 'text/plain', ...corsHeaders });
        res.end('Internal Server Error');
        metrics.incrementCounter('http_errors_total', 1, { type: 'internal_error', endpoint: 'metrics' });
      }
    } else if (req.url === '/ready' || req.url === '/readyz') {
      const ready = isReady(node);
      const statusCode = ready ? 200 : 503;
      const body = safeStringify({ ready });
      const headers = {
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(body),
        ...corsHeaders,
      };
      res.writeHead(statusCode, headers);
      res.end(body);
      metrics.incrementCounter('http_requests_total', 1, { endpoint: 'ready', status: String(statusCode) });
      if (ENABLE_REQUEST_LOGGING && (LOG_SUCCESS_REQUESTS || statusCode !== 200)) {
        console.log(`[Health] ${statusCode} ${req.method} ${sanitizeForLog(req.url)} - ${clientIp} - ${Date.now() - startTime}ms`);
      }
    } else if (req.url === '/livez') {
      const body = safeStringify({ alive: true });
      const headers = {
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(body),
        ...corsHeaders,
      };
      res.writeHead(200, headers);
      res.end(body);
      metrics.incrementCounter('http_requests_total', 1, { endpoint: 'livez', status: '200' });
      if (ENABLE_REQUEST_LOGGING && LOG_SUCCESS_REQUESTS) {
        console.log(`[Health] 200 ${req.method} /livez - ${clientIp} - ${Date.now() - startTime}ms`);
      }
    } else if (req.url === '/withdrawal-status' || req.url.startsWith('/withdrawal-status?')) {
      try {
        let statusData = { available: false, reason: 'withdrawal_status_not_available' };
        if (node.getWithdrawalEpochStatus && typeof node.getWithdrawalEpochStatus === 'function') {
          statusData = node.getWithdrawalEpochStatus();
        }
        const body = safeStringify(statusData);
        const headers = {
          'Content-Type': 'application/json',
          'Content-Length': Buffer.byteLength(body),
          ...corsHeaders,
        };
        res.writeHead(200, headers);
        res.end(body);
        metrics.incrementCounter('http_requests_total', 1, { endpoint: 'withdrawal-status', status: '200' });
        if (ENABLE_REQUEST_LOGGING && LOG_SUCCESS_REQUESTS) {
          console.log(`[Health] 200 ${req.method} /withdrawal-status - ${clientIp} - ${Date.now() - startTime}ms`);
        }
      } catch (e) {
        const body = safeStringify({ error: e.message });
        res.writeHead(500, { 'Content-Type': 'application/json', ...corsHeaders });
        res.end(body);
        metrics.incrementCounter('http_errors_total', 1, { type: 'internal_error', endpoint: 'withdrawal-status' });
      }
    } else if (req.url === '/bribe-status' || req.url.startsWith('/bribe-status?')) {
      try {
        let statusData = { available: false, reason: 'bribe_status_not_available' };
        if (node.getBribeStatus && typeof node.getBribeStatus === 'function') {
          statusData = await node.getBribeStatus();
        }
        const body = safeStringify(statusData);
        const headers = {
          'Content-Type': 'application/json',
          'Content-Length': Buffer.byteLength(body),
          ...corsHeaders,
        };
        res.writeHead(200, headers);
        res.end(body);
        metrics.incrementCounter('http_requests_total', 1, { endpoint: 'bribe-status', status: '200' });
        if (ENABLE_REQUEST_LOGGING && LOG_SUCCESS_REQUESTS) {
          console.log(`[Health] 200 ${req.method} /bribe-status - ${clientIp} - ${Date.now() - startTime}ms`);
        }
      } catch (e) {
        const body = safeStringify({ error: e.message });
        res.writeHead(500, { 'Content-Type': 'application/json', ...corsHeaders });
        res.end(body);
        metrics.incrementCounter('http_errors_total', 1, { type: 'internal_error', endpoint: 'bribe-status' });
      }
    } else {
      res.writeHead(404, { 'Content-Type': 'text/plain', ...corsHeaders });
      res.end('Not Found');
      metrics.incrementCounter('http_errors_total', 1, { type: 'not_found' });
      if (ENABLE_REQUEST_LOGGING) {
        console.log(`[Health] 404 ${req.method} ${sanitizeForLog(req.url)} - ${clientIp} - ${Date.now() - startTime}ms`);
      }
    }
    const duration = (Date.now() - startTime) / 1000;
    metrics.recordHistogram('http_request_duration_seconds', duration);
  });
  server.on('connection', (conn) => {
    connections.add(conn);
    conn.on('close', () => connections.delete(conn));
  });
  server.on('error', (err) => {
    console.error('[HealthServer] Error:', err.message);
    metrics.incrementCounter('http_errors_total', 1, { type: 'server_error' });
  });
  server.shutdown = async () => {
    isShuttingDown = true;
    console.log('[HealthServer] Initiating graceful shutdown...');
    return new Promise((resolve) => {
      const forceTimeout = setTimeout(() => {
        console.log('[HealthServer] Force closing remaining connections');
        connections.forEach(conn => conn.destroy());
        resolve();
      }, GRACEFUL_SHUTDOWN_TIMEOUT_MS);
      server.close(() => {
        clearTimeout(forceTimeout);
        rateLimiter.destroy();
        console.log('[HealthServer] Shutdown complete');
        resolve();
      });
      connections.forEach(conn => {
        if (!conn.destroyed) {
          conn.end();
        }
      });
    });
  };
  server.startWithRetry = async (targetPort) => {
    const portAvailable = await waitForPortWithRetry(targetPort, HEALTH_PORT_RETRY_ATTEMPTS);
    if (!portAvailable) {
      throw new Error(`Port ${targetPort} unavailable after ${HEALTH_PORT_RETRY_ATTEMPTS} attempts`);
    }
    return new Promise((resolve, reject) => {
      server.listen(targetPort, '0.0.0.0', (err) => {
        if (err) {
          reject(err);
        } else {
          console.log(`[Health] Server listening on port ${targetPort}`);
          resolve(server);
        }
      });
    });
  };
  return server;
}
export { createHealthServer };
