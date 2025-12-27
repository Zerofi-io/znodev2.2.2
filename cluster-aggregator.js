import http from 'http';
import https from 'https';
import dotenv from 'dotenv';
import fs from 'fs';
import { ethers } from 'ethers';
import { registryABI, stakingABI } from './modules/app/contracts/abis.js';
import net from 'net';
import { Buffer } from 'buffer';
import { lookup } from 'dns/promises';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const dotenvResult = dotenv.config({ path: __dirname + '/.env' });
if (dotenvResult.error) {
  console.warn('[ClusterAgg] Warning: failed to load .env file:', dotenvResult.error.message);
  console.warn('[ClusterAgg] Proceeding with existing environment variables.');
}

function envNumber(name, fallback, min, max) {
  const raw = process.env[name];
  const parsed = raw != null ? Number(raw) : NaN;
  let v = Number.isFinite(parsed) ? parsed : fallback;
  if (min != null && v < min) v = min;
  if (max != null && v > max) v = max;
  return v;
}

function envInt(name, fallback, min, max) {
  return Math.floor(envNumber(name, fallback, min, max));
}

const CORS_ORIGIN = process.env.CLUSTER_AGG_CORS_ORIGIN || '*';
const TRUST_PROXY = process.env.CLUSTER_AGG_TRUST_PROXY === '1';
const RATE_LIMIT_WINDOW_MS = envInt('CLUSTER_AGG_RATE_LIMIT_WINDOW_MS', 60000, 1000, 3600000);
const RATE_LIMIT_MAX_GET = envInt('CLUSTER_AGG_RATE_LIMIT_MAX_GET', 6000, 1, 1000000);
const RATE_LIMIT_MAX_POST = envInt('CLUSTER_AGG_RATE_LIMIT_MAX_POST', 600, 1, 1000000);
const MAX_BODY_BYTES = envInt('CLUSTER_AGG_MAX_BODY_BYTES', 32768, 1024, 1048576);
const MAX_NODE_RESPONSE_BYTES = envInt('CLUSTER_AGG_MAX_NODE_RESPONSE_BYTES', 2097152, 1024, 52428800);
const MAX_RATE_LIMIT_ENTRIES = envInt('CLUSTER_AGG_MAX_RATE_LIMIT_ENTRIES', 50000, 1000, 500000);
let NODE_AUTH_TOKEN = (process.env.CLUSTER_AGG_NODE_AUTH_TOKEN || '').trim();
if (!NODE_AUTH_TOKEN) {
  const candidates = [];
  const dataDir = (process.env.BRIDGE_DATA_DIR || '').trim();
  if (dataDir) candidates.push(join(dataDir, 'bridge_api_auth.json'));
  const homeDir = (process.env.HOME || '').trim();
  if (homeDir) candidates.push(join(homeDir, '.znode-bridge', 'bridge_api_auth.json'));
  candidates.push(join(__dirname, '.znode-bridge', 'bridge_api_auth.json'));
  for (const p of candidates) {
    try {
      const raw = fs.readFileSync(p, 'utf8');
      const data = JSON.parse(raw);
      const token = data && typeof data === 'object' ? data.token : null;
      if (typeof token === 'string' && token.trim()) {
        NODE_AUTH_TOKEN = token.trim();
        break;
      }
    } catch {}
  }
}

const DISCOVERY_ENABLED = process.env.CLUSTER_AGG_ENABLE_DISCOVERY !== '0';
const MAX_DISCOVERED_NODES = envInt('CLUSTER_AGG_MAX_DISCOVERED_NODES', 2000, 0, 200000);
const DNS_CACHE_TTL_MS = envInt('CLUSTER_AGG_DNS_CACHE_TTL_MS', 300000, 1000, 86400000);
const MAX_DNS_CACHE_ENTRIES = envInt('CLUSTER_AGG_MAX_DNS_CACHE_ENTRIES', 10000, 100, 1000000);
const MAX_CONCURRENT_REQUESTS = envInt('CLUSTER_AGG_MAX_CONCURRENT_REQUESTS', 20, 1, 100);
let allowedPorts = new Set(
  (process.env.CLUSTER_AGG_ALLOWED_NODE_PORTS || '3002,3003,443')
    .split(',')
    .map((s) => s.trim())
    .filter((s) => s.length > 0)
    .map((s) => Number(s))
    .filter((n) => Number.isFinite(n) && n > 0 && n <= 65535)
    .map((n) => String(Math.floor(n))),
);
if (allowedPorts.size == 0) allowedPorts = new Set(['3002', '3003', '443']);
const ALLOWED_NODE_PORTS = allowedPorts;
const rateLimitMap = new Map();
const dnsCache = new Map();

const NODE_TLS_KEY_PATH = (process.env.BRIDGE_API_TLS_KEY_PATH || '').trim();
const NODE_TLS_CERT_PATH = (process.env.BRIDGE_API_TLS_CERT_PATH || '').trim();
const NODE_TLS_CA_PATH = (process.env.BRIDGE_API_TLS_CA_PATH || '').trim();
let _nodeHttpAgent = null;
let _nodeHttpsAgent = null;
function getNodeHttpAgent() {
  if (_nodeHttpAgent) return _nodeHttpAgent;
  const keepAlive = process.env.CLUSTER_AGG_NODE_KEEP_ALIVE !== '0';
  _nodeHttpAgent = new http.Agent({ keepAlive, keepAliveMsecs: 10000, maxSockets: 64, maxFreeSockets: 16 });
  return _nodeHttpAgent;
}
function getNodeHttpsAgent() {
  if (_nodeHttpsAgent) return _nodeHttpsAgent;
  const keepAlive = process.env.CLUSTER_AGG_NODE_KEEP_ALIVE !== '0';
  const agentOpts = { keepAlive, keepAliveMsecs: 10000, maxSockets: 64, maxFreeSockets: 16 };
  const hasTlsFiles = NODE_TLS_KEY_PATH && NODE_TLS_CERT_PATH && NODE_TLS_CA_PATH;
  if (hasTlsFiles) {
    _nodeHttpsAgent = new https.Agent({
      ...agentOpts,
      key: fs.readFileSync(NODE_TLS_KEY_PATH),
      cert: fs.readFileSync(NODE_TLS_CERT_PATH),
      ca: fs.readFileSync(NODE_TLS_CA_PATH),
      rejectUnauthorized: true,
      minVersion: 'TLSv1.2',
    });
    return _nodeHttpsAgent;
  }
  _nodeHttpsAgent = new https.Agent(agentOpts);
  return _nodeHttpsAgent;
}

async function promiseAllSettledWithConcurrency(fns, limit) {
  const results = new Array(fns.length);
  let idx = 0;
  const execute = async () => {
    while (idx < fns.length) {
      const i = idx++;
      try {
        results[i] = { status: 'fulfilled', value: await fns[i]() };
      } catch (e) {
        results[i] = { status: 'rejected', reason: e };
      }
    }
  };
  const workers = [];
  for (let i = 0; i < Math.min(limit, fns.length); i++) {
    workers.push(execute());
  }
  await Promise.all(workers);
  return results;
}
function getClientIP(req) {
  if (TRUST_PROXY) {
    const xff = req.headers['x-forwarded-for'];
    if (typeof xff === 'string' && xff) {
      const first = xff.split(',')[0];
      if (first) return first.trim();
    }
  }
  return req.socket?.remoteAddress || req.connection?.remoteAddress || 'unknown';
}

function checkRateLimit(req, bucket, maxRequests) {
  const ip = getClientIP(req);
  const key = ip + '|' + bucket;
  const now = Date.now();
  let info = rateLimitMap.get(key);
  if (!info || now - info.windowStart > RATE_LIMIT_WINDOW_MS) {
    info = { windowStart: now, count: 0 };
  }
  info.count += 1;
  rateLimitMap.set(key, info);
  if (rateLimitMap.size > MAX_RATE_LIMIT_ENTRIES) {
    const cutoff = now - RATE_LIMIT_WINDOW_MS;
    for (const [k, v] of rateLimitMap) {
      if (v.windowStart < cutoff) rateLimitMap.delete(k);
    }
  }
  if (info.count > maxRequests) {
    const retryAfter = Math.ceil((info.windowStart + RATE_LIMIT_WINDOW_MS - now) / 1000);
    return { ok: false, retryAfter };
  }
  return { ok: true, retryAfter: 0 };
}

function isPrivateIPv4(ip) {
  const parts = ip.split('.');
  if (parts.length != 4) return true;
  const a = Number(parts[0]);
  const b = Number(parts[1]);
  if (!Number.isFinite(a) || !Number.isFinite(b)) return true;
  if (a == 10) return true;
  if (a == 172 && b >= 16 && b <= 31) return true;
  if (a == 192 && b == 168) return true;
  if (a == 127) return true;
  if (a == 169 && b == 254) return true;
  if (a == 100 && b >= 64 && b <= 127) return true;
  if (a == 198 && (b == 18 || b == 19)) return true;
  if (a == 0 || a >= 224) return true;
  return false;
}

function isPrivateIPv6(ip) {
  const lower = ip.toLowerCase();
  if (!lower || lower == '::') return true;
  if (lower == '::1' || lower == '0:0:0:0:0:0:0:1') return true;
  if (lower.startsWith('fe8') || lower.startsWith('fe9') || lower.startsWith('fea') || lower.startsWith('feb')) return true;
  if (lower.startsWith('fc') || lower.startsWith('fd')) return true;
  const idx = lower.indexOf('::ffff:');
  if (idx != -1) {
    const v4 = lower.slice(idx + 7);
    if (v4 && v4.indexOf('.') != -1) return isPrivateIPv4(v4);
  }
  return false;
}

function isPrivateIP(ip) {
  const v = net.isIP(ip);
  if (v == 4) return isPrivateIPv4(ip);
  if (v == 6) return isPrivateIPv6(ip);
  return true;
}

function normalizeNodeBase(value) {
  if (typeof value !== 'string') return null;
  const s = value.trim();
  if (!s || s.length > 256) return null;
  let u;
  try {
    u = new URL(s);
  } catch {
    return null;
  }
  if (u.username || u.password) return null;
  if (u.protocol !== 'http:' && u.protocol !== 'https:') return null;
  const port = u.port ? u.port : u.protocol === 'https:' ? '443' : '80';
  if (ALLOWED_NODE_PORTS.size > 0 && !ALLOWED_NODE_PORTS.has(port)) return null;
  if (!u.hostname) return null;
  if (net.isIP(u.hostname) && isPrivateIP(u.hostname)) return null;
  let pathname = u.pathname || '';
  if (pathname.endsWith('/')) pathname = pathname.replace(/\/+$/, '');
  const base = `${u.protocol}//${u.host}${pathname}`;
  return base;
}

async function hostnameResolvesPublic(hostname) {
  const now = Date.now();
  const cached = dnsCache.get(hostname);
  if (cached && now - cached.ts < DNS_CACHE_TTL_MS) return cached.ok;
  let ok = false;
  try {
    const records = await lookup(hostname, { all: true, verbatim: true });
    ok = Array.isArray(records) && records.length > 0;
    if (ok) {
      for (const r of records) {
        if (!r || !r.address) {
          ok = false;
          break;
        }
        if (isPrivateIP(r.address)) {
          ok = false;
          break;
        }
      }
    }
  } catch {
    ok = false;
  }
  dnsCache.set(hostname, { ok, ts: now });
  if (dnsCache.size > MAX_DNS_CACHE_ENTRIES) {
    const cutoff = now - DNS_CACHE_TTL_MS;
    for (const [k, v] of dnsCache) {
      if (v.ts < cutoff) dnsCache.delete(k);
    }
    if (dnsCache.size > MAX_DNS_CACHE_ENTRIES) {
      let extra = dnsCache.size - MAX_DNS_CACHE_ENTRIES;
      for (const k of dnsCache.keys()) {
        dnsCache.delete(k);
        extra -= 1;
        if (extra <= 0) break;
      }
    }
  }
  return ok;
}

async function validateNodeBase(value) {
  const base = normalizeNodeBase(value);
  if (!base) return null;
  let u;
  try {
    u = new URL(base);
  } catch {
    return null;
  }
  const hostname = u.hostname;
  if (net.isIP(hostname)) return base;
  const ok = await hostnameResolvesPublic(hostname);
  return ok ? base : null;
}

async function resolvePublicAddress(hostname) {
  const host = String(hostname || '').trim();
  if (!host) return null;
  const ipVersion = net.isIP(host);
  if (ipVersion) {
    if (isPrivateIP(host)) return null;
    return { address: host, family: ipVersion };
  }
  let records;
  try {
    records = await lookup(host, { all: true, verbatim: true });
  } catch {
    return null;
  }
  if (!Array.isArray(records) || records.length === 0) return null;
  for (const r of records) {
    if (!r || !r.address) continue;
    const v = net.isIP(r.address);
    if (!v) continue;
    if (isPrivateIP(r.address)) continue;
    return { address: r.address, family: v };
  }
  return null;
}

function makeFixedLookup(hostname, resolved) {
  const expected = String(hostname || '').toLowerCase();
  const addr = resolved && resolved.address ? resolved.address : null;
  const family = resolved && resolved.family ? resolved.family : 0;
  return (h, opts, cb) => {
    const name = String(h || '').toLowerCase();
    if (!addr || !family) return cb(new Error('lookup_failed'));
    if (name && name != expected) return cb(new Error('unexpected_lookup'));
    cb(null, addr, family);
  };
}

async function safeJsonRequest(url, opts = {}) {
  const method = opts.method || 'GET';
  const timeoutMsRaw = opts.timeoutMs;
  const timeoutMsParsed = timeoutMsRaw != null ? Number(timeoutMsRaw) : NaN;
  const timeoutMs = Number.isFinite(timeoutMsParsed) && timeoutMsParsed >= 0 ? Math.floor(timeoutMsParsed) : NODE_TIMEOUT_MS;
  const allowInvalidJson = opts.allowInvalidJson === true;
  let body = opts.body;
  if (body != null && typeof body !== 'string') body = String(body);
  const headers = opts.headers && typeof opts.headers === 'object' ? { ...opts.headers } : {};
  if (NODE_AUTH_TOKEN && headers.Authorization == null && headers.authorization == null) {
    headers.Authorization = `Bearer ${NODE_AUTH_TOKEN}`;
  }
  let u;
  try {
    u = new URL(url);
  } catch {
    return { ok: false, error: 'invalid_url' };
  }
  if (u.username || u.password) return { ok: false, error: 'invalid_url' };
  if (u.protocol !== 'http:' && u.protocol !== 'https:') return { ok: false, error: 'invalid_url' };
  const port = u.port ? u.port : u.protocol === 'https:' ? '443' : '80';
  if (ALLOWED_NODE_PORTS.size > 0 && !ALLOWED_NODE_PORTS.has(port)) return { ok: false, error: 'unsafe_node_base' };
  const resolved = await resolvePublicAddress(u.hostname);
  if (!resolved) return { ok: false, error: 'unsafe_node_base' };
  const fixedLookup = makeFixedLookup(u.hostname, resolved);
  const client = u.protocol === 'https:' ? https : http;
  const requestOptions = {
    protocol: u.protocol,
    hostname: u.hostname,
    port: Number(port),
    method,
    path: u.pathname + (u.search || ''),
    headers,
    lookup: fixedLookup,
  };
  requestOptions.agent = u.protocol === 'https:' ? getNodeHttpsAgent() : getNodeHttpAgent();
  if (u.protocol === 'https:') requestOptions.servername = u.hostname;
  if (body != null && body !== '' && headers['Content-Length'] == null && headers['content-length'] == null) {
    headers['Content-Length'] = Buffer.byteLength(body);
  }
  try {
    const result = await new Promise((resolve, reject) => {
      const req = client.request(requestOptions, (resp) => {
        const status = resp.statusCode || 0;
        const chunks = [];
        let size = 0;
        resp.on('data', (chunk) => {
          size += chunk.length;
          if (MAX_NODE_RESPONSE_BYTES && size > MAX_NODE_RESPONSE_BYTES) {
            resp.destroy(new Error('response_too_large'));
            return;
          }
          chunks.push(chunk);
        });
        resp.on('end', () => {
          const raw = chunks.length ? Buffer.concat(chunks).toString('utf8') : '';
          resolve({ status, raw });
        });
        resp.on('error', reject);
      });
      if (timeoutMs > 0) {
        req.setTimeout(timeoutMs, () => {
          req.destroy(new Error('timeout'));
        });
      }
      req.on('error', reject);
      if (body != null && body !== '') req.write(body);
      req.end();
    });
    if (result.status < 200 || result.status >= 300) {
      return { ok: false, error: `HTTP ${result.status}`, status: result.status };
    }
    let data;
    try {
      data = result.raw ? JSON.parse(result.raw) : {};
    } catch {
      if (allowInvalidJson) data = {};
      else return { ok: false, error: 'invalid_json', status: result.status };
    }
    return { ok: true, data, status: result.status };
  } catch (e) {
    const msg = e && e.message ? e.message : String(e);
    return { ok: false, error: msg };
  }
}

function readJsonBody(req) {
  return new Promise((resolve, reject) => {
    let raw = '';
    let size = 0;
    const onData = (chunk) => {
      size += chunk.length;
      if (size > MAX_BODY_BYTES) {
        try { req.pause(); } catch {}
        req.removeListener('data', onData);
        req.removeListener('end', onEnd);
        const err = new Error('Body too large');
        err.code = 'BODY_TOO_LARGE';
        reject(err);
        return;
      }
      raw += chunk;
    };
    const onEnd = () => {
      try {
        resolve(raw ? JSON.parse(raw) : {});
      } catch {
        resolve({});
      }
    };
    req.on('data', onData);
    req.on('end', onEnd);
    req.on('error', reject);
  });
}

function sanitizeErrors(errors) {
  const list = Array.isArray(errors) ? errors : [];
  return list.map((e) => {
    const s = typeof e === 'string' ? e : '';
    return s.replace(/^\[[^\]]+\]\s*/, '');
  }).filter((s) => s);
}

function sanitizeCluster(cluster) {
  if (!cluster || typeof cluster !== 'object') return cluster;
  const c = { ...cluster };
  if (Array.isArray(c.nodes)) c.nodes = c.nodes.map(({ apiBase, ...rest }) => rest);
  if (Array.isArray(c.members)) c.members = c.members.map(({ apiBase, ...rest }) => rest);
  return c;
}

function sanitizeAggregate(data) {
  if (!data || typeof data !== 'object') return data;
  const out = { ...data };
  if (Array.isArray(out.clusters)) out.clusters = out.clusters.map(sanitizeCluster);
  out.errors = sanitizeErrors(out.errors);
  return out;
}

function deriveNodeBasesFromEnv() {
  const bases = [];
  const apiScheme = process.env.BRIDGE_API_TLS_ENABLED === '1' ? 'https' : 'http';

  const bridgePortRaw = process.env.BRIDGE_API_PORT;
  const bridgePortParsed = bridgePortRaw != null ? Number(bridgePortRaw) : NaN;
  const bridgePort =
    Number.isFinite(bridgePortParsed) && bridgePortParsed > 0 ? bridgePortParsed : 3002;

  const publicIp = (process.env.PUBLIC_IP || '').trim();
  if (publicIp) {
    bases.push(`${apiScheme}://${publicIp}:${bridgePort}`);
  }

  const peersRaw = process.env.P2P_BOOTSTRAP_PEERS || '';
  if (peersRaw) {
    const tokens = peersRaw
      .split(',')
      .map((s) => s.trim())
      .filter((s) => s.length > 0);

    for (const token of tokens) {
      let host = null;

      const ip4Match = token.match(/\/ip4\/([^/]+)/);
      if (ip4Match && ip4Match[1]) {
        host = ip4Match[1];
      }

      if (!host) {
        const dns4Match = token.match(/\/dns4\/([^/]+)/);
        if (dns4Match && dns4Match[1]) {
          host = dns4Match[1];
        }
      }

      if (!host) {
        const cleaned = token.replace(/^[a-zA-Z]+:\/\//, '');
        const firstSlash = cleaned.indexOf('/');
        const withoutPath = firstSlash >= 0 ? cleaned.slice(0, firstSlash) : cleaned;
        const parts = withoutPath.split(':');
        if (parts[0]) {
          host = parts[0];
        }
      }

      if (host && (!publicIp || host !== publicIp)) {
        bases.push(`${apiScheme}://${host}:${bridgePort}`);
      }
    }
  }

  return Array.from(new Set(bases));
}

const ENV_NODE_BASES = (process.env.ZNODE_API_BASES || '')
  .split(',')
  .map((s) => s.trim())
  .filter((s) => s.length > 0);

let NODE_BASES = ENV_NODE_BASES.length ? ENV_NODE_BASES : deriveNodeBasesFromEnv();
NODE_BASES = NODE_BASES.map((b) => normalizeNodeBase(b)).filter((b) => b);
const discoveredNodes = new Map();
let clustersCache = null;
let clustersCacheTs = 0;
const CACHE_TTL_RAW = process.env.CLUSTER_AGG_CACHE_TTL_MS;
const CACHE_TTL_PARSED = CACHE_TTL_RAW != null ? Number(CACHE_TTL_RAW) : NaN;
const CLUSTERS_CACHE_TTL_MS = Number.isFinite(CACHE_TTL_PARSED) && CACHE_TTL_PARSED > 0 ? CACHE_TTL_PARSED : 30000;

const PORT_RAW = process.env.CLUSTER_AGG_PORT;
const PORT_PARSED = PORT_RAW != null ? Number(PORT_RAW) : NaN;
const PORT = Number.isFinite(PORT_PARSED) && PORT_PARSED > 0 ? PORT_PARSED : 4000;

const TIMEOUT_RAW = process.env.CLUSTER_AGG_NODE_TIMEOUT_MS;
const TIMEOUT_PARSED = TIMEOUT_RAW != null ? Number(TIMEOUT_RAW) : NaN;
const NODE_TIMEOUT_MS =
  Number.isFinite(TIMEOUT_PARSED) && TIMEOUT_PARSED > 0 ? TIMEOUT_PARSED : 3000;

const MIN_ELIGIBLE_RAW = process.env.CLUSTER_MIN_ELIGIBLE_NODES;
const MIN_ELIGIBLE_PARSED = MIN_ELIGIBLE_RAW != null ? Number(MIN_ELIGIBLE_RAW) : NaN;
const MIN_ELIGIBLE_NODES =
  Number.isFinite(MIN_ELIGIBLE_PARSED) && MIN_ELIGIBLE_PARSED > 0
    ? MIN_ELIGIBLE_PARSED
    : 7;

if (!NODE_BASES.length) {
  console.error(
    '[ClusterAgg] No nodes configured. Set ZNODE_API_BASES to a comma-separated list of node API base URLs, or ensure PUBLIC_IP / P2P_BOOTSTRAP_PEERS / BRIDGE_API_PORT are configured.',
  );
}
const P2P_SOCKET_PATH = process.env.CLUSTER_AGG_P2P_SOCKET_PATH || '/tmp/znode-p2p.sock';
const HEARTBEATS_TTL_MS = envInt('CLUSTER_AGG_HEARTBEATS_TTL_MS', 300000, 0, 86400000);
const HEARTBEATS_CACHE_TTL_MS = envInt('CLUSTER_AGG_HEARTBEATS_CACHE_TTL_MS', 5000, 0, 60000);
const DISCOVERY_STALE_MS = envInt('CLUSTER_AGG_DISCOVERY_STALE_MS', 3600000, 60000, 604800000);
const NETWORK_CACHE_TTL_MS = envInt('CLUSTER_AGG_NETWORK_CACHE_TTL_MS', 5000, 0, 60000);
const CHAIN_CACHE_TTL_MS = envInt('CLUSTER_AGG_CHAIN_CACHE_TTL_MS', 15000, 0, 600000);
const RPC_URL = (process.env.RPC_URL || '').trim();
const REGISTRY_ADDR = (process.env.REGISTRY_ADDR || '').trim();
const STAKING_ADDR = (process.env.STAKING_ADDR || '').trim();
let _provider = null;
let _registry = null;
let _staking = null;
let heartbeatsCache = { fetchedAt: 0, data: new Map() };
let chainCache = { fetchedAt: 0, data: { activeNodes: [], clusters: [] } };
let networkCache = { fetchedAt: 0, data: null };
function getProvider() {
  if (_provider) return _provider;
  if (!RPC_URL) return null;
  try {
    const req = new ethers.FetchRequest(RPC_URL);
    if (process.env.RPC_API_KEY) req.setHeader('X-API-Key', process.env.RPC_API_KEY);
    _provider = new ethers.JsonRpcProvider(req);
  } catch {
    _provider = null;
  }
  return _provider;
}
function getRegistry() {
  if (_registry) return _registry;
  const p = getProvider();
  if (!p) return null;
  let addr = null;
  try {
    addr = ethers.getAddress(REGISTRY_ADDR);
  } catch {}
  if (!addr) return null;
  _registry = new ethers.Contract(addr, registryABI, p);
  return _registry;
}
function getStaking() {
  if (_staking) return _staking;
  const p = getProvider();
  if (!p) return null;
  let addr = null;
  try {
    addr = ethers.getAddress(STAKING_ADDR);
  } catch {}
  if (!addr) return null;
  _staking = new ethers.Contract(addr, stakingABI, p);
  return _staking;
}
function normalizeEthAddress(value) {
  try {
    return ethers.getAddress(String(value || ''));
  } catch {
    return null;
  }
}
function normalizeClusterId(value) {
  const s = typeof value === 'string' ? value : '';
  if (!/^0x[0-9a-fA-F]{64}$/.test(s)) return null;
  return s.toLowerCase();
}
function pruneDiscoveredNodes(now) {
  const cutoff = now - DISCOVERY_STALE_MS;
  for (const [b, seen] of discoveredNodes) {
    if (seen < cutoff) discoveredNodes.delete(b);
  }
  if (discoveredNodes.size <= MAX_DISCOVERED_NODES) return;
  const entries = Array.from(discoveredNodes.entries());
  entries.sort((a, b) => a[1] - b[1]);
  for (const [b] of entries) {
    if (discoveredNodes.size <= MAX_DISCOVERED_NODES) break;
    discoveredNodes.delete(b);
  }
}
function markDiscoveredNode(apiBase, ts) {
  const b = normalizeNodeBase(apiBase);
  if (!b || NODE_BASES.includes(b)) return;
  const now = Number.isFinite(ts) && ts > 0 ? Math.floor(ts) : Date.now();
  if (discoveredNodes.has(b)) {
    discoveredNodes.set(b, now);
    return;
  }
  if (discoveredNodes.size >= MAX_DISCOVERED_NODES) {
    pruneDiscoveredNodes(now);
    if (discoveredNodes.size >= MAX_DISCOVERED_NODES) return;
  }
  discoveredNodes.set(b, now);
}
async function p2pRpcCall(method, params, timeoutMs) {
  const socketPath = String(P2P_SOCKET_PATH || '').trim();
  if (!socketPath) throw new Error('p2p_socket_disabled');
  const m = String(method || '').trim();
  if (!m) throw new Error('invalid_method');
  const id = Math.floor(Math.random() * 1000000000);
  const payload = JSON.stringify({ jsonrpc: '2.0', method: m, params: params || {}, id }) + '\n';
  const t = Number.isFinite(timeoutMs) && timeoutMs > 0 ? Math.floor(timeoutMs) : 2000;
  return new Promise((resolve, reject) => {
    const sock = net.createConnection(socketPath);
    let buffer = '';
    let done = false;
    const finish = (err, val) => {
      if (done) return;
      done = true;
      try {
        sock.destroy();
      } catch {}
      if (err) reject(err);
      else resolve(val);
    };
    const timer = setTimeout(() => finish(new Error('timeout')), t);
    sock.on('connect', () => {
      sock.write(payload, (err) => {
        if (err) {
          clearTimeout(timer);
          finish(err);
        }
      });
    });
    sock.on('data', (data) => {
      buffer += data.toString();
      if (buffer.length > 5000000) {
        clearTimeout(timer);
        finish(new Error('response_too_large'));
        return;
      }
      let idx;
      while ((idx = buffer.indexOf('\n')) !== -1) {
        const line = buffer.slice(0, idx);
        buffer = buffer.slice(idx + 1);
        if (!line.trim()) continue;
        let msg = null;
        try {
          msg = JSON.parse(line);
        } catch {
          continue;
        }
        if (!msg || msg.id !== id) continue;
        clearTimeout(timer);
        if (msg.error) finish(new Error(msg.error.message || msg.error.code || 'p2p_error'));
        else finish(null, msg.result);
        return;
      }
    });
    sock.on('error', (err) => {
      clearTimeout(timer);
      finish(err);
    });
    sock.on('close', () => {
      if (!done) {
        clearTimeout(timer);
        finish(new Error('socket_closed'));
      }
    });
  });
}
async function getHeartbeatsMap() {
  const now = Date.now();
  if (heartbeatsCache.data && now - heartbeatsCache.fetchedAt < HEARTBEATS_CACHE_TTL_MS) return heartbeatsCache.data;
  const map = new Map();
  try {
    const result = await p2pRpcCall('P2P.GetHeartbeats', { ttlMs: HEARTBEATS_TTL_MS }, Math.min(NODE_TIMEOUT_MS, 3000));
    const heartbeats = result && Array.isArray(result.heartbeats) ? result.heartbeats : [];
    for (const hb of heartbeats) {
      const addr = normalizeEthAddress(hb && hb.address);
      if (!addr) continue;
      const apiBaseRaw = hb && typeof hb.apiBase === 'string' ? hb.apiBase : '';
      const apiBase = normalizeNodeBase(apiBaseRaw) || '';
      if (apiBase) markDiscoveredNode(apiBase, now);
      const ts = hb && hb.timestamp != null ? Number(hb.timestamp) : 0;
      map.set(addr.toLowerCase(), { timestamp: Number.isFinite(ts) ? ts : 0, apiBase });
    }
  } catch {}
  heartbeatsCache = { fetchedAt: now, data: map };
  return map;
}
async function getChainSnapshot() {
  const now = Date.now();
  if (chainCache.data && now - chainCache.fetchedAt < CHAIN_CACHE_TTL_MS) return chainCache.data;
  const staking = getStaking();
  const registry = getRegistry();
  const out = { activeNodes: [], clusters: [] };
  if (!staking || !registry) {
    chainCache = { fetchedAt: now, data: out };
    return out;
  }
  let activeNodesRaw = [];
  try {
    activeNodesRaw = await staking.getActiveNodes();
  } catch {}
  const activeNodes = [];
  for (const a of Array.isArray(activeNodesRaw) ? activeNodesRaw : []) {
    const addr = normalizeEthAddress(a);
    if (addr) activeNodes.push(addr);
  }
  let clusterIdsRaw = [];
  try {
    clusterIdsRaw = await registry.getActiveClusters();
  } catch {}
  const clusterIds = [];
  for (const cid of Array.isArray(clusterIdsRaw) ? clusterIdsRaw : []) {
    const id = normalizeClusterId(cid);
    if (id) clusterIds.push(id);
  }
  const fetchFns = clusterIds.map((clusterId) => async () => {
    let info = null;
    try {
      info = await registry.clusters(clusterId);
    } catch {}
    let membersRaw = [];
    try {
      membersRaw = await registry.getClusterMembers(clusterId);
    } catch {}
    const members = [];
    for (const m of Array.isArray(membersRaw) ? membersRaw : []) {
      const addr = normalizeEthAddress(m);
      if (addr) members.push(addr);
    }
    let multisigAddress = null;
    if (info && typeof info.moneroAddress === 'string') multisigAddress = info.moneroAddress;
    else if (Array.isArray(info) && typeof info[0] === 'string') multisigAddress = info[0];
    const finalized = !!(info && (info.finalized === true || (Array.isArray(info) && info[2] === true)));
    return { id: clusterId, members, multisigAddress, finalized };
  });
  const results = await promiseAllSettledWithConcurrency(fetchFns, 10);
  const clusters = [];
  for (const r of results) {
    if (r.status !== 'fulfilled' || !r.value) continue;
    clusters.push(r.value);
  }
  out.activeNodes = activeNodes;
  out.clusters = clusters;
  chainCache = { fetchedAt: now, data: out };
  return out;
}
function toHealthBase(apiBase) {
  const base = normalizeNodeBase(apiBase);
  if (!base) return null;
  let u;
  try {
    u = new URL(base);
  } catch {
    return null;
  }
  u.protocol = 'http:';
  u.port = '3003';
  u.pathname = '';
  u.search = '';
  u.hash = '';
  const s = u.toString();
  return s.endsWith('/') ? s.slice(0, -1) : s;
}
function normalizeMoneroHealth(value) {
  const v = String(value || '').toUpperCase();
  if (v === 'HEALTHY') return 'healthy';
  if (v === 'QUARANTINED') return 'quarantined';
  if (v === 'NEEDS_ATTEMPT_RESET' || v === 'NEEDS_GLOBAL_RESET') return 'degraded';
  return 'unknown';
}
async function fetchNodeHealth(apiBase) {
  const hb = toHealthBase(apiBase);
  if (!hb) return { ok: false, error: 'invalid_base' };
  const safeBase = await validateNodeBase(hb);
  if (!safeBase) return { ok: false, error: 'unsafe_node_base' };
  const r = await safeJsonRequest(safeBase + '/health', { timeoutMs: NODE_TIMEOUT_MS });
  if (!r.ok) return { ok: false, error: r.error, baseUrl: safeBase };
  return { ok: true, data: r.data, baseUrl: safeBase };
}
async function aggregateSignatures() {
  const allBases = [...new Set([...NODE_BASES, ...discoveredNodes.keys()])];
  const signaturesMap = new Map();
  const fetchFns = allBases.map((base) => async () => {
    const safeBase = await validateNodeBase(base);
    if (!safeBase) return null;
    const r = await safeJsonRequest(safeBase + '/bridge/signatures', { method: 'GET', timeoutMs: NODE_TIMEOUT_MS, allowInvalidJson: true });
    if (r.ok && r.data && Array.isArray(r.data.signatures)) return r.data.signatures;
    return null;
  });
  const results = await promiseAllSettledWithConcurrency(fetchFns, MAX_CONCURRENT_REQUESTS);
  for (const result of results) {
    if (result.status !== 'fulfilled' || !result.value) continue;
    for (const sig of result.value) {
      if (!sig || !sig.xmrTxid) continue;
      const key = sig.xmrTxid;
      let existing = signaturesMap.get(key);
      if (!existing) {
        existing = { ...sig, signatures: [] };
        signaturesMap.set(key, existing);
      }
      if (Array.isArray(sig.signatures)) {
        const existingSigs = new Set(existing.signatures.map((s) => s.signer?.toLowerCase()));
        for (const s of sig.signatures) {
          if (s && s.signer && !existingSigs.has(s.signer.toLowerCase())) {
            existing.signatures.push(s);
            existingSigs.add(s.signer.toLowerCase());
          }
        }
      }
      existing.ready = existing.signatures.length >= MIN_ELIGIBLE_NODES;
    }
  }
  return Array.from(signaturesMap.values());
}
function atomicToXmr4(amountAtomic) {
  let v;
  try {
    v = BigInt(String(amountAtomic));
  } catch {
    return null;
  }
  const denom = 1000000000000n;
  const scaled = (v * 10000n + denom / 2n) / denom;
  const intPart = scaled / 10000n;
  const frac = scaled % 10000n;
  return intPart.toString() + '.' + frac.toString().padStart(4, '0');
}
async function getNetworkSnapshot() {
  const now = Date.now();
  if (networkCache.data && now - networkCache.fetchedAt < NETWORK_CACHE_TTL_MS) return networkCache.data;
  const [heartbeats, chain] = await Promise.all([getHeartbeatsMap(), getChainSnapshot()]);
  const nodeToCluster = new Map();
  const clusters = [];
  for (const c of Array.isArray(chain.clusters) ? chain.clusters : []) {
    if (!c || !c.id) continue;
    const members = Array.isArray(c.members) ? c.members : [];
    for (const m of members) {
      const addr = normalizeEthAddress(m);
      if (!addr) continue;
      nodeToCluster.set(addr.toLowerCase(), c.id);
    }
    clusters.push({
      id: c.id,
      threshold: MIN_ELIGIBLE_NODES,
      size: members.length,
      members: members.map((x) => String(x)),
      multisigAddress: c.multisigAddress || null,
      finalized: !!c.finalized,
      viewKey: null,
      xmrBalance: null,
    });
  }
  const addrSet = new Set();
  for (const addr of Array.isArray(chain.activeNodes) ? chain.activeNodes : []) {
    const a = normalizeEthAddress(addr);
    if (a) addrSet.add(a.toLowerCase());
  }
  for (const addr of heartbeats.keys()) {
    addrSet.add(String(addr).toLowerCase());
  }
  for (const addr of nodeToCluster.keys()) {
    addrSet.add(String(addr).toLowerCase());
  }
  const nodes = [];
  for (const addrLower of addrSet) {
    const hb = heartbeats.get(addrLower);
    const apiBase = hb && hb.apiBase ? hb.apiBase : '';
    nodes.push({
      address: addrLower,
      apiBase: apiBase || null,
      clusterId: nodeToCluster.get(addrLower) || null,
      p2pConnected: false,
      moneroHealth: 'unknown',
    });
    if (apiBase) markDiscoveredNode(apiBase, now);
  }
  const healthFns = nodes.map((n) => async () => {
    if (!n || !n.apiBase) return null;
    const r = await fetchNodeHealth(n.apiBase);
    if (!r.ok || !r.data) return null;
    const d = r.data || {};
    const p2p = d.p2p || {};
    const monero = d.monero || {};
    return { apiBase: n.apiBase, p2pConnected: !!p2p.connected, moneroHealth: normalizeMoneroHealth(monero.health), viewKey: monero.viewKey || null };
  });
  const healthResults = await promiseAllSettledWithConcurrency(healthFns, MAX_CONCURRENT_REQUESTS);
  const healthByBase = new Map();
  for (const r of healthResults) {
    if (r.status !== 'fulfilled' || !r.value) continue;
    healthByBase.set(r.value.apiBase, r.value);
  }
  for (const n of nodes) {
    const h = n.apiBase ? healthByBase.get(n.apiBase) : null;
    if (!h) continue;
    n.p2pConnected = h.p2pConnected;
    n.moneroHealth = h.moneroHealth;
  }
  const reserveFns = clusters.map((c) => async () => {
    if (!c || !Array.isArray(c.members) || c.members.length === 0) return null;
    let base = null;
    for (const m of c.members) {
      const addr = normalizeEthAddress(m);
      if (!addr) continue;
      const hb = heartbeats.get(addr.toLowerCase());
      if (hb && hb.apiBase) {
        base = hb.apiBase;
        break;
      }
    }
    if (!base) return null;
    const r = await fetchClusterReserve(base);
    if (!r.ok || !r.data) return null;
    const d = r.data || {};
    return { clusterId: c.id, xmrBalance: d.balance || null };
  });
  const reserveResults = await promiseAllSettledWithConcurrency(reserveFns, MAX_CONCURRENT_REQUESTS);
  for (const r of reserveResults) {
    if (r.status !== 'fulfilled' || !r.value) continue;
    for (const c of clusters) {
      if (c && c.id === r.value.clusterId) {
        c.xmrBalance = r.value.xmrBalance;
        break;
      }
    }
  }
  for (const c of clusters) {
    if (!c || !Array.isArray(c.members) || c.members.length === 0) continue;
    let vk = null;
    for (const m of c.members) {
      const addr = normalizeEthAddress(m);
      if (!addr) continue;
      const hb = heartbeats.get(addr.toLowerCase());
      if (!hb || !hb.apiBase) continue;
      const h = healthByBase.get(hb.apiBase);
      if (h && h.viewKey) {
        vk = h.viewKey;
        break;
      }
    }
    c.viewKey = vk;
  }
  const data = { timestamp: now, nodes, clusters };
  networkCache = { fetchedAt: now, data };
  return data;
}

async function fetchClusterStatus(baseUrl) {
  const safeBase = await validateNodeBase(baseUrl);
  if (!safeBase) {
    return { ok: false, error: 'unsafe_node_base', baseUrl };
  }
  const url = safeBase + '/cluster-status';
  const r = await safeJsonRequest(url, { timeoutMs: NODE_TIMEOUT_MS });
  if (!r.ok) return { ok: false, error: r.error, baseUrl: safeBase };
  return { ok: true, data: r.data, baseUrl: safeBase };
}

function isNodeEligible(status) {
  if (!status || typeof status !== 'object') return false;
  if (status.eligibleForBridging === true) return true;
  const cluster = status.cluster || {};
  const finalized = !!cluster.finalized;
  const moneroHealth = status.monero && status.monero.health;
  const p2p = status.p2p || {};
  const p2pConnected = !!p2p.connected;
  return !!cluster.id && finalized && moneroHealth === 'HEALTHY' && p2pConnected;
}

async function fetchClusterReserve(baseUrl) {
  const safeBase = await validateNodeBase(baseUrl);
  if (!safeBase) {
    return { ok: false, error: 'unsafe_node_base', baseUrl };
  }
  const url = safeBase + '/bridge/reserves';
  const r = await safeJsonRequest(url, { timeoutMs: NODE_TIMEOUT_MS });
  if (!r.ok) return { ok: false, error: r.error, baseUrl: safeBase };
  return { ok: true, data: r.data, baseUrl: safeBase };
}

async function fetchNodeTxs(baseUrl, limit) {
  const safeBase = await validateNodeBase(baseUrl);
  if (!safeBase) {
    return { ok: false, error: 'unsafe_node_base', baseUrl };
  }
  let url = safeBase + '/bridge/txs';
  const n = Number(limit);
  if (Number.isFinite(n) && n > 0) {
    url += '?limit=' + Math.floor(n);
  }
  const r = await safeJsonRequest(url, { timeoutMs: NODE_TIMEOUT_MS });
  if (!r.ok) return { ok: false, error: r.error, baseUrl: safeBase };
  return { ok: true, data: r.data, baseUrl: safeBase };
}

async function aggregateClusters() {
  const timestamp = Date.now();

  try {
    await getHeartbeatsMap();
  } catch {}
  pruneDiscoveredNodes(timestamp);

  const allBases = [...new Set([...NODE_BASES, ...discoveredNodes.keys()])];
  if (!allBases.length) {
    return {
      timestamp,
      totalNodes: 0,
      liveNodes: 0,
      onlineNodes: 0,
      totalClusters: 0,
      minEligibleNodes: MIN_ELIGIBLE_NODES,
      clusters: [],
      errors: ['no_nodes_discovered'],
    };
  }

  const results = await promiseAllSettledWithConcurrency(allBases.map((base) => () => fetchClusterStatus(base)), MAX_CONCURRENT_REQUESTS);

  const clusters = new Map();
  const errors = [];
  let onlineNodes = 0;

  for (const r of results) {
    if (r.status !== 'fulfilled') {
      errors.push('Request failed');
      continue;
    }
    const { ok, data, error, baseUrl } = r.value;
    if (!ok) {
      errors.push(String(error || 'error'));
      continue;
    }
    const p2pConnected = !!(data && data.p2p && data.p2p.connected);
    if (p2pConnected) onlineNodes += 1;

    const eligible = isNodeEligible(data);
    const cluster = (data && data.cluster) || {};
    const clusterId = cluster.id;
    if (!clusterId) {
      continue;
    }

    const members = Array.isArray(cluster.members) ? cluster.members : [];
    const normalizedMembers = [];
    let liveMembers = 0;
    for (const m of members) {
      if (!m || !m.address) continue;
      if (typeof m.inCluster === 'boolean' && !m.inCluster) continue;
      const status = typeof m.status === 'string' ? m.status : null;
      const s = (status || '').toLowerCase();
      if (s === 'healthy' || s === 'online') liveMembers += 1;
      const apiBase = normalizeNodeBase(typeof m.apiBase === 'string' ? m.apiBase : '') || '';
      if (DISCOVERY_ENABLED && apiBase) markDiscoveredNode(apiBase, Date.now());
      normalizedMembers.push({
        address: m.address,
        status,
        apiBase,
        isSelf: !!m.isSelf,
      });
    }

    let entry = clusters.get(clusterId);
    if (!entry) {
      entry = {
        id: clusterId,
        finalAddress: cluster.finalAddress || null,
        size:
          typeof cluster.size === 'number'
            ? cluster.size
            : normalizedMembers.length > 0
              ? normalizedMembers.length
              : null,
        eligibleNodes: 0,
        totalNodes: 0,
        liveMembers: 0,
        nodes: [],
        members: [],
      };
      clusters.set(clusterId, entry);
    }

    if (normalizedMembers.length > 0) {
      if (!Array.isArray(entry.members)) entry.members = [];
      const existingByAddr = new Map();
      for (const m of entry.members) {
        if (!m || !m.address) continue;
        existingByAddr.set(m.address.toLowerCase(), m);
      }
      for (const m of normalizedMembers) {
        const key = m.address.toLowerCase();
        const existing = existingByAddr.get(key);
        if (!existing) {
          entry.members.push(m);
          existingByAddr.set(key, m);
        } else {
          if (!existing.status && m.status) existing.status = m.status;
          if (!existing.apiBase && m.apiBase) existing.apiBase = m.apiBase;
        }
      }
      if (!entry.size && typeof cluster.size !== 'number' && entry.members.length > 0) {
        entry.size = entry.members.length;
      }
    }

    entry.totalNodes += 1;
    if (eligible) {
      entry.eligibleNodes += 1;
    }
    if (liveMembers > 0 && (typeof entry.liveMembers !== 'number' || liveMembers > entry.liveMembers)) {
      entry.liveMembers = liveMembers;
    }

    const p2p = data.p2p || {};

    entry.nodes.push({
      apiBase: baseUrl,
      eligibleForBridging: eligible,
      clusterState: data.clusterState || null,
      moneroHealth: data.monero && data.monero.health ? data.monero.health : null,
      p2pConnected: !!p2p.connected,
      lastHeartbeatAgoSec:
        typeof p2p.lastHeartbeatAgoSec === 'number' ? p2p.lastHeartbeatAgoSec : null,
    });
  }

  const allClusters = Array.from(clusters.values());
  const filtered = allClusters.filter(
    (c) => c.eligibleNodes > 0 && !!c.finalAddress,
  );

  let liveNodes = 0;
  for (const c of allClusters) {
    if (typeof c.liveMembers === 'number') liveNodes += c.liveMembers;
  }

  return {
    timestamp,
    totalNodes: allBases.length,
    liveNodes,
    onlineNodes,
    totalClusters: filtered.length,
    minEligibleNodes: MIN_ELIGIBLE_NODES,
    clusters: filtered,
    errors,
  };
}

const server = http.createServer(async (req, res) => {
  try {
    const { method, url } = req;
    res.setHeader('Access-Control-Allow-Origin', CORS_ORIGIN);
    if (CORS_ORIGIN !== '*') res.setHeader('Vary', 'Origin');
    res.setHeader('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
    if (method === 'OPTIONS') {
      res.writeHead(204);
      res.end();
      return;
    }
    if (!url) {
      res.writeHead(400, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Bad request' }));
      return;
    }

    const bucket = method === 'POST' ? 'POST' : 'GET';
    const max = method === 'POST' ? RATE_LIMIT_MAX_POST : RATE_LIMIT_MAX_GET;
    const rl = checkRateLimit(req, bucket, max);
    if (!rl.ok) {
      res.writeHead(429, { 'Content-Type': 'application/json', 'Retry-After': String(rl.retryAfter) });
      res.end(JSON.stringify({ error: 'Too many requests', retryAfter: rl.retryAfter }));
      return;
    }

    const u = new URL(url, 'http://localhost');

    if (method === 'GET' && u.pathname === '/api/network/overview') {
      const snap = await getNetworkSnapshot();
      const nodes = Array.isArray(snap.nodes) ? snap.nodes : [];
      const clusters = Array.isArray(snap.clusters) ? snap.clusters : [];
      let onlineNodes = 0;
      for (const n of nodes) {
        if (n && n.p2pConnected) onlineNodes += 1;
      }
      let totalReservesXmr = 0;
      for (const c of clusters) {
        const v = c && c.xmrBalance != null ? Number(c.xmrBalance) : NaN;
        if (Number.isFinite(v)) totalReservesXmr += v;
      }
      let clusterFinalized = false;
      for (const c of clusters) {
        if (!c || c.finalized !== true) continue;
        const addr = c.multisigAddress != null ? String(c.multisigAddress) : '';
        if (addr.trim()) {
          clusterFinalized = true;
          break;
        }
      }
      const bridgeEnabled = process.env.BRIDGE_ENABLED === undefined || process.env.BRIDGE_ENABLED === '1';
      const bridgeStatus = clusterFinalized && bridgeEnabled ? 'online' : 'offline';
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ bridgeStatus, totalNodes: nodes.length, onlineNodes, activeClusters: clusters.length, totalReservesXmr }));
      return;
    }
    if (method === 'GET' && u.pathname === '/api/network/nodes') {
      const snap = await getNetworkSnapshot();
      const nodes = Array.isArray(snap.nodes) ? snap.nodes : [];
      const out = [];
      for (const n of nodes) {
        if (!n) continue;
        out.push({ address: n.address || null, apiBase: n.apiBase || null, p2pConnected: !!n.p2pConnected, moneroHealth: n.moneroHealth || 'unknown', clusterId: n.clusterId || null });
      }
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ nodes: out }));
      return;
    }
    if (method === 'GET' && u.pathname === '/api/network/node') {
      const addrRaw = u.searchParams.get('address') || u.searchParams.get('addr') || u.searchParams.get('a') || '';
      const addr = normalizeEthAddress(addrRaw);
      if (!addr) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'Bad address' }));
        return;
      }
      const snap = await getNetworkSnapshot();
      const nodes = Array.isArray(snap.nodes) ? snap.nodes : [];
      const target = addr.toLowerCase();
      let found = null;
      for (const n of nodes) {
        if (!n || !n.address) continue;
        if (String(n.address).toLowerCase() === target) {
          found = n;
          break;
        }
      }
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({
        found: !!found,
        address: target,
        p2pConnected: found ? !!found.p2pConnected : false,
        moneroHealth: (found && found.moneroHealth) ? found.moneroHealth : 'unknown',
        clusterId: found ? (found.clusterId || null) : null,
      }));
      return;
    }
    if (method === 'GET' && u.pathname === '/api/network/clusters') {
      const snap = await getNetworkSnapshot();
      const clusters = Array.isArray(snap.clusters) ? snap.clusters : [];
      const out = [];
      for (const c of clusters) {
        if (!c) continue;
        out.push({ id: c.id || null, threshold: c.threshold || MIN_ELIGIBLE_NODES, size: c.size || 0, members: Array.isArray(c.members) ? c.members : [], multisigAddress: c.multisigAddress || null, xmrBalance: c.xmrBalance || null, viewKey: c.viewKey || null });
      }
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ clusters: out }));
      return;
    }
    if (method === 'GET' && u.pathname === '/api/network/proofs') {
      const sigs = await aggregateSignatures();
      const deposits = [];
      for (const s of Array.isArray(sigs) ? sigs : []) {
        if (!s || !s.xmrTxid) continue;
        const amountAtomic = s.amount != null ? String(s.amount) : null;
        deposits.push({ txid: s.xmrTxid, amount: amountAtomic ? atomicToXmr4(amountAtomic) : null, amountAtomic, confirmations: 0, recipient: s.recipient || null, depositId: s.depositId || null, clusterId: s.clusterId || null, signatures: Array.isArray(s.signatures) ? s.signatures : [], ready: !!s.ready });
      }
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ context: { chainId: process.env.CHAIN_ID ? Number(process.env.CHAIN_ID) : null, bridgeAddr: process.env.BRIDGE_ADDR || null }, deposits, withdrawals: [] }));
      return;
    }

    if (u.pathname === '/bridge/deposit/request' && method === 'POST') {
      const now = Date.now();
      try {
        await getHeartbeatsMap();
      } catch {}
      pruneDiscoveredNodes(now);
      let bases = [...new Set([...NODE_BASES, ...discoveredNodes.keys()])];
      if (!bases.length) {
        res.writeHead(503, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'No nodes available' }));
        return;
      }
      try {
        const snap = await getNetworkSnapshot();
        const nodes = Array.isArray(snap && snap.nodes) ? snap.nodes : [];
        const score = new Map();
        for (const n of nodes) {
          if (!n || !n.apiBase) continue;
          const b = normalizeNodeBase(n.apiBase);
          if (!b) continue;
          const s = (n.p2pConnected ? 2 : 0) + (n.moneroHealth === 'healthy' ? 1 : 0);
          if (!score.has(b) || s > score.get(b)) score.set(b, s);
        }
        bases.sort((a, b) => (score.get(b) || 0) - (score.get(a) || 0));
      } catch {}

      let body;
      try {
        body = await readJsonBody(req);
      } catch (e) {
        if (e && e.code === 'BODY_TOO_LARGE') {
          res.writeHead(413, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ error: 'Payload too large' }));
          return;
        }
        throw e;
      }

      let lastError = null;
      for (const base of bases) {
        const safeBase = await validateNodeBase(base);
        if (!safeBase) continue;
        const target = safeBase + '/bridge/deposit/request';
        try {
          const r = await safeJsonRequest(target, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body), timeoutMs: NODE_TIMEOUT_MS, allowInvalidJson: true });
          if (!r.ok) {
            lastError = new Error(r.error || 'error');
            continue;
          }
          const data = r.data || {};
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify(data));
          return;
        } catch (e) {
          lastError = e;
        }
      }

      res.writeHead(503, { 'Content-Type': 'application/json' });
      res.end(
        JSON.stringify({
          error: 'Deposit routing unavailable',
          message: lastError ? lastError.message || String(lastError) : 'no nodes responded',
        }),
      );
      return;
    }

    if (u.pathname.startsWith('/bridge/withdrawal/status/') && method === 'GET') {
      const now = Date.now();
      try {
        await getHeartbeatsMap();
      } catch {}
      pruneDiscoveredNodes(now);
      const allBases = [...new Set([...NODE_BASES, ...discoveredNodes.keys()])];
      if (!allBases.length) {
        res.writeHead(503, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'No nodes available' }));
        return;
      }

      const clusterId = (u.searchParams.get('clusterId') || '').toLowerCase();
      let bases = allBases;
      if (clusterId) {
        try {
          const agg = await aggregateClusters();
          const list = Array.isArray(agg.clusters) ? agg.clusters : [];
          const match = list.find((c) => c && typeof c.id === 'string' && c.id.toLowerCase() === clusterId);
          if (match && Array.isArray(match.nodes) && match.nodes.length > 0) {
            const filtered = [];
            for (const n of match.nodes) {
              if (!n || !n.apiBase) continue;
              filtered.push(n.apiBase);
            }
            if (filtered.length > 0) bases = filtered;
          }
        } catch {}
      }

      let lastError = null;
      let lastResult = null;
      for (const base of bases) {
        const safeBase = await validateNodeBase(base);
        if (!safeBase) continue;
        const target = safeBase + u.pathname;
        try {
          const r = await safeJsonRequest(target, { method: 'GET', timeoutMs: NODE_TIMEOUT_MS, allowInvalidJson: true });
          if (!r.ok) {
            lastError = new Error(r.error || 'error');
            continue;
          }
          const data = r.data || {};
          if (data && data.status === 'found' && Array.isArray(data.xmrTxHashes) && data.xmrTxHashes.length > 0) {
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify(data));
            return;
          }
          if (!lastResult && data) lastResult = data;
        } catch (e) {
          lastError = e;
        }
      }

      if (lastResult) {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify(lastResult));
        return;
      }

      res.writeHead(503, { 'Content-Type': 'application/json' });
      res.end(
        JSON.stringify({
          error: 'Withdrawal routing unavailable',
          message: lastError ? lastError.message || String(lastError) : 'no nodes responded',
        }),
      );
      return;
    }

    if (method === 'GET' && u.pathname.startsWith('/bridge/deposit/status/')) {
      const txid = u.pathname.slice('/bridge/deposit/status/'.length);
      if (!txid || txid.length < 10) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'Invalid txid' }));
        return;
      }
      const now = Date.now();
      try {
        await getHeartbeatsMap();
      } catch {}
      pruneDiscoveredNodes(now);
      const bases = [...new Set([...NODE_BASES, ...discoveredNodes.keys()])];
      if (!bases.length) {
        res.writeHead(503, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'No nodes available' }));
        return;
      }

      let lastError = null;
      let lastProcessed = null;
      let lastPending = null;
      const ready = new Map();
      for (const base of bases) {
        const safeBase = await validateNodeBase(base);
        if (!safeBase) continue;
        const target = safeBase + '/bridge/deposit/status/' + encodeURIComponent(txid);
        try {
          const r = await safeJsonRequest(target, { method: 'GET', timeoutMs: NODE_TIMEOUT_MS, allowInvalidJson: true });
          if (!r.ok) {
            lastError = new Error(r.error || 'error');
            continue;
          }
          const data = r.data || {};
          if (data && data.status === 'ready') {
            const key = `${data.clusterId || ''}|${data.depositId || ''}|${data.recipient || ''}|${data.amount || ''}`;
            let g = ready.get(key);
            if (!g) {
              g = { data, sigs: new Map() };
              ready.set(key, g);
            }
            if (Array.isArray(data.signatures)) {
              for (const s of data.signatures) {
                if (!s || typeof s.signer !== 'string' || typeof s.signature !== 'string') continue;
                const k = s.signer.toLowerCase();
                if (!g.sigs.has(k)) g.sigs.set(k, { signer: s.signer, signature: s.signature });
              }
            }
            continue;
          }
          if (data && data.status === 'processed') {
            if (!lastProcessed || (data.depositId && !lastProcessed.depositId)) lastProcessed = data;
            continue;
          }
          if (data) lastPending = data;
        } catch (e) {
          lastError = e;
        }
      }

      if (ready.size > 0) {
        let best = null;
        for (const g of ready.values()) {
          if (!best || g.sigs.size > best.sigs.size) best = g;
        }
        const merged = best ? Array.from(best.sigs.values()) : [];
        const out = best ? { ...best.data, signatures: merged, ready: merged.length >= 7 } : { status: 'pending' };
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify(out));
        return;
      }

      if (lastProcessed) {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify(lastProcessed));
        return;
      }

      if (lastPending) {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify(lastPending));
        return;
      }

      res.writeHead(503, { 'Content-Type': 'application/json' });
      res.end(
        JSON.stringify({
          error: 'Deposit status routing unavailable',
          message: lastError ? lastError.message || String(lastError) : 'no nodes responded',
        }),
      );
      return;
    }

    if (u.pathname === '/bridge/signatures' && method === 'GET') {
      const now = Date.now();
      try {
        await getHeartbeatsMap();
      } catch {}
      pruneDiscoveredNodes(now);
      const allBases = [...new Set([...NODE_BASES, ...discoveredNodes.keys()])];
      if (!allBases.length) {
        res.writeHead(503, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'No nodes available' }));
        return;
      }
      const signaturesMap = new Map();

      const fetchFns = allBases.map((base) => async () => {
        const safeBase = await validateNodeBase(base);
        if (!safeBase) return null;
        try {
          const r = await safeJsonRequest(safeBase + '/bridge/signatures', {
            method: 'GET',
            timeoutMs: NODE_TIMEOUT_MS,
            allowInvalidJson: true,
          });
          if (r.ok && r.data && Array.isArray(r.data.signatures)) {
            return r.data.signatures;
          }
        } catch {}
        return null;
      });

      const results = await promiseAllSettledWithConcurrency(fetchFns, MAX_CONCURRENT_REQUESTS);
      for (const result of results) {
        if (result.status !== 'fulfilled' || !result.value) continue;
        for (const sig of result.value) {
          if (!sig || !sig.xmrTxid) continue;
          const key = sig.xmrTxid;
          let existing = signaturesMap.get(key);
          if (!existing) {
            existing = { ...sig, signatures: [] };
            signaturesMap.set(key, existing);
          }
          if (Array.isArray(sig.signatures)) {
            const existingSigs = new Set(existing.signatures.map((s) => s.signer?.toLowerCase()));
            for (const s of sig.signatures) {
              if (s && s.signer && !existingSigs.has(s.signer.toLowerCase())) {
                existing.signatures.push(s);
                existingSigs.add(s.signer.toLowerCase());
              }
            }
          }
          existing.ready = existing.signatures.length >= 7;
        }
      }

      const aggregated = Array.from(signaturesMap.values());
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ signatures: aggregated }));
      return;
    }



    if (u.pathname === '/bridge/burn/ready-signatures' && method === 'GET') {
      const clusterIdRaw = u.searchParams.get('clusterId') || u.searchParams.get('cluster') || '';
      const clusterId =
        typeof clusterIdRaw === 'string' && /^0x[0-9a-fA-F]{64}$/.test(clusterIdRaw)
          ? clusterIdRaw.toLowerCase()
          : null;
      if (!clusterId) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'Invalid clusterId' }));
        return;
      }

      const tsRaw = u.searchParams.get('timestamp');
      let timestamp = null;
      if (tsRaw != null && tsRaw !== '') {
        const parsed = Number(tsRaw);
        if (!Number.isFinite(parsed) || parsed <= 0) {
          res.writeHead(400, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ error: 'Invalid timestamp' }));
          return;
        }
        timestamp = Math.floor(parsed);
      }

      const snap = await getNetworkSnapshot();
      const nodes = Array.isArray(snap && snap.nodes) ? snap.nodes : [];
      const candidates = nodes
        .filter(
          (n) =>
            n &&
            n.apiBase &&
            n.clusterId &&
            typeof n.clusterId === 'string' &&
            n.clusterId.toLowerCase() === clusterId,
        )
        .sort((a, b) => (b.p2pConnected ? 1 : 0) - (a.p2pConnected ? 1 : 0));

      if (candidates.length === 0) {
        res.writeHead(404, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'Cluster not found or no routable nodes' }));
        return;
      }

      let messageHash = null;
      let timestampSource = null;
      if (timestamp == null) {
        let lastError = null;
        for (const n of candidates) {
          const safeBase = await validateNodeBase(n.apiBase);
          if (!safeBase) continue;
          const target =
            safeBase +
            '/bridge/burn/ready-signature?clusterId=' +
            encodeURIComponent(clusterId);
          try {
            const r = await safeJsonRequest(target, {
              method: 'GET',
              timeoutMs: NODE_TIMEOUT_MS,
              allowInvalidJson: true,
            });
            if (r.ok && r.data && r.data.signature && r.data.timestamp) {
              const ts = Number(r.data.timestamp);
              if (Number.isFinite(ts) && ts > 0) {
                timestamp = Math.floor(ts);
                messageHash = r.data.messageHash || null;
                timestampSource = safeBase;
                break;
              }
            }
            lastError = new Error(r.error || 'error');
          } catch (e) {
            lastError = e;
          }
        }
        if (timestamp == null) {
          res.writeHead(503, { 'Content-Type': 'application/json' });
          res.end(
            JSON.stringify({
              error: 'Burn readiness routing unavailable',
              message: lastError ? lastError.message || String(lastError) : 'no nodes responded',
            }),
          );
          return;
        }
      }

      const query =
        'clusterId=' +
        encodeURIComponent(clusterId) +
        '&timestamp=' +
        encodeURIComponent(String(timestamp));

      const fetchFns = candidates.map((n) => async () => {
        const safeBase = await validateNodeBase(n.apiBase);
        if (!safeBase) return null;
        try {
          const r = await safeJsonRequest(safeBase + '/bridge/burn/ready-signature?' + query, {
            method: 'GET',
            timeoutMs: NODE_TIMEOUT_MS,
            allowInvalidJson: true,
          });
          if (r.ok && r.data && r.data.signature && r.data.signer) return { base: safeBase, ...r.data };
        } catch {}
        return null;
      });

      const results = await promiseAllSettledWithConcurrency(fetchFns, MAX_CONCURRENT_REQUESTS);
      const signatures = [];
      const signers = [];
      const errors = [];
      const seen = new Set();
      for (const r of results) {
        if (r.status !== 'fulfilled' || !r.value) continue;
        const d = r.value;
        const signer = d.signer ? String(d.signer).toLowerCase() : '';
        if (!signer || seen.has(signer)) continue;
        if (messageHash && d.messageHash && String(d.messageHash).toLowerCase() !== String(messageHash).toLowerCase()) {
          errors.push('message_hash_mismatch');
          continue;
        }
        if (!messageHash && d.messageHash) messageHash = d.messageHash;
        seen.add(signer);
        signatures.push(d.signature);
        signers.push(d.signer);
      }

      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(
        JSON.stringify({
          clusterId,
          timestamp,
          messageHash: messageHash || null,
          threshold: MIN_ELIGIBLE_NODES,
          signatures,
          signers,
          timestampSource,
          errors,
        }),
      );
      return;
    }
    if (method === 'GET' && u.pathname === '/clusters') {
      const now = Date.now();
      if (clustersCache && now - clustersCacheTs < CLUSTERS_CACHE_TTL_MS) {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify(sanitizeAggregate(clustersCache)));
        return;
      }
      const data = await aggregateClusters();
      clustersCache = data;
      clustersCacheTs = Date.now();
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(sanitizeAggregate(data)));
      return;
    }

    if (method === 'GET' && u.pathname === '/api/clusters/active') {
      const now = Date.now();
      let data;
      if (clustersCache && now - clustersCacheTs < CLUSTERS_CACHE_TTL_MS) {
        data = clustersCache;
      } else {
        data = await aggregateClusters();
        clustersCache = data;
        clustersCacheTs = Date.now();
      }
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(
        JSON.stringify({
          clusters: data.clusters.map((c) => ({
            id: c.id,
            multisigAddress: c.finalAddress,
            nodeCount: c.totalNodes,
            eligibleNodes: c.eligibleNodes,
            timestamp: data.timestamp,
          })),
        }),
      );
      return;
    }

    if (method === 'GET' && u.pathname === '/api/reserves/xmr') {
      const data = await aggregateClusters();
      const clusters = [];
      let totalAtomic = 0n;
      for (const c of data.clusters) {
        const nodes = Array.isArray(c.nodes) ? c.nodes : [];
        const primary = nodes.find((n) => n.eligibleForBridging) || nodes[0];
        if (!primary || !primary.apiBase) {
          clusters.push({
            id: c.id,
            multisigAddress: c.finalAddress || null,
            error: 'no_node',
          });
          continue;
        }
        const r = await fetchClusterReserve(primary.apiBase);
        if (!r.ok) {
          clusters.push({
            id: c.id,
            multisigAddress: c.finalAddress || null,
            error: r.error || 'reserve_error',
          });
          continue;
        }
        const d = r.data || {};
        const balRaw = d.balanceAtomic != null ? d.balanceAtomic : d.unlockedBalanceAtomic;
        let balAtomic = 0n;
        try {
          if (balRaw != null) balAtomic = BigInt(String(balRaw));
        } catch {}
        totalAtomic += balAtomic;
        clusters.push({
          id: c.id,
          multisigAddress: d.multisigAddress || c.finalAddress || null,
          addressVerified: !!d.addressVerified,
          balanceAtomic: balAtomic.toString(),
          balance: d.balance || null,
          unlockedBalanceAtomic: d.unlockedBalanceAtomic != null ? String(d.unlockedBalanceAtomic) : null,
          unlockedBalance: d.unlockedBalance || null,
          checkedAt: d.checkedAt || data.timestamp,
        });
      }
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
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(
        JSON.stringify({
          timestamp: data.timestamp,
          totalClusters: clusters.length,
          clusters,
          totalAtomic: totalAtomic.toString(),
          total: toDecimal(totalAtomic),
          errors: sanitizeErrors(data.errors),
        }),
      );
      return;
    }

    if (method === 'GET' && u.pathname === '/api/txs') {
      const limitParam = u.searchParams.get('limit');
      const envLimitRaw = process.env.CLUSTER_AGG_TX_LIMIT;
      const limitRaw = limitParam != null && limitParam !== '' ? limitParam : envLimitRaw;
      const limitParsed = limitRaw != null ? Number(limitRaw) : NaN;
      const limit =
        Number.isFinite(limitParsed) && limitParsed > 0 ? Math.floor(limitParsed) : 20;
      const perNodeLimit = Math.max(limit * 2, 20);

      const errors = [];
      const txMap = new Map();

      const now = Date.now();
      try {
        await getHeartbeatsMap();
      } catch {}
      pruneDiscoveredNodes(now);
      const allBases = [...new Set([...NODE_BASES, ...discoveredNodes.keys()])];

      const results = await promiseAllSettledWithConcurrency(
        allBases.map((base) => () => fetchNodeTxs(base, perNodeLimit)), MAX_CONCURRENT_REQUESTS
      );

      for (const r of results) {
        if (r.status !== 'fulfilled') {
          errors.push('Request failed');
          continue;
        }
        const { ok, data, error } = r.value;
        if (!ok) {
          errors.push(String(error || 'error'));
          continue;
        }
        const list = Array.isArray(data.transactions) ? data.transactions : [];
        for (const tx of list) {
          const key = String(tx.txHash || '') + ':' + String(tx.type || '');
          if (!key.trim()) continue;
          const existing = txMap.get(key);
          if (!existing || (tx.blockNumber ?? 0) > (existing.blockNumber ?? 0)) {
            txMap.set(key, { ...tx });
          }
        }
      }

      const all = Array.from(txMap.values());
      all.sort((a, b) => {
        const ta = typeof a.timestamp === 'number' ? a.timestamp : 0;
        const tb = typeof b.timestamp === 'number' ? b.timestamp : 0;
        if (ta !== tb) return tb - ta;
        if ((a.blockNumber || 0) !== (b.blockNumber || 0)) {
          return (b.blockNumber || 0) - (a.blockNumber || 0);
        }
        if (a.logIndex != null && b.logIndex != null) return b.logIndex - a.logIndex;
        return 0;
      });

      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(
        JSON.stringify({
          timestamp: Date.now(),
          totalNodes: allBases.length,
          totalTransactions: all.length,
          transactions: all.slice(0, limit),
          errors,
        }),
      );
      return;
    }

    if (method === 'GET' && u.pathname === '/healthz') {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(
        JSON.stringify({
          status: 'ok',
          nodesConfigured: NODE_BASES.length,
          minEligibleNodes: MIN_ELIGIBLE_NODES,
        }),
      );
      return;
    }

    res.writeHead(404, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: 'Not found' }));
  } catch (e) {
    const msg = e && e.message ? e.message : String(e);
    res.writeHead(500, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: 'Internal error', message: msg }));
  }
});

server.listen(PORT, () => {
  console.log(
    `[ClusterAgg] HTTP aggregator listening on port ${PORT} (nodes: ${NODE_BASES.length}, minEligible: ${MIN_ELIGIBLE_NODES})`,
  );
});
