import fs from 'fs';
import http from 'http';
import https from 'https';
import axios from 'axios';
import { tryParseJSON } from '../json.js';
import { deriveRound } from '../pbft-utils.js';
import { computeTxsetHash, getTxset, putTxset } from '../txset-store.js';
import { scheduleWithdrawalStateSave } from '../pending-store.js';
import { withMoneroLock } from '../../monero-lock.js';
import metrics from '../../../core/metrics.js';
import { logSwallowedError } from '../../../utils/rate-limited-log.js';
import { getBurnOnChainStatus } from '../verification.js';
import { requestFraudSlashBatch } from '../../../cluster/fraud-slash.js';
import {
  WITHDRAWAL_SECURITY_HARDEN,
  WITHDRAWAL_VERIFY_BURNS_ONCHAIN,
  WITHDRAWAL_VERIFY_TXSET,
  WITHDRAWAL_ROUND_V2,
  WITHDRAWAL_ROUND_V2_DUALWRITE,
  WITHDRAWAL_SIGN_STEP_RESPONSE_ROUND,
  WITHDRAWAL_SIGN_STEP_TIMEOUT_MS,
  WITHDRAWAL_BATCH_MANIFEST_PBFT,
  WITHDRAWAL_MANIFEST_WAIT_MS,
} from '../config.js';
import { waitForBatchManifest } from '../batch/manifest.js';

const HANDLED_SIGN_STEPS_MAX = Number(process.env.HANDLED_SIGN_STEPS_MAX || 5000);
const SIGN_STEP_HTTP_TIMEOUT_MS = Number(process.env.SIGN_STEP_HTTP_TIMEOUT_MS || 60000);
const SIGN_STEP_HTTP_RETRIES = Number(process.env.SIGN_STEP_HTTP_RETRIES || 3);
const SIGN_STEP_POST_CACHE_MAX = Number(process.env.SIGN_STEP_POST_CACHE_MAX || 5000);

let _apiHttpClient = null;
function getApiHttpClient() {
  if (_apiHttpClient) return _apiHttpClient;
  const keepAlive = process.env.SIGN_STEP_HTTP_KEEP_ALIVE !== '0';
  const agentOpts = { keepAlive, keepAliveMsecs: 10000, maxSockets: 32, maxFreeSockets: 10 };
  const httpAgent = new http.Agent(agentOpts);
  const tlsEnabled = process.env.BRIDGE_API_TLS_ENABLED === '1';
  let httpsAgent;
  if (tlsEnabled) {
    const keyPath = String(process.env.BRIDGE_API_TLS_KEY_PATH || '').trim();
    const certPath = String(process.env.BRIDGE_API_TLS_CERT_PATH || '').trim();
    const caPath = String(process.env.BRIDGE_API_TLS_CA_PATH || '').trim();
    if (!keyPath || !certPath || !caPath) throw new Error('BRIDGE_API_TLS_ENABLED=1 requires BRIDGE_API_TLS_KEY_PATH/BRIDGE_API_TLS_CERT_PATH/BRIDGE_API_TLS_CA_PATH');
    httpsAgent = new https.Agent({
      ...agentOpts,
      key: fs.readFileSync(keyPath),
      cert: fs.readFileSync(certPath),
      ca: fs.readFileSync(caPath),
      rejectUnauthorized: true,
      minVersion: 'TLSv1.2',
    });
  } else {
    httpsAgent = new https.Agent(agentOpts);
  }
  _apiHttpClient = axios.create({ httpAgent, httpsAgent });
  return _apiHttpClient;
}

function sleepMs(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function markHandledSignStep(node, key) {
  if (!node || !key) return;
  const set = node._handledSignSteps;
  if (!set || typeof set.add !== 'function') return;
  set.add(key);
  const max = HANDLED_SIGN_STEPS_MAX;
  if (!Number.isFinite(max) || max <= 0) return;
  while (set.size > max) {
    const oldest = set.values().next().value;
    if (!oldest) break;
    set.delete(oldest);
  }
}

function getSignStepPostCache(node) {
  if (!node._signStepPostCache) node._signStepPostCache = new Map();
  return node._signStepPostCache;
}

function putSignStepPostCache(node, cacheKey, entry) {
  const cache = getSignStepPostCache(node);
  cache.set(cacheKey, entry);
  const max = SIGN_STEP_POST_CACHE_MAX;
  if (Number.isFinite(max) && max > 0) {
    while (cache.size > max) {
      const oldest = cache.keys().next().value;
      if (!oldest) break;
      cache.delete(oldest);
    }
  }
}

function normalizeApiBase(apiBase) {
  const b = String(apiBase || '').trim();
  if (!b) return '';
  return b.replace(/\/+$/, '');
}

function getAuthToken() {
  const t = process.env.BRIDGE_API_AUTH_TOKEN;
  return typeof t === 'string' && t ? t : '';
}

async function fetchSignerSetFromCoordinator(node, withdrawalKey, attempt, coordinatorLc, expectedSyncDigest) {
  if (!node || !node.p2p || !node._activeClusterId) return null;
  const key = String(withdrawalKey || '').toLowerCase();
  if (!key) return null;
  const coord = coordinatorLc ? String(coordinatorLc).toLowerCase() : '';
  if (!coord) return null;
  const n = Number(attempt);
  const a = Number.isFinite(n) && n >= 0 ? Math.floor(n) : 0;
  const round = deriveRound('withdrawal-signer-set', `${key}:${a}`);
  if (!round || !Number.isFinite(round) || round <= 0) return null;
  const clusterId = node._activeClusterId;
  const sessionId = node._sessionId || 'bridge';
  let roundData = null;
  try {
    roundData = await node.p2p.getRoundData(clusterId, sessionId, round);
  } catch {
    roundData = null;
  }
  const entries = roundData && typeof roundData === 'object' ? Object.entries(roundData) : [];
  for (const [sender, raw] of entries) {
    const senderLc = sender ? String(sender).toLowerCase() : '';
    if (senderLc !== coord) continue;
    const data = tryParseJSON(raw, `withdrawal-signer-set from ${senderLc}`);
    if (!data || data.type !== 'withdrawal-signer-set') continue;
    if (!data.withdrawalTxHash || String(data.withdrawalTxHash).toLowerCase() !== key) continue;
    const ar = Number(data.attempt);
    const aa = Number.isFinite(ar) && ar >= 0 ? Math.floor(ar) : null;
    if (aa === null || aa !== a) continue;
    const dig = typeof data.syncDigest === 'string' ? String(data.syncDigest).toLowerCase() : '';
    if (expectedSyncDigest && dig && dig !== expectedSyncDigest) continue;
    const set = Array.isArray(data.signerSet)
      ? data.signerSet.map((x) => String(x || '').toLowerCase()).filter(Boolean)
      : [];
    if (set.length === 0) continue;
    const hash = typeof data.signerSetHash === 'string' ? String(data.signerSetHash).toLowerCase() : null;
    return { signerSet: set, signerSetHash: hash };
  }
  return null;
}

async function getApiBaseForPeer(node, peerAddressLc) {
  if (!node || !peerAddressLc || !node.p2p || typeof node.p2p.getHeartbeats !== 'function') return '';
  const now = Date.now();
  const cached = node._withdrawalApiBaseCache;
  if (!cached || !cached.ts || !cached.map || now - cached.ts > 3000) {
    const hbRaw = process.env.HEARTBEAT_INTERVAL;
    const hbParsed = hbRaw != null ? Number(hbRaw) : NaN;
    const hbIntervalSec = Number.isFinite(hbParsed) && hbParsed > 0 ? hbParsed : 30;
    const ttlMs = hbIntervalSec * 5 * 1000;
    let map;
    try {
      map = await node.p2p.getHeartbeats(ttlMs);
    } catch {
      map = null;
    }
    node._withdrawalApiBaseCache = { ts: now, map };
  }
  const m = node._withdrawalApiBaseCache && node._withdrawalApiBaseCache.map;
  const data = m && typeof m.get === 'function' ? m.get(peerAddressLc) : null;
  const apiBase = data && data.apiBase ? String(data.apiBase) : '';
  return normalizeApiBase(apiBase);
}

async function fetchTxsetFromCoordinator(apiBase, txsetHash, authToken) {
  const base = normalizeApiBase(apiBase);
  if (!base || !txsetHash) return null;
  const url = `${base}/bridge/withdrawal/txset/${txsetHash}`;
  const headers = authToken ? { Authorization: `Bearer ${authToken}` } : {};
  try {
    const resp = await getApiHttpClient().get(url, {
      headers,
      timeout: SIGN_STEP_HTTP_TIMEOUT_MS,
      maxContentLength: Infinity,
      maxBodyLength: Infinity,
      validateStatus: () => true,
    });
    if (!resp || resp.status !== 200 || !resp.data) return null;
    const txDataHex = resp.data.txDataHex;
    if (typeof txDataHex !== 'string' || !txDataHex) return null;
    return txDataHex;
  } catch (e) {
    logSwallowedError('withdrawal_sign_step_fetch_txset_error', e);
    return null;
  }
}

async function postSignStepResult(apiBase, authToken, body) {
  const base = normalizeApiBase(apiBase);
  if (!base) return { ok: false };
  const url = `${base}/bridge/withdrawal/sign-step`;
  const headers = authToken ? { Authorization: `Bearer ${authToken}` } : {};
  try {
    const resp = await getApiHttpClient().post(url, body, {
      headers,
      timeout: SIGN_STEP_HTTP_TIMEOUT_MS,
      maxContentLength: Infinity,
      maxBodyLength: Infinity,
      validateStatus: () => true,
    });
    if (resp && resp.status === 200 && resp.data && resp.data.ok) return { ok: true };
    if (resp && resp.status === 409) return { ok: true, ignore: true };
    return { ok: false, status: resp ? resp.status : null };
  } catch (e) {
    logSwallowedError('withdrawal_sign_step_post_result_error', e);
    return { ok: false };
  }
}

async function broadcastSignStepNotice(node, withdrawalKey, attempt, notice) {
  if (!node || !node.p2p || !node._activeClusterId || !notice) return;
  const clusterId = node._activeClusterId;
  const sessionId = node._sessionId || 'bridge';
  const keyBase = String(withdrawalKey || '').toLowerCase();
  const attemptRaw = Number(attempt);
  const a = Number.isFinite(attemptRaw) && attemptRaw >= 0 ? Math.floor(attemptRaw) : 0;
  const key = keyBase ? `${keyBase}:${a}` : '';
  if (WITHDRAWAL_ROUND_V2 && key) {
    const r2 = deriveRound('withdrawal-sign-step-response', key);
    if (r2 && Number.isFinite(r2) && r2 > 0) {
      try {
        await node.p2p.broadcastRoundData(clusterId, sessionId, r2, notice);
      } catch {}
    }
  }
  if (!WITHDRAWAL_ROUND_V2 || WITHDRAWAL_ROUND_V2_DUALWRITE) {
    if (Number.isFinite(WITHDRAWAL_SIGN_STEP_RESPONSE_ROUND) && WITHDRAWAL_SIGN_STEP_RESPONSE_ROUND > 0) {
      try {
        await node.p2p.broadcastRoundData(clusterId, sessionId, WITHDRAWAL_SIGN_STEP_RESPONSE_ROUND, notice);
      } catch {}
    }
  }
}

async function sendCachedSignStep(node, cached, coordinatorLc, authToken) {
  const apiBase = await getApiBaseForPeer(node, coordinatorLc);
  if (!apiBase || !authToken) return { ok: false };
  const now = Date.now();
  const lastAttemptAt = cached && typeof cached.lastAttemptAt === 'number' ? cached.lastAttemptAt : 0;
  if (now - lastAttemptAt < 1000) return { ok: false };
  cached.lastAttemptAt = now;
  if (cached.busy) {
    const res = await postSignStepResult(apiBase, authToken, {
      withdrawalTxHash: cached.withdrawalKey,
      stepIndex: cached.stepIndex,
      signer: cached.signer,
      txsetHashIn: cached.txsetHashIn,
      busy: true,
    });
    return res && res.ok ? { ok: true, ignore: !!res.ignore } : { ok: false };
  }
  if (cached.stale) {
    const res = await postSignStepResult(apiBase, authToken, {
      withdrawalTxHash: cached.withdrawalKey,
      stepIndex: cached.stepIndex,
      signer: cached.signer,
      txsetHashIn: cached.txsetHashIn,
      stale: true,
    });
    return res && res.ok ? { ok: true, ignore: !!res.ignore } : { ok: false };
  }
  const txDataHexOut = cached.txsetHashOut ? getTxset(node, cached.txsetHashOut) : null;
  if (!txDataHexOut) return { ok: false, drop: true };
  const res = await postSignStepResult(apiBase, authToken, {
    withdrawalTxHash: cached.withdrawalKey,
    stepIndex: cached.stepIndex,
    signer: cached.signer,
    txsetHashIn: cached.txsetHashIn,
    txDataHexOut,
  });
  return res && res.ok ? { ok: true, ignore: !!res.ignore } : { ok: false };
}

async function getPendingBurnStatus(node, pending) {
  if (!node || !pending) return 'unknown';
  const clusterId = node._activeClusterId || null;
  if (!clusterId) return 'unknown';
  if (pending.isBatch && Array.isArray(pending.batchWithdrawals) && pending.batchWithdrawals.length > 0) {
    let anyUnknown = false;
    for (const w of pending.batchWithdrawals) {
      if (!w || !w.txHash) return 'unknown';
      const status = await getBurnOnChainStatus(node, w.txHash, {
        user: w.user,
        clusterId,
        xmrAddress: w.xmrAddress,
        burnAmount: w.zxmrAmount,
      });
      if (status === 'missing') return 'missing';
      if (status !== 'verified') anyUnknown = true;
    }
    return anyUnknown ? 'unknown' : 'verified';
  }
  const txHash = pending.txHash || pending.ethTxHash;
  if (!txHash) return 'unknown';
  return getBurnOnChainStatus(node, txHash, {
    user: pending.user,
    clusterId,
    xmrAddress: pending.xmrAddress,
    burnAmount: pending.zxmrAmount,
  });
}

function buildBatchPendingFromManifest(manifest, coordinatorLc) {
  const burns = manifest && Array.isArray(manifest.burns) ? manifest.burns : [];
  const batchWithdrawals = [];
  const ethTxHashes = [];
  for (const b of burns) {
    const txHash = b && typeof b.txHash === 'string' ? b.txHash.toLowerCase() : '';
    const xmrAddress = b && b.xmrAddress != null ? String(b.xmrAddress) : '';
    const xmrAmountAtomic = b && b.xmrAmountAtomic != null ? String(b.xmrAmountAtomic) : '';
    if (!/^0x[0-9a-f]{64}$/.test(txHash) || !xmrAddress || !xmrAmountAtomic) continue;
    batchWithdrawals.push({
      txHash,
      user: b.user != null ? b.user : null,
      xmrAddress,
      zxmrAmount: b.zxmrAmount != null ? b.zxmrAmount : null,
      xmrAmount: xmrAmountAtomic,
      xmrAmountAtomic,
      blockNumber: typeof b.blockNumber === 'number' ? b.blockNumber : null,
      timestamp: typeof b.timestamp === 'number' ? b.timestamp : null,
      priorityClass: b.priorityClass != null ? b.priorityClass : null,
    });
    ethTxHashes.push(txHash);
  }
  const batchId = manifest && typeof manifest.batchId === 'string' ? manifest.batchId.toLowerCase() : null;
  const manifestHash = manifest && typeof manifest.manifestHash === 'string' ? manifest.manifestHash.toLowerCase() : null;
  const a0 = manifest && typeof manifest.attempt !== 'undefined' ? Number(manifest.attempt) : NaN;
  const manifestAttempt = Number.isFinite(a0) && a0 >= 0 ? Math.floor(a0) : null;
  return { isBatch: true, batchId, coordinator: coordinatorLc || null, batchWithdrawals, ethTxHashes, manifestHash, manifestAttempt };
}

export function setupWithdrawalSignStepListener(
  node,
  {
    verifyPendingBurnsOnChain,
    verifyTxsetMatchesPending,
  } = {},
) {
  if (!node || !node.p2p || node._withdrawalSignStepListenerSetup) return;
  if (typeof verifyPendingBurnsOnChain !== 'function') {
    throw new TypeError('setupWithdrawalSignStepListener requires verifyPendingBurnsOnChain');
  }
  if (typeof verifyTxsetMatchesPending !== 'function') {
    throw new TypeError('setupWithdrawalSignStepListener requires verifyTxsetMatchesPending');
  }
  node._withdrawalSignStepListenerSetup = true;
  node._handledSignSteps = node._handledSignSteps || new Set();
  const pollSignSteps = async () => {
    if (!node._withdrawalMonitorRunning) return;
    try {
      const clusterId = node._activeClusterId;
      const sessionId = node._sessionId || 'bridge';
      const memberSet = new Set(
        Array.isArray(node._clusterMembers)
          ? node._clusterMembers.map((a) => String(a || '').toLowerCase())
          : [],
      );
      const self = node.wallet && node.wallet.address ? String(node.wallet.address).toLowerCase() : '';
      const authToken = getAuthToken();
      const roundsToPoll = [9900];
      if (self && node._pendingWithdrawals && typeof node._pendingWithdrawals.entries === 'function') {
        for (const [k, v] of node._pendingWithdrawals.entries()) {
          const withdrawalKey = k ? String(k).toLowerCase() : '';
          if (!withdrawalKey) continue;
          const pbftKey = v && typeof v.pbftUnsignedHash === 'string' ? String(v.pbftUnsignedHash).toLowerCase() : '';
          const attemptRaw = v && typeof v.attempt !== 'undefined' ? Number(v.attempt) : NaN;
          const attempt = Number.isFinite(attemptRaw) && attemptRaw >= 0 ? Math.floor(attemptRaw) : 0;
          const r = deriveRound('withdrawal-sign-step-request', `${withdrawalKey}:${attempt}:${pbftKey}:${self}`);
          if (r && Number.isFinite(r) && r > 0 && !roundsToPoll.includes(r)) roundsToPoll.push(r);
        }
      }
      for (const round of roundsToPoll) {
        let roundData = null;
        try {
          roundData = await node.p2p.getRoundData(clusterId, sessionId, round);
        } catch {
          continue;
        }
        const entries = roundData && typeof roundData === 'object' ? Object.entries(roundData) : [];
        if (entries.length === 0) continue;
        for (const [sender, raw] of entries) {
          const senderLc = sender ? String(sender).toLowerCase() : '';
          if (!senderLc || (memberSet.size > 0 && !memberSet.has(senderLc))) continue;
          try {
            const data = tryParseJSON(raw, `sign-step from ${senderLc}`);
            if (!data) continue;
            if (!data || data.type !== 'withdrawal-sign-step-request') continue;
            if (!data.withdrawalTxHash || !data.signer) continue;
            const signerLc = String(data.signer).toLowerCase();
            if (!self || signerLc !== self) continue;
            const idx = Number(data.stepIndex);
            if (!Number.isFinite(idx) || idx < 0) continue;
            const withdrawalKey = String(data.withdrawalTxHash).toLowerCase();
            const txsetHashIn = typeof data.txsetHashIn === 'string' && /^0x[0-9a-fA-F]{64}$/.test(data.txsetHashIn)
              ? data.txsetHashIn.toLowerCase()
              : '';
            if (!txsetHashIn) continue;
            const reqAttemptRaw = typeof data.attempt !== 'undefined' ? Number(data.attempt) : NaN;
            const reqAttempt = Number.isFinite(reqAttemptRaw) && reqAttemptRaw >= 0 ? Math.floor(reqAttemptRaw) : 0;
            const requestKey = `${withdrawalKey}:${reqAttempt}:${idx}:${txsetHashIn}`;
            const cacheKey = `${withdrawalKey}:${reqAttempt}:${idx}:${txsetHashIn}`;
            const cache = getSignStepPostCache(node);
            const cached = cache.get(cacheKey);
            if (cached) {
              if (node._handledSignSteps.has(requestKey)) {
                const okAt = cached && typeof cached.lastOkAt === 'number' ? cached.lastOkAt : 0;
                if (okAt && Date.now() - okAt < 60000) continue;
              }
              if (cached.busy || cached.stale) {
                const a0 = Number(cached.attempt);
                const a = Number.isFinite(a0) && a0 >= 0 ? Math.floor(a0) : null;
                if (a === null || a !== reqAttempt) {
                  cache.delete(cacheKey);
                } else {
                  const sent = await sendCachedSignStep(node, cached, senderLc, authToken);
                  if (sent.ok || sent.drop) {
                    if (sent.ok) cached.lastOkAt = Date.now();
                    if (sent.drop || cached.busy || cached.stale) cache.delete(cacheKey);
                    markHandledSignStep(node, requestKey);
                  }
                  continue;
                }
              } else {
                const sent = await sendCachedSignStep(node, cached, senderLc, authToken);
                if (sent.ok || sent.drop) {
                  if (sent.ok) cached.lastOkAt = Date.now();
                  if (sent.drop || cached.busy || cached.stale) cache.delete(cacheKey);
                  markHandledSignStep(node, requestKey);
                }
                continue;
              }
            }
            if (node._handledSignSteps.has(requestKey)) continue;
            const startedAt = Date.now();
            const pending = node._pendingWithdrawals && node._pendingWithdrawals.get(withdrawalKey);
            const manifestHashReq = typeof data.manifestHash === 'string' && /^0x[0-9a-fA-F]{64}$/.test(data.manifestHash)
              ? data.manifestHash.toLowerCase()
              : '';
            const mar = typeof data.manifestAttempt !== 'undefined' ? Number(data.manifestAttempt) : NaN;
            const manifestAttempt = Number.isFinite(mar) && mar >= 0 ? Math.floor(mar) : reqAttempt;
            const isBatchKey = withdrawalKey.includes(':');
            let pendingForVerify = pending;
            if (WITHDRAWAL_BATCH_MANIFEST_PBFT && manifestHashReq && isBatchKey) {
              const localHash = pending && typeof pending.manifestHash === 'string' ? String(pending.manifestHash).toLowerCase() : '';
              const need = !pending || !pending.isBatch || !Array.isArray(pending.batchWithdrawals) || pending.batchWithdrawals.length === 0 || !localHash || localHash !== manifestHashReq;
              if (need) {
                const remainingMs = startedAt + WITHDRAWAL_SIGN_STEP_TIMEOUT_MS - 4000 - Date.now();
                const waitMs = remainingMs > 0 ? Math.min(WITHDRAWAL_MANIFEST_WAIT_MS, remainingMs) : 0;
                if (waitMs > 0) {
                  const mres = await waitForBatchManifest(node, withdrawalKey, manifestAttempt, senderLc, waitMs);
                  if (mres && mres.manifest && mres.manifestHash && mres.manifestHash === manifestHashReq) {
                    const built = buildBatchPendingFromManifest(mres.manifest, senderLc);
                    if (built && Array.isArray(built.batchWithdrawals) && built.batchWithdrawals.length > 0) {
                      pendingForVerify = built;
                      if (pending) {
                        pending.isBatch = true;
                        pending.batchId = withdrawalKey;
                        pending.batchWithdrawals = built.batchWithdrawals;
                        pending.ethTxHashes = built.ethTxHashes;
                        pending.manifestHash = manifestHashReq;
                        pending.manifestAttempt = manifestAttempt;
                        pending.burnsOnChainVerified = false;
                        pending.txsetVerified = false;
                        scheduleWithdrawalStateSave(node);
                      }
                    }
                  }
                }
              }
            }
            let lockVal = node._withdrawalMultisigLock;
            if (lockVal && lockVal !== withdrawalKey) {
              const deadline = startedAt + Math.max(0, WITHDRAWAL_SIGN_STEP_TIMEOUT_MS - 2000);
              while (Date.now() < deadline) {
                await sleepMs(250);
                lockVal = node._withdrawalMultisigLock;
                if (!lockVal || lockVal === withdrawalKey) break;
              }
            }
            const busyBecauseLock = !!(lockVal && lockVal !== withdrawalKey);
            if (busyBecauseLock) {
              try {
                const reason = `withdrawal_lock_active:${String(lockVal || '')}`;
                console.log(`[Withdrawal] Sign-step busy (${reason}) for ${withdrawalKey.slice(0, 18)} step=${idx} attempt=${reqAttempt}`);
              } catch {}
              try {
                const notice = JSON.stringify({
                  type: 'withdrawal-sign-step-busy',
                  withdrawalTxHash: data.withdrawalTxHash,
                  stepIndex: idx,
                  signer: node.wallet.address,
                  txsetHashIn,
                  pbftUnsignedHash: data.pbftUnsignedHash || null,
                  attempt: reqAttempt,
                });
                await broadcastSignStepNotice(node, withdrawalKey, reqAttempt, notice);
              } catch {}
              putSignStepPostCache(node, cacheKey, {
                withdrawalKey,
                stepIndex: idx,
                signer: node.wallet.address,
                txsetHashIn,
                attempt: reqAttempt,
                busy: true,
                lastAttemptAt: 0,
              });
              try {
                const sent = await sendCachedSignStep(node, getSignStepPostCache(node).get(cacheKey), senderLc, authToken);
                if (sent.ok || sent.drop) {
                  getSignStepPostCache(node).delete(cacheKey);
                  markHandledSignStep(node, requestKey);
                }
              } catch {}
              continue;
            }
            if (WITHDRAWAL_SECURITY_HARDEN && pending) {
              const coordinator = pending.coordinator ? String(pending.coordinator).toLowerCase() : '';
              if (!coordinator || coordinator !== senderLc) continue;
              const h1 = pending.pbftUnsignedHash ? String(pending.pbftUnsignedHash).toLowerCase() : '';
              const h2 = data.pbftUnsignedHash ? String(data.pbftUnsignedHash).toLowerCase() : '';
              if (!h1 || !h2 || h1 !== h2) continue;
            }
            const expectedSyncDigest = typeof data.syncDigest === 'string' ? String(data.syncDigest).toLowerCase() : '';
            if (expectedSyncDigest) {
              const localDigest = pending && typeof pending.multisigSyncDigest === 'string' ? String(pending.multisigSyncDigest).toLowerCase() : '';
              if (!localDigest || localDigest !== expectedSyncDigest) {
                try {
                  const notice = JSON.stringify({
                    type: 'withdrawal-sign-step-stale',
                    withdrawalTxHash: data.withdrawalTxHash,
                    stepIndex: idx,
                    signer: node.wallet.address,
                    txsetHashIn,
                    pbftUnsignedHash: data.pbftUnsignedHash || null,
                    attempt: reqAttempt,
                  });
                  await broadcastSignStepNotice(node, withdrawalKey, reqAttempt, notice);
                } catch {}
                putSignStepPostCache(node, cacheKey, {
                  withdrawalKey,
                  stepIndex: idx,
                  signer: node.wallet.address,
                  txsetHashIn,
                  attempt: reqAttempt,
                  stale: true,
                  lastAttemptAt: 0,
                });
                try {
                  const sent = await sendCachedSignStep(node, getSignStepPostCache(node).get(cacheKey), senderLc, authToken);
                  if (sent.ok || sent.drop) {
                    getSignStepPostCache(node).delete(cacheKey);
                    markHandledSignStep(node, requestKey);
                  }
                } catch {}
                continue;
              }
            }
            const expectedSignerSetHash = typeof data.signerSetHash === 'string' ? String(data.signerSetHash).toLowerCase() : '';
            if (pending) {
              const ssaRaw = typeof pending.signerSetAttempt === 'number' ? Number(pending.signerSetAttempt) : null;
              const ssa = ssaRaw !== null && Number.isFinite(ssaRaw) && ssaRaw >= 0 ? Math.floor(ssaRaw) : null;
              let signerSet = Array.isArray(pending.signerSet)
                ? pending.signerSet.map((a) => String(a || '').toLowerCase()).filter(Boolean)
                : null;
              let signerSetHash = typeof pending.signerSetHash === 'string' ? String(pending.signerSetHash).toLowerCase() : '';
              if (!signerSet || signerSet.length === 0 || ssa !== reqAttempt || (expectedSignerSetHash && (!signerSetHash || signerSetHash !== expectedSignerSetHash))) {
                const loaded = await fetchSignerSetFromCoordinator(node, withdrawalKey, reqAttempt, senderLc, expectedSyncDigest);
                if (loaded && Array.isArray(loaded.signerSet) && loaded.signerSet.length > 0) {
                  signerSet = loaded.signerSet;
                  signerSetHash = loaded.signerSetHash || signerSetHash;
                  pending.signerSet = signerSet;
                  pending.signerSetAttempt = reqAttempt;
                  pending.signerSetHash = signerSetHash || null;
                  scheduleWithdrawalStateSave(node);
                }
              }
              if (!signerSet || signerSet.length === 0) continue;
              if (idx >= signerSet.length) continue;
              if (signerSet[idx] !== self) continue;
              if (expectedSignerSetHash && (!signerSetHash || signerSetHash !== expectedSignerSetHash)) continue;
            }
            if (pending) {
              const prevAttemptRaw = typeof pending.lastSignStepAttempt === 'number' ? Number(pending.lastSignStepAttempt) : null;
              const prevAttempt = prevAttemptRaw !== null && Number.isFinite(prevAttemptRaw) && prevAttemptRaw >= 0 ? Math.floor(prevAttemptRaw) : null;
              if (prevAttempt !== null && prevAttempt === reqAttempt) {
                const prevIdxRaw = typeof pending.lastSignStepIndex === 'number' ? Number(pending.lastSignStepIndex) : null;
                const prevIdx = prevIdxRaw !== null && Number.isFinite(prevIdxRaw) && prevIdxRaw >= 0 ? Math.floor(prevIdxRaw) : null;
                if (prevIdx !== null && prevIdx !== idx) continue;
                const prevTxsetHashIn = typeof pending.lastSignStepTxsetHashIn === 'string' ? String(pending.lastSignStepTxsetHashIn).toLowerCase() : '';
                if (prevTxsetHashIn && prevTxsetHashIn !== txsetHashIn) continue;
              }
            }
            if (WITHDRAWAL_VERIFY_BURNS_ONCHAIN) {
              const burnPending = pendingForVerify;
              if (!burnPending) {
                if (!/^0x[a-fA-F0-9]{64}$/.test(withdrawalKey)) {
                  if (isBatchKey) {
                    try {
                      const notice = JSON.stringify({
                        type: 'withdrawal-sign-step-error',
                        withdrawalTxHash: data.withdrawalTxHash,
                        stepIndex: idx,
                        signer: node.wallet.address,
                        txsetHashIn,
                        pbftUnsignedHash: data.pbftUnsignedHash || null,
                        attempt: reqAttempt,
                        reason: 'pending_missing',
                      });
                      await broadcastSignStepNotice(node, withdrawalKey, reqAttempt, notice);
                    } catch {}
                    markHandledSignStep(node, requestKey);
                  }
                  continue;
                }
                const status = await getBurnOnChainStatus(node, withdrawalKey);
                if (status === 'missing') {
                  await requestFraudSlashBatch(node, [sender], withdrawalKey, 'missing_burn');
                }
                continue;
              }
              if (!(pending && pending.burnsOnChainVerified)) {
                const ok = await verifyPendingBurnsOnChain(node, burnPending);
                if (!ok) {
                  const status = await getPendingBurnStatus(node, burnPending);
                  if (status === 'missing') {
                    await requestFraudSlashBatch(node, [sender], withdrawalKey, 'missing_burn');
                  }
                  continue;
                }
                if (pending) {
                  pending.burnsOnChainVerified = true;
                  scheduleWithdrawalStateSave(node);
                }
              }
            }
            const coordinatorApiBase = await getApiBaseForPeer(node, senderLc);
            if (!coordinatorApiBase || !authToken) {
              continue;
            }
            let txDataHex = null;
            for (let attempt = 0; attempt < SIGN_STEP_HTTP_RETRIES; attempt += 1) {
              txDataHex = await fetchTxsetFromCoordinator(coordinatorApiBase, txsetHashIn, authToken);
              if (txDataHex) break;
              await sleepMs(200 * 2 ** attempt);
            }
            if (!txDataHex) continue;
            const computed = computeTxsetHash(txDataHex);
            if (!computed || computed.toLowerCase() !== txsetHashIn) {
              continue;
            }
            if (WITHDRAWAL_VERIFY_TXSET) {
              let verifyPending = pendingForVerify;
              if (!verifyPending) {
                if (isBatchKey) {
                  try {
                    const notice = JSON.stringify({
                      type: 'withdrawal-sign-step-error',
                      withdrawalTxHash: data.withdrawalTxHash,
                      stepIndex: idx,
                      signer: node.wallet.address,
                      txsetHashIn,
                      pbftUnsignedHash: data.pbftUnsignedHash || null,
                      attempt: reqAttempt,
                      reason: 'pending_missing',
                    });
                    await broadcastSignStepNotice(node, withdrawalKey, reqAttempt, notice);
                  } catch {}
                  markHandledSignStep(node, requestKey);
                }
                continue;
              }
              if (!(pending && pending.txsetVerified)) {
                let ok = await verifyTxsetMatchesPending(node, txDataHex, verifyPending);
                if (!ok && WITHDRAWAL_BATCH_MANIFEST_PBFT && manifestHashReq && isBatchKey) {
                  const remainingMs = startedAt + WITHDRAWAL_SIGN_STEP_TIMEOUT_MS - 3000 - Date.now();
                  const waitMs = remainingMs > 0 ? Math.min(WITHDRAWAL_MANIFEST_WAIT_MS, remainingMs) : 0;
                  if (waitMs > 0) {
                    const mres = await waitForBatchManifest(node, withdrawalKey, manifestAttempt, senderLc, waitMs);
                    if (mres && mres.manifest && mres.manifestHash && mres.manifestHash === manifestHashReq) {
                      const built = buildBatchPendingFromManifest(mres.manifest, senderLc);
                      if (built && Array.isArray(built.batchWithdrawals) && built.batchWithdrawals.length > 0) {
                        verifyPending = built;
                        pendingForVerify = built;
                        ok = await verifyTxsetMatchesPending(node, txDataHex, built);
                        if (ok && pending) {
                          pending.isBatch = true;
                          pending.batchId = withdrawalKey;
                          pending.batchWithdrawals = built.batchWithdrawals;
                          pending.ethTxHashes = built.ethTxHashes;
                          pending.manifestHash = manifestHashReq;
                          pending.manifestAttempt = manifestAttempt;
                          pending.burnsOnChainVerified = false;
                          pending.txsetVerified = false;
                        }
                      }
                    }
                  }
                }
                if (!ok) {
                  try {
                    const notice = JSON.stringify({
                      type: 'withdrawal-sign-step-error',
                      withdrawalTxHash: data.withdrawalTxHash,
                      stepIndex: idx,
                      signer: node.wallet.address,
                      txsetHashIn,
                      pbftUnsignedHash: data.pbftUnsignedHash || null,
                      attempt: reqAttempt,
                      reason: 'txset_pending_mismatch',
                    });
                    await broadcastSignStepNotice(node, withdrawalKey, reqAttempt, notice);
                  } catch {}
                  markHandledSignStep(node, requestKey);
                  continue;
                }
                if (pending) {
                  pending.txsetVerified = true;
                  scheduleWithdrawalStateSave(node);
                }
              }
            }
            if (!pendingForVerify) continue;
            const existingLock2 = node._withdrawalMultisigLock;
            const lockAcquired2 = !existingLock2;
            if (lockAcquired2) node._withdrawalMultisigLock = withdrawalKey;
            let signedTx;
            try {
              const remaining = startedAt + WITHDRAWAL_SIGN_STEP_TIMEOUT_MS - 1000 - Date.now();
              const timeoutMs = remaining > 0 ? remaining : 1;
              signedTx = await withMoneroLock(node, { key: withdrawalKey, op: 'withdrawal_sign_step', timeoutMs }, async () => {
                return await node.monero.signMultisig(txDataHex);
              });
            } catch (e) {
              const msg = (e && e.message) ? e.message : String(e);
              const code = e && (e.code !== undefined ? e.code : e.errorCode !== undefined ? e.errorCode : e.status);
              console.log('[Withdrawal] signMultisig error:', code, msg);
              if (msg.includes('stale data') || msg.includes('export fresh multisig data')) {
                metrics.incrementCounter('monero_withdrawal_sign_step_stale_total', 1);
              console.log('[Withdrawal] Sign step stale multisig data, notifying coordinator');
                try {
                  const notice = JSON.stringify({
                    type: 'withdrawal-sign-step-stale',
                    withdrawalTxHash: data.withdrawalTxHash,
                    stepIndex: idx,
                    signer: node.wallet.address,
                    txsetHashIn,
                    pbftUnsignedHash: data.pbftUnsignedHash || null,
                    attempt: reqAttempt,
                  });
                  await broadcastSignStepNotice(node, withdrawalKey, reqAttempt, notice);
                } catch (e) { logSwallowedError('withdrawal_sign_step_stale_notice', e); }
                putSignStepPostCache(node, cacheKey, {
                  withdrawalKey,
                  stepIndex: idx,
                  signer: node.wallet.address,
                  txsetHashIn,
                  attempt: reqAttempt,
                  stale: true,
                  lastAttemptAt: 0,
                });
                try {
                  const sent = await sendCachedSignStep(node, getSignStepPostCache(node).get(cacheKey), senderLc, authToken);
                  if (sent.ok || sent.drop) {
                    getSignStepPostCache(node).delete(cacheKey);
                    markHandledSignStep(node, requestKey);
                  }
                } catch {}
              } else {
                console.log('[Withdrawal] Failed to sign step tx:', msg);
              }
              continue;
            } finally {
              if (lockAcquired2 && node._withdrawalMultisigLock === withdrawalKey) node._withdrawalMultisigLock = null;
            }
            if (!signedTx || !signedTx.txDataHex) {
              continue;
            }
            const txDataHexOut = String(signedTx.txDataHex);
            const txsetHashOut = computeTxsetHash(txDataHexOut);
            if (!txsetHashOut) {
              continue;
            }
            const stored = putTxset(node, txsetHashOut, txDataHexOut);
            if (!stored.ok) {
              continue;
            }
            if (pending) {
              pending.lastSignStepAttempt = reqAttempt;
              pending.lastSignStepIndex = idx;
              pending.lastSignStepTxsetHashIn = txsetHashIn;
              pending.lastSignStepTxsetHashOut = txsetHashOut;
              pending.lastSignStepAt = Date.now();
              scheduleWithdrawalStateSave(node);
            }
            putSignStepPostCache(node, cacheKey, {
              withdrawalKey,
              stepIndex: idx,
              signer: node.wallet.address,
              txsetHashIn,
              txsetHashOut,
              lastAttemptAt: 0,
            });
            let delivered = false;
            for (let attempt = 0; attempt < SIGN_STEP_HTTP_RETRIES; attempt += 1) {
              const cached2 = getSignStepPostCache(node).get(cacheKey);
              if (!cached2) break;
              const sent = await sendCachedSignStep(node, cached2, senderLc, authToken);
              if (sent.ok || sent.drop) {
                if (sent.drop) getSignStepPostCache(node).delete(cacheKey);
                delivered = true;
                break;
              }
              await sleepMs(200 * 2 ** attempt);
            }
            if (delivered) {
              markHandledSignStep(node, requestKey);
            }
          } catch (e) { logSwallowedError('withdrawal_sign_step_entry_error', e); }
        }
      }
    } catch (e) { logSwallowedError('withdrawal_sign_step_poll_error', e); }
    if (node._withdrawalMonitorRunning) setTimeout(pollSignSteps, 3000);
  };
  pollSignSteps();
}
