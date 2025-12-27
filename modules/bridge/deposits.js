import { ethers } from 'ethers';
import { loadBridgeState, saveBridgeState, startBridgeStateSaver, startCleanupTimer } from './bridge-state.js';
import { isClusterOperational } from '../core/health.js';
import { DepositBatchManager, DEPOSIT_MAX_AMOUNT_ATOMIC } from './deposit-batch.js';
const DEPOSIT_REQUEST_ROUND = Number(process.env.DEPOSIT_REQUEST_ROUND || 9700);
const MINT_SIGNATURE_ROUND = Number(process.env.MINT_SIGNATURE_ROUND || 9800);
const BATCH_MINT_SIGNATURE_ROUND = Number(process.env.BATCH_MINT_SIGNATURE_ROUND || 9860);
const REQUIRED_MINT_SIGNATURES = 7;
const MINT_SIGNATURE_TIMEOUT_MS = Number(process.env.MINT_SIGNATURE_TIMEOUT_MS || 120000);
const MINT_SIGNATURE_WINDOW_MS = Number(process.env.MINT_SIGNATURE_WINDOW_MS || 10000);
const MINT_SIGNATURE_STALE_MS = Number(process.env.MINT_SIGNATURE_STALE_MS || MINT_SIGNATURE_TIMEOUT_MS * 2);
const MAX_STAKE_QUERY_FAILURES = 3;
const DEPOSIT_RECIPIENT_GRACE_MS = Number(process.env.DEPOSIT_RECIPIENT_GRACE_MS || 300000);
const DEPOSIT_MAX_RETRY_MS = Number(process.env.DEPOSIT_MAX_RETRY_MS || 3600000);
const DEPOSIT_HISTORY_TTL_MS = Number(process.env.DEPOSIT_HISTORY_TTL_MS || 86400000);
const DEPOSIT_SCAN_LOOKBACK_BLOCKS = Math.floor(Number(process.env.DEPOSIT_SCAN_LOOKBACK_BLOCKS || (Math.ceil(Math.max(DEPOSIT_MAX_RETRY_MS, DEPOSIT_RECIPIENT_GRACE_MS) / 120000) + 10)));
const BATCH_MODE_ENABLED = process.env.DEPOSIT_BATCH_MODE === '1';
function getRetryBackoffMs(attempt) {
  const n = typeof attempt === 'number' && Number.isFinite(attempt) ? Math.floor(attempt) : 0;
  const p = Math.max(0, Math.min(5, n - 1));
  return Math.min(1800000, 60000 * (2 ** p));
}
function isBridgeEnabled() {
  const v = process.env.BRIDGE_ENABLED;
  return v === undefined || v === '1';
}
async function getEpochDurationSec(node) {
  const now = Date.now();
  const cached = node && typeof node._bridgeEpochDurationSec === 'number' ? node._bridgeEpochDurationSec : null;
  const cachedAt = node && typeof node._bridgeEpochDurationCachedAt === 'number' ? node._bridgeEpochDurationCachedAt : 0;
  if (cached && now - cachedAt < 300000) return cached;
  if (node && node.bridge && typeof node.bridge.epochDuration === 'function') {
    try {
      const v = await node.bridge.epochDuration();
      const sec = Number(v);
      if (Number.isFinite(sec) && sec > 0) {
        node._bridgeEpochDurationSec = Math.floor(sec);
        node._bridgeEpochDurationCachedAt = now;
        return node._bridgeEpochDurationSec;
      }
    } catch {}
  }
  return 1800;
}
async function isNodeStaked(node, address) {
  if (!node.staking) return false;
  try {
    const info = await node.staking.getNodeInfo(address);
    const stakedAmount = BigInt(info[0].toString());
    const active = info[3];
    return active && stakedAmount >= BigInt('1000000000000000000000000');
  } catch {
    return false;
  }
}
function normalizeAddress(value) {
  try {
    return ethers.getAddress(value.toLowerCase());
  } catch {
    return null;
  }
}
function normalizeXmrTxid(value) {
  if (!value || typeof value !== 'string') return null;
  const v = value.trim().toLowerCase();
  if (!/^[0-9a-f]{64}$/.test(v)) return null;
  return v;
}
function parseAmountAtomic(value) {
  if (typeof value === 'bigint') return value;
  if (typeof value === 'number') {
    if (!Number.isFinite(value) || !Number.isSafeInteger(value)) return null;
    return BigInt(value);
  }
  if (typeof value === 'string') {
    const s = value.trim();
    if (!s || !/^\d+$/.test(s)) return null;
    try { return BigInt(s); } catch { return null; }
  }
  return null;
}
export function getDepositRequestUserMessageHash(paymentId, ethAddress) {
  return ethers.solidityPackedKeccak256(
    ['string', 'string', 'address'],
    ['deposit-request-user-v1', paymentId, ethAddress],
  );
}
function verifyDepositRequestUserSignature(paymentId, ethAddress, signature) {
  try {
    const message = getDepositRequestUserMessageHash(paymentId, ethAddress);
    const recovered = ethers.verifyMessage(ethers.getBytes(message), signature);
    return recovered.toLowerCase() === ethAddress.toLowerCase();
  } catch {
    return false;
  }
}
function getDepositRequestNodeMessageHash(paymentId, ethAddress, clusterId, requestKey) {
  return ethers.solidityPackedKeccak256(
    ['string', 'string', 'address', 'bytes32', 'bytes32'],
    ['deposit-request-node-v1', paymentId, ethAddress, clusterId || ethers.ZeroHash, requestKey || ethers.ZeroHash],
  );
}
function signDepositRequestNodePayload(wallet, paymentId, ethAddress, clusterId, requestKey) {
  const message = getDepositRequestNodeMessageHash(paymentId, ethAddress, clusterId, requestKey);
  return wallet.signMessage(ethers.getBytes(message));
}
function verifyDepositRequestNodeSignature(paymentId, ethAddress, clusterId, requestKey, signature, expectedSigner) {
  try {
    const message = getDepositRequestNodeMessageHash(paymentId, ethAddress, clusterId, requestKey);
    const recovered = ethers.verifyMessage(ethers.getBytes(message), signature);
    return recovered.toLowerCase() === expectedSigner.toLowerCase();
  } catch {
    return false;
  }
}
function getBridgeAddressForSigning(node) {
  const addr = (node && node.bridge && (node.bridge.target || node.bridge.address))
    ? String(node.bridge.target || node.bridge.address)
    : '';
  try {
    return ethers.getAddress(addr);
  } catch {
    return null;
  }
}

function getChainIdForSigning(node) {
  const n = node && typeof node.chainId === 'number' ? node.chainId : null;
  if (typeof n === 'number' && Number.isFinite(n) && Number.isInteger(n) && n > 0) {
    return BigInt(n);
  }
  const raw = process.env.CHAIN_ID;
  const parsed = raw != null && raw !== '' ? Number(raw) : NaN;
  if (Number.isFinite(parsed) && Number.isInteger(parsed) && parsed > 0) {
    return BigInt(parsed);
  }
  return null;
}

function getSigningContext(node) {
  const bridgeAddr = getBridgeAddressForSigning(node);
  const chainId = getChainIdForSigning(node);
  if (!bridgeAddr || chainId == null) return null;
  return { bridgeAddr, chainId };
}

function getMintMessageHash(recipient, clusterId, depositId, amountAtomic, chainId, bridgeAddr) {
  return ethers.solidityPackedKeccak256(
    ['string', 'address', 'uint256', 'address', 'bytes32', 'bytes32', 'uint256'],
    ['znode-mint-v2', bridgeAddr, chainId, recipient, clusterId, depositId, amountAtomic],
  );
}

function getBatchMintMessageHash(merkleRoot, clusterId, chainId, bridgeAddr) {
  return ethers.solidityPackedKeccak256(
    ['string', 'address', 'uint256', 'bytes32', 'bytes32'],
    ['batch-mint-v2', bridgeAddr, chainId, merkleRoot, clusterId],
  );
}

function verifyBatchMintSignature(node, merkleRoot, clusterId, signature, expectedSigner) {
  try {
    const ctx = getSigningContext(node);
    if (!ctx) return false;
    const messageHash = getBatchMintMessageHash(merkleRoot, clusterId, ctx.chainId, ctx.bridgeAddr);
    const recovered = ethers.verifyMessage(ethers.getBytes(messageHash), signature);
    return recovered.toLowerCase() === expectedSigner.toLowerCase();
  } catch {
    return false;
  }
}

function verifyMintSignature(node, recipient, clusterId, depositId, amountAtomic, signature, expectedSigner) {
  try {
    const ctx = getSigningContext(node);
    if (!ctx) return false;
    const messageHash = getMintMessageHash(recipient, clusterId, depositId, amountAtomic, ctx.chainId, ctx.bridgeAddr);
    const recovered = ethers.verifyMessage(ethers.getBytes(messageHash), signature);
    return recovered.toLowerCase() === expectedSigner.toLowerCase();
  } catch {
    return false;
  }
}
function isMember(members, address) {
  if (!Array.isArray(members) || members.length === 0) return false;
  const needle = address.toLowerCase();
  for (const m of members) {
    if (m && m.toLowerCase() === needle) return true;
  }
  return false;
}
function normalizeDepositRequestPayload(data) {
  if (!data || typeof data !== 'object') return null;
  if (!data.paymentId || !data.ethAddress || !data.signer || !data.nodeSignature || !data.userSignature) return null;
  const pid = String(data.paymentId || '').toLowerCase();
  if (!/^[0-9a-f]{16}$/.test(pid)) return null;
  const ethAddress = normalizeAddress(data.ethAddress);
  const signer = normalizeAddress(data.signer);
  if (!ethAddress || !signer) return null;
  const nodeSignature = typeof data.nodeSignature === 'string' ? data.nodeSignature : '';
  const userSignature = typeof data.userSignature === 'string' ? data.userSignature : '';
  if (!nodeSignature || !userSignature) return null;
  const clusterId = data.clusterId && typeof data.clusterId === 'string' ? data.clusterId : null;
  const requestKey = data.requestKey && typeof data.requestKey === 'string' ? data.requestKey : null;
  const tsRaw = data.timestamp;
  const timestamp = typeof tsRaw === 'number' && Number.isFinite(tsRaw) && tsRaw > 0 ? tsRaw : Date.now();
  return { paymentId: pid, ethAddress, clusterId, requestKey, signer, nodeSignature, userSignature, timestamp };
}
function getDepositRequestBroadcastItems(node, now, limit) {
  if (!node._pendingDepositRequests || node._pendingDepositRequests.size === 0) return [];
  const items = [];
  for (const [pid, raw] of node._pendingDepositRequests) {
    if (!pid || !raw || typeof raw !== 'object') continue;
    if (!raw.ethAddress || !raw.signer || !raw.nodeSignature || !raw.userSignature) continue;
    const paymentId = String(pid).toLowerCase();
    if (!/^[0-9a-f]{16}$/.test(paymentId)) continue;
    const ethAddress = normalizeAddress(raw.ethAddress);
    const signer = normalizeAddress(raw.signer);
    if (!ethAddress || !signer) continue;
    const nodeSignature = typeof raw.nodeSignature === 'string' ? raw.nodeSignature : '';
    const userSignature = typeof raw.userSignature === 'string' ? raw.userSignature : '';
    if (!nodeSignature || !userSignature) continue;
    const clusterId = raw.clusterId && typeof raw.clusterId === 'string' ? raw.clusterId : null;
    const requestKey = raw.requestKey && typeof raw.requestKey === 'string' ? raw.requestKey : null;
    const tsRaw = raw.timestamp;
    const timestamp = typeof tsRaw === 'number' && Number.isFinite(tsRaw) && tsRaw > 0 ? tsRaw : now;
    items.push({ paymentId, ethAddress, clusterId, requestKey, signer, nodeSignature, userSignature, timestamp });
  }
  items.sort((a, b) => b.timestamp - a.timestamp);
  const lim = typeof limit === 'number' && Number.isFinite(limit) ? Math.floor(limit) : 0;
  return lim > 0 && items.length > lim ? items.slice(0, lim) : items;
}
async function maybeBroadcastDepositRequests(node, force = false, clusterIdOverride = null) {
  if (!node || !node.p2p) return;
  const clusterId = clusterIdOverride || node._activeClusterId;
  if (!clusterId) return;
  const now = Date.now();
  const interval = 60000;
  if (!force && node._lastDepositRequestBroadcastAt && now - node._lastDepositRequestBroadcastAt < interval) return;
  const sessionId = node._sessionId || 'bridge';
  const requests = getDepositRequestBroadcastItems(node, now, 200);
  if (requests.length === 0) return;
  const payload = JSON.stringify({ type: 'deposit-requests', requests });
  await node.p2p.broadcastRoundData(clusterId, sessionId, DEPOSIT_REQUEST_ROUND, payload);
  node._lastDepositRequestBroadcastAt = now;
}
export async function syncDepositRequests(node) {
  if (!node || !node.p2p || !Array.isArray(node._clusterMembers) || node._clusterMembers.length === 0) return;
  if (!node._activeClusterId) return;
  const sessionId = node._sessionId || 'bridge';
  const testMode = process.env.TEST_MODE === '1';
  let roundData;
  try {
    roundData = await node.p2p.getRoundData(node._activeClusterId, sessionId, DEPOSIT_REQUEST_ROUND);
  } catch {
    try { await maybeBroadcastDepositRequests(node); } catch {}
    return;
  }
  if (!roundData || typeof roundData !== 'object') {
    try { await maybeBroadcastDepositRequests(node); } catch {}
    return;
  }
  if (!node._pendingDepositRequests) node._pendingDepositRequests = new Map();
  const activeClusterLc = String(node._activeClusterId).toLowerCase();
  const stakedCache = new Map();
  let updated = false;
  for (const [sender, payload] of Object.entries(roundData)) {
    if (!payload || typeof payload !== 'string') continue;
    const senderAddr = normalizeAddress(sender);
    if (senderAddr && !isMember(node._clusterMembers, senderAddr)) continue;
    let data;
    try { data = JSON.parse(payload); } catch { continue; }
    const requests = [];
    if (data && data.type === 'deposit-requests' && Array.isArray(data.requests)) {
      for (const r of data.requests) requests.push(r);
    } else {
      requests.push(data);
    }
    for (const raw of requests) {
      const req = normalizeDepositRequestPayload(raw);
      if (!req) continue;
      if (req.clusterId && String(req.clusterId).toLowerCase() !== activeClusterLc) continue;
      const existing = node._pendingDepositRequests.get(req.paymentId);
      const existingTs = existing && typeof existing === 'object' && typeof existing.timestamp === 'number' ? existing.timestamp : 0;
      if (existing && existingTs >= req.timestamp) continue;
      if (!verifyDepositRequestUserSignature(req.paymentId, req.ethAddress, req.userSignature)) continue;
      if (!verifyDepositRequestNodeSignature(req.paymentId, req.ethAddress, req.clusterId, req.requestKey, req.nodeSignature, req.signer)) continue;
      if (!testMode) {
        const signerLc = req.signer.toLowerCase();
        let staked = stakedCache.get(signerLc);
        if (staked === undefined) {
          staked = await isNodeStaked(node, req.signer);
          stakedCache.set(signerLc, staked);
        }
        if (!staked) continue;
      }
      node._pendingDepositRequests.set(req.paymentId, {
        ethAddress: req.ethAddress, clusterId: req.clusterId || null, requestKey: req.requestKey || null,
        signer: req.signer, nodeSignature: req.nodeSignature, userSignature: req.userSignature, timestamp: req.timestamp,
      });
      updated = true;
    }
  }
  if (updated) saveBridgeState(node);
  try { await maybeBroadcastDepositRequests(node); } catch {}
}
function maybeMarkStaleSignatures(node) {
  if (!node._pendingMintSignatures || node._pendingMintSignatures.size === 0) return;
  const now = Date.now();
  for (const [, sigData] of node._pendingMintSignatures) {
    if (!sigData || typeof sigData !== 'object') continue;
    if (sigData.status === 'stale' || sigData.status === 'orphaned') continue;
    const sigCount = Array.isArray(sigData.signatures) ? sigData.signatures.length : 0;
    if (sigCount >= REQUIRED_MINT_SIGNATURES) continue;
    const age = now - (sigData.timestamp || 0);
    if (age >= MINT_SIGNATURE_STALE_MS && age < DEPOSIT_MAX_RETRY_MS) {
      sigData.status = 'stale';
      sigData.staleAt = now;
    } else if (age >= DEPOSIT_MAX_RETRY_MS) {
      sigData.status = 'orphaned';
      sigData.orphanedAt = now;
    }
  }
}
async function checkForDeposits(node, minConfirmations) {
  await syncDepositRequests(node);
  maybeMarkStaleSignatures(node);
  if (node._pendingMintSignatures && node._pendingMintSignatures.size > 0) {
    if (!node._signatureCollectBackoff) node._signatureCollectBackoff = new Map();
    const now = Date.now();
    for (const [txid, sigData] of node._pendingMintSignatures) {
      if (sigData && sigData.status === 'stale') {
        const next = node._signatureCollectBackoff.get(txid) || 0;
        if (now >= next) {
          await collectMintSignatures(node, txid);
          const jitter = (Number.parseInt(txid.slice(0, 8), 16) % 5000) || 0;
          node._signatureCollectBackoff.set(txid, now + 60000 + jitter);
          break;
        }
      }
    }
    for (const txid of node._pendingMintSignatures.keys()) {
      const sigData = node._pendingMintSignatures.get(txid);
      if (sigData && (sigData.status === 'stale' || sigData.status === 'orphaned')) continue;
      const next = node._signatureCollectBackoff.get(txid) || 0;
      if (now < next) continue;
      await collectMintSignatures(node, txid);
      const jitter = (Number.parseInt(txid.slice(0, 8), 16) % 5000) || 0;
      node._signatureCollectBackoff.set(txid, now + 30000 + jitter);
      break;
    }
  }
  if (BATCH_MODE_ENABLED && node._depositBatchManager) {
    await maybeProcessBatch(node);
  }
  if (!node.monero) return;
  if (node._moneroSigningInProgress || node._withdrawalMultisigLock) return;
  try { await node.monero.refresh(); } catch {}
  let minHeight = 0;
  let height = null;
  try {
    if (typeof node._depositLastScanHeight === 'number' && Number.isFinite(node._depositLastScanHeight) && node._depositLastScanHeight >= 0) {
      const lookback = Number.isFinite(DEPOSIT_SCAN_LOOKBACK_BLOCKS) && DEPOSIT_SCAN_LOOKBACK_BLOCKS > 0 ? DEPOSIT_SCAN_LOOKBACK_BLOCKS : 0;
      minHeight = Math.max(0, Math.floor(node._depositLastScanHeight) - lookback);
    }
    const h = await node.monero.getHeight();
    if (typeof h === 'number' && Number.isFinite(h) && h >= 0) height = h;
  } catch {}
  let deposits;
  try { deposits = await node.monero.getConfirmedDeposits(minConfirmations, { minHeight }); }
  catch (e) {
    console.log('[Bridge] Failed to get deposits:', e.message || String(e));
    return;
  }
  if (typeof height === 'number' && Number.isFinite(height) && height >= 0) node._depositLastScanHeight = Math.floor(height);
  if (!deposits || deposits.length === 0) return;
  for (const deposit of deposits) {
    const txid = normalizeXmrTxid(deposit.txid);
    if (!txid) continue;
    if (deposit.txid !== txid) deposit.txid = txid;
    if (DEPOSIT_HISTORY_TTL_MS > 0) {
      let tooOld = false;
      try {
        const nowMs = Date.now();
        if (typeof deposit.timestamp === 'number' && deposit.timestamp > 0) {
          const ageMs = nowMs - deposit.timestamp * 1000;
          if (ageMs > DEPOSIT_HISTORY_TTL_MS) tooOld = true;
        } else if (typeof deposit.confirmations === 'number' && deposit.confirmations > 0) {
          const approxAgeMs = deposit.confirmations * 120000;
          if (approxAgeMs > DEPOSIT_HISTORY_TTL_MS) tooOld = true;
        }
      } catch {}
      if (tooOld) continue;
    }
    if (node._processedDeposits && node._processedDeposits.has(txid)) continue;
    if (node._pendingMintSignatures && node._pendingMintSignatures.has(txid)) continue;
    if (!node._inProgressDeposits) node._inProgressDeposits = new Set();
    if (node._inProgressDeposits.has(txid)) continue;
    if (BATCH_MODE_ENABLED && node._depositBatchManager && node._depositBatchManager.hasPendingDeposit(txid)) continue;
    let alreadyMinted = false;
    if (node.bridge && typeof node.bridge.usedDepositIds === 'function') {
      try {
        const depositId = ethers.keccak256(ethers.toUtf8Bytes(txid));
        alreadyMinted = await node.bridge.usedDepositIds(depositId);
      } catch (e) {
        console.log(`[Bridge] Failed to check usedDepositIds for deposit ${txid}:`, e.message || String(e));
      }
    }
    if (alreadyMinted) {
      console.log(`[Bridge] Skipping deposit ${txid}: already minted on-chain`);
      if (!node._processedDeposits) node._processedDeposits = new Set();
      if (!node._processedDepositsMeta) node._processedDepositsMeta = new Map();
      node._processedDeposits.add(txid);
      node._processedDepositsMeta.set(txid, { timestamp: Date.now(), subaddrIndex: deposit.subaddrIndex });
      if (node._unresolvedDeposits) node._unresolvedDeposits.delete(txid);
      if (node._failedConsensusDeposits) node._failedConsensusDeposits.delete(txid);
      saveBridgeState(node);
      continue;
    }
    const amountAtomic = parseAmountAtomic(deposit.amount);
    if (DEPOSIT_MAX_AMOUNT_ATOMIC > 0n && amountAtomic !== null && amountAtomic > DEPOSIT_MAX_AMOUNT_ATOMIC) {
      console.log(`[Bridge] Deposit ${txid} exceeds max amount (${amountAtomic} > ${DEPOSIT_MAX_AMOUNT_ATOMIC}), marking as failed`);
      if (!node._processedDeposits) node._processedDeposits = new Set();
      if (!node._processedDepositsMeta) node._processedDepositsMeta = new Map();
      node._processedDeposits.add(txid);
      node._processedDepositsMeta.set(txid, { timestamp: Date.now(), subaddrIndex: deposit.subaddrIndex, error: 'deposit_amount_exceeds_limit' });
      saveBridgeState(node);
      continue;
    }
    const recipient = parseRecipientFromPaymentId(node, deposit.paymentId);
    if (!recipient) {
      const pid = deposit.paymentId || '';
      const looksLikeGeneratedId = /^[0-9a-fA-F]{16}$/.test(pid);
      const now = Date.now();
      if (!node._unresolvedDeposits) node._unresolvedDeposits = new Map();
      let state = node._unresolvedDeposits.get(txid);
      if (!state || typeof state !== 'object') {
        const firstSeen = typeof state === 'number' ? state : now;
        state = { firstSeen, nextAttemptAt: now, attempts: 0 };
      }
      const ageMs = now - state.firstSeen;
      const limitMs = looksLikeGeneratedId ? DEPOSIT_MAX_RETRY_MS : DEPOSIT_RECIPIENT_GRACE_MS;
      if (ageMs >= limitMs) {
        const ageSec = Math.floor(ageMs / 1000);
        console.log(`[Bridge] Marking deposit ${txid} as orphan after ${ageSec}s with unknown recipient (paymentId=${pid})`);
        if (!node._processedDeposits) node._processedDeposits = new Set();
        if (!node._processedDepositsMeta) node._processedDepositsMeta = new Map();
        node._processedDeposits.add(txid);
        node._processedDepositsMeta.set(txid, { timestamp: Date.now(), subaddrIndex: deposit.subaddrIndex });
        node._unresolvedDeposits.delete(txid);
        saveBridgeState(node);
        continue;
      }
      if (typeof state.nextAttemptAt === 'number' && now < state.nextAttemptAt) continue;
      state.attempts = (state.attempts || 0) + 1;
      const jitter = (Number.parseInt(txid.slice(0, 8), 16) % 5000) || 0;
      state.nextAttemptAt = now + getRetryBackoffMs(state.attempts) + jitter;
      node._unresolvedDeposits.set(txid, state);
      continue;
    }
    if (BATCH_MODE_ENABLED && node._depositBatchManager) {
      const depositId = ethers.keccak256(ethers.toUtf8Bytes(txid));
      const addResult = node._depositBatchManager.addDeposit(txid, depositId, recipient, deposit.amount);
      if (addResult && addResult.ok) {
        console.log(`[Bridge] Deposit ${txid} queued for batch processing`);
        if (node._unresolvedDeposits) node._unresolvedDeposits.delete(txid);
      } else if (addResult && addResult.reason === 'deposit_amount_exceeds_limit') {
        console.log(`[Bridge] Deposit ${txid} exceeds batch max amount, marking as failed`);
        if (!node._processedDeposits) node._processedDeposits = new Set();
        if (!node._processedDepositsMeta) node._processedDepositsMeta = new Map();
        node._processedDeposits.add(txid);
        node._processedDepositsMeta.set(txid, { timestamp: Date.now(), subaddrIndex: deposit.subaddrIndex, error: 'deposit_amount_exceeds_limit' });
        saveBridgeState(node);
      }
      continue;
    }
    if (node._failedConsensusDeposits) {
      const state = node._failedConsensusDeposits.get(txid);
      if (state && typeof state === 'object' && typeof state.nextAttemptAt === 'number' && Date.now() < state.nextAttemptAt) continue;
    }
    console.log(`[Bridge] New deposit detected: ${txid}`);
    console.log(`  Amount: ${deposit.amount / 1e12} XMR`);
    console.log(`  Confirmations: ${deposit.confirmations}`);
    console.log(`  Payment ID: ${deposit.paymentId || 'none'}`);
    node._inProgressDeposits.add(txid);
    let consensusSuccess = false;
    try {
      const consensusResult = await runDepositConsensus(node, deposit, recipient);
      if (consensusResult.success) {
        console.log(`[Bridge] Consensus reached for deposit ${txid}`);
        const sigData = await generateAndShareMintSignature(node, deposit, recipient);
        if (!sigData) {
          console.log(`[Bridge] Failed to generate mint signature for deposit ${txid}`);
        } else {
          await collectMintSignatures(node, txid);
          consensusSuccess = true;
        }
      } else {
        console.log(`[Bridge] Consensus failed for deposit ${txid}: ${consensusResult.reason}`);
      }
    } catch (e) {
      console.log(`[Bridge] Consensus error for deposit ${txid}:`, e.message || String(e));
    }
    node._inProgressDeposits.delete(txid);
    if (consensusSuccess) {
      if (!node._processedDeposits) node._processedDeposits = new Set();
      if (!node._processedDepositsMeta) node._processedDepositsMeta = new Map();
      node._processedDeposits.add(txid);
      node._processedDepositsMeta.set(txid, { timestamp: Date.now(), subaddrIndex: deposit.subaddrIndex });
      if (node._unresolvedDeposits) node._unresolvedDeposits.delete(txid);
      if (node._failedConsensusDeposits) node._failedConsensusDeposits.delete(txid);
      saveBridgeState(node);
    } else {
      const now = Date.now();
      if (!node._failedConsensusDeposits) node._failedConsensusDeposits = new Map();
      let state = node._failedConsensusDeposits.get(txid);
      if (!state || typeof state !== 'object') {
        const firstSeen = typeof state === 'number' ? state : now;
        state = { firstSeen, nextAttemptAt: now, attempts: 0 };
      }
      const ageMs = now - state.firstSeen;
      if (ageMs >= DEPOSIT_MAX_RETRY_MS) {
        const ageSec = Math.floor(ageMs / 1000);
        console.log(`[Bridge] Marking deposit ${txid} as orphan after repeated consensus failures for ${ageSec}s`);
        if (!node._processedDeposits) node._processedDeposits = new Set();
        if (!node._processedDepositsMeta) node._processedDepositsMeta = new Map();
        node._processedDeposits.add(txid);
        node._processedDepositsMeta.set(txid, { timestamp: Date.now(), subaddrIndex: deposit.subaddrIndex });
        node._failedConsensusDeposits.delete(txid);
        saveBridgeState(node);
      } else {
        state.attempts = (state.attempts || 0) + 1;
        const jitter = (Number.parseInt(txid.slice(0, 8), 16) % 5000) || 0;
        state.nextAttemptAt = now + getRetryBackoffMs(state.attempts) + jitter;
        node._failedConsensusDeposits.set(txid, state);
      }
    }
  }
}
async function maybeProcessBatch(node) {
  if (!node._depositBatchManager) return;
  if (node._batchProcessingInProgress) return;
  if (node._withdrawalMultisigLock || node._moneroSigningInProgress || node._withdrawalBatchInProgress) return;
  let withdrawalEpochEndMs = null;
  if (node.bridge && typeof node.bridge.getCurrentEpoch === 'function') {
    try {
      const epoch = await node.bridge.getCurrentEpoch();
      const epochDuration = await getEpochDurationSec(node);
      withdrawalEpochEndMs = (Number(epoch) + 1) * epochDuration * 1000;
    } catch {}
  }
  if (!node._depositBatchManager.shouldProcessBatch(withdrawalEpochEndMs)) return;
  const batchKey = `batch-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
  if (!node._depositBatchManager.acquirePrepareLock(batchKey)) return;
  node._batchProcessingInProgress = true;
  try {
    await processBatch(node, batchKey);
  } finally {
    node._batchProcessingInProgress = false;
    node._depositBatchManager.releasePrepareLock(batchKey);
  }
}
async function processBatch(node, batchKey) {
  if (!node._depositBatchManager) return;
  const clusterId = node._activeClusterId;
  if (!clusterId) return;
  const batch = node._depositBatchManager.prepareBatch(clusterId);
  if (!batch || batch.deposits.length === 0) return;
  saveBridgeState(node);
  console.log(`[Bridge] Processing batch of ${batch.deposits.length} deposits (merkleRoot: ${batch.merkleRoot.slice(0, 18)}...)`);
  const clusterStatus = await isClusterOperational(node);
  if (!clusterStatus.operational) {
    console.log(`[Bridge] Batch processing aborted: cluster not operational - ${clusterStatus.reason}`);
    return;
  }
  const consensusResult = await runBatchConsensus(node, batch);
  if (!consensusResult.success) {
    console.log(`[Bridge] Batch consensus failed: ${consensusResult.reason}`);
    return;
  }
  console.log(`[Bridge] Batch consensus reached for ${batch.deposits.length} deposits`);
  node._depositBatchManager.lockActiveBatch(batch.merkleRoot);
  saveBridgeState(node);
  const sigData = await generateAndShareBatchSignature(node, batch);
  if (!sigData) {
    console.log('[Bridge] Failed to generate batch signature');
    return;
  }
  await collectBatchMintSignatures(node, batch.merkleRoot);
  const finalSigs = node._pendingBatchSignatures && node._pendingBatchSignatures.get(batch.merkleRoot);
  if (finalSigs && finalSigs.signatures && finalSigs.signatures.length >= REQUIRED_MINT_SIGNATURES) {
    const sigs = finalSigs.signatures.map((s) => ({ signer: s.signer, signature: s.signature }));
    node._depositBatchManager.finalizeBatch(batch, sigs);
    if (node._pendingBatchSignatures) node._pendingBatchSignatures.delete(batch.merkleRoot);
    console.log(`[Bridge] Batch finalized with ${sigs.length} signatures`);
    for (const d of batch.deposits) {
      if (!node._processedDeposits) node._processedDeposits = new Set();
      if (!node._processedDepositsMeta) node._processedDepositsMeta = new Map();
      node._processedDeposits.add(d.xmrTxid);
      node._processedDepositsMeta.set(d.xmrTxid, { timestamp: Date.now(), batchRoot: batch.merkleRoot });
    }
    saveBridgeState(node);
  } else {
    console.log('[Bridge] Batch signature collection incomplete');
  }
}
async function runBatchConsensus(node, batch) {
  if (!node.p2p || !node._clusterMembers || node._clusterMembers.length === 0) {
    return { success: false, reason: 'no cluster members' };
  }
  const clusterId = node._activeClusterId;
  const sessionId = node._sessionId || 'bridge';
  const batchHash = ethers.keccak256(
    ethers.AbiCoder.defaultAbiCoder().encode(
      ['bytes32', 'bytes32', 'uint256'],
      [batch.merkleRoot, clusterId, batch.deposits.length]
    )
  );
  console.log(`[Bridge] Running PBFT consensus for batch ${batch.merkleRoot.slice(0, 18)}...`);
  const topic = `batch-${batch.merkleRoot.slice(0, 18)}`;
  const timeoutMs = Number(process.env.DEPOSIT_CONSENSUS_TIMEOUT_MS || 120000);
  try {
    const result = await node.p2p.runConsensus(clusterId, sessionId, topic, batchHash, node._clusterMembers, timeoutMs);
    if (result && result.success) return { success: true };
    return { success: false, reason: result ? result.reason : 'unknown' };
  } catch (e) {
    return { success: false, reason: e.message || String(e) };
  }
}
async function generateAndShareBatchSignature(node, batch) {
  if (!node.wallet) {
    console.log('[Bridge] Cannot generate batch signature: no wallet');
    return null;
  }
  const clusterId = node._activeClusterId || ethers.ZeroHash;
  const signer = normalizeAddress(node.wallet.address);
  if (!signer) return null;
  if (!isMember(node._clusterMembers, signer)) return null;
  const ctx = getSigningContext(node);
  if (!ctx) return null;
  const messageHash = getBatchMintMessageHash(batch.merkleRoot, clusterId, ctx.chainId, ctx.bridgeAddr);
  const signature = await node.wallet.signMessage(ethers.getBytes(messageHash));
  if (!verifyBatchMintSignature(node, batch.merkleRoot, clusterId, signature, signer)) {
    return null;
  }
  console.log(`[Bridge] Generated batch mint signature for ${batch.deposits.length} deposits`);
  if (!node._pendingBatchSignatures) node._pendingBatchSignatures = new Map();
  const existing = node._pendingBatchSignatures.get(batch.merkleRoot);
  const sigData = existing && typeof existing === 'object'
    ? existing
    : { merkleRoot: batch.merkleRoot, clusterId, signatures: [], timestamp: Date.now() };
  sigData.merkleRoot = batch.merkleRoot;
  sigData.clusterId = clusterId;
  sigData.timestamp = Date.now();
  if (!Array.isArray(sigData.signatures)) sigData.signatures = [];
  const signerLc = signer.toLowerCase();
  const already = sigData.signatures.some((s) => s && typeof s.signer === 'string' && s.signer.toLowerCase() === signerLc);
  if (!already) {
    sigData.signatures.push({ signer, signature });
  }
  node._pendingBatchSignatures.set(batch.merkleRoot, sigData);
  saveBridgeState(node);
  try {
    await node.p2p.broadcastRoundData(
      node._activeClusterId,
      node._sessionId || 'bridge',
      BATCH_MINT_SIGNATURE_ROUND,
      JSON.stringify({
        type: 'batch-mint-signature',
        merkleRoot: batch.merkleRoot,
        clusterId,
        signer,
        signature,
      }),
    );
  } catch (e) {
    console.log('[Bridge] Failed to broadcast batch signature:', e.message || String(e));
  }
  return sigData;
}
async function collectBatchMintSignatures(node, merkleRoot) {
  if (!node._pendingBatchSignatures || !node._pendingBatchSignatures.has(merkleRoot)) return;
  if (!node.p2p || !node._clusterMembers || node._clusterMembers.length === 0) return;
  if (!node.staking) return;
  const testMode = process.env.TEST_MODE === '1';
  const sigData = node._pendingBatchSignatures.get(merkleRoot);
  const clusterId = sigData.clusterId;
  const cleaned = [];
  const existingSigners = new Set();
  for (const entry of sigData.signatures || []) {
    const signer = normalizeAddress(entry.signer);
    const signature = entry.signature;
    if (!signer || !signature) continue;
    const key = signer.toLowerCase();
    if (existingSigners.has(key)) continue;
    if (!isMember(node._clusterMembers, signer)) continue;
    if (!verifyBatchMintSignature(node, merkleRoot, clusterId, signature, signer)) continue;
    if (!testMode) {
      const staked = await isNodeStaked(node, signer);
      if (!staked) continue;
    }
    cleaned.push({ signer, signature });
    existingSigners.add(key);
  }
  sigData.signatures = cleaned;
  try {
    const startedAt = Date.now();
    const complete = await node.p2p.waitForRoundCompletion(
      node._activeClusterId,
      node._sessionId || 'bridge',
      BATCH_MINT_SIGNATURE_ROUND,
      node._clusterMembers,
      MINT_SIGNATURE_TIMEOUT_MS,
    );
    if (complete) {
      const remaining = Math.min(MINT_SIGNATURE_WINDOW_MS, MINT_SIGNATURE_TIMEOUT_MS) - (Date.now() - startedAt);
      if (remaining > 0) await new Promise((resolve) => setTimeout(resolve, remaining));
    }
    const payloads = await node.p2p.getPeerPayloads(
      node._activeClusterId,
      node._sessionId || 'bridge',
      BATCH_MINT_SIGNATURE_ROUND,
      node._clusterMembers,
    );
    for (const payload of payloads) {
      try {
        const data = JSON.parse(payload);
        if (!data || data.type !== 'batch-mint-signature') continue;
        if (data.merkleRoot !== merkleRoot) continue;
        if (!data.signer || !data.signature) continue;
        if (data.clusterId && data.clusterId !== clusterId) continue;
        const signer = normalizeAddress(data.signer);
        if (!signer) continue;
        const key = signer.toLowerCase();
        if (existingSigners.has(key)) continue;
        if (!isMember(node._clusterMembers, signer)) continue;
        if (!verifyBatchMintSignature(node, merkleRoot, clusterId, data.signature, signer)) continue;
        if (!testMode) {
          const staked = await isNodeStaked(node, signer);
          if (!staked) continue;
        }
        sigData.signatures.push({ signer, signature: data.signature });
        existingSigners.add(key);
      } catch {}
    }
    console.log(`[Bridge] Collected ${sigData.signatures.length} batch signatures for ${merkleRoot.slice(0, 18)}...`);
    saveBridgeState(node);
  } catch (e) {
    console.log('[Bridge] Error collecting batch signatures:', e.message || String(e));
  }
}
function parseRecipientFromPaymentId(node, paymentId) {
  if (!paymentId || paymentId.length === 0) return null;
  let candidate = paymentId;
  if (candidate.length === 40 && /^[0-9a-fA-F]{40}$/.test(candidate)) {
    candidate = '0x' + candidate;
  }
  if (candidate.length === 42 && candidate.startsWith('0x')) {
    try { return ethers.getAddress(candidate); } catch {}
  }
  const key = String(paymentId).toLowerCase();
  if (node._pendingDepositRequests && node._pendingDepositRequests.has(key)) {
    const entry = node._pendingDepositRequests.get(key);
    if (entry && typeof entry === 'object' && typeof entry.ethAddress === 'string') return entry.ethAddress;
    if (typeof entry === 'string') return entry;
  }
  return null;
}
async function runDepositConsensus(node, deposit, recipient) {
  if (!node.p2p || !node._clusterMembers || node._clusterMembers.length === 0) {
    return { success: false, reason: 'no cluster members' };
  }
  const clusterStatus = await isClusterOperational(node);
  if (!clusterStatus.operational) {
    console.log(`[Bridge] Cluster not operational: ${clusterStatus.reason}`);
    return { success: false, reason: "cluster_not_operational: " + clusterStatus.reason };
  }
  const clusterId = node._activeClusterId;
  const sessionId = node._sessionId || 'bridge';
  const depositHash = ethers.keccak256(ethers.AbiCoder.defaultAbiCoder().encode(['string', 'uint256', 'uint256', 'address'], [deposit.txid, deposit.amount, deposit.blockHeight, recipient]));
  console.log(`[Bridge] Running PBFT consensus for deposit ${deposit.txid.slice(0, 16)}...`);
  const topic = `deposit-${deposit.txid.slice(0, 16)}`;
  const timeoutMs = Number(process.env.DEPOSIT_CONSENSUS_TIMEOUT_MS || 120000);
  try {
    const result = await node.p2p.runConsensus(clusterId, sessionId, topic, depositHash, node._clusterMembers, timeoutMs);
    if (result && result.success) return { success: true };
    return { success: false, reason: result ? result.reason : 'unknown' };
  } catch (e) {
    return { success: false, reason: e.message || String(e) };
  }
}
async function generateAndShareMintSignature(node, deposit, recipient) {
  if (!node.wallet) {
    console.log('[Bridge] Cannot generate signature: no wallet');
    return null;
  }
  const depositId = ethers.keccak256(ethers.toUtf8Bytes(deposit.txid));
  const amountAtomic = parseAmountAtomic(deposit.amount);
  if (amountAtomic === null || amountAtomic <= 0n) return null;
  const clusterId = node._activeClusterId || ethers.ZeroHash;
  const signer = normalizeAddress(node.wallet.address);
  if (!signer) return null;
  if (!isMember(node._clusterMembers, signer)) return null;
  let blockNumber = 0;
  try {
    blockNumber = await node.provider.getBlockNumber();
  } catch {}
  const ctx = getSigningContext(node);
  if (!ctx) return null;
  const messageHash = getMintMessageHash(recipient, clusterId, depositId, amountAtomic, ctx.chainId, ctx.bridgeAddr);
  const signature = await node.wallet.signMessage(ethers.getBytes(messageHash));
  if (!verifyMintSignature(node, recipient, clusterId, depositId, amountAtomic, signature, signer)) return null;
  console.log(`[Bridge] Generated mint signature for ${recipient}`);
  if (!node._pendingMintSignatures) node._pendingMintSignatures = new Map();
  const sigData = {
    clusterId,
    depositId,
    amount: amountAtomic.toString(),
    recipient,
    signatures: [{ signer, signature, blockNumber }],
    xmrTxid: deposit.txid,
    timestamp: Date.now(),
    blockNumber,
  };
  node._pendingMintSignatures.set(deposit.txid, sigData);
  try {
    await node.p2p.broadcastRoundData(
      node._activeClusterId,
      node._sessionId || 'bridge',
      MINT_SIGNATURE_ROUND,
      JSON.stringify({
        type: 'mint-signature',
        xmrTxid: deposit.txid,
        depositId,
        amount: amountAtomic.toString(),
        recipient,
        signer,
        signature,
        blockNumber,
      }),
    );
  } catch (e) {
    console.log('[Bridge] Failed to broadcast signature:', e.message || String(e));
  }
  return sigData;
}
async function collectMintSignatures(node, xmrTxid) {
  if (!node._pendingMintSignatures || !node._pendingMintSignatures.has(xmrTxid)) return;
  if (!node.p2p || !node._clusterMembers || node._clusterMembers.length === 0) return;
  if (!node.staking) return;
  const testMode = process.env.TEST_MODE === '1';
  const sigData = node._pendingMintSignatures.get(xmrTxid);
  const recipient = sigData.recipient;
  const clusterId = sigData.clusterId;
  const depositId = sigData.depositId;
  const amountAtomic = BigInt(sigData.amount);
  const cleaned = [];
  const existingSigners = new Set();
  for (const entry of sigData.signatures || []) {
    const signer = normalizeAddress(entry.signer);
    const signature = entry.signature;
    const blockNum = entry.blockNumber;
    if (!signer || !signature) continue;
    const key = signer.toLowerCase();
    if (existingSigners.has(key)) continue;
    if (!isMember(node._clusterMembers, signer)) continue;
    if (!verifyMintSignature(node, recipient, clusterId, depositId, amountAtomic, signature, signer)) continue;
    if (!testMode) {
      const staked = await isNodeStaked(node, signer);
      if (!staked) continue;
    }
    cleaned.push({ signer, signature, blockNumber: blockNum });
    existingSigners.add(key);
  }
  sigData.signatures = cleaned;
  try {
    const startedAt = Date.now();
    const complete = await node.p2p.waitForRoundCompletion(
      node._activeClusterId,
      node._sessionId || 'bridge',
      MINT_SIGNATURE_ROUND,
      node._clusterMembers,
      MINT_SIGNATURE_TIMEOUT_MS,
    );
    if (complete) {
      const remaining = Math.min(MINT_SIGNATURE_WINDOW_MS, MINT_SIGNATURE_TIMEOUT_MS) - (Date.now() - startedAt);
      if (remaining > 0) await new Promise((resolve) => setTimeout(resolve, remaining));
    }
    const payloads = await node.p2p.getPeerPayloads(
      node._activeClusterId,
      node._sessionId || 'bridge',
      MINT_SIGNATURE_ROUND,
      node._clusterMembers,
    );
    for (const payload of payloads) {
      try {
        const data = JSON.parse(payload);
        if (!data || data.type !== 'mint-signature' || data.xmrTxid !== xmrTxid) continue;
        if (!data.signer || !data.signature) continue;
        if (data.depositId && data.depositId !== depositId) continue;
        if (data.amount && String(data.amount) !== String(sigData.amount)) continue;
        if (data.recipient) {
          const r = normalizeAddress(data.recipient);
          if (!r || r.toLowerCase() !== recipient.toLowerCase()) continue;
        }
        const signer = normalizeAddress(data.signer);
        if (!signer) continue;
        const key = signer.toLowerCase();
        if (existingSigners.has(key)) continue;
        if (!isMember(node._clusterMembers, signer)) continue;
        const blockNum = data.blockNumber;
        if (!verifyMintSignature(node, recipient, clusterId, depositId, amountAtomic, data.signature, signer)) continue;
        if (!testMode) {
          const staked = await isNodeStaked(node, signer);
          if (!staked) continue;
        }
        sigData.signatures.push({ signer, signature: data.signature, blockNumber: blockNum });
        existingSigners.add(key);
      } catch {}
    }
    if (!complete && sigData.signatures.length < REQUIRED_MINT_SIGNATURES) {
      console.log('[Bridge] Timeout waiting for mint signatures');
    }
    if (sigData.signatures.length >= REQUIRED_MINT_SIGNATURES && sigData.status === 'stale') {
      delete sigData.status;
      delete sigData.staleAt;
    }
    console.log(`[Bridge] Collected ${sigData.signatures.length} mint signatures for ${xmrTxid.slice(0, 16)}...`);
    saveBridgeState(node);
  } catch (e) {
    console.log('[Bridge] Error collecting signatures:', e.message || String(e));
  }
}
export function getMintSignature(node, xmrTxid) {
  if (!node._pendingMintSignatures) return null;
  const data = node._pendingMintSignatures.get(xmrTxid);
  if (!data) return null;
  return {
    clusterId: data.clusterId,
    depositId: data.depositId,
    amount: data.amount,
    recipient: data.recipient,
    signatures: data.signatures || [],
    xmrTxid: data.xmrTxid,
    timestamp: data.timestamp,
    blockNumber: data.blockNumber,
    ready: (data.signatures || []).length >= REQUIRED_MINT_SIGNATURES,
    status: data.status || null,
  };
}
export function getAllPendingSignatures(node) {
  if (!node._pendingMintSignatures) return [];
  const result = [];
  for (const [txid, data] of node._pendingMintSignatures) {
    result.push({
      xmrTxid: txid,
      clusterId: data.clusterId,
      depositId: data.depositId,
      amount: data.amount,
      recipient: data.recipient,
      signatures: data.signatures || [],
      timestamp: data.timestamp,
      blockNumber: data.blockNumber,
      ready: (data.signatures || []).length >= REQUIRED_MINT_SIGNATURES,
      status: data.status || null,
    });
  }
  return result;
}
export function getBatchClaimData(node, depositId) {
  if (!node._depositBatchManager) return null;
  return node._depositBatchManager.getClaimData(depositId);
}
export function getBatchInfo(node, merkleRoot) {
  if (!node._depositBatchManager) return null;
  const batch = node._depositBatchManager.getBatch(merkleRoot);
  if (!batch) return null;
  return {
    merkleRoot: batch.merkleRoot,
    clusterId: batch.clusterId,
    depositCount: batch.deposits.length,
    deposits: batch.deposits.map((d) => ({
      depositId: d.depositId,
      recipient: d.recipient,
      amount: d.amount,
    })),
    signatures: batch.signatures || [],
    finalizedAt: batch.finalizedAt,
    ready: (batch.signatures || []).length >= REQUIRED_MINT_SIGNATURES,
  };
}
export function getAllBatches(node) {
  if (!node._depositBatchManager) return [];
  return node._depositBatchManager.getAllBatches().map((b) => ({
    merkleRoot: b.merkleRoot,
    clusterId: b.clusterId,
    depositCount: b.deposits.length,
    finalizedAt: b.finalizedAt,
    signatureCount: (b.signatures || []).length,
    ready: (b.signatures || []).length >= REQUIRED_MINT_SIGNATURES,
  }));
}
export function getDepositStatus(node, xmrTxid) {
  const txid = normalizeXmrTxid(xmrTxid);
  if (!txid) return { status: 'unknown', type: null };
  if (node._processedDeposits && node._processedDeposits.has(txid)) {
    const meta = node._processedDepositsMeta && node._processedDepositsMeta.get(txid);
    if (meta && meta.batchRoot) {
      return { status: 'claimable', type: 'batch', merkleRoot: meta.batchRoot };
    }
    if (meta && meta.error) {
      return { status: 'failed', type: 'legacy', error: meta.error };
    }
    return { status: 'processed', type: 'legacy' };
  }
  if (node._pendingMintSignatures && node._pendingMintSignatures.has(txid)) {
    const data = node._pendingMintSignatures.get(txid);
    const ready = (data.signatures || []).length >= REQUIRED_MINT_SIGNATURES;
    if (data.status === 'stale') {
      return { status: 'stale_signatures', type: 'legacy', sigCount: (data.signatures || []).length };
    }
    if (data.status === 'orphaned') {
      return { status: 'orphaned', type: 'legacy' };
    }
    return { status: ready ? 'claimable' : 'pending_signatures', type: 'legacy' };
  }
  if (node._depositBatchManager && node._depositBatchManager.hasPendingDeposit(txid)) {
    return { status: 'pending_batch', type: 'batch' };
  }
  if (node._inProgressDeposits && node._inProgressDeposits.has(txid)) {
    return { status: 'in_progress', type: 'legacy' };
  }
  return { status: 'unknown', type: null };
}
async function selectClusterForDeposit(node, ethAddress, paymentId) {
  if (!node.registry || !node.staking) {
    return { clusterId: node._activeClusterId || ethers.ZeroHash, requestKey: null };
  }
  let clusters;
  try {
    clusters = await node.registry.getActiveClusters();
  } catch (e) {
    console.log('[Bridge] Warning: getActiveClusters failed, falling back to active cluster:', e.reason || e.message || String(e));
    return { clusterId: node._activeClusterId || ethers.ZeroHash, requestKey: null };
  }
  if (!clusters || clusters.length === 0) {
    return { clusterId: node._activeClusterId || ethers.ZeroHash, requestKey: null };
  }
  const requestKeyHex = ethers.keccak256(ethers.solidityPacked(['address', 'string'], [ethAddress, paymentId]));
  const requestKey = ethers.toBigInt(requestKeyHex);
  const stakes = [];
  let total = 0n;
  let nonZero = false;
  let failedQueries = 0;
  for (const id of clusters) {
    let stake = 0n;
    try {
      const v = await node.registry.getClusterStake(id);
      stake = BigInt(v.toString());
    } catch {
      failedQueries++;
      if (failedQueries >= MAX_STAKE_QUERY_FAILURES) {
        throw new Error('Too many stake query failures');
      }
      stake = 0n;
    }
    stakes.push(stake);
    total += stake;
    if (stake > 0n) nonZero = true;
  }
  let selected = null;
  if (nonZero && total > 0n) {
    const r = requestKey % total;
    let acc = 0n;
    for (let i = 0; i < clusters.length; i += 1) {
      acc += stakes[i];
      if (r < acc) {
        selected = clusters[i];
        break;
      }
    }
    if (!selected) selected = clusters[clusters.length - 1];
  } else {
    const idx = Number(requestKey % BigInt(clusters.length));
    selected = clusters[idx];
  }
  return { clusterId: selected, requestKey: requestKeyHex };
}
export async function startDepositMonitor(node) {
  if (node._depositMonitorRunning) return;
  const bridgeEnabled = isBridgeEnabled();
  if (!bridgeEnabled) {
    console.log('[Bridge] Deposit monitoring disabled by configuration (BRIDGE_ENABLED=0)');
    return;
  }
  if (!node._clusterFinalized || !node._clusterFinalAddress) {
    console.log('[Bridge] Cannot start deposit monitor: cluster not finalized');
    return;
  }
  if (!node.bridge) {
    console.log('[Bridge] Cannot start deposit monitor: bridge contract not configured');
    return;
  }
  loadBridgeState(node);
  if (BATCH_MODE_ENABLED) {
    node._depositBatchManager = new DepositBatchManager();
    if (node._depositBatchState && typeof node._depositBatchState === 'object') {
      node._depositBatchManager.importState(node._depositBatchState);
      node._depositBatchState = null;
    }
    if (!node._pendingBatchSignatures) node._pendingBatchSignatures = new Map();
    console.log('[Bridge] Batch deposit mode enabled');
  }
  node._depositMonitorRunning = true;
  const pollIntervalMs = Number(process.env.DEPOSIT_POLL_INTERVAL_MS || 30000);
  const minConfirmations = Number(process.env.MIN_DEPOSIT_CONFIRMATIONS || 2);
  console.log(`[Bridge] Starting deposit monitor (poll: ${pollIntervalMs / 1000}s, minConf: ${minConfirmations})`);
  const poll = async () => {
    if (!node._depositMonitorRunning || !node._clusterFinalized) return;
    try { await checkForDeposits(node, minConfirmations); }
    catch (e) { console.log('[Bridge] Deposit check error:', e.message || String(e)); }
  };
  await poll();
  node._depositMonitorTimer = setInterval(poll, pollIntervalMs);
  startBridgeStateSaver(node);
  startCleanupTimer(node);
}
export function stopDepositMonitor(node) {
  node._depositMonitorRunning = false;
  if (node._depositMonitorTimer) {
    clearInterval(node._depositMonitorTimer);
    node._depositMonitorTimer = null;
  }
  console.log('[Bridge] Deposit monitor stopped');
}
export function getDepositQueueStatus(node) {
  if (!BATCH_MODE_ENABLED || !node._depositBatchManager) {
    return { pendingCount: 0, maxSlots: 0, availableSlots: 0, isFull: false, windowSecondsRemaining: 0, batchModeEnabled: false };
  }
  const status = node._depositBatchManager.getQueueStatus();
  return { ...status, batchModeEnabled: true };
}
export function checkDepositQueueAvailability(node) {
  if (!BATCH_MODE_ENABLED || !node._depositBatchManager) {
    return { available: true };
  }
  const status = node._depositBatchManager.getQueueStatus();
  if (status.isFull) {
    return { available: false, retryAfterSeconds: status.windowSecondsRemaining };
  }
  return { available: true, slotsRemaining: status.availableSlots };
}
export async function registerDepositRequest(node, ethAddress, paymentId, userSignature) {
  if (!node._pendingDepositRequests) node._pendingDepositRequests = new Map();
  if (!node._clusterFinalized || !node._clusterFinalAddress || !Array.isArray(node._clusterMembers) || node._clusterMembers.length === 0) {
    throw new Error('Bridge cluster not ready');
  }
  const self = node.wallet && node.wallet.address ? node.wallet.address.toLowerCase() : null;
  if (!self || !node._clusterMembers.some((m) => m && m.toLowerCase() === self)) {
    throw new Error('Node not member of active cluster');
  }
  const normalizedEthAddress = normalizeAddress(ethAddress);
  if (!normalizedEthAddress) throw new Error('Invalid ethAddress');
  const id = typeof paymentId === 'string' ? paymentId : '';
  if (!/^[0-9a-f]{16}$/.test(id)) throw new Error('Invalid paymentId');
  if (!userSignature || typeof userSignature !== 'string') throw new Error('userSignature required');
  if (!verifyDepositRequestUserSignature(id, normalizedEthAddress, userSignature)) {
    throw new Error('Invalid user signature');
  }
  const sel = await selectClusterForDeposit(node, normalizedEthAddress, id);
  let clusterId = sel.clusterId || node._activeClusterId || ethers.ZeroHash;
  const requestKey = sel.requestKey || null;
  if (node._activeClusterId && clusterId && clusterId !== node._activeClusterId) {
    const err = new Error('Selected cluster does not match active cluster on this node');
    err.code = 'WRONG_CLUSTER';
    err.selectedClusterId = clusterId;
    err.activeClusterId = node._activeClusterId;
    err.requestKey = requestKey;
    throw err;
  }
  clusterId = node._activeClusterId || clusterId;
  let finalAddress = null;
  if (node.registry && clusterId && clusterId !== ethers.ZeroHash) {
    try {
      const info = await node.registry.clusters(clusterId);
      if (info && info[0] && info[2]) finalAddress = info[0];
    } catch {}
  }
  if (!finalAddress && node._clusterFinalAddress) finalAddress = node._clusterFinalAddress;
  if (!finalAddress) throw new Error('No active clusters');
  let signer = null;
  let nodeSignature = null;
  if (node.wallet && node.wallet.address) {
    signer = node.wallet.address;
    nodeSignature = await signDepositRequestNodePayload(node.wallet, id, normalizedEthAddress, clusterId, requestKey);
  }
  node._pendingDepositRequests.set(id, {
    ethAddress: normalizedEthAddress, clusterId, requestKey, signer, nodeSignature, userSignature, timestamp: Date.now(),
  });
  try {
    await maybeBroadcastDepositRequests(node, true, clusterId);
  } catch (e) {
    console.log('[Bridge] Failed to broadcast deposit request:', e.message || String(e));
    throw new Error('Failed to broadcast deposit request');
  }
  saveBridgeState(node);
  let integratedAddress = null;
  if (node.monero) {
    try {
      integratedAddress = await node.monero.makeIntegratedAddress(id, finalAddress);
    } catch {}
  }
  return {
    paymentId: id,
    multisigAddress: finalAddress,
    integratedAddress: integratedAddress || finalAddress,
    clusterId,
    ethAddress: normalizedEthAddress,
  };
}
