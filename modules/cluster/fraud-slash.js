import { ethers } from 'ethers';
import { saveDowntimeSeen } from './downtime-seen-store.js';
const BACKOFF_MS = Number(process.env.FRAUD_SLASH_BACKOFF_MS || 60000);
const SIGN_ROUND = Number(process.env.FRAUD_SLASH_ROUND || 9921);
const SIGN_TIMEOUT_MS = Number(process.env.FRAUD_SLASH_SIGN_TIMEOUT_MS || 60000);
const COORDINATOR_FAILOVER_COUNT = Number(process.env.FRAUD_SLASH_FAILOVER_COUNT || 3);
const STARTUP_GRACE_MS = Number(process.env.FRAUD_DETECTION_STARTUP_GRACE_MS || 120000);
const FRAUD_DETECTION_STARTED_AT_MS = Date.now();
function isWithinStartupGrace() {
  if (!Number.isFinite(STARTUP_GRACE_MS) || STARTUP_GRACE_MS <= 0) return false;
  return (Date.now() - FRAUD_DETECTION_STARTED_AT_MS) < STARTUP_GRACE_MS;
}
function getStakingAddress(node) {
  const addr = node && node.staking && (node.staking.target || node.staking.address)
    ? String(node.staking.target || node.staking.address)
    : '';
  try {
    return ethers.getAddress(addr);
  } catch {
    return null;
  }
}
function normalizeAddress(value) {
  try {
    return ethers.getAddress(String(value));
  } catch {
    return null;
  }
}
function normalizeBytes32(value) {
  const v = String(value || '');
  if (!/^0x[0-9a-fA-F]{64}$/.test(v)) return null;
  return v;
}
function toChainIdBigInt(value) {
  try {
    if (typeof value === 'bigint') return value > 0n ? value : null;
    if (typeof value === 'number') {
      if (!Number.isFinite(value) || !Number.isInteger(value) || value <= 0) return null;
      return BigInt(value);
    }
    if (typeof value === 'string') {
      const v = value.trim();
      if (!v) return null;
      if (!/^0x[0-9a-fA-F]+$/.test(v) && !/^[0-9]+$/.test(v)) return null;
      const bi = BigInt(v);
      return bi > 0n ? bi : null;
    }
    return null;
  } catch {
    return null;
  }
}
function sortUniqueAddresses(values) {
  const seen = new Set();
  const out = [];
  for (const v of Array.isArray(values) ? values : []) {
    const a = normalizeAddress(v);
    if (!a) continue;
    const k = a.toLowerCase();
    if (seen.has(k)) continue;
    seen.add(k);
    out.push(a);
  }
  out.sort((a, b) => {
    const aa = BigInt(a);
    const bb = BigInt(b);
    if (aa < bb) return -1;
    if (aa > bb) return 1;
    return 0;
  });
  return out;
}
function canonicalizeMembers(members) {
  if (!Array.isArray(members)) return [];
  const seen = new Set();
  const out = [];
  for (const m of members) {
    const lc = String(m || '').toLowerCase();
    if (!lc || seen.has(lc)) continue;
    seen.add(lc);
    out.push(lc);
  }
  out.sort((a, b) => a.localeCompare(b));
  return out;
}
function selectFraudSlashCoordinators(members, fraudId, targetsHash, targetSet, maxCount) {
  const canonical = canonicalizeMembers(members);
  if (canonical.length === 0) return [];
  const seed = ethers.keccak256(
    ethers.solidityPacked(['bytes32', 'bytes32'], [fraudId, targetsHash]),
  );
  const seedBig = BigInt(seed);
  const coordinators = [];
  const eligibleCount = canonical.filter((m) => !targetSet.has(m)).length;
  const limit = Math.min(maxCount, eligibleCount);
  for (let offset = 0; coordinators.length < limit && offset < canonical.length * 2; offset++) {
    const idx = Number((seedBig + BigInt(offset)) % BigInt(canonical.length));
    const candidate = canonical[idx];
    if (targetSet.has(candidate)) continue;
    if (coordinators.includes(candidate)) continue;
    coordinators.push(candidate);
  }
  return coordinators;
}
function getTargetsHash(targets) {
  try {
    return ethers.solidityPackedKeccak256(['address[]'], [targets]);
  } catch {
    return null;
  }
}
function getFraudMessageHash(chainId, clusterId, fraudId, stakingAddr, targetsHash) {
  return ethers.solidityPackedKeccak256(
    ['string', 'address', 'uint256', 'bytes32', 'bytes32', 'bytes32'],
    ['fraud-slash-v1', stakingAddr, chainId, clusterId, fraudId, targetsHash],
  );
}
function verifyFraudSignature(signature, chainId, clusterId, fraudId, stakingAddr, targetsHash, expectedSigner) {
  try {
    const messageHash = getFraudMessageHash(chainId, clusterId, fraudId, stakingAddr, targetsHash);
    const ethSignedHash = ethers.hashMessage(ethers.getBytes(messageHash));
    const recovered = ethers.recoverAddress(ethSignedHash, signature);
    return recovered.toLowerCase() === expectedSigner.toLowerCase();
  } catch {
    return false;
  }
}
function isClusterMember(node, address) {
  if (!node || !Array.isArray(node._clusterMembers)) return false;
  const a = String(address || '').toLowerCase();
  return node._clusterMembers.some((m) => String(m || '').toLowerCase() === a);
}
async function signFraudAccusation(node, clusterId, fraudId, targets, targetsHash, stakingAddr) {
  if (!node || !node.wallet || !node.provider) return null;
  try {
    const network = await node.provider.getNetwork();
    const chainIdRaw = network && network.chainId != null ? network.chainId : null;
    const chainId = toChainIdBigInt(chainIdRaw);
    if (!chainId) return null;
    const messageHash = getFraudMessageHash(chainId, clusterId, fraudId, stakingAddr, targetsHash);
    const signature = await node.wallet.signMessage(ethers.getBytes(messageHash));
    return {
      type: 'fraud-slash-sig',
      clusterId,
      fraudId,
      targets,
      targetsHash,
      signer: node.wallet.address,
      signature,
      chainId: Number(chainId),
    };
  } catch {
    return null;
  }
}
function flushDowntimeStoreSync(node) {
  try {
    saveDowntimeSeen(node);
  } catch {}
}
export async function requestFraudSlashBatch(node, targets, fraudId, reason) {
  if (!node || !node.staking || !node.txManager || !node.p2p) return { attempted: false, ok: false };
  if (isWithinStartupGrace()) {
    console.log('[Fraud] Skipping slash - within startup grace period');
    return { attempted: false, ok: false, reason: 'startup_grace' };
  }
  const clusterId = node._activeClusterId;
  if (!clusterId || clusterId === ethers.ZeroHash) return { attempted: false, ok: false };
  const fid = normalizeBytes32(fraudId);
  if (!fid) return { attempted: false, ok: false };
  const sortedTargets = sortUniqueAddresses(targets);
  if (sortedTargets.length === 0) return { attempted: false, ok: false };
  const targetSet = new Set(sortedTargets.map((t) => String(t || '').toLowerCase()));
  const stakingAddr = getStakingAddress(node);
  if (!stakingAddr) return { attempted: false, ok: false };
  const targetsHash = getTargetsHash(sortedTargets);
  if (!targetsHash) return { attempted: false, ok: false };
  const selfLc = node.wallet && node.wallet.address ? node.wallet.address.toLowerCase() : '';
  if (targetSet.has(selfLc)) {
    return { attempted: false, ok: false };
  }
  const coordinators = selectFraudSlashCoordinators(
    node._clusterMembers || [],
    fid,
    targetsHash,
    targetSet,
    COORDINATOR_FAILOVER_COUNT,
  );
  const isCoordinator = coordinators.length > 0 && coordinators[0] === selfLc;
  const isFailover = !isCoordinator && coordinators.includes(selfLc);
  const backoffKey = `${fid.toLowerCase()}:${targetsHash.toLowerCase()}`;
  node._fraudSlashBackoff = node._fraudSlashBackoff || new Map();
  const last = node._fraudSlashBackoff.get(backoffKey) || 0;
  const now = Date.now();
  if (BACKOFF_MS > 0 && now - last < BACKOFF_MS) {
    return { attempted: false, ok: false };
  }
  node._fraudSlashBackoff.set(backoffKey, now);
  flushDowntimeStoreSync(node);
  const label = reason ? String(reason) : 'fraud';
  const ownSig = await signFraudAccusation(node, clusterId, fid, sortedTargets, targetsHash, stakingAddr);
  if (!ownSig) return { attempted: false, ok: false };
  try {
    await node.p2p.broadcastRoundData(clusterId, node._sessionId || 'bridge', SIGN_ROUND, JSON.stringify(ownSig));
  } catch {
    return { attempted: true, ok: false };
  }
  if (!isCoordinator && !isFailover) {
    return { attempted: true, ok: false };
  }
  const failoverDelayMs = Number(process.env.FRAUD_SLASH_FAILOVER_DELAY_MS || 5000);
  if (isFailover) {
    const myFailoverIndex = coordinators.indexOf(selfLc);
    const delay = failoverDelayMs * myFailoverIndex;
    if (delay > 0) {
      await new Promise((r) => setTimeout(r, delay));
    }
    const recheck = node._fraudSlashBackoff.get(backoffKey) || 0;
    if (recheck !== now) {
      return { attempted: true, ok: false };
    }
  }
  const signatures = [{ signer: ownSig.signer, signature: ownSig.signature }];
  const seen = new Set([String(ownSig.signer || '').toLowerCase()]);
  const start = Date.now();
  while (Date.now() - start < SIGN_TIMEOUT_MS) {
    if (signatures.length >= 3) break;
    try {
      const roundData = await node.p2p.getRoundData(clusterId, node._sessionId || 'bridge', SIGN_ROUND);
      if (roundData && typeof roundData === 'object') {
        for (const [, raw] of Object.entries(roundData)) {
          let data;
          try {
            data = typeof raw === 'string' ? JSON.parse(raw) : raw;
          } catch {
            continue;
          }
          if (!data || data.type !== 'fraud-slash-sig') continue;
          if (!data.signer || !data.signature) continue;
          if (String(data.clusterId || '').toLowerCase() !== String(clusterId).toLowerCase()) continue;
          if (String(data.fraudId || '').toLowerCase() !== fid.toLowerCase()) continue;
          if (String(data.targetsHash || '').toLowerCase() !== String(targetsHash).toLowerCase()) continue;
          const signerLc = String(data.signer).toLowerCase();
          if (seen.has(signerLc)) continue;
          if (targetSet.has(signerLc)) continue;
          if (!isClusterMember(node, data.signer)) continue;
          const chainId = toChainIdBigInt(data.chainId != null ? data.chainId : ownSig.chainId);
          if (!chainId) continue;
          if (!verifyFraudSignature(data.signature, chainId, clusterId, fid, stakingAddr, targetsHash, data.signer)) {
            continue;
          }
          seen.add(signerLc);
          signatures.push({ signer: data.signer, signature: data.signature });
        }
      }
    } catch {}
    if (signatures.length < 3) {
      await new Promise((r) => setTimeout(r, 2000));
    }
  }
  if (signatures.length < 3) {
    return { attempted: true, ok: false };
  }
  try {
    console.log(`[Fraud] Submitting slashForFraudBatch targets=${sortedTargets.length} sigs=${signatures.length} reason=${label}`);
    const sigArray = signatures.map((s) => s.signature);
    const tx = await node.txManager.sendTransaction(
      (overrides) => node.staking.slashForFraudBatch(clusterId, fid, sortedTargets, sigArray, overrides),
      { operation: 'slashForFraudBatch', retryWithSameNonce: true, urgency: 'high' },
    );
    console.log(`[Fraud] slashForFraudBatch tx: ${tx.hash}`);
    await tx.wait();
    return { attempted: true, ok: true };
  } catch (e) {
    const msg = e && e.message ? e.message : String(e);
    console.log(`[Fraud] slashForFraudBatch failed: ${msg.slice(0, 140)}`);
    return { attempted: true, ok: false };
  }
}
