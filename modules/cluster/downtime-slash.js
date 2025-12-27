import { ethers } from 'ethers';

export const DOWNTIME_SLASH_SESSION_ID = 'slash';
export const DOWNTIME_SLASH_SIGN_ROUND = Number(process.env.DOWNTIME_SLASH_SIGN_ROUND || 9932);
export const DOWNTIME_SLASH_SIGN_TIMEOUT_MS = Number(process.env.DOWNTIME_SLASH_SIGN_TIMEOUT_MS || 60000);
export const DOWNTIME_SLASH_SIGNATURES_REQUIRED = Number(process.env.DOWNTIME_SLASH_SIGNATURES_REQUIRED || 6);
export const DOWNTIME_SLASH_BACKOFF_MS = Number(process.env.DOWNTIME_SLASH_BACKOFF_MS || 10 * 60 * 1000);

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

function isClusterMember(node, address) {
  if (!node || !Array.isArray(node._clusterMembers)) return false;
  const a = String(address || '').toLowerCase();
  return node._clusterMembers.some((m) => String(m || '').toLowerCase() === a);
}

function getDowntimeOpenMessageHash(chainId, clusterId, stakingAddr, target, stage, requestId, lastSeen) {
  return ethers.solidityPackedKeccak256(
    ['string', 'address', 'uint256', 'bytes32', 'address', 'uint8', 'bytes32', 'uint256'],
    ['downtime-open-v1', stakingAddr, chainId, clusterId, target, stage, requestId, lastSeen],
  );
}

function verifyDowntimeOpenSignature(signature, chainId, clusterId, stakingAddr, target, stage, requestId, lastSeen, expectedSigner) {
  try {
    const messageHash = getDowntimeOpenMessageHash(chainId, clusterId, stakingAddr, target, stage, requestId, lastSeen);
    const ethSignedHash = ethers.hashMessage(ethers.getBytes(messageHash));
    const recovered = ethers.recoverAddress(ethSignedHash, signature);
    return recovered.toLowerCase() === expectedSigner.toLowerCase();
  } catch {
    return false;
  }
}

export async function ensureSlashSessionCluster(node, membersOverride = null) {
  if (!node || !node.p2p || !node.p2p.node) return false;
  const clusterId = node._activeClusterId;
  if (!clusterId || clusterId === ethers.ZeroHash) return false;
  const members = Array.isArray(membersOverride) ? membersOverride : node._clusterMembers;
  if (!Array.isArray(members) || members.length === 0) return false;
  const topic = typeof node.p2p.topic === 'function'
    ? node.p2p.topic(clusterId, DOWNTIME_SLASH_SESSION_ID)
    : `${clusterId}:${DOWNTIME_SLASH_SESSION_ID}`;
  try {
    await node.p2p.joinCluster(topic, members, false);
    return true;
  } catch {
    return false;
  }
}

export async function buildDowntimeOpenSignature(node, target, stage, requestId, lastSeen) {
  if (!node || !node.wallet || !node.provider) return null;
  const clusterId = normalizeBytes32(node._activeClusterId);
  const stakingAddr = getStakingAddress(node);
  const targetAddr = normalizeAddress(target);
  const req = normalizeBytes32(requestId);
  const stageNum = Number(stage);
  const seenNum = Number(lastSeen);
  if (!clusterId || !stakingAddr || !targetAddr || !req) return null;
  if (!Number.isFinite(stageNum) || stageNum < 1 || stageNum > 3) return null;
  if (!Number.isFinite(seenNum) || seenNum <= 0) return null;
  let chainId;
  try {
    const network = await node.provider.getNetwork();
    const chainIdRaw = network && network.chainId != null ? network.chainId : null;
    chainId = typeof chainIdRaw === 'bigint' ? chainIdRaw : BigInt(chainIdRaw);
  } catch {
    return null;
  }
  const messageHash = getDowntimeOpenMessageHash(chainId, clusterId, stakingAddr, targetAddr, stageNum, req, BigInt(seenNum));
  let signature;
  try {
    signature = await node.wallet.signMessage(ethers.getBytes(messageHash));
  } catch {
    return null;
  }
  return {
    type: 'downtime-open-sig',
    clusterId,
    target: targetAddr,
    stage: stageNum,
    requestId: req,
    lastSeen: seenNum,
    signer: node.wallet.address,
    signature,
    chainId: Number(chainId),
  };
}

export function normalizeDowntimeOpenEntry(entry) {
  if (!entry || typeof entry !== 'object') return null;
  if (entry.type !== 'downtime-open-sig') return null;
  const clusterId = normalizeBytes32(entry.clusterId);
  const target = normalizeAddress(entry.target);
  const requestId = normalizeBytes32(entry.requestId);
  const signer = normalizeAddress(entry.signer);
  const signature = typeof entry.signature === 'string' ? entry.signature : '';
  const stage = Number(entry.stage);
  const lastSeen = Number(entry.lastSeen);
  const chainId = Number(entry.chainId);
  if (!clusterId || !target || !requestId || !signer) return null;
  if (!signature || !signature.startsWith('0x')) return null;
  if (!Number.isFinite(stage) || stage < 1 || stage > 3) return null;
  if (!Number.isFinite(lastSeen) || lastSeen <= 0) return null;
  if (!Number.isFinite(chainId) || chainId <= 0) return null;
  return { clusterId, target, requestId, signer, signature, stage, lastSeen, chainId };
}

export async function collectDowntimeOpenSignatures(node, target, stage, requestId, minSignatures) {
  if (!node || !node.p2p || !node.provider) return [];
  const clusterId = normalizeBytes32(node._activeClusterId);
  const stakingAddr = getStakingAddress(node);
  const targetAddr = normalizeAddress(target);
  const req = normalizeBytes32(requestId);
  const stageNum = Number(stage);
  const min = Number.isFinite(minSignatures) && minSignatures > 0 ? Math.floor(minSignatures) : DOWNTIME_SLASH_SIGNATURES_REQUIRED;
  if (!clusterId || !stakingAddr || !targetAddr || !req) return [];
  if (!Number.isFinite(stageNum) || stageNum < 1 || stageNum > 3) return [];
  let chainId;
  try {
    const network = await node.provider.getNetwork();
    const chainIdRaw = network && network.chainId != null ? network.chainId : null;
    chainId = typeof chainIdRaw === 'bigint' ? chainIdRaw : BigInt(chainIdRaw);
  } catch {
    return [];
  }
  let roundData;
  try {
    roundData = await node.p2p.getRoundData(clusterId, DOWNTIME_SLASH_SESSION_ID, DOWNTIME_SLASH_SIGN_ROUND);
  } catch {
    roundData = null;
  }
  if (!roundData || typeof roundData !== 'object') return [];
  const out = [];
  const seen = new Set();
  for (const [, raw] of Object.entries(roundData)) {
    let parsed;
    try {
      parsed = typeof raw === 'string' ? JSON.parse(raw) : raw;
    } catch {
      continue;
    }
    const data = normalizeDowntimeOpenEntry(parsed);
    if (!data) continue;
    if (data.clusterId.toLowerCase() != clusterId.toLowerCase()) continue;
    if (data.target.toLowerCase() != targetAddr.toLowerCase()) continue;
    if (data.stage !== stageNum) continue;
    if (data.requestId.toLowerCase() != req.toLowerCase()) continue;
    const signerLc = data.signer.toLowerCase();
    if (seen.has(signerLc)) continue;
    if (!isClusterMember(node, data.signer)) continue;
    if (!verifyDowntimeOpenSignature(data.signature, chainId, clusterId, stakingAddr, targetAddr, stageNum, req, BigInt(data.lastSeen), data.signer)) {
      continue;
    }
    seen.add(signerLc);
    out.push({ signer: data.signer, lastSeen: data.lastSeen, signature: data.signature });
    if (out.length >= min) break;
  }
  return out;
}

export async function broadcastDowntimeOpenSignature(node, entry) {
  if (!node || !node.p2p) return false;
  try {
    await node.p2p.broadcastRoundData(
      node._activeClusterId,
      DOWNTIME_SLASH_SESSION_ID,
      DOWNTIME_SLASH_SIGN_ROUND,
      JSON.stringify(entry),
    );
    return true;
  } catch {
    return false;
  }
}
