import { ethers } from 'ethers';
const WEIGHT_STAKE = Number(process.env.HEALTH_SCORE_WEIGHT_STAKE || 30);
const WEIGHT_HEARTBEAT = Number(process.env.HEALTH_SCORE_WEIGHT_HEARTBEAT || 25);
const WEIGHT_CONSENSUS = Number(process.env.HEALTH_SCORE_WEIGHT_CONSENSUS || 20);
const WEIGHT_CONNECTIVITY = Number(process.env.HEALTH_SCORE_WEIGHT_CONNECTIVITY || 15);
const WEIGHT_BLACKLIST = Number(process.env.HEALTH_SCORE_WEIGHT_BLACKLIST || 10);
const REQUIRED_STAKE_ZFI = BigInt(process.env.REQUIRED_STAKE_ZFI || '1000000');
const ZFI_DECIMALS = Number(process.env.ZFI_DECIMALS || 18);
const HEARTBEAT_ONLINE_TTL_MS = Number(process.env.HEARTBEAT_ONLINE_TTL_MS || 120000);
const CONSENSUS_HISTORY_WINDOW = Number(process.env.CONSENSUS_HISTORY_WINDOW || 100);
export function categorizeScore(score) {
  if (score >= 80) return 'healthy';
  if (score >= 60) return 'degraded';
  if (score >= 40) return 'warning';
  return 'critical';
}
export function scoreMember(memberData) {
  const { stakedAmount = 0n, lastHeartbeatMs = 0, consensusParticipation = 1.0, isBlacklisted = false, p2pConnected = true } = memberData;
  const now = Date.now();
  const requiredStake = ethers.parseUnits(String(REQUIRED_STAKE_ZFI), ZFI_DECIMALS);
  const stakeBigInt = typeof stakedAmount === 'bigint' ? stakedAmount : BigInt(String(stakedAmount || 0));
  let stakeScore = 0;
  if (stakeBigInt >= requiredStake) {
    stakeScore = 100;
  } else if (stakeBigInt > 0n) {
    stakeScore = Number((stakeBigInt * 100n) / requiredStake);
  }
  let heartbeatScore = 0;
  if (lastHeartbeatMs > 0) {
    const ageMs = now - lastHeartbeatMs;
    if (ageMs <= 0) {
      heartbeatScore = 100;
    } else if (ageMs < HEARTBEAT_ONLINE_TTL_MS) {
      heartbeatScore = Math.round(100 * (1 - ageMs / HEARTBEAT_ONLINE_TTL_MS));
    } else if (ageMs < HEARTBEAT_ONLINE_TTL_MS * 3) {
      heartbeatScore = Math.round(30 * (1 - (ageMs - HEARTBEAT_ONLINE_TTL_MS) / (HEARTBEAT_ONLINE_TTL_MS * 2)));
    }
  }
  const consensusRate = typeof consensusParticipation === 'number' ? Math.max(0, Math.min(1, consensusParticipation)) : 1;
  const consensusScore = Math.round(consensusRate * 100);
  const connectivityScore = p2pConnected ? 100 : 0;
  const blacklistScore = isBlacklisted ? 0 : 100;
  const totalWeight = WEIGHT_STAKE + WEIGHT_HEARTBEAT + WEIGHT_CONSENSUS + WEIGHT_CONNECTIVITY + WEIGHT_BLACKLIST;
  const weightedSum = stakeScore * WEIGHT_STAKE + heartbeatScore * WEIGHT_HEARTBEAT + consensusScore * WEIGHT_CONSENSUS + connectivityScore * WEIGHT_CONNECTIVITY + blacklistScore * WEIGHT_BLACKLIST;
  const total = Math.round(weightedSum / totalWeight);
  return { total, breakdown: { stake: stakeScore, heartbeat: heartbeatScore, consensus: consensusScore, connectivity: connectivityScore, blacklist: blacklistScore }, status: categorizeScore(total) };
}
export function scoreCluster(memberScores, clusterData = {}) {
  if (!Array.isArray(memberScores) || memberScores.length === 0) {
    return { total: 0, status: 'critical', memberCount: 0, healthyCount: 0, degradedCount: 0, warningCount: 0, criticalCount: 0, averageScore: 0, minScore: 0, maxScore: 0, signingCapable: false };
  }
  const { threshold = 7 } = clusterData;
  const scores = memberScores.map((m) => (typeof m === 'number' ? m : m.total || 0));
  const statuses = memberScores.map((m) => (typeof m === 'object' && m.status ? m.status : categorizeScore(m)));
  const healthyCount = statuses.filter((s) => s === 'healthy').length;
  const degradedCount = statuses.filter((s) => s === 'degraded').length;
  const warningCount = statuses.filter((s) => s === 'warning').length;
  const criticalCount = statuses.filter((s) => s === 'critical').length;
  const sum = scores.reduce((a, b) => a + b, 0);
  const averageScore = Math.round(sum / scores.length);
  const minScore = Math.min(...scores);
  const maxScore = Math.max(...scores);
  const signingCapable = healthyCount + degradedCount >= threshold;
  let clusterPenalty = 0;
  const buffer = (healthyCount + degradedCount) - threshold;
  if (buffer <= 0) clusterPenalty += 30;
  else if (buffer <= 2) clusterPenalty += 15;
  else if (buffer <= 4) clusterPenalty += 5;
  if (criticalCount > 0) clusterPenalty += Math.min(20, criticalCount * 5);
  if (warningCount > memberScores.length / 3) clusterPenalty += 10;
  const total = Math.max(0, Math.min(100, averageScore - clusterPenalty));
  const status = !signingCapable ? 'critical' : categorizeScore(total);
  return { total, status, memberCount: memberScores.length, healthyCount, degradedCount, warningCount, criticalCount, averageScore, minScore, maxScore, signingCapable, threshold, buffer, penalty: clusterPenalty };
}
export async function buildMemberHealthData(node, memberAddress, heartbeatMap = {}) {
  const address = memberAddress.toLowerCase();
  const data = { address, stakedAmount: 0n, lastHeartbeatMs: 0, consensusParticipation: 1.0, isBlacklisted: false, p2pConnected: false };
  if (node.staking) {
    try {
      const info = await node.staking.getNodeInfo(memberAddress);
      if (Array.isArray(info) && info.length >= 4) {
        data.stakedAmount = BigInt(info[0]);
        data.isBlacklisted = await node.staking.isBlacklisted(memberAddress).catch(() => false);
      }
    } catch {}
  }
  if (heartbeatMap && heartbeatMap[address]) {
    const hb = heartbeatMap[address];
    data.lastHeartbeatMs = typeof hb === 'number' ? hb : (hb.timestamp || hb.lastSeen || 0);
    data.p2pConnected = true;
  }
  if (node._consensusStats && node._consensusStats[address]) {
    const stats = node._consensusStats[address];
    if (stats.total > 0) data.consensusParticipation = stats.participated / stats.total;
  }
  return data;
}
export async function calculateClusterHealth(node, options = {}) {
  const { heartbeatMap = null, threshold = 7 } = options;
  if (!node._clusterMembers || node._clusterMembers.length === 0) {
    return { clusterId: node._activeClusterId || null, cluster: scoreCluster([], { threshold }), members: [], timestamp: Date.now() };
  }
  let hbMap = heartbeatMap;
  if (!hbMap && node.p2p && typeof node.p2p.getHeartbeatMap === 'function') {
    try { hbMap = await node.p2p.getHeartbeatMap(); } catch { hbMap = {}; }
  }
  hbMap = hbMap || {};
  const memberResults = [];
  for (const member of node._clusterMembers) {
    const healthData = await buildMemberHealthData(node, member, hbMap);
    const score = scoreMember(healthData);
    memberResults.push({ address: member, data: healthData, score });
  }
  const memberScores = memberResults.map((r) => r.score);
  const clusterScore = scoreCluster(memberScores, { threshold });
  return { clusterId: node._activeClusterId || null, cluster: clusterScore, members: memberResults, timestamp: Date.now() };
}
export function trackConsensusParticipation(node, memberAddress, participated) {
  if (!node._consensusStats) node._consensusStats = {};
  const address = memberAddress.toLowerCase();
  if (!node._consensusStats[address]) node._consensusStats[address] = { total: 0, participated: 0 };
  const stats = node._consensusStats[address];
  stats.total += 1;
  if (participated) stats.participated += 1;
  if (stats.total > CONSENSUS_HISTORY_WINDOW) {
    const factor = CONSENSUS_HISTORY_WINDOW / stats.total;
    stats.total = CONSENSUS_HISTORY_WINDOW;
    stats.participated = Math.round(stats.participated * factor);
  }
}
export default { scoreMember, scoreCluster, categorizeScore, buildMemberHealthData, calculateClusterHealth, trackConsensusParticipation };
