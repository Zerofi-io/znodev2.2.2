import { ethers } from 'ethers';
import { AsyncMutex } from '../../core/async-mutex.js';
import { getWithdrawalShortHash, getPBFTPhases, runConsensusWithFallback } from './pbft-utils.js';
const consensusMutex = new AsyncMutex();
function normalizeEnvInt(name, fallback) {
  const raw = process.env[name];
  const n = raw != null ? Number(raw) : NaN;
  if (!Number.isFinite(n)) return fallback;
  return Math.floor(n);
}
async function checkPbftReadiness(node) {
  if (!node || !node.p2p || !node.p2p.node || typeof node.p2p.getHeartbeats !== 'function') {
    return { ok: false, reason: 'p2p_not_ready' };
  }
  const members = Array.isArray(node._clusterMembers)
    ? node._clusterMembers.map((a) => String(a || '').toLowerCase()).filter(Boolean)
    : [];
  if (members.length === 0) return { ok: false, reason: 'no_cluster_members' };
  const memberCount = members.length;
  const ttlMs0 = normalizeEnvInt('HEARTBEAT_ONLINE_TTL_MS', 120000);
  const ttlMs = ttlMs0 > 0 ? ttlMs0 : 120000;
  let heartbeats;
  try {
    heartbeats = await node.p2p.getHeartbeats(ttlMs);
  } catch {
    heartbeats = null;
  }
  if (!heartbeats || typeof heartbeats.has !== 'function') return { ok: false, reason: 'heartbeat_unavailable' };
  const pbftOverride = normalizeEnvInt('PBFT_QUORUM', 0);
  const clusterThreshold = normalizeEnvInt('CLUSTER_THRESHOLD', 0);
  const f = memberCount > 1 ? Math.floor((memberCount - 1) / 3) : 0;
  let quorum = pbftOverride > 0 && pbftOverride <= memberCount
    ? pbftOverride
    : clusterThreshold > 0 && clusterThreshold <= memberCount
      ? clusterThreshold
      : 2 * f + 1;
  quorum = Math.min(memberCount, Math.max(1, quorum));
  const self = node.wallet && node.wallet.address ? String(node.wallet.address).toLowerCase() : '';
  let healthy = 0;
  for (const m of members) {
    if (self && m === self) {
      healthy += 1;
      continue;
    }
    if (heartbeats.has(m)) healthy += 1;
  }
  if (healthy < quorum) return { ok: false, reason: `too_few_heartbeats:${healthy}/${quorum}` };
  return { ok: true, healthy, quorum };
}
async function logPBFTDebugState(node, clusterId, sessionId, phase, label) {
  if (!node || !node.p2p || typeof node.p2p.getPBFTDebugState !== 'function') return;
  try {
    const topicCluster =
      typeof node.p2p.topic === 'function'
        ? node.p2p.topic(clusterId, sessionId)
        : (sessionId ? `${clusterId}:${sessionId}` : clusterId);
    const debug = await node.p2p.getPBFTDebugState(topicCluster, phase);
    if (!debug || debug.exists === false) {
      console.log(
        `[Withdrawal] PBFT debug state (${label}) not found for`,
        topicCluster,
        phase,
      );
      return;
    }
    const state = debug.state || debug;
    console.log(
      `[Withdrawal] PBFT debug state (${label}) for phase`,
      phase,
      ':',
      JSON.stringify(state, null, 2),
    );
  } catch (e) {
    console.log(
      `[Withdrawal] Failed to fetch PBFT debug state (${label})`,
      e && e.message ? e.message : String(e),
    );
  }
}
export async function runUnsignedWithdrawalTxConsensus(node, txHash, unsignedHash) {
  if (!node || !node.p2p || !Array.isArray(node._clusterMembers) || node._clusterMembers.length === 0) {
    return { success: false, reason: 'no_cluster_members' };
  }
  if (!unsignedHash) {
    return { success: false, reason: 'no_unsigned_hash' };
  }
  const readiness = await checkPbftReadiness(node);
  if (!readiness.ok) {
    return { success: false, reason: `cluster_not_ready:${readiness.reason}` };
  }
  const clusterId = node._activeClusterId;
  const sessionId = node._sessionId || 'bridge';
  const shortEth = getWithdrawalShortHash(txHash);
  const phaseInput = `${String(txHash || '').toLowerCase()}:${String(unsignedHash || '').toLowerCase()}`;
  const phases = getPBFTPhases('withdrawal-tx', phaseInput);
  const phaseKey = phases.primary;
  return consensusMutex.withLock(async () => {
    if (!node._unsignedTxConsensusInflight) node._unsignedTxConsensusInflight = new Map();
    const inflight = node._unsignedTxConsensusInflight.get(phaseKey);
    if (inflight) {
      if (inflight.unsignedHash && inflight.unsignedHash !== unsignedHash) {
        return { success: false, reason: 'unsigned_hash_mismatch_inflight' };
      }
      try {
        const res = await inflight.promise;
        return res || { success: false, reason: 'pbft_inflight_no_result' };
      } catch (e) {
        const msg = e && e.message ? e.message : String(e);
        return { success: false, reason: msg };
      }
    }
    const timeoutMs = Number(process.env.WITHDRAWAL_TX_PBFT_TIMEOUT_MS || process.env.WITHDRAWAL_SIGN_TIMEOUT_MS || 300000);
    const promise = (async () => {
      try {
        if (process.env.WITHDRAWAL_DEBUG_PBFT) {
          console.log(`[Withdrawal] Running unsigned-tx PBFT for ${shortEth} with unsignedHash=${unsignedHash.slice(0, 18)}...`);
        }
        const result = await runConsensusWithFallback(node, phases, unsignedHash, node._clusterMembers, timeoutMs);
        if (result && result.success) {
          return { success: true, isCoordinator: result.isCoordinator, coordinator: result.coordinator };
        }
        let reason = 'pbft_failed';
        if (result) {
          if (result.aborted) {
            reason = result.abortReason || 'pbft_aborted';
          } else if (Array.isArray(result.missing) && result.missing.length > 0) {
            reason = `pbft_missing:${result.missing.join(',')}`;
          } else if (result.reason) {
            reason = result.reason;
          }
        }
        if (process.env.WITHDRAWAL_DEBUG_PBFT) {
          console.log('[Withdrawal] Unsigned-tx PBFT failed', shortEth, 'reason=', reason, 'result=', result || null);
          await logPBFTDebugState(node, clusterId, sessionId, phases.primary, 'unsigned-tx');
          if (phases.fallback) await logPBFTDebugState(node, clusterId, sessionId, phases.fallback, 'unsigned-tx-legacy');
        }
        return { success: false, reason };
      } catch (e) {
        const msg = e && e.message ? e.message : String(e);
        if (process.env.WITHDRAWAL_DEBUG_PBFT) {
          console.log('[Withdrawal] Unsigned-tx PBFT error', shortEth, msg);
          await logPBFTDebugState(node, clusterId, sessionId, phases.primary, 'unsigned-tx-error');
          if (phases.fallback) await logPBFTDebugState(node, clusterId, sessionId, phases.fallback, 'unsigned-tx-error-legacy');
        }
        return { success: false, reason: msg };
      }
    })();
    node._unsignedTxConsensusInflight.set(phaseKey, { unsignedHash, promise });
    try {
      return await promise;
    } finally {
      const cur = node._unsignedTxConsensusInflight && node._unsignedTxConsensusInflight.get(phaseKey);
      if (cur && cur.promise === promise) node._unsignedTxConsensusInflight.delete(phaseKey);
    }
  });
}
export async function runWithdrawalCoordinatorElection(node, withdrawalKey) {
  if (!node || !node.p2p || !Array.isArray(node._clusterMembers) || node._clusterMembers.length === 0) {
    return { success: false, reason: 'no_cluster_members', isCoordinator: false };
  }
  const keyLc = String(withdrawalKey || '').toLowerCase();
  if (!keyLc) {
    return { success: false, reason: 'no_withdrawal_key', isCoordinator: false };
  }
  const readiness = await checkPbftReadiness(node);
  if (!readiness.ok) {
    return { success: false, reason: `cluster_not_ready:${readiness.reason}`, isCoordinator: false };
  }
  const shortKey = getWithdrawalShortHash(keyLc);
  const digest = ethers.keccak256(ethers.toUtf8Bytes(keyLc));
  const timeoutMs = Number(process.env.WITHDRAWAL_COORDINATOR_ELECTION_TIMEOUT_MS || 60000);
  const phases = getPBFTPhases('withdrawal-coordinator', keyLc);
  try {
    console.log(`[Withdrawal] Running coordinator election PBFT for ${shortKey}`);
    const result = await runConsensusWithFallback(node, phases, digest, node._clusterMembers, timeoutMs);
    if (result && result.success) {
      const isCoord = !!result.isCoordinator;
      console.log(`[Withdrawal] Coordinator election completed: isCoordinator=${isCoord}`);
      return { success: true, isCoordinator: isCoord, coordinator: result.coordinator };
    }
    let reason = 'election_failed';
    if (result) {
      if (result.aborted) {
        reason = result.abortReason || 'election_aborted';
      } else if (Array.isArray(result.missing) && result.missing.length > 0) {
        reason = `election_missing:${result.missing.join(',')}`;
      } else if (result.reason) {
        reason = result.reason;
      }
    }
    console.log(`[Withdrawal] Coordinator election failed: ${reason}`);
    return { success: false, reason, isCoordinator: false };
  } catch (e) {
    const msg = e && e.message ? e.message : String(e);
    console.log(`[Withdrawal] Coordinator election error: ${msg}`);
    return { success: false, reason: msg, isCoordinator: false };
  }
}

export async function runWithdrawalBatchManifestConsensus(node, batchId, attempt, manifestHash) {
  if (!node || !node.p2p || !Array.isArray(node._clusterMembers) || node._clusterMembers.length === 0) {
    return { success: false, reason: 'no_cluster_members' };
  }
  const keyLc = String(batchId || '').toLowerCase();
  if (!keyLc) {
    return { success: false, reason: 'no_batch_id' };
  }
  const hashLc = typeof manifestHash === 'string' ? String(manifestHash).toLowerCase() : '';
  if (!/^0x[0-9a-f]{64}$/.test(hashLc)) {
    return { success: false, reason: 'no_manifest_hash' };
  }
  const a0 = Number(attempt);
  const a = Number.isFinite(a0) && a0 >= 0 ? Math.floor(a0) : 0;
  const readiness = await checkPbftReadiness(node);
  if (!readiness.ok) {
    return { success: false, reason: `cluster_not_ready:${readiness.reason}` };
  }
  const shortKey = getWithdrawalShortHash(keyLc);
  const phaseInput = `${keyLc}:${a}:${hashLc}`;
  const phases = getPBFTPhases('withdrawal-batch-manifest', phaseInput);
  const phaseKey = phases.primary;
  return consensusMutex.withLock(async () => {
    if (!node._batchManifestConsensusInflight) node._batchManifestConsensusInflight = new Map();
    const inflight = node._batchManifestConsensusInflight.get(phaseKey);
    if (inflight) {
      if (inflight.manifestHash && inflight.manifestHash !== hashLc) {
        return { success: false, reason: 'manifest_hash_mismatch_inflight' };
      }
      try {
        const res = await inflight.promise;
        return res || { success: false, reason: 'pbft_inflight_no_result' };
      } catch (e) {
        const msg = e && e.message ? e.message : String(e);
        return { success: false, reason: msg };
      }
    }
    const timeoutMs = Number(process.env.WITHDRAWAL_MANIFEST_PBFT_TIMEOUT_MS || 60000);
    const promise = (async () => {
      try {
        if (process.env.WITHDRAWAL_DEBUG_PBFT) {
          console.log(`[Withdrawal] Running manifest PBFT for ${shortKey} attempt=${a} hash=${hashLc.slice(0, 18)}...`);
        }
        const result = await runConsensusWithFallback(node, phases, hashLc, node._clusterMembers, timeoutMs);
        if (result && result.success) {
          return { success: true, isCoordinator: result.isCoordinator, coordinator: result.coordinator };
        }
        let reason = 'pbft_failed';
        if (result) {
          if (result.aborted) {
            reason = result.abortReason || 'pbft_aborted';
          } else if (Array.isArray(result.missing) && result.missing.length > 0) {
            reason = `pbft_missing:${result.missing.join(',')}`;
          } else if (result.reason) {
            reason = result.reason;
          }
        }
        if (process.env.WITHDRAWAL_DEBUG_PBFT) {
          console.log('[Withdrawal] Manifest PBFT failed', shortKey, 'reason=', reason, 'result=', result || null);
        }
        return { success: false, reason };
      } catch (e) {
        const msg = e && e.message ? e.message : String(e);
        if (process.env.WITHDRAWAL_DEBUG_PBFT) {
          console.log('[Withdrawal] Manifest PBFT error', shortKey, msg);
        }
        return { success: false, reason: msg };
      }
    })();
    node._batchManifestConsensusInflight.set(phaseKey, { manifestHash: hashLc, promise });
    try {
      return await promise;
    } finally {
      const cur = node._batchManifestConsensusInflight && node._batchManifestConsensusInflight.get(phaseKey);
      if (cur && cur.promise === promise) node._batchManifestConsensusInflight.delete(phaseKey);
    }
  });
}
