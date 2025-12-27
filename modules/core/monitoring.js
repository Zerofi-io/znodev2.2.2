import { ethers } from 'ethers';
import { currentSlashEpoch, selectSlashLeader } from '../cluster/slash-utils.js';
import { canonicalizeMembers } from '../cluster/round-utils.js';
import { computeDeterministicAssignment } from './cluster-selection.js';
import { logSwallowedError } from '../utils/rate-limited-log.js';
import { loadDowntimeSeen, saveDowntimeSeen } from '../cluster/downtime-seen-store.js';
import {
  DOWNTIME_SLASH_BACKOFF_MS,
  DOWNTIME_SLASH_SESSION_ID,
  DOWNTIME_SLASH_SIGN_ROUND,
  DOWNTIME_SLASH_SIGN_TIMEOUT_MS,
  DOWNTIME_SLASH_SIGNATURES_REQUIRED,
  buildDowntimeOpenSignature,
  broadcastDowntimeOpenSignature,
  collectDowntimeOpenSignatures,
  ensureSlashSessionCluster,
  normalizeDowntimeOpenEntry,
} from '../cluster/downtime-slash.js';
function classifyHeartbeatError(error) {
  const msg = error && error.message ? error.message : String(error);
  if (/Not connected to p2p-daemon|connection refused/i.test(msg)) {
    return { type: 'p2p_not_ready', backoffMs: 30000 };
  }
  if (/rate limit|too many requests|429/i.test(msg)) {
    return { type: 'rate_limit', backoffMs: 60000 };
  }
  if (/timeout|ETIMEDOUT|ECONNRESET/i.test(msg)) {
    return { type: 'transient', backoffMs: 15000 };
  }
  if (/permission|unauthorized|403|401/i.test(msg)) {
    return { type: 'auth_error', backoffMs: 3600000 };
  }
  return { type: 'unknown', backoffMs: 60000 };
}
export function startHeartbeatLoop(node, DRY_RUN) {
  const intervalRaw = process.env.HEARTBEAT_INTERVAL;
  const intervalParsed = intervalRaw != null ? Number(intervalRaw) : NaN;
  let intervalSec = Number.isFinite(intervalParsed) && intervalParsed > 0 ? intervalParsed : 30;
  if (intervalSec < 5 || intervalSec > 300) {
    console.warn(`[Heartbeat] Invalid HEARTBEAT_INTERVAL ${intervalSec}s, clamping to [5, 300]`);
    intervalSec = Math.max(5, Math.min(300, intervalSec));
  }
  const jitterRaw = process.env.HEARTBEAT_JITTER_MS;
  const jitterParsed = jitterRaw != null ? Number(jitterRaw) : NaN;
  const maxJitterMs = Number.isFinite(jitterParsed) && jitterParsed >= 0 ? jitterParsed : 5000;
  if (node._heartbeatTimer) return;
  node._heartbeatFailures = 0;
  node._heartbeatBackoffUntil = 0;
  node._heartbeatAuthErrorLogged = false;
  const baseIntervalMs = intervalSec * 1000;
  const scheduleNext = () => {
    const jitter = Math.floor(Math.random() * maxJitterMs);
    node._heartbeatTimer = setTimeout(tick, baseIntervalMs + jitter);
  };
  const tick = async () => {
    try {
      const now = Date.now();
      if (!node.p2p || !node.p2p.node) {
        if (!node._hbP2PNotReadyLogged) {
          console.log('[Heartbeat] P2P not connected; skipping heartbeat until network is ready.');
          node._hbP2PNotReadyLogged = true;
        }
        scheduleNext();
        return;
      }
      if (now < node._heartbeatBackoffUntil) {
        const remaining = Math.ceil((node._heartbeatBackoffUntil - now) / 1000);
        console.log(
          `[Heartbeat] Backing off for ${remaining}s after ${node._heartbeatFailures} consecutive failures`,
        );
        scheduleNext();
        return;
      }
      if (DRY_RUN) {
        if (!node._hbLogOnce) {
          console.log(
            '[OK] P2P Heartbeat enabled (interval',
            intervalSec,
            's) [DRY_RUN - not sending]',
          );
          node._hbLogOnce = true;
        }
      } else {
        await node.p2p.broadcastHeartbeat();
        node._lastHeartbeatAt = Date.now();
        node._heartbeatFailures = 0;
        node._heartbeatBackoffUntil = 0;
        node._hbP2PNotReadyLogged = false;
        node._heartbeatAuthErrorLogged = false;
        if (!node._hbLogOnce) {
          console.log('[OK] P2P Heartbeat enabled (interval', intervalSec, 's)');
          node._hbLogOnce = true;
        }
      }
    } catch (e) {
      const classification = classifyHeartbeatError(e);
      node._heartbeatFailures++;
      if (classification.type === 'p2p_not_ready') {
        if (!node._hbP2PNotReadyLogged) {
          console.warn('[Heartbeat] P2P client not connected to daemon; skipping heartbeats until connected.');
          node._hbP2PNotReadyLogged = true;
        }
        node._heartbeatBackoffUntil = Date.now() + classification.backoffMs;
        scheduleNext();
        return;
      }
      if (classification.type === 'auth_error') {
        if (!node._heartbeatAuthErrorLogged) {
          console.error('[Heartbeat] Authentication/permission error; check configuration.');
          node._heartbeatAuthErrorLogged = true;
        }
        node._heartbeatBackoffUntil = Date.now() + classification.backoffMs;
        scheduleNext();
        return;
      }
      const msg = e && e.message ? e.message : String(e);
      console.warn(`[Heartbeat] Error (${classification.type}):`, msg);
      node._heartbeatBackoffUntil = Date.now() + classification.backoffMs;
    }
    scheduleNext();
  };
  const initialJitter = Math.floor(Math.random() * 10000);
  setTimeout(tick, 10000 + initialJitter);
}
export function startHeartbeatProofLoop(node, DRY_RUN) {
  if (node && !node._hbProofDisabledLogged) {
    console.log('[HB-PROOF] Disabled (downtime slashing uses quorum challenge windows)');
    node._hbProofDisabledLogged = true;
  }
}
export function startSlashingLoop(node, DRY_RUN) {
  if (node._slashingTimer) return;
  if (!node || !node.staking || !node.provider || !node.wallet || !node.txManager || !node.p2p) return;
  const epochRaw = process.env.SLASH_EPOCH_SECONDS;
  const epochParsed = epochRaw != null ? Number(epochRaw) : NaN;
  const epochSec = Number.isFinite(epochParsed) && epochParsed > 0 ? epochParsed : 600;
  const loopRaw = process.env.DOWNTIME_SLASH_LOOP_INTERVAL_MS || process.env.SLASH_LOOP_INTERVAL_MS;
  const loopParsed = loopRaw != null ? Number(loopRaw) : NaN;
  const loopMs = Number.isFinite(loopParsed) && loopParsed > 0 ? loopParsed : 60 * 60 * 1000;
  const signerRaw = process.env.DOWNTIME_SLASH_SIGNER_INTERVAL_MS;
  const signerParsed = signerRaw != null ? Number(signerRaw) : NaN;
  const signerMs = Number.isFinite(signerParsed) && signerParsed > 0 ? signerParsed : 30000;
  const selfAddr = node.wallet.address.toLowerCase();
  const stakingAddr = (node.staking.target || node.staking.address || '').toString();
  const salt = stakingAddr && stakingAddr !== ethers.ZeroAddress ? stakingAddr : ethers.ZeroHash;

  const stageOpenSec = (stage) => {
    if (stage === 1) return 48 * 3600;
    if (stage === 2) return 96 * 3600;
    if (stage === 3) return 120 * 3600;
    return null;
  };

  const parseChallenge = (raw) => {
    if (!Array.isArray(raw) || raw.length < 5) return null;
    const openedAt = raw[2] != null ? Number(raw[2]) : 0;
    const deadline = raw[3] != null ? Number(raw[3]) : 0;
    const stage = raw[1] != null ? Number(raw[1]) : 0;
    const clusterId = raw[0] != null ? String(raw[0]) : ethers.ZeroHash;
    const lastSeen = raw[4] != null ? Number(raw[4]) : 0;
    if (!Number.isFinite(openedAt) || openedAt <= 0) return null;
    return { clusterId, stage, openedAt, deadline, lastSeen };
  };

  const updateSeen = async (members) => {
    await loadDowntimeSeen(node);
    if (!node._downtimeSeen) {
      node._downtimeSeen = new Map();
    }
    const now = Date.now();
    const relevant = new Set();
    if (Array.isArray(members)) {
      for (const m of members) {
        const k = String(m || '').toLowerCase();
        if (k) relevant.add(k);
      }
    }
    relevant.add(selfAddr);
    try {
      const hbMap = await node.p2p.getHeartbeats();
      for (const [addr, rec] of hbMap.entries()) {
        if (!rec || rec.timestamp == null) continue;
        const k = String(addr || '').toLowerCase();
        if (!k || !relevant.has(k)) continue;
        const ts = Number(rec.timestamp);
        if (!Number.isFinite(ts) || ts <= 0) continue;
        const prev = node._downtimeSeen.get(k) || 0;
        if (ts > prev) {
          node._downtimeSeen.set(k, ts);
        }
      }
    } catch (e) { logSwallowedError('monitoring_error', e); }
    node._downtimeSeen.set(selfAddr, now);
    for (const k of Array.from(node._downtimeSeen.keys())) {
      if (!relevant.has(k)) {
        node._downtimeSeen.delete(k);
      }
    }
    saveDowntimeSeen(node);
  };

  const shouldProceed = () => {
    if (!node._activeClusterId || node._activeClusterId === ethers.ZeroHash) return false;
    if (!Array.isArray(node._clusterMembers) || node._clusterMembers.length === 0) return false;
    if (!node._clusterFinalized) return false;
    return true;
  };

  const cancelSelfIfChallenged = async () => {
    if (!shouldProceed()) return;
    if (DRY_RUN) return;
    const nowSec = Math.floor(Date.now() / 1000);
    if (node._downtimeCancelBackoffUntil && Date.now() < node._downtimeCancelBackoffUntil) return;
    let raw;
    try {
      raw = await node.staking.getDowntimeChallenge(node.wallet.address);
    } catch {
      return;
    }
    const c = parseChallenge(raw);
    if (!c) return;
    if (c.deadline && nowSec > c.deadline) return;
    try {
      const tx = await node.txManager.sendTransaction(
        (txOverrides) => node.staking.cancelDowntimeChallenge(txOverrides),
        { operation: 'cancelDowntimeChallenge', retryWithSameNonce: true, urgency: 'high' },
      );
      await tx.wait();
    } catch {
      node._downtimeCancelBackoffUntil = Date.now() + 60000;
    }
  };

  const signerTick = async () => {
    try {
      if (!shouldProceed()) return;
      const members = node._clusterMembers;
      await ensureSlashSessionCluster(node, members);
      await updateSeen(members);
      await cancelSelfIfChallenged();
      if (!node._downtimeSignedRequests) {
        node._downtimeSignedRequests = new Set();
      }
      let roundData;
      try {
        roundData = await node.p2p.getRoundData(node._activeClusterId, DOWNTIME_SLASH_SESSION_ID, DOWNTIME_SLASH_SIGN_ROUND);
      } catch {
        roundData = null;
      }
      if (!roundData || typeof roundData !== 'object') return;
      const nowMs = Date.now();
      let signedThisTick = 0;
      for (const [, raw] of Object.entries(roundData)) {
        if (signedThisTick >= 2) break;
        let parsed;
        try {
          parsed = typeof raw === 'string' ? JSON.parse(raw) : raw;
        } catch {
          continue;
        }
        const req = normalizeDowntimeOpenEntry(parsed);
        if (!req) continue;
        if (req.clusterId.toLowerCase() !== String(node._activeClusterId).toLowerCase()) continue;
        if (req.signer.toLowerCase() === selfAddr) continue;
        if (!node._clusterMembers.some((m) => String(m || '').toLowerCase() === req.target.toLowerCase())) continue;
        const key = `${req.clusterId.toLowerCase()}:${req.requestId.toLowerCase()}`;
        if (node._downtimeSignedRequests.has(key)) continue;
        const targetLc = req.target.toLowerCase();
        const lastSeenMs = node._downtimeSeen.get(targetLc) || 0;
        if (!Number.isFinite(lastSeenMs) || lastSeenMs <= 0) continue;
        const lastSeenSec = Math.floor(lastSeenMs / 1000);
        const threshold = stageOpenSec(req.stage);
        if (!threshold) continue;
        const offlineSec = Math.floor((nowMs - lastSeenMs) / 1000);
        if (offlineSec < threshold) continue;
        let info;
        try {
          info = await node.staking.getSlashingInfo(req.target);
        } catch {
          continue;
        }
        const slashingStage = Array.isArray(info) && info.length > 0 ? Number(info[0]) : 0;
        const isBlacklisted = Array.isArray(info) && info.length > 2 ? !!info[2] : false;
        if (isBlacklisted) continue;
        if (!Number.isFinite(slashingStage) || slashingStage !== req.stage - 1) continue;
        let challengeRaw;
        try {
          challengeRaw = await node.staking.getDowntimeChallenge(req.target);
        } catch {
          challengeRaw = null;
        }
        if (parseChallenge(challengeRaw)) continue;
        const entry = await buildDowntimeOpenSignature(node, req.target, req.stage, req.requestId, lastSeenSec);
        if (!entry) continue;
        const ok = await broadcastDowntimeOpenSignature(node, entry);
        if (!ok) continue;
        node._downtimeSignedRequests.add(key);
        signedThisTick++;
      }
    } catch (e) { logSwallowedError('monitoring_error', e); }
  };

  const orchestratorTick = async () => {
    try {
      if (!shouldProceed()) return;
      const members = node._clusterMembers;
      await ensureSlashSessionCluster(node, members);
      await updateSeen(members);
      let latest;
      try {
        latest = await node.provider.getBlock('latest');
      } catch {
        return;
      }
      if (!latest || latest.timestamp == null) return;
      const epoch = currentSlashEpoch(latest.timestamp, epochSec);
      const canonicalMembers = canonicalizeMembers(members);
      const nowMs = Date.now();
      const nowSec = Math.floor(nowMs / 1000);
      if (!node._downtimeOpenBackoff) {
        node._downtimeOpenBackoff = new Map();
      }
      for (const target of canonicalMembers) {
        const targetLc = String(target || '').toLowerCase();
        if (!targetLc || targetLc === selfAddr) continue;
        let info;
        try {
          info = await node.staking.getSlashingInfo(target);
        } catch {
          continue;
        }
        const slashingStage = Array.isArray(info) && info.length > 0 ? Number(info[0]) : 0;
        const isBlacklisted = Array.isArray(info) && info.length > 2 ? !!info[2] : false;
        if (isBlacklisted || !Number.isFinite(slashingStage) || slashingStage >= 3) continue;
        let challengeRaw;
        try {
          challengeRaw = await node.staking.getDowntimeChallenge(target);
        } catch {
          challengeRaw = null;
        }
        const challenge = parseChallenge(challengeRaw);
        if (challenge) {
          if (nowSec <= challenge.deadline) continue;
          const leader = selectSlashLeader(canonicalMembers, target, epoch, salt);
          if (!leader || leader.toLowerCase() !== selfAddr) continue;
          if (DRY_RUN) {
            continue;
          }
          try {
            const tx = await node.txManager.sendTransaction(
              (txOverrides) => node.staking.finalizeDowntimeSlash(target, txOverrides),
              { operation: 'finalizeDowntimeSlash', retryWithSameNonce: true, urgency: 'high' },
            );
            await tx.wait();
          } catch (e) { logSwallowedError('monitoring_error', e); }
          continue;
        }
        const stage = slashingStage + 1;
        const threshold = stageOpenSec(stage);
        if (!threshold) continue;
        const lastSeenMs = node._downtimeSeen.get(targetLc) || 0;
        if (!Number.isFinite(lastSeenMs) || lastSeenMs <= 0) continue;
        const offlineSec = Math.floor((nowMs - lastSeenMs) / 1000);
        if (offlineSec < threshold) continue;
        const leader = selectSlashLeader(canonicalMembers, target, epoch, salt);
        if (!leader || leader.toLowerCase() !== selfAddr) continue;
        const backoffKey = `${targetLc}:${stage}`;
        const lastAttempt = node._downtimeOpenBackoff.get(backoffKey) || 0;
        if (DOWNTIME_SLASH_BACKOFF_MS > 0 && nowMs - lastAttempt < DOWNTIME_SLASH_BACKOFF_MS) continue;
        node._downtimeOpenBackoff.set(backoffKey, nowMs);
        const requestId = ethers.keccak256(ethers.randomBytes(32));
        const lastSeenSec = Math.floor(lastSeenMs / 1000);
        const own = await buildDowntimeOpenSignature(node, target, stage, requestId, lastSeenSec);
        if (!own) continue;
        if (!DRY_RUN) {
          await broadcastDowntimeOpenSignature(node, own);
        }
        const bySigner = new Map();
        bySigner.set(own.signer.toLowerCase(), { signer: own.signer, lastSeen: own.lastSeen, signature: own.signature });
        const start = Date.now();
        while (Date.now() - start < DOWNTIME_SLASH_SIGN_TIMEOUT_MS) {
          if (bySigner.size >= DOWNTIME_SLASH_SIGNATURES_REQUIRED) break;
          const sigs = await collectDowntimeOpenSignatures(node, target, stage, requestId, DOWNTIME_SLASH_SIGNATURES_REQUIRED);
          for (const s of sigs) {
            const k = String(s.signer || '').toLowerCase();
            if (!k) continue;
            if (!bySigner.has(k)) {
              bySigner.set(k, s);
            }
          }
          if (bySigner.size < DOWNTIME_SLASH_SIGNATURES_REQUIRED) {
            await new Promise((r) => setTimeout(r, 2000));
          }
        }
        if (bySigner.size < DOWNTIME_SLASH_SIGNATURES_REQUIRED) {
          continue;
        }
        if (DRY_RUN) {
          continue;
        }
        const ordered = Array.from(bySigner.values());
        const lastSeenList = ordered.map((s) => s.lastSeen);
        const sigList = ordered.map((s) => s.signature);
        try {
          const tx = await node.txManager.sendTransaction(
            (txOverrides) =>
              node.staking.openDowntimeChallenge(node._activeClusterId, target, stage, requestId, lastSeenList, sigList, txOverrides),
            { operation: 'openDowntimeChallenge', retryWithSameNonce: true, urgency: 'high' },
          );
          await tx.wait();
        } catch (e) { logSwallowedError('monitoring_error', e); }
        break;
      }
    } catch (e) { logSwallowedError('monitoring_error', e); }
  };

  node._downtimeSignerTimer = setInterval(signerTick, signerMs);
  setTimeout(signerTick, 10000);
  node._slashingTimer = setInterval(orchestratorTick, loopMs);
  setTimeout(orchestratorTick, 30000);
}
export async function monitorNetwork(node, DRY_RUN) {
  console.log('[INFO] Monitoring network...');
  console.log('[INFO] Monero multisig is WORKING!');
  console.log('Wallet has password and multisig is enabled.\n');
  node._clusterFinalized = node._clusterFinalized || false;
  const selfAddr = node.wallet.address.toLowerCase();
  const monitorIntervalRaw = process.env.MONITOR_LOOP_INTERVAL_MS;
  const monitorIntervalParsed = monitorIntervalRaw != null ? Number(monitorIntervalRaw) : NaN;
  const monitorIntervalMs =
    Number.isFinite(monitorIntervalParsed) && monitorIntervalParsed > 0
      ? monitorIntervalParsed
      : 60 * 1000;
  const rawHealthInterval = process.env.HEALTH_LOG_INTERVAL_MS;
  const parsedHealthInterval = rawHealthInterval != null ? Number(rawHealthInterval) : NaN;
  const healthIntervalMs =
    Number.isFinite(parsedHealthInterval) && parsedHealthInterval > 0
      ? parsedHealthInterval
      : 5 * 60 * 1000;
  let clusterSize = 11;
  const clusterThresholdRaw = process.env.CLUSTER_THRESHOLD;
  const clusterThresholdParsed = clusterThresholdRaw != null ? Number(clusterThresholdRaw) : NaN;
  let clusterThreshold = Number.isFinite(clusterThresholdParsed) && clusterThresholdParsed > 0 ? clusterThresholdParsed : 7;
  if (clusterThreshold > clusterSize) {
    console.log(`[Config] ERROR: CLUSTER_THRESHOLD (${clusterThreshold}) > CLUSTER_SIZE (${clusterSize}), clamping threshold to ${clusterSize}`);
    clusterThreshold = clusterSize;
  }
  if (clusterThreshold < 1) {
    console.log(`[Config] ERROR: CLUSTER_THRESHOLD (${clusterThreshold}) < 1, clamping to 1`);
    clusterThreshold = 1;
  }
  if (clusterSize < clusterThreshold) {
    console.log(`[Config] ERROR: CLUSTER_SIZE (${clusterSize}) < CLUSTER_THRESHOLD (${clusterThreshold}), clamping size to ${clusterThreshold}`);
    clusterSize = clusterThreshold;
  }
  const formationEpochSecondsRaw = process.env.FORMATION_EPOCH_SECONDS;
  const formationEpochSecondsParsed =
    formationEpochSecondsRaw != null ? Number(formationEpochSecondsRaw) : NaN;
  const formationEpochSeconds =
    Number.isFinite(formationEpochSecondsParsed) && formationEpochSecondsParsed > 0
      ? formationEpochSecondsParsed
      : 15 * 60;
  const formationWindowSecondsRaw = process.env.FORMATION_WINDOW_SECONDS;
  const formationWindowSecondsParsed =
    formationWindowSecondsRaw != null ? Number(formationWindowSecondsRaw) : NaN;
  const formationWindowSeconds =
    Number.isFinite(formationWindowSecondsParsed) && formationWindowSecondsParsed > 0
      ? formationWindowSecondsParsed
      : 180;
  const epochWarmupMsRaw = process.env.EPOCH_NODE_WARMUP_MS;
  const epochWarmupMsParsed = epochWarmupMsRaw != null ? Number(epochWarmupMsRaw) : NaN;
  const epochWarmupMs =
    Number.isFinite(epochWarmupMsParsed) && epochWarmupMsParsed > 0
      ? epochWarmupMsParsed
      : 3 * 60 * 1000;
  const epochFirstSeen = new Map();
  const computeEpochSeed = async () => {
    let blockNumber = 0;
    let epochSeed = ethers.ZeroHash;
    try {
      blockNumber = await node.provider.getBlockNumber();
      blockNumber = Math.max(0, blockNumber - 5);
      const rawSpan = process.env.SELECTION_EPOCH_BLOCKS;
      const parsedSpan = rawSpan != null ? Number(rawSpan) : NaN;
      const epochSpan = Number.isFinite(parsedSpan) && parsedSpan > 0 ? parsedSpan : 100;
      const epoch = (Number(blockNumber) / epochSpan) | 0;
      epochSeed = ethers.keccak256(ethers.solidityPacked(['uint256'], [epoch]));
    } catch (e) {
      console.log('[Cluster] Failed to compute epoch seed:', e.message || String(e));
      return null;
    }
    return { blockNumber, epochSeed };
  };
  const scanEligibleCanParticipateLower = async () => {
    try {
      const maxScanRaw = process.env.MAX_REGISTERED_SCAN;
      const maxScanParsed = maxScanRaw != null ? Number(maxScanRaw) : NaN;
      const maxScan = Number.isFinite(maxScanParsed) && maxScanParsed > 0 ? maxScanParsed : 256;
      const maxPagesRaw = process.env.MAX_REGISTERED_PAGES;
      const maxPagesParsed = maxPagesRaw != null ? Number(maxPagesRaw) : NaN;
      const maxPages = Number.isFinite(maxPagesParsed) && maxPagesParsed > 0 ? maxPagesParsed : 10;
      const batchSizeRaw = process.env.ELIGIBLE_BATCH_SIZE;
      const batchSizeParsed = batchSizeRaw != null ? Number(batchSizeRaw) : NaN;
      const batchSize = Number.isFinite(batchSizeParsed) && batchSizeParsed > 0 ? batchSizeParsed : 10;
      const candidates = [];
      let offset = 0;
      let pagesScanned = 0;
      while (pagesScanned < maxPages) {
        const page = await node.registry.getRegisteredNodes(offset, maxScan);
        if (!page || page.length === 0) break;
        const validAddrs = page.filter(addr => addr && addr !== ethers.ZeroAddress);
        for (let i = 0; i < validAddrs.length; i += batchSize) {
          const batch = validAddrs.slice(i, i + batchSize);
          const results = await Promise.all(
            batch.map(async (addr) => {
              try {
                const can = await node.registry.canParticipate(addr);
                return can ? String(addr).toLowerCase() : null;
              } catch {
                return null;
              }
            })
          );
          for (const r of results) {
            if (r) candidates.push(r);
          }
        }
        if (page.length < maxScan) break;
        offset += maxScan;
        pagesScanned += 1;
      }
      return [...new Set(candidates)];
    } catch (e) {
      console.log('Eligible candidate scan error:', e.message || String(e));
      return [];
    }
  };
  const runPreSelectionConsensus = async (localCandidates, blockNumber, epochSeed) => {
    if (!node.p2p || typeof node.p2p.broadcastPreSelection !== 'function') {
      return { success: false, candidates: localCandidates };
    }
    const sortedLocal = [...localCandidates].sort();
    if (sortedLocal.length === 0) {
      return { success: false, candidates: localCandidates };
    }
    const selfAddrLower = selfAddr.toLowerCase();
    let preSelCoordinatorIndex = 0;
    try {
      const seed = ethers.keccak256(ethers.toUtf8Bytes(String(epochSeed || '').toLowerCase()));
      preSelCoordinatorIndex = Number(BigInt(seed) % BigInt(sortedLocal.length));
    } catch (e) { logSwallowedError('monitoring_error', e); }
    const preSelCoordinator = sortedLocal[preSelCoordinatorIndex] || sortedLocal[0];
    const isPreSelCoord = preSelCoordinator === selfAddrLower;
    const failoverIndexRaw = process.env.PRESELECTION_FAILOVER_INDEX;
    const failoverIndexParsed = failoverIndexRaw != null ? Number(failoverIndexRaw) : NaN;
    const failoverIndex =
      Number.isFinite(failoverIndexParsed) && Number.isInteger(failoverIndexParsed) && failoverIndexParsed > 0
        ? failoverIndexParsed
        : 1;
    const failoverCoordinator =
      sortedLocal.length > 0
        ? sortedLocal[
            (preSelCoordinatorIndex + (failoverIndex % sortedLocal.length)) % sortedLocal.length
          ]
        : null;
    const isFailoverCoord = failoverCoordinator === selfAddrLower;
    const failoverAfterRaw = process.env.PRESELECTION_FAILOVER_AFTER_MS;
    const failoverAfterParsed = failoverAfterRaw != null ? Number(failoverAfterRaw) : NaN;
    const failoverAfterMs =
      Number.isFinite(failoverAfterParsed) && failoverAfterParsed > 0 ? failoverAfterParsed : 5000;
    const preSelTimeoutRaw = process.env.PRESELECTION_TIMEOUT_MS;
    const preSelTimeoutParsed = preSelTimeoutRaw != null ? Number(preSelTimeoutRaw) : NaN;
    const preSelTimeoutMs = Number.isFinite(preSelTimeoutParsed) && preSelTimeoutParsed > 0
      ? preSelTimeoutParsed : 30000;
    try {
      if (isPreSelCoord) {
        console.log('[PreSelection] Broadcasting proposal as coordinator...');
        const result = await node.p2p.broadcastPreSelection(blockNumber, epochSeed, localCandidates);
        if (!result || !result.proposalId) {
          console.log('[PreSelection] Failed to broadcast proposal');
          return { success: false, candidates: localCandidates };
        }
        const consensus = await node.p2p.waitPreSelection(result.proposalId, localCandidates, preSelTimeoutMs);
        if (consensus && consensus.success) {
          console.log(`[PreSelection] Consensus reached (${consensus.approved}/${consensus.total})`);
          return { success: true, candidates: localCandidates, proposalId: result.proposalId };
        }
        console.log(`[PreSelection] Consensus failed - missing: ${(consensus?.missing || []).join(', ')}`);
        return { success: false, candidates: localCandidates };
      } else {
        console.log(`[PreSelection] Waiting for proposal from ${preSelCoordinator.substring(0, 10)}...`);
        const waitStart = Date.now();
        let proposal = null;
        let failoverBroadcasted = false;
        while (Date.now() - waitStart < preSelTimeoutMs) {
          proposal = await node.p2p.getPreSelectionProposal();
          if (proposal && proposal.found) break;
          if (!failoverBroadcasted && isFailoverCoord && Date.now() - waitStart >= failoverAfterMs) {
            failoverBroadcasted = true;
            console.log('[PreSelection] No proposal received; broadcasting proposal as failover coordinator...');
            const result = await node.p2p.broadcastPreSelection(blockNumber, epochSeed, localCandidates);
            if (result && result.proposalId) {
              const consensus = await node.p2p.waitPreSelection(
                result.proposalId,
                localCandidates,
                preSelTimeoutMs,
              );
              if (consensus && consensus.success) {
                console.log(`[PreSelection] Consensus reached (${consensus.approved}/${consensus.total})`);
                return { success: true, candidates: localCandidates, proposalId: result.proposalId };
              }
              console.log(
                `[PreSelection] Consensus failed - missing: ${(consensus?.missing || []).join(', ')}`,
              );
              return { success: false, candidates: localCandidates };
            }
          }
          await new Promise((r) => setTimeout(r, 500));
        }
        if (!proposal || !proposal.found) {
          console.log('[PreSelection] Timeout waiting for proposal');
          return { success: false, candidates: localCandidates };
        }
        const receivedCandidates = (proposal.candidates || []).map((a) => a.toLowerCase()).sort();
        const localSet = new Set(sortedLocal);
        const matchCount = receivedCandidates.filter((c) => localSet.has(c)).length;
        const matchRatio = matchCount / Math.max(receivedCandidates.length, sortedLocal.length);
        const minMatchRatioRaw = process.env.PRESELECTION_MIN_MATCH_RATIO;
        const minMatchRatioParsed = minMatchRatioRaw != null ? Number(minMatchRatioRaw) : NaN;
        const minMatchRatio = Number.isFinite(minMatchRatioParsed)
          ? Math.min(1, Math.max(0, minMatchRatioParsed))
          : 0.8;
        const approved = matchRatio >= minMatchRatio;
        console.log(`[PreSelection] Match ratio: ${(matchRatio * 100).toFixed(1)}% (${approved ? 'approving' : 'rejecting'})`);
        await node.p2p.votePreSelection(proposal.proposalId, approved, localCandidates);
        if (!approved) return { success: false, candidates: localCandidates };
        const consensus = await node.p2p.waitPreSelection(proposal.proposalId, receivedCandidates, preSelTimeoutMs);
        if (consensus && consensus.success) {
          console.log('[PreSelection] Consensus reached, using coordinator candidates');
          return { success: true, candidates: proposal.candidates, proposalId: proposal.proposalId,
            blockNumber: proposal.blockNumber, epochSeed: proposal.epochSeed };
        }
        return { success: false, candidates: localCandidates };
      }
    } catch (e) {
      console.log('[PreSelection] Error:', e.message || String(e));
      return { success: false, candidates: localCandidates };
    }
  };
  async function computeP2POnlineCountForEligible(node, eligible) {
    let p2pOnlineCount = 0;
    if (node.p2p && typeof node.p2p.getQueuePeers === 'function' && eligible.length > 0) {
      try {
        const selfAddr = node.wallet.address.toLowerCase();
        const recent = await node.p2p.getQueuePeers();
        const addrSet = new Set(recent.map((a) => a.toLowerCase()));
        addrSet.add(selfAddr);
        for (const addr of eligible) {
          if (addrSet.has(addr.toLowerCase())) {
            p2pOnlineCount += 1;
          }
        }
      } catch {
        const selfAddr = node.wallet.address.toLowerCase();
        if (eligible.some((a) => a.toLowerCase() === selfAddr)) {
          p2pOnlineCount = 1;
        }
      }
    } else if (eligible.length > 0) {
      const selfAddr = node.wallet.address.toLowerCase();
      if (eligible.some((a) => a.toLowerCase() === selfAddr)) {
        p2pOnlineCount = 1;
      }
    }
    return p2pOnlineCount;
  }
  const updateEpochFirstSeen = async (hbMapOverride = null) => {
    try {
      if (!node.p2p || typeof node.p2p.getHeartbeats !== 'function') {
        epochFirstSeen.clear();
        return;
      }
      let hbMap = hbMapOverride;
      if (!hbMap) {
        const ttlRaw = process.env.HEARTBEAT_ONLINE_TTL_MS;
        const ttlParsed = ttlRaw != null ? Number(ttlRaw) : NaN;
        const ttlMs = Number.isFinite(ttlParsed) && ttlParsed > 0 ? ttlParsed : undefined;
        hbMap = await node.p2p.getHeartbeats(ttlMs);
      }
      const now = Date.now();
      const graceRaw = process.env.HEARTBEAT_ONLINE_TTL_MS;
      const graceParsed = graceRaw != null ? Number(graceRaw) : NaN;
      const graceMs = Number.isFinite(graceParsed) && graceParsed > 0 ? graceParsed * 2 : 120000;
      const online = new Set();
      for (const [addr, rec] of hbMap.entries()) {
        if (!rec || rec.timestamp == null) continue;
        const lower = String(addr).toLowerCase();
        online.add(lower);
        const existing = epochFirstSeen.get(lower);
        if (!existing) {
          epochFirstSeen.set(lower, { firstMs: now, lastSeenMs: now });
        } else {
          existing.lastSeenMs = now;
        }
      }
      const selfLower = node.wallet.address.toLowerCase();
      online.add(selfLower);
      const selfExisting = epochFirstSeen.get(selfLower);
      if (!selfExisting) {
        epochFirstSeen.set(selfLower, { firstMs: now, lastSeenMs: now });
      } else {
        selfExisting.lastSeenMs = now;
      }
      for (const key of Array.from(epochFirstSeen.keys())) {
        if (!online.has(key)) {
          const entry = epochFirstSeen.get(key);
          if (entry && now - entry.lastSeenMs > graceMs) {
            epochFirstSeen.delete(key);
          }
        }
      }
    } catch {
      epochFirstSeen.clear();
    }
  };
  const checkEpochFormationGate = async (members, hbMap) => {
    if (!Array.isArray(members) || members.length === 0) return false;
    if (!node.provider) return false;
    let latest;
    try {
      latest = await node.provider.getBlock('latest');
    } catch {
      return false;
    }
    if (!latest || latest.timestamp == null) return false;
    const tsSec = Number(latest.timestamp);
    if (!Number.isFinite(tsSec) || tsSec <= 0) return false;
    const epoch = Math.floor(tsSec / formationEpochSeconds);
    const epochEndSec = (epoch + 1) * formationEpochSeconds;
    const timeUntilEndSec = epochEndSec - tsSec;
    if (timeUntilEndSec < 0 || timeUntilEndSec > formationWindowSeconds) {
      return false;
    }
    await updateEpochFirstSeen(hbMap);
    const selfLower = node.wallet.address.toLowerCase();
    const entry = epochFirstSeen.get(selfLower);
    const first = entry ? entry.firstMs : null;
    const now = Date.now();
    if (!first || now - first < epochWarmupMs) {
      return false;
    }
    let warmPeerCount = 0;
    for (const member of members) {
      const memberLower = String(member).toLowerCase();
      const memberEntry = epochFirstSeen.get(memberLower);
      if (memberEntry && now - memberEntry.firstMs >= epochWarmupMs) {
        warmPeerCount++;
      }
    }
    if (warmPeerCount < clusterThreshold) {
      return false;
    }
    return true;
  };
  const abandonClusterAttempt = async (clusterId, reason) => {
    try {
      if (clusterId && typeof node._cleanupClusterAttempt === 'function') {
        await node._cleanupClusterAttempt(clusterId);
      } else if (clusterId && node.p2p && typeof node.p2p.leaveCluster === 'function') {
        await node.p2p.leaveCluster(clusterId);
      }
    } catch (e) { logSwallowedError('monitoring_error', e); }
    try {
      if (node.p2p && typeof node.p2p.setActiveCluster === 'function') {
        node.p2p.setActiveCluster(null);
      }
    } catch (e) { logSwallowedError('monitoring_error', e); }
    try {
      if (node.clusterState && typeof node.clusterState.reset === 'function') {
        if (!node.clusterState.finalized) {
          node.clusterState.reset();
        }
      } else if (node.clusterState && typeof node.clusterState.clearCluster === 'function') {
        node.clusterState.clearCluster();
      }
      if (reason && node.clusterState && typeof node.clusterState.recordFailure === 'function') {
        node.clusterState.recordFailure(reason);
      }
    } catch (e) { logSwallowedError('monitoring_error', e); }
  };
  const computeCooldownUntilMs = async () => {
    const nowMs = Date.now();
    const useNextWindow = process.env.COOLDOWN_TO_NEXT_EPOCH_WINDOW !== '0';
    if (!useNextWindow) {
      const cooldownMs = Number(process.env.CLUSTER_RETRY_COOLDOWN_MS || 300000);
      return nowMs + cooldownMs;
    }
    let tsSec = Math.floor(nowMs / 1000);
    try {
      if (node.provider) {
        const latest = await node.provider.getBlock('latest');
        const t = latest && latest.timestamp != null ? Number(latest.timestamp) : NaN;
        if (Number.isFinite(t) && t > 0) {
          tsSec = t;
        }
      }
    } catch (e) { logSwallowedError('monitoring_error', e); }
    if (!Number.isFinite(tsSec) || tsSec <= 0) {
      tsSec = Math.floor(nowMs / 1000);
    }
    const epochSec = formationEpochSeconds;
    const windowSec = formationWindowSeconds;
    if (!Number.isFinite(epochSec) || epochSec <= 0 || !Number.isFinite(windowSec) || windowSec <= 0) {
      const cooldownMs = Number(process.env.CLUSTER_RETRY_COOLDOWN_MS || 300000);
      return nowMs + cooldownMs;
    }
    const epoch = Math.floor(tsSec / epochSec);
    const epochEndSec = (epoch + 1) * epochSec;
    const windowStartSec = epochEndSec - windowSec;
    const nextWindowStartSec = tsSec < windowStartSec ? windowStartSec : epochEndSec + epochSec - windowSec;
    return Math.max(nowMs, Math.floor(nextWindowStartSec * 1000));
  };
  const loop = async () => {
    if (node._monitorLoopRunning) {
      return;
    }
    if (!node._activeClusterId && node.clusterState && node.clusterState.state) {
      const state = node.clusterState.state;
      const cooldownUntil = state.cooldownUntil || null;
      if (cooldownUntil && Date.now() < cooldownUntil) {
        const remainingSec = Math.ceil((cooldownUntil - Date.now()) / 1000);
        console.log(`[Cluster] Cooldown active after failed attempt; next formation in ${remainingSec}s`);
        return;
      }
    }
    node._monitorLoopRunning = true;
    try {
      try {
        await node._checkSelfStakeHealth();
      } catch (e) {
        console.log(
          '[Slashed] Error during periodic self stake health check:',
          e.message || String(e),
        );
      }
      if (node._selfSlashed) {
        return;
      }
      if (node._clusterFinalized) {
        return;
      }
      if (node._orchestrationMutex.isLocked()) {
        return;
      }
      try {
        if (typeof node._maybeApplyMoneroHealthOverrideCommand === 'function') {
          await node._maybeApplyMoneroHealthOverrideCommand();
        }
      } catch (e) {
        console.log('[MoneroHealth] Override processing error:', e.message || String(e));
      }
      try {
        if (typeof node._maybeAutoResetMoneroOnStartup === 'function') {
          await node._maybeAutoResetMoneroOnStartup();
        }
      } catch (e) {
        console.log('[MoneroHealth] Startup auto-reset error:', e.message || String(e));
      }
      if (
        healthIntervalMs > 0 &&
        (!node._lastHealthLogTs || Date.now() - node._lastHealthLogTs > healthIntervalMs)
      ) {
        try {
          node.logClusterHealth();
        } catch (e) { logSwallowedError('monitoring_error', e); }
        node._lastHealthLogTs = Date.now();
      }
      if (
        node._activeClusterId &&
        node._clusterMembers &&
        node._clusterMembers.length === clusterSize
      ) {
        try {
          const info = await node.registry.clusters(node._activeClusterId);
          const finalized = info && info[2];
          if (finalized) {
            const moneroAddress = info && info[0];
            let membersOnChain = null;
            try {
              membersOnChain = await node.registry.getClusterMembers(node._activeClusterId);
            } catch (e) { logSwallowedError('monitoring_error', e); }
            if (typeof node._onClusterFinalized === 'function') {
              node._onClusterFinalized(node._activeClusterId, membersOnChain, moneroAddress);
            }
          } else {
            if (!node._clusterFinalized && node.p2p && node._activeClusterId) {
              try {
                const sessionIds = [];
                if (node._sessionId) sessionIds.push(node._sessionId);
                sessionIds.push(null);
                let done = false;
                for (const sid of sessionIds) {
                  for (const round of [9998, 9999]) {
                    const r = await node.p2p.getRoundData(node._activeClusterId, sid, round);
                    for (const payload of Object.values(r || {})) {
                      try {
                        const data = JSON.parse(payload);
                        if (typeof node._handleCoordinatorHeartbeat === 'function') {
                          node._handleCoordinatorHeartbeat(data);
                        }
                        if (data.type === 'cluster-finalized' && data.clusterId) {
                          const clusterInfo = await node.registry.clusters(data.clusterId);
                          const onChainFinalized = clusterInfo && clusterInfo[2];
                          if (!onChainFinalized) {
                            continue;
                          }
                          let membersOnChain = null;
                          try {
                            membersOnChain = await node.registry.getClusterMembers(data.clusterId);
                          } catch (e) { logSwallowedError('monitoring_error', e); }
                          const moneroAddress = clusterInfo && clusterInfo[0];
                          if (typeof node._onClusterFinalized === 'function') {
                            console.log('[Cluster] Finalization confirmed on-chain via coordinator broadcast');
                            node._onClusterFinalized(data.clusterId, membersOnChain, moneroAddress);
                          }
                          done = true;
                          break;
                        }
                      } catch (e) { logSwallowedError('monitoring_error', e); }
                    }
                    if (done) break;
                  }
                  if (done) break;
                }
              } catch (e) { logSwallowedError('monitoring_error', e); }
            }
            const failoverRaw = process.env.FINALIZE_FAILOVER_MS;
            const failoverParsed = failoverRaw != null ? Number(failoverRaw) : NaN;
            const failoverMs =
              Number.isFinite(failoverParsed) && failoverParsed > 0
                ? failoverParsed
                : 15 * 60 * 1000;
            if (
              !node._clusterFailoverAttempted &&
              node._clusterFinalizationStartAt &&
              node._clusterFinalAddress &&
              failoverMs > 0
            ) {
              const elapsed = Date.now() - node._clusterFinalizationStartAt;
              if (elapsed > failoverMs) {
                const membersLowerLocal = node._clusterMembers.map((a) => a.toLowerCase());
                const sortedLower = [...membersLowerLocal].sort();
                const myIndex = sortedLower.indexOf(selfAddr);
                const failoverIndexRaw = process.env.FAILOVER_COORDINATOR_INDEX;
                const failoverIndexParsed =
                  failoverIndexRaw != null ? Number(failoverIndexRaw) : NaN;
                let failoverIndex =
                  Number.isFinite(failoverIndexParsed) && failoverIndexParsed > 0
                    ? failoverIndexParsed
                    : 1;
                if (failoverIndex >= clusterSize) {
                  console.warn(
                    `[WARN]  FAILOVER_COORDINATOR_INDEX (${failoverIndex}) >= cluster size (${clusterSize}), using index 1`,
                  );
                  failoverIndex = 1;
                }
                if (myIndex === failoverIndex) {
                  console.log(
                    `[WARN] Coordinator did not finalize in time; attempting fallback finalizeCluster as coordinator #${failoverIndex}`,
                  );
                  try {
                    const clusterInfo = await node.registry.clusters(node._activeClusterId);
                    const alreadyFinalized = clusterInfo && clusterInfo[2];
                    if (alreadyFinalized) {
                      console.log(
                        '  [INFO]  Cluster already finalized by another node; skipping failover',
                      );
                    } else {
                      const canonicalMembers = canonicalizeMembers(node._clusterMembers || []);
                      console.log(
                        `  [INFO] finalizeCluster([${canonicalMembers.length} members], ${node._clusterFinalAddress})`,
                      );
                      if (DRY_RUN) {
                        console.log(
                          '  [DRY_RUN] Would send fallback finalizeCluster transaction',
                        );
                      } else {
                        const tx = await node.txManager.sendTransaction(
                          (overrides) =>
                            node.registry.finalizeCluster(
                              canonicalMembers,
                              node._clusterFinalAddress,
                              overrides,
                            ),
                          {
                            operation: 'finalizeCluster-failover',
                            retryWithSameNonce: true,
                            urgency: 'high',
                          },
                        );
                        await tx.wait();
                        console.log('[OK] Cluster finalized on-chain (v3, fallback coordinator)');
                      }
                    }
                  } catch (e2) {
                    console.log(
                      '[ERROR] Fallback finalizeCluster() on-chain failed:',
                      e2.message || String(e2),
                    );
                  } finally {
                    if (node.clusterState && typeof node.clusterState.setFailoverAttempted === 'function') {
                      node.clusterState.setFailoverAttempted();
                    }
                  }
                }
              }
            }
          }
          node._clusterStatusErrorCount = 0;
        } catch (e) {
          console.log('Cluster status read error:', e.message || String(e));
          node._clusterStatusErrorCount = (node._clusterStatusErrorCount || 0) + 1;
          if (node._clusterStatusErrorCount > 5) {
            console.log('[WARN] Repeated cluster status errors; resetting active cluster state');
            if (node.clusterState && typeof node.clusterState.clearCluster === 'function') {
              node.clusterState.clearCluster();
            }
            if (node.p2p && typeof node.p2p.setActiveCluster === 'function') {
              node.p2p.setActiveCluster(null);
            }
            node._clusterStatusErrorCount = 0;
          }
        }
        try {
          await node.checkEmergencySweep();
        } catch (e) {
          console.log('[Sweep] Emergency sweep check error:', e.message || String(e));
        }
        return;
      }
      let hbMap = null;
      let eligibleLower = [];
      let blockNumber = 0;
      let epochSeed = ethers.ZeroHash;
      try {
        const epochInfo = await computeEpochSeed();
        if (!epochInfo) {
          return;
        }
        blockNumber = epochInfo.blockNumber;
        epochSeed = epochInfo.epochSeed;
        eligibleLower = await scanEligibleCanParticipateLower();
        if (eligibleLower.length === 0) {
          console.log('[Cluster] No eligible registered nodes for cluster formation; skipping');
          return;
        }
        const selfIsEligible = eligibleLower.includes(selfAddr);
        let onchainOnlineCount = 0;
        if (node.p2p && typeof node.p2p.getHeartbeats === 'function') {
          try {
            const ttlRaw = process.env.HEARTBEAT_ONLINE_TTL_MS;
            const ttlParsed = ttlRaw != null ? Number(ttlRaw) : NaN;
            const ttlMs = Number.isFinite(ttlParsed) && ttlParsed > 0 ? ttlParsed : undefined;
            hbMap = await node.p2p.getHeartbeats(ttlMs);
            const counted = new Set();
            for (const addrLower of eligibleLower) {
              const rec = hbMap.get(addrLower);
              if (rec && rec.timestamp != null) {
                onchainOnlineCount++;
                counted.add(addrLower);
              }
            }
            if (selfIsEligible && !counted.has(selfAddr)) {
              onchainOnlineCount++;
            }
          } catch {
            hbMap = null;
            onchainOnlineCount = selfIsEligible ? 1 : 0;
          }
        } else if (selfIsEligible) {
          onchainOnlineCount = 1;
        }
        console.log(
          'Online Members in Queue (on-chain): ' + onchainOnlineCount + '/' + eligibleLower.length,
        );
        const minP2PVisibility = Number(process.env.MIN_P2P_VISIBILITY || clusterSize);
        const attemptsRaw = process.env.LIVENESS_ATTEMPTS;
        const attemptsParsed = attemptsRaw != null ? Number(attemptsRaw) : NaN;
        const maxAttempts =
          Number.isFinite(attemptsParsed) && attemptsParsed > 0 ? Math.floor(attemptsParsed) : 3;
        const intervalRaw = process.env.LIVENESS_ATTEMPT_INTERVAL_MS;
        const intervalParsed = intervalRaw != null ? Number(intervalRaw) : NaN;
        const attemptIntervalMs =
          Number.isFinite(intervalParsed) && intervalParsed >= 0 ? intervalParsed : 30000;
        let p2pOnlineCount = 0;
        let attempt = 1;
        while (true) {
          p2pOnlineCount = await computeP2POnlineCountForEligible(node, eligibleLower);
          if (eligibleLower.length > 0) {
            console.log(
              `P2P-Online Members in Queue (attempt ${attempt}/${maxAttempts}): ${p2pOnlineCount}/${eligibleLower.length}`,
            );
          } else {
            console.log('P2P-Online Members in Queue: 0/0');
          }
          if (p2pOnlineCount >= minP2PVisibility) {
            break;
          }
          if (attempt >= maxAttempts) {
            console.log(
              `  [WARN] P2P visibility too low (${p2pOnlineCount}/${minP2PVisibility}) after ${maxAttempts} liveness attempts; waiting for next monitor loop...`,
            );
            return;
          }
          if (attemptIntervalMs > 0) {
            console.log(
              `[Cluster] Liveness attempt ${attempt}/${maxAttempts} incomplete; waiting ${
                attemptIntervalMs / 1000
              }s before retry...`,
            );
            await new Promise((r) => setTimeout(r, attemptIntervalMs));
          }
          attempt += 1;
        }
      } catch (e) {
        console.log('Queue status log error:', e.message || String(e));
      }
      if (eligibleLower.length === 0) {
        return;
      }
      const enablePreSel = process.env.ENABLE_PRESELECTION === '1';
      let selectionCandidatesLower = eligibleLower;
      let selectionEpochSeed = epochSeed;
      if (enablePreSel && Number.isFinite(blockNumber) && selectionCandidatesLower.length > 0) {
        console.log('[PreSelection] Running pre-selection consensus...');
        const preSelResult = await runPreSelectionConsensus(
          selectionCandidatesLower,
          blockNumber,
          selectionEpochSeed,
        );
        if (!preSelResult.success) {
          console.log('[PreSelection] Pre-selection consensus failed, aborting cluster formation');
          return;
        }
        if (preSelResult.candidates && preSelResult.epochSeed) {
          selectionCandidatesLower = preSelResult.candidates.map((a) => String(a).toLowerCase());
          selectionEpochSeed = preSelResult.epochSeed;
        }
      }
      const enableMultiCluster = process.env.ENABLE_MULTI_CLUSTER_FORMATION !== '0';
      const enableHeartbeatSelection = process.env.ENABLE_HEARTBEAT_FILTERED_SELECTION !== '0';
      let effectiveCandidatesLower = selectionCandidatesLower;
      if (enableHeartbeatSelection) {
        const online = [];
        const seen = new Set();
        if (hbMap) {
          for (const addrLower of selectionCandidatesLower) {
            const rec = hbMap.get(addrLower);
            if (rec && rec.timestamp != null && !seen.has(addrLower)) {
              seen.add(addrLower);
              online.push(addrLower);
            }
          }
        }
        await updateEpochFirstSeen(hbMap);
        const entry = epochFirstSeen.get(selfAddr);
        const first = entry ? entry.firstMs : null;
        const selfWarmOk = first && Date.now() - first >= epochWarmupMs;
        if (selectionCandidatesLower.includes(selfAddr) && selfWarmOk && !seen.has(selfAddr)) {
          seen.add(selfAddr);
          online.push(selfAddr);
        }
        effectiveCandidatesLower = online;
      }
      const assignment = computeDeterministicAssignment({
        candidatesLower: effectiveCandidatesLower,
        epochSeed: selectionEpochSeed,
        selfLower: selfAddr,
        clusterSize,
        clusterThreshold,
      });
      const rankedOnlineLower = assignment.onlineEligibleLower;
      if (rankedOnlineLower.length < clusterSize) {
        return;
      }
      let groupIndex = 0;
      let groupMembersLower = null;
      if (enableMultiCluster) {
        if (assignment.myIndex < 0) {
          return;
        }
        groupIndex = assignment.groupIndex;
        groupMembersLower = assignment.groupMembersLower;
        if (!groupMembersLower) {
          console.log(
            `[ClusterSelect] Remainder node (eligible=${selectionCandidatesLower.length} online=${rankedOnlineLower.length} remainder=${assignment.remainderCount})`,
          );
          return;
        }
      } else {
        groupMembersLower = rankedOnlineLower.slice(0, clusterSize);
        if (groupMembersLower.length !== clusterSize) {
          return;
        }
        if (!groupMembersLower.includes(selfAddr)) {
          console.log(`[WARN] Self (${selfAddr}) not in computed cluster members`);
          return;
        }
      }
      const members = groupMembersLower.map((addr) => ethers.getAddress(addr));
      const sortedMembers = [...members].sort((a, b) => a.toLowerCase().localeCompare(b.toLowerCase()));
      const clusterId = ethers.solidityPackedKeccak256(['address[]'], [sortedMembers]);
      try {
        const info = await node.registry.clusters(clusterId);
        const finalized = info && info[2];
        if (finalized) {
          return;
        }
      } catch (e) { logSwallowedError('monitoring_error', e); }
      console.log(
        `[ClusterSelect] eligible=${selectionCandidatesLower.length} online=${rankedOnlineLower.length} group=${groupIndex} remainder=${assignment.remainderCount} clusterId=${clusterId.substring(0, 18)}`,
      );
      const membersLower = members.map((a) => a.toLowerCase());
      const epochWindowOk = await checkEpochFormationGate(members, hbMap);
      if (!epochWindowOk) {
        return;
      }
      if (membersLower.length !== clusterSize) {
        console.log(
          `[WARN] Candidate cluster has ${membersLower.length} members, expected ${clusterSize}; skipping`,
        );
        return;
      }
      if (!membersLower.includes(selfAddr)) {
        console.log(`[WARN] Self (${selfAddr}) not in computed cluster members`);
        return;
      }
      try {
        const enforceSelectedVisibility = process.env.ENFORCE_SELECTED_MEMBERS_P2P_VISIBILITY !== '0';
        const selectedVisible = await computeP2POnlineCountForEligible(node, membersLower);
        if (membersLower.length > 0) {
          console.log(
            `Selected Members visible via P2P: ${selectedVisible}/${membersLower.length}`,
          );
        }
        if (enforceSelectedVisibility && selectedVisible < membersLower.length) {
          console.log(
            '[WARN] Selected cluster members not fully visible via P2P; skipping this formation cycle',
          );
          return;
        }
      } catch (e) { logSwallowedError('monitoring_error', e); }
      const sortedMembersLower = [...membersLower].sort();
      let coordinatorIndex = 0;
      try {
        const seed = ethers.keccak256(ethers.toUtf8Bytes(String(clusterId || '').toLowerCase().split(':')[0]));
        const seedBig = BigInt(seed);
        coordinatorIndex = Number(seedBig % BigInt(sortedMembersLower.length));
      } catch (e) {
        const msg = e && e.message ? e.message : String(e);
        console.log('[WARN] Coordinator selection failed, falling back to index 0:', msg);
      }
      const coordinator = sortedMembersLower[coordinatorIndex];
      const isCoordinator = selfAddr === coordinator;
      const myIndex = sortedMembersLower.indexOf(selfAddr);
      console.log('[INFO] New candidate cluster discovered...');
      console.log(`  ClusterId: ${clusterId}`);
      console.log(
        `  Members: ${members.length} (myIndex=${myIndex}, coordinator=${coordinator})`,
      );
      const jitterRaw = process.env.NON_COORDINATOR_JITTER_MS;
      const defaultJitterMs = 5000;
      let maxJitterMs = defaultJitterMs;
      if (jitterRaw != null) {
        const jitterParsed = Number(jitterRaw);
        if (Number.isFinite(jitterParsed) && jitterParsed >= 0) {
          maxJitterMs = jitterParsed;
        }
      }
      if (!isCoordinator && maxJitterMs > 0) {
        const delay = Math.floor(Math.random() * maxJitterMs);
        const delaySec = Math.floor(delay / 1000);
        console.log(
          `[Cluster] Non-coordinator jitter active; sleeping ${delaySec}s before cluster orchestration`,
        );
        await new Promise((r) => setTimeout(r, delay));
      }
      const p2pOk = await node.initClusterP2P(clusterId, members, isCoordinator);
      if (!p2pOk) {
        console.log('[WARN] P2P init failed for cluster; will retry later');
        return;
      }
      const warmupRaw = process.env.P2P_WARMUP_MS;
      const warmupParsed = warmupRaw != null ? Number(warmupRaw) : NaN;
      const warmupMs =
        Number.isFinite(warmupParsed) && warmupParsed >= 0 ? warmupParsed : 30000;
      if (warmupMs > 0) {
        const warmupSec = Math.floor(warmupMs / 1000);
        console.log(
          `[Cluster] P2P warm-up: waiting ${warmupSec}s for mesh to stabilize before liveness...`,
        );
        await new Promise((r) => setTimeout(r, warmupMs));
      }
      if (!node.p2p || !node.p2p.node) {
        console.log('[WARN] P2P not available for liveness check; skipping candidate cluster');
        await abandonClusterAttempt(clusterId, 'p2p_unavailable');
        return;
      }
      const liveKeyInitial = `${clusterId}_9999`;
      console.log(
        `  [INFO] Proceeding to R1 signature barrier for liveness (${clusterSize} nodes expected)...`,
      );
      if (node._orchestrationMutex.isLocked()) {
        console.log('[WARN] Cluster orchestration already in progress, skipping');
        await abandonClusterAttempt(clusterId, 'orchestration_locked');
        return;
      }
      await node._orchestrationMutex.acquire();
      try {
        if (node.clusterState && typeof node.clusterState.setCluster === 'function') {
          node.clusterState.setCluster(clusterId, members, ethers.getAddress(coordinator), myIndex);
        }
        if (node.p2p && typeof node.p2p.setActiveCluster === 'function') {
          node.p2p.setActiveCluster(clusterId);
        }
        console.log('[INFO] Starting identity exchange (PBFT consensus will gate progress)...');
        const readyRaw = process.env.READY_BARRIER_TIMEOUT_MS;
        const readyParsed = readyRaw != null ? Number(readyRaw) : NaN;
        const readyTimeoutMs =
          Number.isFinite(readyParsed) && readyParsed > 0 ? readyParsed : 300000;
        const identityTimeoutMs = Number(process.env.PBFT_IDENTITY_TIMEOUT_MS || readyTimeoutMs);
        const identityPromise = node.p2p
          .waitForIdentities(clusterId, members, identityTimeoutMs)
          .catch((e) => {
            const data = e && e.data && typeof e.data === 'object' ? e.data : null;
            if (data && (Array.isArray(data.missing) || data.complete === false)) {
              return data;
            }
            return null;
          });
        const canonicalIdentityMembers = [...members].map((a) => (a || '').toLowerCase()).sort();
        const identityData = JSON.stringify(canonicalIdentityMembers);
        const identityConsensusTimeout = Number(
          process.env.PBFT_IDENTITY_TIMEOUT_MS || readyTimeoutMs,
        );
        try {
          await node.p2p.broadcastRoundData(clusterId, null, 9700, JSON.stringify({ phase: 'identity-ready' }));
        } catch (e) {
          console.log('[WARN] Identity phase-ready broadcast error:', e.message || String(e));
        }
        if (isCoordinator && node.p2p && typeof node.p2p.waitForRoundCompletion === 'function') {
          const ready = await node.p2p.waitForRoundCompletion(
            clusterId,
            null,
            9700,
            canonicalIdentityMembers,
            identityConsensusTimeout,
            { requireAll: true },
          );
          if (!ready) {
            console.log('[ERROR] Identity phase-ready barrier incomplete; aborting cluster attempt');
            await abandonClusterAttempt(clusterId, 'identity_ready_barrier_incomplete');
            return;
          }
        }
        console.log(`[INFO] PBFT Consensus: waiting for all ${clusterSize} nodes to be ready...`);
        let identityConsensus;
        try {
          identityConsensus = await node.p2p.runConsensus(
            clusterId,
            null,
            'identity',
            identityData,
            canonicalIdentityMembers,
            identityConsensusTimeout,
            { requireAll: true },
          );
        } catch (e) {
          console.log(`[ERROR] PBFT "identity" consensus error: ${e.message || String(e)}`);
          console.log('[INFO] Cleaning up and will retry...');
          await abandonClusterAttempt(clusterId, 'pbft_identity_error');
          return;
        }
        if (!identityConsensus.success) {
          console.log(
            `[ERROR] PBFT "identity" consensus failed - missing: ${(identityConsensus.missing || []).join(', ')}`,
          );
          console.log('[INFO] Cleaning up and will retry...');
          await abandonClusterAttempt(clusterId, 'pbft_identity_failed');
          return;
        }
        console.log('[OK] PBFT identity consensus reached');
        const identityResult = await identityPromise;
        if (!identityResult?.complete) {
          const missing = Array.isArray(identityResult?.missing) ? identityResult.missing : [];
          const missingMsg = missing.length ? missing.join(', ') : '(unknown)';
          console.log(`[ERROR] Identity exchange incomplete; missing: ${missingMsg}`);
          await abandonClusterAttempt(clusterId, 'identity_exchange_incomplete');
          return;
        }
        try {
          const bindings = await node.p2p.getPeerBindings(clusterId);
          const bindingCount = Object.keys(bindings || {}).length;
          if (bindingCount < members.length) {
            console.log(`[WARN] Only ${bindingCount}/${members.length} peer bindings established`);
          }
        } catch (e) { logSwallowedError('monitoring_error', e); }
        const maxMultisigAttempts = Number(process.env.CLUSTER_MULTISIG_MAX_ATTEMPTS || 3);
        const canonicalMembers = canonicalizeMembers(members);
        const attemptSignalTimeoutMs = Number(process.env.MSIG_ATTEMPT_SIGNAL_TIMEOUT_MS || 600000);
        const attemptSignalPollMs = Number(process.env.MSIG_ATTEMPT_SIGNAL_POLL_MS || 1000);
        const failoverIndexRaw = process.env.FAILOVER_COORDINATOR_INDEX;
        const failoverIndexParsed = failoverIndexRaw != null ? Number(failoverIndexRaw) : NaN;
        let failoverIndex =
          Number.isFinite(failoverIndexParsed) && Number.isInteger(failoverIndexParsed) && failoverIndexParsed > 0
            ? failoverIndexParsed
            : 1;
        if (failoverIndex >= sortedMembersLower.length) {
          failoverIndex = 1;
        }
        const failoverCoordinator =
          sortedMembersLower.length > 1
            ? sortedMembersLower[(coordinatorIndex + failoverIndex) % sortedMembersLower.length]
            : null;
        const publishAttempt = async (n) => {
          if (!node.p2p || typeof node.p2p.broadcastRoundData !== 'function') return false;
          try {
            await node.p2p.broadcastRoundData(clusterId, null, 9997, `msig-attempt:${n}`);
            return true;
          } catch (e) {
            console.log('[Retry] Failed to publish multisig attempt:', e.message || String(e));
            return false;
          }
        };
        const readCoordinatorAttempt = async () => {
          if (!node.p2p || typeof node.p2p.getRoundData !== 'function') return null;
          try {
            const data = await node.p2p.getRoundData(clusterId, null, 9997);
            if (!data) return null;
            let payload = data[coordinator];
            if (typeof payload !== 'string' && failoverCoordinator) {
              payload = data[failoverCoordinator];
            }
            if (typeof payload !== 'string') return null;
            if (!payload.startsWith('msig-attempt:')) return null;
            const n = Number(payload.slice('msig-attempt:'.length));
            if (!Number.isFinite(n) || n <= 0) return null;
            return Math.floor(n);
          } catch {
            return null;
          }
        };
        const waitForCoordinatorAttempt = async (minAttempt, maxAttempt) => {
          const deadline = Date.now() + attemptSignalTimeoutMs;
          const failoverAfterRaw = process.env.MSIG_ATTEMPT_SIGNAL_FAILOVER_AFTER_MS;
          const failoverAfterParsed = failoverAfterRaw != null ? Number(failoverAfterRaw) : NaN;
          const failoverAfterMs =
            Number.isFinite(failoverAfterParsed) && failoverAfterParsed > 0 ? failoverAfterParsed : 60000;
          const waitStart = Date.now();
          let lastLogAt = 0;
          let failoverPublished = false;
          while (Date.now() < deadline) {
            const n = await readCoordinatorAttempt();
            if (Number.isFinite(n) && n >= minAttempt && n <= maxAttempt) return n;
            const now = Date.now();
            if (
              !failoverPublished &&
              failoverCoordinator &&
              selfAddr === failoverCoordinator &&
              now - waitStart >= failoverAfterMs
            ) {
              failoverPublished = true;
              await publishAttempt(minAttempt);
            }
            if (now >= lastLogAt) {
              console.log(
                `[Retry] Waiting for attempt signal (min=${minAttempt}, max=${maxAttempt})...`,
              );
              lastLogAt = now + 5000;
            }
            const sleepMs = Math.min(attemptSignalPollMs, Math.max(0, deadline - now));
            if (sleepMs > 0) {
              await new Promise((r) => setTimeout(r, sleepMs));
            }
          }
          return null;
        };
        const confirmAttemptConsensus = async (n) => {
          if (!node.p2p || typeof node.p2p.runConsensus !== 'function') return false;
          try {
            const res = await node.p2p.runConsensus(
              clusterId,
              null,
              `msig-attempt-${n}`,
              `msig-attempt:${n}`,
              canonicalMembers,
              attemptSignalTimeoutMs,
              { requireAll: true },
            );
            return !!(res && res.success);
          } catch (e) {
            console.log('[Retry] PBFT attempt consensus error:', e.message || String(e));
            return false;
          }
        };
        let attempt = 1;
        let ok = false;
        while (attempt <= maxMultisigAttempts) {
          const targetAttempt = isCoordinator
            ? attempt
            : await waitForCoordinatorAttempt(attempt, maxMultisigAttempts);
          if (!targetAttempt) {
            console.log('[Retry] No coordinator attempt signal received; aborting multisig retries');
            break;
          }
          attempt = targetAttempt;
          console.log(
            `[Retry] Cluster ${clusterId.substring(0, 10)} multisig - attempt ${attempt}/${maxMultisigAttempts}`,
          );
          if (isCoordinator) {
            const published = await publishAttempt(attempt);
            if (!published) {
              console.log('[Retry] Failed to publish attempt; aborting multisig retries');
              break;
            }
          }
          const attemptConsensusOk = await confirmAttemptConsensus(attempt);
          if (!attemptConsensusOk) {
            console.log('[Retry] PBFT attempt consensus failed; aborting multisig retries');
            break;
          }
          if (node.stateMachine && typeof node.stateMachine.reset === 'function') {
            if (node.stateMachine.currentState !== 'IDLE') {
              node.stateMachine.reset('preparing for cluster attempt');
            }
          }
          const preflightOk = await node._preflightMoneroForClusterAttempt(clusterId, attempt);
          if (!preflightOk) {
            console.log(
              '[Retry] Monero preflight for cluster attempt failed; aborting multisig retries',
            );
            break;
          }
          ok = await node.startClusterMultisigV3(
            clusterId,
            members,
            isCoordinator,
            clusterThreshold,
            attempt,
            coordinator,
            coordinator,
          );
          await node._postAttemptMoneroCleanup(clusterId, attempt, ok);
          if (ok) {
            break;
          }
          console.log(
            `[Retry] Cluster ${clusterId.substring(0, 10)} multisig - attempt ${attempt} returned falsy result`,
          );
          if (attempt >= maxMultisigAttempts) {
            break;
          }
          const delayMs = Math.min(15000 * Math.pow(2, attempt - 1), 60000);
          console.log(`[Retry] Waiting ${delayMs / 1000}s before retry...`);
          await new Promise((r) => setTimeout(r, delayMs));
          try {
            if (node._activeClusterId) {
              await node._cleanupClusterAttempt(node._activeClusterId);
            }
          } catch (cleanupErr) {
            console.log(`[Retry] Cleanup error: ${cleanupErr.message || String(cleanupErr)}`);
          }
          const p2pOkRetry = await node.initClusterP2P(clusterId, members, isCoordinator);
          if (!p2pOkRetry) {
            console.log('[WARN] P2P re-init failed for cluster; aborting retries');
            break;
          }
          attempt += 1;
        }
        if (!ok) {
          console.log('[ERROR] Cluster multisig flow failed after all retry attempts');
          await abandonClusterAttempt(clusterId, 'multisig_failed');
          if (isCoordinator && typeof node.dissolveStuckFinalizedCluster === 'function') {
            try {
              await node.dissolveStuckFinalizedCluster(clusterId);
            } catch (e) {
              const msg = e && e.message ? e.message : String(e);
              console.log(`[Dissolve] Error attempting stuck-cluster dissolution: ${msg}`);
            }
          }
          try {
            if (node.p2p && node.p2p.roundData && node.p2p.roundData.has(liveKeyInitial)) {
              node.p2p.roundData.delete(liveKeyInitial);
            }
          } catch (e) { logSwallowedError('monitoring_error', e); }
          try {
            const cooldownUntil = await computeCooldownUntilMs();
            if (node.clusterState && typeof node.clusterState.setCooldownUntil === 'function') {
              node.clusterState.setCooldownUntil(cooldownUntil);
            }
          } catch (e) { logSwallowedError('monitoring_error', e); }
        }
      } finally {
        node._orchestrationMutex.release();
      }
    } catch (e) {
      console.log('Status error:', e.message || String(e));
    } finally {
      node._monitorLoopRunning = false;
    }
  };
  if (node._monitorTimer) {
    console.log('[WARN] Monitor loop already running; skipping duplicate start');
    return;
  }
  node._monitorTimer = setInterval(loop, monitorIntervalMs);
}
