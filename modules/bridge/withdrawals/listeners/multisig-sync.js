import { tryParseJSON } from '../json.js';
import { hasRespondedSyncSession, markRespondedSyncSession } from '../sync-session.js';
import { runMaintenanceMultisigSync, runMultisigInfoSync } from '../signing/multisig-sync.js';
import { deriveRound, scheduleClearMaintenanceMultisigSyncRounds } from '../pbft-utils.js';
import { scheduleWithdrawalStateSave } from '../pending-store.js';
import metrics from '../../../core/metrics.js';
import { logSwallowedError } from '../../../utils/rate-limited-log.js';
import { MULTISIG_MAINTENANCE_INTERVAL_MS, MULTISIG_SYNC_ROUND, WITHDRAWAL_MAX_ROUND_ATTEMPTS, WITHDRAWAL_ROUND_V2 } from '../config.js';

export function setupMultisigSyncResponder(node) {
  if (!node || !node.p2p || node._multisigSyncResponderSetup) return;
  node._multisigSyncResponderSetup = true;
  const maintenanceIntervalMs = MULTISIG_MAINTENANCE_INTERVAL_MS;
  const refusalLogIntervalMs = 60000;
  const pollMultisigSync = async () => {
    if (!node._withdrawalMonitorRunning) return;
    const busyReason = node._moneroSigningInProgress
      ? 'signing_in_progress'
      : node._withdrawalMultisigLock
        ? 'withdrawal_lock_active'
        : null;
    if (!node._multisigSyncPendingQueue || !(node._multisigSyncPendingQueue instanceof Map)) {
      node._multisigSyncPendingQueue = new Map();
    }
    const now0 = Date.now();
    for (const [k, v] of node._multisigSyncPendingQueue) {
      const last = v && typeof v.lastSeenAt === 'number' ? v.lastSeenAt : 0;
      if (!last || now0 - last > 300000) node._multisigSyncPendingQueue.delete(k);
    }
    const refreshAt = typeof node._multisigSyncPendingRefreshAt === 'number' ? node._multisigSyncPendingRefreshAt : 0;
    const skipScan = !!(busyReason && node._multisigSyncPendingQueue.size > 0 && now0 < refreshAt);
    if (busyReason && node._multisigSyncPendingQueue.size > 0 && now0 >= refreshAt) {
      node._multisigSyncPendingRefreshAt = now0 + 2500;
    }
    let pollDelayMs = busyReason ? 3000 : 1500;
    if (busyReason && node._multisigSyncPendingQueue.size > 0) pollDelayMs = 250;
    try {
      const clusterId = node._activeClusterId;
      const sessionId = node._sessionId || 'bridge';
      const max = Number.isFinite(WITHDRAWAL_MAX_ROUND_ATTEMPTS) && WITHDRAWAL_MAX_ROUND_ATTEMPTS > 0
        ? Math.floor(WITHDRAWAL_MAX_ROUND_ATTEMPTS)
        : 16;
      let sessionKey = null;
      let requestRound = null;
      let requestSender = null;
      const maybeLogRefusal = (reason, k, round, senderLc) => {
        if (!reason || !k || !round) return;
        if (refusalLogIntervalMs <= 0) return;
        if (!node._multisigSyncRefusalLog || !(node._multisigSyncRefusalLog instanceof Map)) {
          node._multisigSyncRefusalLog = new Map();
        }
        const now = Date.now();
        const mapKey = `${String(reason)}:${String(k)}:${String(round)}`;
        const lastAt = node._multisigSyncRefusalLog.get(mapKey) || 0;
        if (now - lastAt < refusalLogIntervalMs) return;
        node._multisigSyncRefusalLog.set(mapKey, now);
        if (node._multisigSyncRefusalLog.size > 500) {
          const cutoff = now - 3600000;
          for (const [rk, ts] of node._multisigSyncRefusalLog) {
            if (typeof ts === 'number' && ts < cutoff) node._multisigSyncRefusalLog.delete(rk);
          }
        }
        metrics.incrementCounter('monero_multisig_sync_responder_refusal_total', 1, { reason: String(reason) });
        const from = senderLc ? String(senderLc).toLowerCase() : '';
        const lock = node._withdrawalMultisigLock ? String(node._withdrawalMultisigLock) : '';
        const lockShort = lock ? lock.slice(0, 18) : '';
        const extra = lockShort && reason === 'withdrawal_lock_active' ? ` lock=${lockShort}` : '';
        console.log(
          `[MultisigSync] Refusing sync request: session=${String(k).slice(0, 18)} round=${round} from=${from ? from.slice(0, 10) : 'unknown'} reason=${reason}${extra}`,
        );
      };
      const enqueuePending = (k, round, senderLc) => {
        const s0 = typeof k === 'string' ? k.toLowerCase() : '';
        if (!s0) return;
        const marker = ':sync:';
        const idx = s0.lastIndexOf(marker);
        if (idx <= 0) return;
        const withdrawalKey = s0.slice(0, idx);
        const rawAttempt = s0.slice(idx + marker.length);
        const attemptPart = rawAttempt.split(':')[0];
        const n = Number(attemptPart);
        const attempt = Number.isFinite(n) && n >= 0 ? Math.floor(n) : null;
        if (attempt === null) return;
        const prev = node._multisigSyncPendingQueue.get(withdrawalKey);
        const prevAttempt = prev && typeof prev.attempt === 'number' ? prev.attempt : -1;
        if (prevAttempt > attempt) {
          prev.lastSeenAt = now0;
          node._multisigSyncPendingQueue.set(withdrawalKey, prev);
          return;
        }
        const sender = senderLc ? String(senderLc).toLowerCase() : '';
        node._multisigSyncPendingQueue.set(withdrawalKey, {
          sessionKey: s0,
          requestRound: round,
          sender,
          attempt,
          lastSeenAt: now0,
          lastAttemptAt: prev && typeof prev.lastAttemptAt === 'number' ? prev.lastAttemptAt : 0,
        });
        if (node._multisigSyncPendingQueue.size > 20) {
          let oldestKey = null;
          let oldestAt = Infinity;
          for (const [wk, ev] of node._multisigSyncPendingQueue) {
            const t = ev && typeof ev.lastSeenAt === 'number' ? ev.lastSeenAt : 0;
            if (t && t < oldestAt) {
              oldestAt = t;
              oldestKey = wk;
            }
          }
          if (oldestKey) node._multisigSyncPendingQueue.delete(oldestKey);
        }
      };
      if (!skipScan && node.wallet && node.wallet.address && node.monero) {
        const self = String(node.wallet.address).toLowerCase();
        const memberSet = new Set(
          Array.isArray(node._clusterMembers)
            ? node._clusterMembers.map((a) => String(a || '').toLowerCase())
            : [],
        );
        if (WITHDRAWAL_ROUND_V2) {
          const maintenanceMinAttempt = busyReason ? Math.max(0, max - 4) : 0;
          for (let attempt = max - 1; attempt >= maintenanceMinAttempt; attempt -= 1) {
            if (sessionKey) break;
            const ss = `maintenance:sync:${attempt}`;
            const round = deriveRound('multisig-sync', ss);
            if (!round || !Number.isFinite(round) || round <= 0) continue;
            let roundData = null;
            try {
              roundData = await node.p2p.getRoundData(clusterId, sessionId, round);
            } catch {
              continue;
            }
            if (roundData && typeof roundData === 'object') {
              const selfRaw = roundData[self];
              if (typeof selfRaw === 'string' && selfRaw.length > 0) {
                const selfData = tryParseJSON(selfRaw, `multisig-sync maintenance self from ${self}`);
                if (
                  selfData &&
                  (selfData.type === 'multisig-sync-request' || selfData.type === 'multisig-sync-response') &&
                  selfData.syncSession
                ) {
                  const sk = String(selfData.syncSession).toLowerCase();
                  if (sk && !hasRespondedSyncSession(node, sk)) {
                    markRespondedSyncSession(node, sk);
                  }
                }
              }
            }
            const entries = roundData && typeof roundData === 'object' ? Object.entries(roundData) : [];
            if (entries.length === 0) continue;
            for (const [sender, raw] of entries) {
              const senderLc = sender ? String(sender).toLowerCase() : '';
              if (!senderLc || senderLc === self || (memberSet.size > 0 && !memberSet.has(senderLc))) continue;
              const data = tryParseJSON(raw, `multisig-sync maintenance from ${senderLc}`);
              if (!data || !data.syncSession) continue;
              const k = String(data.syncSession).toLowerCase();
              if (k !== ss) continue;
              if (data.type !== 'multisig-sync-request' && data.type !== 'multisig-sync-response') continue;
              if (hasRespondedSyncSession(node, k)) {
                maybeLogRefusal('already_responded', k, round, senderLc);
                continue;
              }
              sessionKey = k;
              requestRound = round;
              requestSender = senderLc;
              break;
            }
          }
          const candidateKeys = [];
          const keyLimit = busyReason ? 3 : 6;
          const primaryKey = node._withdrawalBatchInProgressKey ? String(node._withdrawalBatchInProgressKey).toLowerCase() : '';
          if (primaryKey) candidateKeys.push(primaryKey);
          const lockKey = node._withdrawalMultisigLock && node._withdrawalMultisigLock !== 'maintenance'
            ? String(node._withdrawalMultisigLock).toLowerCase()
            : '';
          if (lockKey && !candidateKeys.includes(lockKey)) candidateKeys.push(lockKey);
          if (node._pendingWithdrawals && node._pendingWithdrawals instanceof Map) {
            for (const k of node._pendingWithdrawals.keys()) {
              const kc = k ? String(k).toLowerCase() : '';
              if (!kc || candidateKeys.includes(kc)) continue;
              candidateKeys.push(kc);
              if (candidateKeys.length >= keyLimit) break;
            }
          }
          for (const keyLc of candidateKeys) {
            if (sessionKey) break;
            const pending = node._pendingWithdrawals && node._pendingWithdrawals instanceof Map
              ? node._pendingWithdrawals.get(keyLc)
              : null;
            const coordinator = pending && pending.coordinator ? String(pending.coordinator).toLowerCase() : '';
            const minAttempt = busyReason ? Math.max(0, max - 4) : 0;
            for (let attempt = max - 1; attempt >= minAttempt; attempt -= 1) {
              const announceRound = deriveRound('multisig-sync-announce', `${keyLc}:${attempt}`);
              let roundData = null;
              try {
                roundData = await node.p2p.getRoundData(clusterId, sessionId, announceRound);
              } catch {
                continue;
              }
              const entries = roundData && typeof roundData === 'object' ? Object.entries(roundData) : [];
              if (entries.length === 0) continue;
              for (const [sender, raw] of entries) {
                const senderLc = sender ? String(sender).toLowerCase() : '';
                if (!senderLc || senderLc === self || (memberSet.size > 0 && !memberSet.has(senderLc))) continue;
                if (coordinator && senderLc !== coordinator) continue;
                const data = tryParseJSON(raw, `multisig-sync announce from ${senderLc}`);
                if (
                  !data ||
                  data.type !== 'multisig-sync-announce' ||
                  !data.syncSession ||
                  !data.withdrawalKey ||
                  String(data.withdrawalKey).toLowerCase() !== keyLc
                ) continue;
                const k = String(data.syncSession).toLowerCase();
                if (!k) continue;
                const marker = ':sync:';
                const idx = k.lastIndexOf(marker);
                if (idx <= 0) continue;
                const withdrawalKey = k.slice(0, idx);
                if (withdrawalKey !== keyLc) continue;
                const rawAttempt = k.slice(idx + marker.length);
                const attemptPart = rawAttempt.split(':')[0];
                const n = Number(attemptPart);
                const parsedAttempt = Number.isFinite(n) && n >= 0 ? Math.floor(n) : null;
                if (parsedAttempt === null || parsedAttempt !== attempt) continue;
                const sr = Number(data.syncRound);
                const round = Number.isFinite(sr) && sr > 0 ? Math.floor(sr) : MULTISIG_SYNC_ROUND + attempt;
                if (hasRespondedSyncSession(node, k)) {
                  maybeLogRefusal('already_responded', k, round, senderLc);
                  continue;
                }
                sessionKey = k;
                requestRound = round;
                requestSender = senderLc;
                break;
              }
              if (sessionKey) break;
            }
            if (!sessionKey) {
              const postBaseRaw = pending && typeof pending.postSubmitSyncAttempt === 'number' ? Number(pending.postSubmitSyncAttempt) : 0;
              const postBase = Number.isFinite(postBaseRaw) && postBaseRaw >= 0 ? Math.floor(postBaseRaw) : 0;
              const postLimit = busyReason ? 2 : 4;
              for (let pa = postBase + postLimit - 1; pa >= postBase; pa -= 1) {
                const postRound = deriveRound('multisig-sync-postsubmit-announce', `${keyLc}:${pa}`);
                let roundData = null;
                try {
                  roundData = await node.p2p.getRoundData(clusterId, sessionId, postRound);
                } catch {
                  continue;
                }
                const entries = roundData && typeof roundData === 'object' ? Object.entries(roundData) : [];
                if (entries.length === 0) continue;
                for (const [sender, raw] of entries) {
                  const senderLc = sender ? String(sender).toLowerCase() : '';
                  if (!senderLc || senderLc === self || (memberSet.size > 0 && !memberSet.has(senderLc))) continue;
                  if (coordinator && senderLc !== coordinator) continue;
                  const data = tryParseJSON(raw, `multisig-sync postsubmit announce from ${senderLc}`);
                  if (
                    !data ||
                    data.type !== 'multisig-sync-postsubmit-announce' ||
                    !data.syncSession ||
                    !data.withdrawalKey ||
                    String(data.withdrawalKey).toLowerCase() !== keyLc
                  ) continue;
                  if (Array.isArray(data.signerSet) && data.signerSet.length > 0) {
                    const set = new Set(data.signerSet.map((a) => String(a || '').toLowerCase()).filter(Boolean));
                    if (!set.has(self)) continue;
                  }
                  const k = String(data.syncSession).toLowerCase();
                  if (!k) continue;
                  const marker = ':sync:';
                  const idx = k.lastIndexOf(marker);
                  if (idx <= 0) continue;
                  const withdrawalKey = k.slice(0, idx);
                  if (withdrawalKey !== keyLc) continue;
                  const rawAttempt = k.slice(idx + marker.length);
                  const attemptPart = rawAttempt.split(':')[0];
                  const n = Number(attemptPart);
                  const parsedAttempt = Number.isFinite(n) && n >= 0 ? Math.floor(n) : null;
                  const sr = Number(data.syncRound);
                  const round = Number.isFinite(sr) && sr > 0
                    ? Math.floor(sr)
                    : parsedAttempt !== null
                      ? MULTISIG_SYNC_ROUND + parsedAttempt
                      : MULTISIG_SYNC_ROUND;
                  if (hasRespondedSyncSession(node, k)) {
                    maybeLogRefusal('already_responded', k, round, senderLc);
                    continue;
                  }
                  sessionKey = k;
                  requestRound = round;
                  requestSender = senderLc;
                  break;
                }
                if (sessionKey) break;
              }
            }
            if (sessionKey) break;
          }
        } else {
          const roundsToPoll = [];
          for (let i = max - 1; i >= 0; i -= 1) roundsToPoll.push(MULTISIG_SYNC_ROUND + i);
          const limit = busyReason ? Math.min(4, roundsToPoll.length) : roundsToPoll.length;
          for (let i = 0; i < limit; i += 1) {
            const round = roundsToPoll[i];
            let roundData = null;
            try {
              roundData = await node.p2p.getRoundData(clusterId, sessionId, round);
            } catch {
              continue;
            }
            if (roundData && typeof roundData === 'object') {
              const selfRaw = roundData[self];
              if (typeof selfRaw === 'string' && selfRaw.length > 0) {
                const selfData = tryParseJSON(selfRaw, `multisig-sync self from ${self}`);
                if (
                  selfData &&
                  (selfData.type === 'multisig-sync-request' || selfData.type === 'multisig-sync-response') &&
                  selfData.syncSession
                ) {
                  const sk = String(selfData.syncSession).toLowerCase();
                  if (sk && !hasRespondedSyncSession(node, sk)) {
                    markRespondedSyncSession(node, sk);
                  }
                }
              }
            }
            const entries = roundData && typeof roundData === 'object' ? Object.entries(roundData) : [];
            if (entries.length === 0) continue;
            for (const [sender, raw] of entries) {
              const senderLc = sender ? String(sender).toLowerCase() : '';
              if (!senderLc || senderLc === self || (memberSet.size > 0 && !memberSet.has(senderLc))) continue;
              const data = tryParseJSON(raw, `multisig-sync from ${senderLc}`);
              if (!data || data.type !== 'multisig-sync-request' || !data.syncSession) continue;
              const k = String(data.syncSession).toLowerCase();
              if (!k) continue;
              if (hasRespondedSyncSession(node, k)) {
                maybeLogRefusal('already_responded', k, round, senderLc);
                continue;
              }
              sessionKey = k;
              requestRound = round;
              requestSender = senderLc;
              break;
            }
            if (sessionKey) break;
          }
        }
      }
      if (!busyReason && (!sessionKey || !requestRound) && node._multisigSyncPendingQueue.size > 0) {
        let best = null;
        for (const [wk, ev] of node._multisigSyncPendingQueue) {
          if (!ev || !ev.sessionKey || !ev.requestRound) {
            node._multisigSyncPendingQueue.delete(wk);
            continue;
          }
          const sk = String(ev.sessionKey).toLowerCase();
          if (!sk || hasRespondedSyncSession(node, sk)) {
            node._multisigSyncPendingQueue.delete(wk);
            continue;
          }
          const lastAttemptAt = typeof ev.lastAttemptAt === 'number' ? ev.lastAttemptAt : 0;
          if (lastAttemptAt && now0 - lastAttemptAt < 1000) continue;
          if (!best) {
            best = { wk, ...ev };
            continue;
          }
          const a1 = typeof ev.attempt === 'number' ? ev.attempt : -1;
          const a2 = typeof best.attempt === 'number' ? best.attempt : -1;
          if (a1 > a2) {
            best = { wk, ...ev };
            continue;
          }
          if (a1 < a2) continue;
          const t1 = typeof ev.lastSeenAt === 'number' ? ev.lastSeenAt : 0;
          const t2 = typeof best.lastSeenAt === 'number' ? best.lastSeenAt : 0;
          if (t1 > t2) best = { wk, ...ev };
        }
        if (best && best.sessionKey && best.requestRound) {
          sessionKey = String(best.sessionKey).toLowerCase();
          requestRound = best.requestRound;
          requestSender = best.sender ? String(best.sender).toLowerCase() : null;
          const cur = node._multisigSyncPendingQueue.get(best.wk);
          if (cur && typeof cur === 'object') {
            cur.lastAttemptAt = now0;
            node._multisigSyncPendingQueue.set(best.wk, cur);
          }
        }
      }
      if (sessionKey && requestRound) {
        const from = requestSender ? String(requestSender).toLowerCase() : '';
        if (busyReason) {
          enqueuePending(sessionKey, requestRound, from);
          maybeLogRefusal(busyReason, sessionKey, requestRound, from);
        } else {
          console.log(
            `[MultisigSync] Detected sync: session=${sessionKey.slice(0, 18)} round=${requestRound} from=${from ? from.slice(0, 10) : 'unknown'}`,
          );
          const result = await runMultisigInfoSync(node, sessionKey, requestRound, { waitForReadySigners: false });
          if (result && result.success) {
            console.log(`[MultisigSync] Synced session ${sessionKey.slice(0, 18)} ready=${result.readyCount || 0}`);
            const marker = ':sync:';
            const idx = sessionKey.lastIndexOf(marker);
            const withdrawalKey = idx > 0 ? sessionKey.slice(0, idx) : '';
            if (withdrawalKey && node._multisigSyncPendingQueue && node._multisigSyncPendingQueue instanceof Map) {
              node._multisigSyncPendingQueue.delete(withdrawalKey);
            }
            if (idx > 0 && node._pendingWithdrawals) {
              const rawAttempt = sessionKey.slice(idx + marker.length);
              const attemptPart = rawAttempt.split(':')[0];
              const n = Number(attemptPart);
              const syncAttempt = Number.isFinite(n) && n >= 0 ? Math.floor(n) : null;
              const pending = syncAttempt !== null ? node._pendingWithdrawals.get(withdrawalKey) : null;
              if (pending) {
                const cur = typeof pending.attempt !== 'undefined' ? Number(pending.attempt) : 0;
                const curAttempt = Number.isFinite(cur) && cur >= 0 ? Math.floor(cur) : 0;
                if (syncAttempt >= curAttempt) {
                  pending.attempt = syncAttempt;
                  pending.multisigSyncDigest = typeof result.syncDigest === 'string' ? result.syncDigest : null;
                  const prefix = `${withdrawalKey}:sync:${syncAttempt}:`;
                  if (sessionKey.startsWith(prefix)) {
                    const sid = sessionKey.slice(prefix.length);
                    if (sid) {
                      if (!pending.multisigSyncSessionIds || typeof pending.multisigSyncSessionIds !== 'object') {
                        pending.multisigSyncSessionIds = {};
                      }
                      pending.multisigSyncSessionIds[String(syncAttempt)] = sid;
                    }
                  }
                  scheduleWithdrawalStateSave(node);
                }
              }
            }
          } else {
            const reason = result && result.reason ? String(result.reason) : 'unknown';
            console.log(
              `[MultisigSync] Failed to respond: session=${sessionKey.slice(0, 18)} round=${requestRound} reason=${reason}`,
            );
          }
        }
      } else if (!busyReason && !(node._multisigSyncPendingQueue && node._multisigSyncPendingQueue.size > 0)) {
        const now = Date.now();
        const nextAt = typeof node._multisigMaintenanceNextAt === 'number' ? node._multisigMaintenanceNextAt : 0;
        const shouldCheck = Number.isFinite(maintenanceIntervalMs) && maintenanceIntervalMs > 0 && now >= nextAt;
        if (shouldCheck && !node._multisigMaintenanceInFlight) {
          node._multisigMaintenanceNextAt = now + Math.floor(maintenanceIntervalMs);
          const hasPending = !!(node._pendingWithdrawals && node._pendingWithdrawals.size > 0);
          const hasBatch = !!(node._withdrawalBatchInProgress || node._withdrawalBatchInProgressKey);
          if (!hasPending && !hasBatch && node.monero) {
            node._multisigMaintenanceInFlight = true;
            (async () => {
              let ran = false;
              let res = null;
              let lockAcquired = false;
              try {
                const pendingNow = !!(node._pendingWithdrawals && node._pendingWithdrawals.size > 0);
                const batchNow = !!(node._withdrawalBatchInProgress || node._withdrawalBatchInProgressKey);
                if (pendingNow || batchNow || node._moneroSigningInProgress) return;
                if (node._withdrawalMultisigLock && node._withdrawalMultisigLock !== 'maintenance') return;
                if (!node._withdrawalMultisigLock) {
                  node._withdrawalMultisigLock = 'maintenance';
                  lockAcquired = true;
                }
                let bal = null;
                try {
                  bal = await node.monero.getBalance({ fresh: true });
                } catch {
                  bal = null;
                }
                if (!bal || !bal.multisigImportNeeded) return;
                ran = true;
                console.log('[MultisigSync] multisig_import_needed detected; running maintenance sync');
                res = await runMaintenanceMultisigSync(node);
                if (res && res.success) {
                  console.log(`[MultisigSync] Maintenance sync completed; importedOutputs=${res.importedOutputs || 0}`);
                } else {
                  console.log('[MultisigSync] Maintenance sync failed:', res && res.reason ? res.reason : 'unknown');
                }
              } finally {
                if (lockAcquired && node._withdrawalMultisigLock === 'maintenance') node._withdrawalMultisigLock = null;
                if (ran && res) {
                  const attempts = Array.isArray(res.attemptsUsed)
                    ? res.attemptsUsed
                    : typeof res.attempt !== 'undefined'
                      ? [res.attempt]
                      : [];
                  for (const a of attempts) {
                    try {
                      scheduleClearMaintenanceMultisigSyncRounds(node, a);
                    } catch {}
                  }
                }
                node._multisigMaintenanceInFlight = false;
              }
            })();
          }
        }
      }
    } catch (e) {
      logSwallowedError('multisig_sync_poll_error', e);
    }
    if (node._withdrawalMonitorRunning) setTimeout(pollMultisigSync, pollDelayMs);
  };
  pollMultisigSync();
}
