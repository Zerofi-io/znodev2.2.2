import { tryParseJSON } from '../json.js';
import { markRespondedSyncSession } from '../sync-session.js';
import { deriveRound } from '../pbft-utils.js';
import { ethers } from 'ethers';
import {
  MULTISIG_SYNC_ROUND,
  REQUIRED_WITHDRAWAL_SIGNATURES,
  WITHDRAWAL_ROUND_V2,
  WITHDRAWAL_SYNC_WINDOW_MS,
  WITHDRAWAL_MAX_ROUND_ATTEMPTS,
  WITHDRAWAL_POSTSUBMIT_SYNC_ENABLED,
  WITHDRAWAL_POSTSUBMIT_SYNC_TIMEOUT_MS,
  WITHDRAWAL_POSTSUBMIT_SYNC_MAX_ATTEMPTS,
} from '../config.js';
import { withMoneroLock } from '../../monero-lock.js';
import { scheduleWithdrawalStateSave } from '../pending-store.js';
import metrics from '../../../core/metrics.js';

const MULTISIG_READY_WAIT_MS = Number(process.env.MULTISIG_READY_WAIT_MS || 30000);
const MULTISIG_SYNC_QUORUM_GRACE_MS = Number(process.env.MULTISIG_SYNC_QUORUM_GRACE_MS || 4000);

const MULTISIG_DIGEST_WAIT_MS = Number(process.env.MULTISIG_DIGEST_WAIT_MS || 15000);
const MULTISIG_INFO_INLINE_MAX = 900000;
const MULTISIG_INFO_CHUNK_SIZE = 180000;
const MULTISIG_INFO_MAX_CHUNKS = 64;

function deriveInfoChunkRound(syncSessionLc, index) {
  return deriveRound('multisig-sync-chunk', `${syncSessionLc}:${index}`);
}

function splitInfoIntoChunks(info) {
  const s = typeof info === 'string' ? info : '';
  if (!s) return [];
  const out = [];
  for (let i = 0; i < s.length; i += MULTISIG_INFO_CHUNK_SIZE) {
    out.push(s.slice(i, i + MULTISIG_INFO_CHUNK_SIZE));
  }
  return out;
}

function normalizeReadyWaitMs(windowMs) {
  const raw = Number.isFinite(MULTISIG_READY_WAIT_MS) ? Math.floor(MULTISIG_READY_WAIT_MS) : 0;
  if (raw > 0) return raw;
  const fallback = Number.isFinite(windowMs) ? Math.floor(windowMs) : 0;
  return fallback > 0 ? fallback : 12000;
}

function computeSyncDigest(entries) {
  if (!Array.isArray(entries) || entries.length === 0) return null;
  try {
    const parts = [];
    for (const item of entries) {
      if (!item || item.length !== 2) continue;
      const sender = item[0] ? String(item[0]).toLowerCase() : '';
      const info = typeof item[1] === 'string' ? item[1] : '';
      if (!sender || !info) continue;
      const h = ethers.keccak256(ethers.toUtf8Bytes(info));
      parts.push(`${sender}:${h}`);
    }
    if (parts.length === 0) return null;
    return ethers.keccak256(ethers.toUtf8Bytes(parts.join('|')));
  } catch {
    return null;
  }
}

function parseReadyPayload(data, expectedMultisigAddress, expectedSyncDigest) {
  if (!data || data.type !== 'multisig-sync-ready') return null;
  const needed = data.multisigImportNeeded;
  if (needed === true) return null;
  if (expectedMultisigAddress) {
    const m = typeof data.multisigAddress === 'string' ? data.multisigAddress : '';
    if (!m || m !== expectedMultisigAddress) return null;
  }
  if (expectedSyncDigest) {
    const d = typeof data.syncDigest === 'string' ? data.syncDigest : '';
    if (!d || d !== expectedSyncDigest) return null;
  }
  return { ok: true };
}
async function getMultisigAddress(node) {
  if (!node || !node.monero || typeof node.monero.call !== 'function') return null;
  const cached = node._moneroMultisigAddress;
  if (typeof cached === 'string' && cached) return cached;
  let addr = null;
  try {
    const r = await node.monero.call('get_address', { account_index: 0 });
    if (r && typeof r.address === 'string' && r.address) addr = r.address;
  } catch {}
  if (!addr) {
    try {
      const r = await node.monero.call('get_address', {});
      if (r && typeof r.address === 'string' && r.address) addr = r.address;
    } catch {}
  }
  if (addr) node._moneroMultisigAddress = addr;
  return addr || null;
}

export async function runMultisigInfoSync(node, syncSession, syncRound, opts = {}) {
  if (!node || !node.monero || !node.p2p || !node._activeClusterId || !Array.isArray(node._clusterMembers) || node._clusterMembers.length === 0) {
    return { success: false, reason: 'no_cluster_or_monero' };
  }
  if (!syncSession) {
    return { success: false, reason: 'no_sync_session' };
  }
  const clusterId = node._activeClusterId;
  const sessionId = node._sessionId || 'bridge';
  const totalMembers = node._clusterMembers.length;
  const minSigners = REQUIRED_WITHDRAWAL_SIGNATURES;
  const windowMs = WITHDRAWAL_SYNC_WINDOW_MS;
  const readyWaitMs = normalizeReadyWaitMs(windowMs);
  const waitForReadySigners = !(opts && typeof opts === 'object' && opts.waitForReadySigners === false);
  const syncSessionLc = syncSession.toLowerCase();
  let lockKey = syncSessionLc;
  const syncIdx = lockKey.indexOf(':sync:');
  if (syncIdx > 0) lockKey = lockKey.slice(0, syncIdx);
  const existingLock = node._withdrawalMultisigLock;
  if (existingLock && existingLock !== lockKey) {
    metrics.incrementCounter('monero_multisig_sync_lock_active_total', 1);
    return {
      success: false,
      reason: 'withdrawal_lock_active',
      importedOutputs: 0,
      responderCount: 0,
      totalMembers,
      responders: [],
      readyCount: 0,
      readySigners: [],
    };
  }
  const lockAcquired = !existingLock;
  if (lockAcquired) node._withdrawalMultisigLock = lockKey;
  try {
    metrics.incrementCounter('monero_multisig_sync_attempt_total', 1);
    if (totalMembers < minSigners) {
      console.log(`[MultisigSync] Cluster too small: session=${syncSessionLc.slice(0, 18)} total=${totalMembers} required=${minSigners}`);
      return {
        success: false,
        reason: 'cluster_too_small',
        importedOutputs: 0,
        responderCount: 1,
        totalMembers,
        responders: [node.wallet?.address?.toLowerCase()].filter(Boolean),
        readyCount: 0,
        readySigners: [],
      };
    }

    let localInfo;
    let multisigAddress = null;
    try {
      const r0 = await withMoneroLock(node, { key: lockKey, op: 'multisig_sync_export' }, async () => {
        const info = await node.monero.exportMultisigInfo();
        const addr = await getMultisigAddress(node);
        return { info, addr };
      });
      localInfo = r0.info;
      multisigAddress = r0.addr;
    } catch (e) {
      metrics.incrementCounter('monero_multisig_sync_export_failed_total', 1);
      const msg = e && e.message ? e.message : String(e);
      console.log(`[MultisigSync] export_multisig_info failed: session=${syncSessionLc.slice(0, 18)} err=${msg}`);
      return {
        success: false,
        reason: 'export_failed',
        importedOutputs: 0,
        responderCount: 0,
        totalMembers,
        responders: [],
        readyCount: 0,
        readySigners: [],
      };
    }

    const selfAddress = node.wallet && node.wallet.address ? node.wallet.address.toLowerCase() : null;
    const responders = new Set();
    if (selfAddress) responders.add(selfAddress);

    const senderRaw = node.wallet && node.wallet.address ? node.wallet.address : null;
    const localInfoStr = typeof localInfo === 'string' ? localInfo : '';
    metrics.setGauge('monero_multisig_export_info_chars', localInfoStr.length);
    if (!localInfoStr) {
      metrics.incrementCounter('monero_multisig_sync_export_empty_total', 1);
      console.log(`[MultisigSync] export_multisig_info empty: session=${syncSessionLc.slice(0, 18)}`);
      return {
        success: false,
        reason: 'export_empty',
        importedOutputs: 0,
        responderCount: 0,
        totalMembers,
        responders: [],
        readyCount: 0,
        readySigners: [],
      };
    }

    const useInlineInfo = localInfoStr.length <= MULTISIG_INFO_INLINE_MAX;
    const infoHash = useInlineInfo ? null : ethers.keccak256(ethers.toUtf8Bytes(localInfoStr));
    const chunks = useInlineInfo ? [] : splitInfoIntoChunks(localInfoStr);
    metrics.setGauge('monero_multisig_export_chunk_count', chunks.length);
    if (!useInlineInfo && (!infoHash || chunks.length === 0)) {
      metrics.incrementCounter('monero_multisig_sync_chunk_prepare_failed_total', 1);
      console.log(`[MultisigSync] Chunk prepare failed: session=${syncSessionLc.slice(0, 18)} infoHashOk=${!!infoHash} chunkCount=${chunks.length}`);
      return {
        success: false,
        reason: 'chunk_prepare_failed',
        importedOutputs: 0,
        responderCount: 0,
        totalMembers,
        responders: [],
        readyCount: 0,
        readySigners: [],
      };
    }
    if (chunks.length > MULTISIG_INFO_MAX_CHUNKS) {
      metrics.incrementCounter('monero_multisig_sync_info_too_large_total', 1);
      console.log(`[MultisigSync] multisig_info_too_large: session=${syncSessionLc.slice(0, 18)} chunkCount=${chunks.length}`);
      return {
        success: false,
        reason: 'multisig_info_too_large',
        importedOutputs: 0,
        responderCount: 0,
        totalMembers,
        responders: [],
        readyCount: 0,
        readySigners: [],
      };
    }

    const requestObj = {
      type: 'multisig-sync-request',
      clusterId,
      syncSession: syncSessionLc,
      sender: senderRaw,
      multisigAddress,
    };
    if (useInlineInfo) {
      requestObj.info = localInfoStr;
    } else {
      requestObj.infoHash = infoHash;
      requestObj.infoChunkCount = chunks.length;
      requestObj.infoChunkSize = MULTISIG_INFO_CHUNK_SIZE;
      requestObj.infoLen = localInfoStr.length;
    }
    const requestPayload = JSON.stringify(requestObj);

    const isMaintenance = syncSessionLc.startsWith('maintenance:');
    const useV2Only = WITHDRAWAL_ROUND_V2 && !isMaintenance;
    const mainRound = useV2Only
      ? 0
      : Number.isFinite(syncRound) && syncRound > 0
        ? Math.floor(syncRound)
        : MULTISIG_SYNC_ROUND;
    const v2Round = WITHDRAWAL_ROUND_V2 ? deriveRound('multisig-sync', syncSessionLc) : 0;
    const roundsToPoll = [];
    const broadcastRounds = [];
    const pushRound = (arr, r) => {
      if (!r || !Number.isFinite(r) || r <= 0 || arr.includes(r)) return;
      arr.push(r);
    };
    pushRound(roundsToPoll, mainRound);
    pushRound(broadcastRounds, mainRound);
    pushRound(roundsToPoll, v2Round);
    pushRound(broadcastRounds, v2Round);

    if (waitForReadySigners && !isMaintenance) {
      console.log(
        `[MultisigSync] Starting sync: session=${syncSessionLc.slice(0, 18)} rounds=${[mainRound, v2Round].filter(Boolean).join(',')} required=${minSigners} members=${totalMembers}`,
      );
    }

    for (const round of broadcastRounds) {
      try {
        await node.p2p.broadcastRoundData(clusterId, sessionId, round, requestPayload);
      } catch (e) {
        const msg = e && e.message ? e.message : String(e);
        if (msg.includes('round payload overwrite rejected')) {
          metrics.incrementCounter('monero_multisig_sync_broadcast_overwrite_rejected_total', 1);
          console.log(
            `[MultisigSync] Sync request broadcast overwrite rejected: session=${syncSessionLc.slice(0, 18)} round=${round}`,
          );
          if (round === mainRound) {
            return {
              success: false,
              reason: msg,
              importedOutputs: 0,
              responderCount: responders.size,
              totalMembers,
              responders: Array.from(responders),
              readyCount: 0,
              readySigners: [],
            };
          }
          continue;
        }
        metrics.incrementCounter('monero_multisig_sync_broadcast_failed_total', 1);
        if (round === mainRound) {
          return {
            success: false,
            reason: `sync_broadcast_failed:${msg}`,
            importedOutputs: 0,
            responderCount: responders.size,
            totalMembers,
            responders: Array.from(responders),
            readyCount: 0,
            readySigners: [],
          };
        }
        console.log(
          `[MultisigSync] Failed to broadcast sync request: session=${syncSessionLc.slice(0, 18)} round=${round} err=${msg}`,
        );
      }
    }

    if (!useInlineInfo) {
      for (let i = 0; i < chunks.length; i += 1) {
        const round = deriveInfoChunkRound(syncSessionLc, i);
        const chunkPayload = JSON.stringify({
          type: 'multisig-sync-chunk',
          clusterId,
          syncSession: syncSessionLc,
          sender: senderRaw,
          multisigAddress,
          infoHash,
          index: i,
          chunk: chunks[i],
        });
        try {
          await node.p2p.broadcastRoundData(clusterId, sessionId, round, chunkPayload);
        } catch (e) {
          const msg = e && e.message ? e.message : String(e);
          if (msg.includes('round payload overwrite rejected')) {
            continue;
          }
          return {
            success: false,
            reason: `sync_broadcast_failed:${msg}`,
            importedOutputs: 0,
            responderCount: responders.size,
            totalMembers,
            responders: Array.from(responders),
            readyCount: 0,
            readySigners: [],
          };
        }
      }
    }
    markRespondedSyncSession(node, syncSessionLc);
    const seenInfosBySender = new Map();
    const chunkMetaBySender = new Map();
    if (selfAddress && localInfo) {
      seenInfosBySender.set(selfAddress, localInfo);
    }

    const memberSet = new Set(node._clusterMembers.map((a) => String(a || '').toLowerCase()));
    let quorumLogged = false;
    let quorumAt = null;
    const start = Date.now();
    while (true) {
      const elapsed = Date.now() - start;
      try {
        for (const round of roundsToPoll) {
          let roundData = null;
          try {
            roundData = await node.p2p.getRoundData(clusterId, sessionId, round);
          } catch {
            continue;
          }
          const entries = roundData && typeof roundData === 'object' ? Object.entries(roundData) : [];
          for (const [sender, raw] of entries) {
            const senderLc = sender ? String(sender).toLowerCase() : '';
            if (!senderLc || !memberSet.has(senderLc)) continue;
            const data = tryParseJSON(raw, `multisig-sync-info from ${senderLc}`);
            if (!data) continue;
            if (!data.syncSession || String(data.syncSession).toLowerCase() != syncSessionLc) continue;
            if (multisigAddress) {
              const m = typeof data.multisigAddress === 'string' ? data.multisigAddress : '';
              if (!m || m !== multisigAddress) continue;
            }
            if (data.type !== 'multisig-sync-response' && data.type !== 'multisig-sync-request') continue;
            if (typeof data.info === 'string' && data.info.length > 0) {
              if (!seenInfosBySender.has(senderLc)) {
                responders.add(senderLc);
                seenInfosBySender.set(senderLc, data.info);
              }
              continue;
            }
            const h = typeof data.infoHash === 'string' ? data.infoHash : '';
            const cRaw = Number(data.infoChunkCount);
            const c = Number.isFinite(cRaw) && cRaw > 0 ? Math.floor(cRaw) : 0;
            if (h && c > 0 && c <= MULTISIG_INFO_MAX_CHUNKS) {
              if (!chunkMetaBySender.has(senderLc)) {
                chunkMetaBySender.set(senderLc, { infoHash: h, infoChunkCount: c, firstSeen: Date.now(), lastTry: 0 });
              }
            } else if (h && c > MULTISIG_INFO_MAX_CHUNKS) {
              console.log(`[MultisigSync] Ignoring multisig info chunks from ${senderLc} with count=${c}`);
            }
          }
        }
        if (chunkMetaBySender.size > 0) {
          const pending = [];
          let maxChunks = 0;
          for (const [addr, meta] of chunkMetaBySender) {
            if (seenInfosBySender.has(addr)) continue;
            const c = meta && typeof meta.infoChunkCount === 'number' ? meta.infoChunkCount : 0;
            if (!c) continue;
            pending.push(addr);
            if (c > maxChunks) maxChunks = c;
          }
          if (pending.length > 0 && maxChunks > 0) {
            const roundDataByIndex = new Map();
            const getChunkRoundData = async (i) => {
              if (roundDataByIndex.has(i)) return roundDataByIndex.get(i);
              let rd = null;
              try {
                rd = await node.p2p.getRoundData(clusterId, sessionId, deriveInfoChunkRound(syncSessionLc, i));
              } catch {}
              roundDataByIndex.set(i, rd);
              return rd;
            };
            const staleMs = windowMs > 0 ? Math.max(1000, Math.min(15000, Math.floor(windowMs / 2))) : 15000;
            for (const senderLc of pending) {
              const meta = chunkMetaBySender.get(senderLc);
              const expectedHash = meta && typeof meta.infoHash === 'string' ? meta.infoHash : '';
              const count = meta && typeof meta.infoChunkCount === 'number' ? meta.infoChunkCount : 0;
              const firstSeen = meta && typeof meta.firstSeen === 'number' ? meta.firstSeen : 0;
              if (!expectedHash || !count) continue;
              const nowTs = Date.now();
              if (firstSeen && nowTs - firstSeen > staleMs) {
                chunkMetaBySender.delete(senderLc);
                continue;
              }
              const lastTry = meta && typeof meta.lastTry === 'number' ? meta.lastTry : 0;
              if (lastTry && nowTs - lastTry < 1000) continue;
              meta.lastTry = nowTs;
              const chunks = new Array(count);
              let ok = true;
              for (let i = 0; i < count; i += 1) {
                const rd = await getChunkRoundData(i);
                const raw = rd && typeof rd === 'object' ? rd[senderLc] : null;
                if (typeof raw !== 'string' || raw.length === 0) {
                  ok = false;
                  break;
                }
                const d = tryParseJSON(raw, `multisig-sync-chunk from ${senderLc}`);
                if (!d || d.type !== 'multisig-sync-chunk') {
                  ok = false;
                  break;
                }
                if (!d.syncSession || String(d.syncSession).toLowerCase() != syncSessionLc) {
                  ok = false;
                  break;
                }
                if (multisigAddress) {
                  const m = typeof d.multisigAddress === 'string' ? d.multisigAddress : '';
                  if (!m || m !== multisigAddress) {
                    ok = false;
                    break;
                  }
                }
                const idx = Number(d.index);
                if (!Number.isFinite(idx) || idx !== i) {
                  ok = false;
                  break;
                }
                const h = typeof d.infoHash === 'string' ? d.infoHash : '';
                if (!h || h !== expectedHash) {
                  ok = false;
                  break;
                }
                const chunk = typeof d.chunk === 'string' ? d.chunk : '';
                if (!chunk) {
                  ok = false;
                  break;
                }
                chunks[i] = chunk;
              }
              if (!ok) continue;
              const info = chunks.join('');
              let actualHash;
              try {
                actualHash = ethers.keccak256(ethers.toUtf8Bytes(info));
              } catch {
                actualHash = '';
              }
              if (!actualHash || actualHash !== expectedHash) continue;
              responders.add(senderLc);
              seenInfosBySender.set(senderLc, info);
            }
          }
        }
      } catch (e) {
        console.log('[MultisigSync] Error polling payloads:', e.message || String(e));
      }

      const responderCount = responders.size;
      if (!quorumLogged && responderCount >= minSigners) {
        console.log(`[MultisigSync] Quorum reached: ${responderCount}/${minSigners} responders for session ${syncSessionLc.slice(0, 18)}`);
        quorumLogged = true;
      }
      if (responderCount >= minSigners && quorumAt === null) quorumAt = Date.now();
      if (responderCount >= totalMembers || elapsed >= windowMs || (quorumAt !== null && Date.now() - quorumAt >= MULTISIG_SYNC_QUORUM_GRACE_MS)) {
        if (elapsed >= windowMs) {
          console.log(`[MultisigSync] Window expired after ${Math.round(elapsed / 1000)}s: ${responderCount}/${minSigners} responders`);
        }
        break;
      }
      await new Promise((resolve) => setTimeout(resolve, 500));
    }

    const responderCount = responders.size;
    if (responderCount < minSigners) {
      metrics.incrementCounter('monero_multisig_sync_not_enough_participants_total', 1);
      const membersArr = Array.isArray(node._clusterMembers)
        ? node._clusterMembers.map((a) => String(a || '').toLowerCase()).filter(Boolean)
        : [];
      const responderArr = Array.from(responders).map((a) => (a ? String(a).toLowerCase() : '')).filter(Boolean);
      responderArr.sort();
      const missingArr = membersArr.filter((a) => a && !responders.has(a));
      missingArr.sort();
      console.log(
        `[MultisigSync] Not enough participants: session=${syncSessionLc.slice(0, 18)} rounds=${[mainRound, v2Round].filter(Boolean).join(',')} responders=${responderCount}/${minSigners} respondersList=${responderArr.join(',')} missingList=${missingArr.join(',')}`,
      );
      return {
        success: false,
        reason: 'not_enough_participants',
        importedOutputs: 0,
        responderCount,
        totalMembers,
        responders: responderArr,
        readyCount: 0,
        readySigners: [],
      };
    }

    const infoEntries = Array.from(seenInfosBySender.entries())
      .map(([k, v]) => [k ? String(k).toLowerCase() : '', typeof v === 'string' ? v : ''])
      .filter(([k, v]) => k && v)
      .sort(([a], [b]) => a.localeCompare(b));
    const syncDigest = computeSyncDigest(infoEntries);
    if (!syncDigest) {
      return {
        success: false,
        reason: 'sync_digest_failed',
        importedOutputs: 0,
        responderCount,
        totalMembers,
        responders: Array.from(responders),
        readyCount: 0,
        readySigners: [],
      };
    }
    const includedAddresses = infoEntries.map(([k]) => k).filter(Boolean);
    if (waitForReadySigners && !isMaintenance) {
      const digestRound = deriveRound('multisig-sync-digest', syncSessionLc);
      const digestPayload = JSON.stringify({
        type: 'multisig-sync-digest',
        clusterId,
        syncSession: syncSessionLc,
        syncDigest,
        includedAddresses,
        multisigAddress,
      });
      if (digestRound && Number.isFinite(digestRound) && digestRound > 0) {
        try {
          await node.p2p.broadcastRoundData(clusterId, sessionId, digestRound, digestPayload);
        } catch {}
      }
    }
    const infos = infoEntries.map(([, v]) => v);
    if (infos.length === 0) {
      console.log('[MultisigSync] No valid multisig info entries parsed from peer payloads');
      return {
        success: false,
        reason: 'no_valid_infos',
        importedOutputs: 0,
        responderCount,
        totalMembers,
        responders: Array.from(responders),
        readyCount: 0,
        readySigners: [],
      };
    }

    let importedOutputs = 0;
    let finalBalance = null;
    let balanceFetchFailed = false;
    try {
      const r1 = await withMoneroLock(node, { key: lockKey, op: 'multisig_sync_import' }, async () => {
        let imported = 0;
        const n = await node.monero.importMultisigInfo(infos);
        if (typeof n === 'number') imported = n;
        let bal = null;
        let balanceFetchFailed = false;
        try {
          bal = await node.monero.getBalance({ fresh: true });
        } catch (e) {
          balanceFetchFailed = true;
          console.log('[MultisigSync] Failed to get balance after multisig sync:', e.message || String(e));
        }
        return { importedOutputs: imported, finalBalance: bal, balanceFetchFailed };
      });
      importedOutputs = r1.importedOutputs;
      finalBalance = r1.finalBalance;
      balanceFetchFailed = !!r1.balanceFetchFailed;
      if (finalBalance && typeof finalBalance.multisigImportNeeded === 'boolean') {
        metrics.setGauge('monero_multisig_import_needed', finalBalance.multisigImportNeeded ? 1 : 0);
      }
    } catch (e) {
      console.log('[MultisigSync] import_multisig_info failed:', e.message || String(e));
      return {
        success: false,
        reason: 'import_failed',
        importedOutputs: 0,
        responderCount,
        totalMembers,
        responders: Array.from(responders),
        readyCount: 0,
        readySigners: [],
      };
    }

    if (balanceFetchFailed || !finalBalance) {
      console.log('[MultisigSync] Failed to confirm balance after multisig sync');
      return {
        success: false,
        reason: 'balance_check_failed_after_import',
        importedOutputs,
        responderCount,
        totalMembers,
        responders: Array.from(responders),
        readyCount: 0,
        readySigners: [],
      };
    }

    if (finalBalance && finalBalance.multisigImportNeeded) {
      console.log('[MultisigSync] multisig_import_needed still true after sync attempt');
      return {
        success: false,
        reason: 'multisig_import_needed_still_true',
        importedOutputs,
        responderCount,
        totalMembers,
        responders: Array.from(responders),
        readyCount: 0,
        readySigners: [],
      };
    }

    let finalSyncDigest = syncDigest;
    if (!waitForReadySigners && !isMaintenance) {
      const digestRound = deriveRound('multisig-sync-digest', syncSessionLc);
      const digestWaitStart = Date.now();
      let coordinatorDigest = null;
      let coordinatorIncludedAddresses = null;
      while (Date.now() - digestWaitStart < MULTISIG_DIGEST_WAIT_MS) {
        let roundData = null;
        try {
          roundData = await node.p2p.getRoundData(clusterId, sessionId, digestRound);
        } catch {
          roundData = null;
        }
        if (roundData && typeof roundData === 'object') {
          for (const [sender, raw] of Object.entries(roundData)) {
            const senderLc = sender ? String(sender).toLowerCase() : '';
            if (!senderLc || !memberSet.has(senderLc)) continue;
            const data = tryParseJSON(raw, `multisig-sync-digest from ${senderLc}`);
            if (!data || data.type !== 'multisig-sync-digest') continue;
            if (!data.syncSession || String(data.syncSession).toLowerCase() !== syncSessionLc) continue;
            if (multisigAddress && data.multisigAddress !== multisigAddress) continue;
            if (typeof data.syncDigest === 'string' && data.syncDigest && Array.isArray(data.includedAddresses)) {
              coordinatorDigest = data.syncDigest;
              coordinatorIncludedAddresses = data.includedAddresses.map((a) => String(a || '').toLowerCase()).filter(Boolean);
              break;
            }
          }
        }
        if (coordinatorDigest) break;
        await new Promise((resolve) => setTimeout(resolve, 500));
      }
      if (coordinatorDigest && coordinatorIncludedAddresses) {
        let hasAllInfo = true;
        for (const addr of coordinatorIncludedAddresses) {
          if (!seenInfosBySender.has(addr)) {
            hasAllInfo = false;
            break;
          }
        }
        if (hasAllInfo) {
          finalSyncDigest = coordinatorDigest;
        } else {
          return {
            success: false,
            reason: 'missing_included_info',
            importedOutputs,
            responderCount,
            totalMembers,
            responders: Array.from(responders),
            readyCount: 0,
            readySigners: [],
          };
        }
      } else {
        return {
          success: false,
          reason: 'coordinator_digest_timeout',
          importedOutputs,
          responderCount,
          totalMembers,
          responders: Array.from(responders),
          readyCount: 0,
          readySigners: [],
        };
      }
    }

    const readyRound = deriveRound('multisig-sync-ready', syncSessionLc);
    const readyPayload = JSON.stringify({
      type: 'multisig-sync-ready',
      clusterId,
      syncSession: syncSessionLc,
      sender: node.wallet && node.wallet.address ? node.wallet.address : null,
      multisigAddress,
      syncDigest: finalSyncDigest,
      multisigImportNeeded: false,
      importedOutputs,
    });

    if (readyRound && Number.isFinite(readyRound) && readyRound > 0) {
      try {
        await node.p2p.broadcastRoundData(clusterId, sessionId, readyRound, readyPayload);
      } catch {}
    }

    const readySigners = new Set();
    if (selfAddress) readySigners.add(selfAddress);
    let readySetForReturn = readySigners;

    if (waitForReadySigners && !isMaintenance) {
      const startReady = Date.now();
      while (Date.now() - startReady < readyWaitMs) {
        let roundData = null;
        try {
          roundData = await node.p2p.getRoundData(clusterId, sessionId, readyRound);
        } catch {
          roundData = null;
        }
        const entries = roundData && typeof roundData === 'object' ? Object.entries(roundData) : [];
        for (const [sender, raw] of entries) {
          const senderLc = sender ? String(sender).toLowerCase() : '';
          if (!senderLc || !memberSet.has(senderLc)) continue;
          const data = tryParseJSON(raw, `multisig-sync-ready from ${senderLc}`);
          if (!data) continue;
          if (!data.syncSession || String(data.syncSession).toLowerCase() != syncSessionLc) continue;
          const parsed = parseReadyPayload(data, multisigAddress, syncDigest);
          if (!parsed) continue;
          readySigners.add(senderLc);
        }
        let eligibleCount = 0;
        for (const a of readySigners) {
          if (responders.has(a)) eligibleCount += 1;
        }
        if (eligibleCount >= minSigners) break;
        await new Promise((resolve) => setTimeout(resolve, 500));
      }

      const eligibleReadySigners = new Set();
      for (const a of readySigners) {
        if (responders.has(a)) eligibleReadySigners.add(a);
      }
      readySetForReturn = eligibleReadySigners;

      if (readySetForReturn.size < minSigners) {
        metrics.incrementCounter('monero_multisig_sync_not_enough_ready_signers_total', 1);
        const responderArr = Array.from(responders).map((a) => (a ? String(a).toLowerCase() : '')).filter(Boolean);
        responderArr.sort();
        const readyArr = Array.from(readySetForReturn).map((a) => (a ? String(a).toLowerCase() : '')).filter(Boolean);
        readyArr.sort();
        console.log(
          `[MultisigSync] Not enough ready signers: session=${syncSessionLc.slice(0, 18)} ready=${readyArr.length}/${minSigners} responders=${responderCount}/${minSigners} readySigners=${readyArr.join(',')} respondersList=${responderArr.join(',')}`,
        );
        return {
          success: false,
          reason: 'not_enough_ready_signers',
          importedOutputs,
          responderCount,
          totalMembers,
          responders: responderArr,
          readyCount: readyArr.length,
          readySigners: readyArr,
        };
      }

    }

    if (waitForReadySigners && !isMaintenance) {
      console.log(
        `[MultisigSync] Sync ready: session=${syncSessionLc.slice(0, 18)} responders=${responderCount}/${minSigners} ready=${readySetForReturn.size}/${minSigners} importedOutputs=${importedOutputs} digest=${finalSyncDigest.slice(0, 12)}`,
      );
    }
    const currentEpoch =
      typeof node._moneroEpoch === 'number' && Number.isFinite(node._moneroEpoch)
        ? node._moneroEpoch + 1
        : 1;
    node._moneroEpoch = currentEpoch;
    metrics.incrementCounter('monero_multisig_sync_success_total', 1);
    return {
      success: true,
      importedOutputs,
      responderCount,
      totalMembers,
      responders: Array.from(responders),
      readyCount: readySetForReturn.size,
      readySigners: Array.from(readySetForReturn),
      syncDigest: finalSyncDigest,
      epoch: currentEpoch,
    };
  } finally {
    if (lockAcquired && node._withdrawalMultisigLock === lockKey) {
      node._withdrawalMultisigLock = null;
    }
  }
}

function normalizeAttempt(v) {
  const n = Number(v);
  if (!Number.isFinite(n) || n < 0) return 0;
  return Math.floor(n);
}

function makeSyncSessionId() {
  return `${Date.now().toString(36)}${Math.random().toString(36).slice(2, 10)}`;
}

function getOrCreateSyncSessionId(node, pending, attempt) {
  const a = normalizeAttempt(attempt);
  const key = String(a);
  if (!pending || typeof pending !== 'object') return makeSyncSessionId();
  if (!pending.multisigSyncSessionIds || typeof pending.multisigSyncSessionIds !== 'object') {
    pending.multisigSyncSessionIds = {};
  }
  const existing = pending.multisigSyncSessionIds[key];
  if (typeof existing === 'string' && existing) return String(existing).toLowerCase();
  const id = makeSyncSessionId().toLowerCase();
  pending.multisigSyncSessionIds[key] = id;
  scheduleWithdrawalStateSave(node);
  return id;
}

function makeSyncSession(keyLc, attempt, sessionId) {
  const a = normalizeAttempt(attempt);
  const base = `${keyLc}:sync:${a}`;
  const sid = sessionId ? String(sessionId).toLowerCase() : '';
  if (WITHDRAWAL_ROUND_V2 && sid) return `${base}:${sid}`;
  return base;
}

function parseAttemptFromSyncSession(keyLc, syncSession) {
  const s = typeof syncSession === 'string' ? syncSession.toLowerCase() : '';
  const prefix = `${keyLc}:sync:`;
  if (!s || !prefix || !s.startsWith(prefix)) return null;
  const rest = s.slice(prefix.length);
  const idx = rest.indexOf(':');
  const attemptStr = idx >= 0 ? rest.slice(0, idx) : rest;
  const n = Number(attemptStr);
  if (!Number.isFinite(n) || n < 0) return null;
  return Math.floor(n);
}

function deriveMultisigSyncAnnounceRound(keyLc, attempt) {
  const key = String(keyLc || '').toLowerCase();
  if (!key) return 0;
  const a = normalizeAttempt(attempt);
  return deriveRound('multisig-sync-announce', `${key}:${a}`);
}

function parseMultisigSyncAnnounce(keyLc, data) {
  if (!data || data.type !== 'multisig-sync-announce') return null;
  const key = String(keyLc || '').toLowerCase();
  if (!key) return null;
  const w = typeof data.withdrawalKey === 'string' ? data.withdrawalKey.toLowerCase() : '';
  if (w && w !== key) return null;
  const ss = typeof data.syncSession === 'string' ? data.syncSession.toLowerCase() : '';
  if (!ss) return null;
  const attempt = parseAttemptFromSyncSession(key, ss);
  if (attempt === null) return null;
  const sr = Number(data.syncRound);
  const syncRound = Number.isFinite(sr) && sr > 0 ? Math.floor(sr) : 0;
  return { attempt, syncSession: ss, syncRound };
}

async function broadcastMultisigSyncAnnounce(node, keyLc, attempt, syncSessionLc, syncRound) {
  if (!node || !node.p2p || !node._activeClusterId) return false;
  const clusterId = node._activeClusterId;
  const sessionId = node._sessionId || 'bridge';
  const key = String(keyLc || '').toLowerCase();
  const ss = typeof syncSessionLc === 'string' ? syncSessionLc.toLowerCase() : '';
  const a = normalizeAttempt(attempt);
  const round = deriveMultisigSyncAnnounceRound(key, a);
  if (!round || !ss) return false;
  const payload = JSON.stringify({
    type: 'multisig-sync-announce',
    clusterId,
    withdrawalKey: key,
    attempt: a,
    syncSession: ss,
    syncRound,
  });
  try {
    await node.p2p.broadcastRoundData(clusterId, sessionId, round, payload);
    return true;
  } catch (e) {
    const msg = e && e.message ? e.message : String(e);
    if (msg.includes('round payload overwrite rejected')) {
      metrics.incrementCounter('monero_multisig_sync_announce_overwrite_rejected_total', 1);
      return true;
    }
    metrics.incrementCounter('monero_multisig_sync_announce_broadcast_failed_total', 1);
    console.log(`[MultisigSync] Failed to broadcast announce: key=${key.slice(0, 18)} attempt=${a} err=${msg}`);
    return false;
  }
}

async function waitForMultisigSyncAnnounce(node, keyLc, coordinatorLc, maxAttempts, timeoutMs, minAttemptExclusive = -1) {
  if (!node || !node.p2p || !node._activeClusterId) return null;
  const key = String(keyLc || '').toLowerCase();
  if (!key) return null;
  const clusterId = node._activeClusterId;
  const sessionId = node._sessionId || 'bridge';
  const coord = coordinatorLc ? String(coordinatorLc).toLowerCase() : '';
  const max = Math.max(1, normalizeAttempt(maxAttempts));
  const waitMs = Number.isFinite(timeoutMs) && timeoutMs > 0 ? Math.floor(timeoutMs) : 30000;
  const minRaw = Number(minAttemptExclusive);
  const minExclusive = Number.isFinite(minRaw) ? Math.floor(minRaw) : -1;
  const start = Date.now();
  while (Date.now() - start < waitMs) {
    for (let i = max - 1; i >= 0; i -= 1) {
      if (i <= minExclusive) break;
      const round = deriveMultisigSyncAnnounceRound(key, i);
      if (!round) continue;
      let roundData = null;
      try {
        roundData = await node.p2p.getRoundData(clusterId, sessionId, round);
      } catch {
        roundData = null;
      }
      if (!roundData || typeof roundData !== 'object') continue;
      const entries = Object.entries(roundData);
      for (const [sender, raw] of entries) {
        const senderLc = sender ? String(sender).toLowerCase() : '';
        if (coord && senderLc !== coord) continue;
        const data = tryParseJSON(raw, `multisig-sync-announce from ${senderLc}`);
        const parsed = parseMultisigSyncAnnounce(key, data);
        if (!parsed) continue;
        if (parsed.attempt !== i) continue;
        return { ...parsed, sender: senderLc };
      }
    }
    await new Promise((r) => setTimeout(r, 500));
  }
  metrics.incrementCounter('monero_multisig_sync_announce_timeout_total', 1);
  return null;
}

async function discoverHighestSyncAttempt(node, keyLc) {
  const max = normalizeAttempt(WITHDRAWAL_MAX_ROUND_ATTEMPTS);
  if (!max || !node || !node.p2p || !node._activeClusterId) return -1;
  const key = String(keyLc || '').toLowerCase();
  const memberSet = new Set(node._clusterMembers.map((a) => String(a || '').toLowerCase()));
  const clusterId = node._activeClusterId;
  const sessionId = node._sessionId || 'bridge';

  if (WITHDRAWAL_ROUND_V2 && key && key !== 'maintenance') {
    let best = -1;
    for (let i = 0; i < max; i += 1) {
      const round = deriveMultisigSyncAnnounceRound(key, i);
      if (!round) continue;
      let roundData = null;
      try {
        roundData = await node.p2p.getRoundData(clusterId, sessionId, round);
      } catch {
        continue;
      }
      const entries = roundData && typeof roundData === 'object' ? Object.entries(roundData) : [];
      for (const [sender, raw] of entries) {
        const senderLc = sender ? String(sender).toLowerCase() : '';
        if (!senderLc || !memberSet.has(senderLc)) continue;
        const data = tryParseJSON(raw, `multisig-sync-announce-discover from ${senderLc}`);
        const parsed = parseMultisigSyncAnnounce(key, data);
        if (!parsed || parsed.attempt !== i) continue;
        best = i;
        break;
      }
    }
    return best;
  }

  const multisigAddress = await getMultisigAddress(node);
  let best = -1;
  for (let i = 0; i < max; i += 1) {
    const roundsToCheck = [MULTISIG_SYNC_ROUND + i];
    if (key === 'maintenance') {
      const ss = makeSyncSession(key, i, null);
      roundsToCheck.push(deriveRound('multisig-sync', ss));
    }
    const seenRounds = new Set();
    for (const round of roundsToCheck) {
      const n = Number(round);
      const r = Number.isFinite(n) && n > 0 ? Math.floor(n) : 0;
      if (!r || seenRounds.has(r)) continue;
      seenRounds.add(r);
      let roundData = null;
      try {
        roundData = await node.p2p.getRoundData(clusterId, sessionId, r);
      } catch {
        continue;
      }
      const entries = roundData && typeof roundData === 'object' ? Object.entries(roundData) : [];
      for (const [sender, raw] of entries) {
        const senderLc = sender ? String(sender).toLowerCase() : '';
        if (!senderLc || !memberSet.has(senderLc)) continue;
        const data = tryParseJSON(raw, `multisig-sync-discover from ${senderLc}`);
        if (!data || !data.syncSession) continue;
        if (data.type !== 'multisig-sync-request' && data.type !== 'multisig-sync-response') continue;
        if (multisigAddress) {
          const m = typeof data.multisigAddress === 'string' ? data.multisigAddress : '';
          if (!m || m !== multisigAddress) continue;
        }
        const a = parseAttemptFromSyncSession(key, data.syncSession);
        if (a === null) continue;
        if (a > best) best = a;
      }
    }
  }
  return best;
}

export async function runWithdrawalMultisigSync(node, withdrawalTxHash) {
  if (!node) return { success: false, reason: 'no_node' };
  const key = String(withdrawalTxHash || '').toLowerCase();
  if (!key) return { success: false, reason: 'no_tx_hash' };
  const pending = node._pendingWithdrawals && node._pendingWithdrawals.get(key);
  const localAttempt = pending && typeof pending.attempt !== 'undefined' ? normalizeAttempt(pending.attempt) : 0;
  const discoveredAttempt = await discoverHighestSyncAttempt(node, key);
  const maxAttempts = Math.max(1, normalizeAttempt(WITHDRAWAL_MAX_ROUND_ATTEMPTS));
  let syncResult = null;
  let chosenAttempt = null;
  const waitForReadySigners = pending ? !!pending.isCoordinator : true;
  const coordinator = pending && pending.coordinator ? String(pending.coordinator).toLowerCase() : null;
  if (WITHDRAWAL_ROUND_V2 && !waitForReadySigners) {
    const overallMs = Math.max(60000, Math.min(300000, WITHDRAWAL_SYNC_WINDOW_MS * maxAttempts));
    const startedAt = Date.now();
    let currentAnnounce = null;
    let currentAttempt = -1;
    let lastReason = '';
    while (Date.now() - startedAt < overallMs) {
      const remaining = overallMs - (Date.now() - startedAt);
      const announce = await waitForMultisigSyncAnnounce(
        node,
        key,
        coordinator,
        maxAttempts,
        Math.min(2000, remaining),
        currentAttempt,
      );
      if (announce && typeof announce.syncSession === 'string' && announce.syncSession) {
        currentAnnounce = announce;
        currentAttempt = announce.attempt;
        const prefix = `${key}:sync:${currentAttempt}:`;
        if (pending && typeof currentAnnounce.syncSession === 'string' && currentAnnounce.syncSession.startsWith(prefix)) {
          const sid = currentAnnounce.syncSession.slice(prefix.length);
          if (sid) {
            if (!pending.multisigSyncSessionIds || typeof pending.multisigSyncSessionIds !== 'object') {
              pending.multisigSyncSessionIds = {};
            }
            pending.multisigSyncSessionIds[String(currentAttempt)] = sid;
            scheduleWithdrawalStateSave(node);
          }
        }
      }
      if (!currentAnnounce || !Number.isFinite(currentAttempt) || currentAttempt < 0) {
        await new Promise((r) => setTimeout(r, 500));
        continue;
      }
      const syncRound = currentAnnounce.syncRound || (MULTISIG_SYNC_ROUND + currentAttempt);
      syncResult = await runMultisigInfoSync(node, currentAnnounce.syncSession, syncRound, { waitForReadySigners });
      if (syncResult && syncResult.success) {
        chosenAttempt = currentAttempt;
        break;
      }
      lastReason = syncResult && syncResult.reason ? String(syncResult.reason) : '';
      await new Promise((r) => setTimeout(r, 1000));
    }
    if (!syncResult || !syncResult.success) {
      return { success: false, reason: lastReason || 'multisig_sync_announce_wait_failed' };
    }
  } else {
    for (let attempt = Math.max(localAttempt, discoveredAttempt); attempt < maxAttempts; attempt += 1) {
      const persistedId =
        pending && pending.multisigSyncSessionIds && typeof pending.multisigSyncSessionIds === 'object'
          ? pending.multisigSyncSessionIds[String(attempt)]
          : null;
      const sessionId = persistedId
        ? String(persistedId).toLowerCase()
        : WITHDRAWAL_ROUND_V2 && waitForReadySigners
          ? getOrCreateSyncSessionId(node, pending, attempt)
          : null;
      let syncSession = makeSyncSession(key, attempt, sessionId);
      let syncRound = MULTISIG_SYNC_ROUND + attempt;
      if (WITHDRAWAL_ROUND_V2) {
        const announced = await broadcastMultisigSyncAnnounce(node, key, attempt, syncSession, syncRound);
        if (!announced) {
          return { success: false, reason: 'multisig_sync_announce_broadcast_failed' };
        }
        const canonical = await waitForMultisigSyncAnnounce(node, key, coordinator, attempt + 1, 2000, attempt - 1);
        if (canonical && typeof canonical.syncSession === 'string' && canonical.syncSession) {
          syncSession = canonical.syncSession;
          if (canonical.syncRound) syncRound = canonical.syncRound;
          const prefix = `${key}:sync:${attempt}:`;
          if (pending && syncSession.startsWith(prefix)) {
            const sid = syncSession.slice(prefix.length);
            if (sid) {
              if (!pending.multisigSyncSessionIds || typeof pending.multisigSyncSessionIds !== 'object') {
                pending.multisigSyncSessionIds = {};
              }
              pending.multisigSyncSessionIds[String(attempt)] = sid;
              scheduleWithdrawalStateSave(node);
            }
          }
        }
      }
      syncResult = await runMultisigInfoSync(node, syncSession, syncRound, { waitForReadySigners });
      if (syncResult && syncResult.success) {
        chosenAttempt = attempt;
        break;
      }
      const reason = syncResult && syncResult.reason ? String(syncResult.reason) : '';
      if (reason.includes('round payload overwrite rejected')) continue;
      if (reason === 'multisig_import_needed_still_true') {
        console.log(`[MultisigSync] multisig_import_needed still true after sync for ${key.slice(0, 18)}; retrying sync attempt ${attempt + 1}`);
        continue;
      }
      if (reason === 'balance_check_failed_after_import') {
        console.log(`[MultisigSync] Failed to confirm balance after import for ${key.slice(0, 18)}; retrying sync attempt ${attempt + 1}`);
        continue;
      }
      return { success: false, reason: reason || 'multisig_sync_failed' };
    }
  }
  if (!syncResult || !syncResult.success) {
    return { success: false, reason: 'multisig_sync_round_exhausted' };
  }
  if (pending && chosenAttempt !== null) {
    pending.attempt = chosenAttempt;
    pending.multisigSyncDigest = typeof syncResult.syncDigest === 'string' ? syncResult.syncDigest : null;
  }

  const readyArr = Array.isArray(syncResult.readySigners) ? syncResult.readySigners : [];
  const readySet = new Set(readyArr.map((a) => (a ? String(a).toLowerCase() : '')).filter(Boolean));
  const members = Array.isArray(node._clusterMembers)
    ? node._clusterMembers.map((a) => String(a || '').toLowerCase())
    : [];

  const responderArr = Array.isArray(syncResult.responders) ? syncResult.responders : [];
  const responderSet = new Set(responderArr.map((a) => (a ? String(a).toLowerCase() : '')).filter(Boolean));
  const candidateReady = [];
  for (const m of members) {
    if (readySet.has(m)) candidateReady.push(m);
  }
  const eligible = responderSet.size > 0
    ? candidateReady.filter((m) => responderSet.has(m))
    : candidateReady;
  const eligibleSet = new Set(eligible);
  const ordered = eligible.length >= candidateReady.length
    ? eligible
    : eligible.concat(candidateReady.filter((m) => !eligibleSet.has(m)));

  const staleAttempt = pending && typeof pending.staleSignersAttempt === 'number'
    ? normalizeAttempt(pending.staleSignersAttempt)
    : null;
  const staleArr = pending && Array.isArray(pending.staleSigners) ? pending.staleSigners : [];
  const staleSet = staleAttempt !== null && chosenAttempt !== null && staleAttempt === chosenAttempt - 1
    ? new Set(staleArr.map((a) => (a ? String(a).toLowerCase() : '')).filter(Boolean))
    : new Set();

  const filtered = staleSet.size > 0 ? ordered.filter((m) => !staleSet.has(m)) : ordered;
  const minSigners = REQUIRED_WITHDRAWAL_SIGNATURES;
  const base = filtered.length >= minSigners ? filtered : ordered;

  let signerSet = base;
  if (!waitForReadySigners) {
    const baseSet = new Set(base);
    const respondersOrdered = [];
    for (const m of members) {
      if (!m || baseSet.has(m)) continue;
      if (responderSet.has(m)) respondersOrdered.push(m);
    }
    for (const m of respondersOrdered) baseSet.add(m);
    const remainingMembers = members.filter((m) => m && !baseSet.has(m));
    signerSet = base.concat(respondersOrdered, remainingMembers);
  }

  const importedOutputs = syncResult.importedOutputs || 0;
  const responderCount = syncResult.responderCount || responderSet.size;
  const readyCount = syncResult.readyCount || readySet.size;
  const totalMembers = syncResult.totalMembers || members.length;

  if (!signerSet.length || signerSet.length < minSigners) {
    return {
      success: false,
      reason: 'not_enough_synced_signers',
      signerSet,
      importedOutputs,
      responderCount,
      readyCount,
      totalMembers,
    };
  }

  let signerSetHash = null;
  try {
    signerSetHash = ethers.keccak256(ethers.toUtf8Bytes(signerSet.join(',')));
  } catch {
    signerSetHash = null;
  }

  if (pending && waitForReadySigners && chosenAttempt !== null) {
    pending.signerSet = signerSet;
    pending.signerSetAttempt = chosenAttempt;
    pending.signerSetHash = signerSetHash;
    scheduleWithdrawalStateSave(node);
  }

  if (waitForReadySigners && chosenAttempt !== null && node.p2p && node._activeClusterId) {
    const clusterId = node._activeClusterId;
    const sessionId = node._sessionId || 'bridge';
    const r = deriveRound('withdrawal-signer-set', `${key}:${chosenAttempt}`);
    const syncDigest = pending && typeof pending.multisigSyncDigest === 'string'
      ? String(pending.multisigSyncDigest).toLowerCase()
      : null;
    const payload = JSON.stringify({
      type: 'withdrawal-signer-set',
      withdrawalTxHash: key,
      attempt: chosenAttempt,
      syncDigest,
      signerSet,
      signerSetHash,
    });
    if (r && Number.isFinite(r) && r > 0) {
      try {
        await node.p2p.broadcastRoundData(clusterId, sessionId, r, payload);
      } catch {}
    }
  }

  return {
    success: true,
    signerSet,
    signerSetHash,
    importedOutputs,
    responderCount,
    readyCount,
    totalMembers,
  };
}


function derivePostSubmitAnnounceRound(keyLc, postAttempt) {
  const key = String(keyLc || '').toLowerCase();
  if (!key) return 0;
  const n = Number(postAttempt);
  const a = Number.isFinite(n) && n >= 0 ? Math.floor(n) : 0;
  return deriveRound('multisig-sync-postsubmit-announce', `${key}:${a}`);
}

function parsePostSubmitAnnounce(keyLc, data) {
  if (!data || data.type !== 'multisig-sync-postsubmit-announce') return null;
  const key = String(keyLc || '').toLowerCase();
  if (!key) return null;
  const w = typeof data.withdrawalKey === 'string' ? data.withdrawalKey.toLowerCase() : '';
  if (w && w !== key) return null;
  const ss = typeof data.syncSession === 'string' ? data.syncSession.toLowerCase() : '';
  if (!ss) return null;
  const sr = Number(data.syncRound);
  const syncRound = Number.isFinite(sr) && sr > 0 ? Math.floor(sr) : 0;
  const at = Number(data.attempt);
  const attempt = Number.isFinite(at) && at >= 0 ? Math.floor(at) : 0;
  const pa0 = Number(data.postAttempt);
  const postAttempt = Number.isFinite(pa0) && pa0 >= 0 ? Math.floor(pa0) : 0;
  const signerSet = Array.isArray(data.signerSet)
    ? data.signerSet.map((a) => String(a || '').toLowerCase()).filter(Boolean)
    : null;
  const signerSetHash = typeof data.signerSetHash === 'string' ? String(data.signerSetHash).toLowerCase() : null;
  return { attempt, postAttempt, syncSession: ss, syncRound, signerSet, signerSetHash };
}

async function broadcastPostSubmitAnnounce(
  node,
  keyLc,
  attempt,
  postAttempt,
  syncSessionLc,
  syncRound,
  signerSet,
  signerSetHash,
) {
  if (!node || !node.p2p || !node._activeClusterId) return false;
  const clusterId = node._activeClusterId;
  const sessionId = node._sessionId || 'bridge';
  const key = String(keyLc || '').toLowerCase();
  if (!key) return false;
  const pa = Number(postAttempt);
  const post = Number.isFinite(pa) && pa >= 0 ? Math.floor(pa) : 0;
  const round = derivePostSubmitAnnounceRound(key, post);
  const ss = typeof syncSessionLc === 'string' ? syncSessionLc.toLowerCase() : '';
  if (!round || !ss) return false;
  const payload = JSON.stringify({
    type: 'multisig-sync-postsubmit-announce',
    clusterId,
    withdrawalKey: key,
    attempt,
    postAttempt: post,
    syncSession: ss,
    syncRound,
    signerSet: Array.isArray(signerSet) ? signerSet : null,
    signerSetHash: signerSetHash || null,
  });
  try {
    await node.p2p.broadcastRoundData(clusterId, sessionId, round, payload);
    return true;
  } catch (e) {
    const msg = e && e.message ? e.message : String(e);
    if (msg.includes('round payload overwrite rejected')) return true;
    return false;
  }
}

async function readPostSubmitAnnounce(node, keyLc, postAttempt, coordinatorLc) {
  if (!node || !node.p2p || !node._activeClusterId) return null;
  const key = String(keyLc || '').toLowerCase();
  if (!key) return null;
  const pa = Number(postAttempt);
  const post = Number.isFinite(pa) && pa >= 0 ? Math.floor(pa) : 0;
  const round = derivePostSubmitAnnounceRound(key, post);
  if (!round) return null;
  const clusterId = node._activeClusterId;
  const sessionId = node._sessionId || 'bridge';
  const coord = coordinatorLc ? String(coordinatorLc).toLowerCase() : '';
  let roundData = null;
  try {
    roundData = await node.p2p.getRoundData(clusterId, sessionId, round);
  } catch {
    roundData = null;
  }
  const entries = roundData && typeof roundData === 'object' ? Object.entries(roundData) : [];
  for (const [sender, raw] of entries) {
    const senderLc = sender ? String(sender).toLowerCase() : '';
    if (coord && senderLc !== coord) continue;
    const data = tryParseJSON(raw, `multisig-sync-postsubmit-announce from ${senderLc}`);
    const parsed = parsePostSubmitAnnounce(key, data);
    if (!parsed) continue;
    if (parsed.postAttempt !== post) continue;
    return { ...parsed, sender: senderLc };
  }
  return null;
}

export async function runPostSubmitMultisigSync(node, withdrawalTxHash) {
  if (!WITHDRAWAL_POSTSUBMIT_SYNC_ENABLED) return { success: true, skipped: true };
  if (!node) return { success: false, reason: 'no_node' };
  const key = String(withdrawalTxHash || '').toLowerCase();
  if (!key) return { success: false, reason: 'no_tx_hash' };
  if (!WITHDRAWAL_ROUND_V2) return { success: false, reason: 'postsubmit_requires_round_v2' };
  const pending = node._pendingWithdrawals && node._pendingWithdrawals.get(key);
  if (!pending || !pending.isCoordinator) return { success: false, reason: 'not_coordinator' };
  const attempt = typeof pending.attempt !== 'undefined' ? normalizeAttempt(pending.attempt) : 0;
  const signerSet = Array.isArray(pending.signerSet)
    ? pending.signerSet.map((a) => String(a || '').toLowerCase()).filter(Boolean)
    : null;
  const signerSetHash = typeof pending.signerSetHash === 'string' ? String(pending.signerSetHash).toLowerCase() : null;
  const maxTriesRaw = Number(WITHDRAWAL_POSTSUBMIT_SYNC_MAX_ATTEMPTS);
  const maxTries = Number.isFinite(maxTriesRaw) && maxTriesRaw > 0 ? Math.floor(maxTriesRaw) : 1;
  const timeoutRaw = Number(WITHDRAWAL_POSTSUBMIT_SYNC_TIMEOUT_MS);
  const overallMs = Number.isFinite(timeoutRaw) && timeoutRaw > 0 ? Math.floor(timeoutRaw) : 90000;
  const startedAt = Date.now();
  let postAttempt = typeof pending.postSubmitSyncAttempt === 'number' ? normalizeAttempt(pending.postSubmitSyncAttempt) : 0;
  const coordinator = pending.coordinator ? String(pending.coordinator).toLowerCase() : null;
  for (let i = 0; i < maxTries; i += 1) {
    if (Date.now() - startedAt > overallMs) break;
    const sid = makeSyncSessionId().toLowerCase();
    const syncSession = makeSyncSession(key, attempt, sid);
    const syncRound = MULTISIG_SYNC_ROUND + attempt;
    await broadcastPostSubmitAnnounce(node, key, attempt, postAttempt, syncSession, syncRound, signerSet, signerSetHash);
    const announced = await readPostSubmitAnnounce(node, key, postAttempt, coordinator);
    const ss = announced && announced.syncSession ? announced.syncSession : syncSession;
    const sr = announced && announced.syncRound ? announced.syncRound : syncRound;
    const res = await runMultisigInfoSync(node, ss, sr, { waitForReadySigners: true });
    if (res && res.success) {
      pending.postSubmitSyncAttempt = postAttempt;
      pending.postSubmitSyncSession = ss;
      pending.postSubmitSyncDigest = typeof res.syncDigest === 'string' ? res.syncDigest : null;
      pending.postSubmitSyncCompletedAt = Date.now();
      scheduleWithdrawalStateSave(node);
      return {
        success: true,
        attempt,
        postAttempt,
        syncSession: ss,
        importedOutputs: res.importedOutputs || 0,
        responderCount: res.responderCount || 0,
        readyCount: res.readyCount || 0,
      };
    }
    postAttempt += 1;
    pending.postSubmitSyncAttempt = postAttempt;
    scheduleWithdrawalStateSave(node);
  }
  return { success: false, reason: 'postsubmit_sync_failed', attempt, postAttempt };
}

export async function runMaintenanceMultisigSync(node) {
  if (!node) return { success: false, reason: 'no_node' };
  const key = 'maintenance';
  const discoveredAttempt = await discoverHighestSyncAttempt(node, key);
  const maxAttempts = Math.max(1, normalizeAttempt(WITHDRAWAL_MAX_ROUND_ATTEMPTS));
  let syncResult = null;
  const attemptsUsed = [];
  for (let attempt = Math.max(0, discoveredAttempt + 1); attempt < maxAttempts; attempt += 1) {
    const syncSession = makeSyncSession(key, attempt);
    const syncRound = deriveRound('multisig-sync', syncSession);
    attemptsUsed.push(attempt);
    syncResult = await runMultisigInfoSync(node, syncSession, syncRound, { waitForReadySigners: true });
    if (syncResult && syncResult.success) {
      return {
        success: true,
        attempt,
        syncSession,
        syncRound,
        attemptsUsed,
        importedOutputs: syncResult.importedOutputs || 0,
        responderCount: syncResult.responderCount || 0,
        readyCount: syncResult.readyCount || 0,
        totalMembers: syncResult.totalMembers || 0,
      };
    }
    const reason = syncResult && syncResult.reason ? String(syncResult.reason) : '';
    if (reason.includes('round payload overwrite rejected')) continue;
    if (reason === 'multisig_import_needed_still_true' || reason === 'balance_check_failed_after_import') continue;
    return { success: false, reason: reason || 'multisig_sync_failed', attempt, syncSession, syncRound, attemptsUsed };
  }
  const lastAttempt = attemptsUsed.length > 0 ? attemptsUsed[attemptsUsed.length - 1] : null;
  return { success: false, reason: 'multisig_sync_round_exhausted', attempt: lastAttempt, attemptsUsed };
}
