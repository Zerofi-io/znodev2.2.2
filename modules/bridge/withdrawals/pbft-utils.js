import { ethers } from 'ethers';
import {
  MULTISIG_SYNC_ROUND,
  WITHDRAWAL_MAX_ROUND_ATTEMPTS,
  WITHDRAWAL_PBFT_PHASE_V2,
  WITHDRAWAL_PBFT_PHASE_V2_TRY_MS,
  WITHDRAWAL_ROUND_CLEAR_DELAY_MS,
  MULTISIG_MAINTENANCE_CLEAR_DELAY_MS,
  WITHDRAWAL_ROUND_V2,
  WITHDRAWAL_POSTSUBMIT_SYNC_MAX_ATTEMPTS,
  WITHDRAWAL_SIGN_STEP_RESPONSE_ROUND,
  WITHDRAWAL_SIGNED_INFO_ROUND,
  WITHDRAWAL_UNSIGNED_INFO_ROUND,
} from './config.js';
export function getWithdrawalShortHash(txHash) {
  if (!txHash) return '';
  return String(txHash).slice(0, 18);
}
export function getPBFTPhases(prefix, key) {
  const k = String(key || '').toLowerCase();
  const legacy = `${prefix}-${getWithdrawalShortHash(k)}`;
  if (!WITHDRAWAL_PBFT_PHASE_V2) return { primary: legacy, fallback: null };
  const h = ethers.keccak256(ethers.toUtf8Bytes(k));
  return { primary: `${prefix}-${h}`, fallback: legacy };
}
export async function runConsensusWithFallback(node, phases, data, members, timeoutMs) {
  const clusterId = node._activeClusterId;
  const sessionId = node._sessionId || 'bridge';
  if (!phases || !phases.primary) return null;
  if (!phases.fallback) {
    return node.p2p.runConsensus(clusterId, sessionId, phases.primary, data, members, timeoutMs);
  }
  const tryMs =
    Number.isFinite(WITHDRAWAL_PBFT_PHASE_V2_TRY_MS) && WITHDRAWAL_PBFT_PHASE_V2_TRY_MS > 0
      ? Math.min(timeoutMs, WITHDRAWAL_PBFT_PHASE_V2_TRY_MS)
      : timeoutMs;
  let result = null;
  try {
    result = await node.p2p.runConsensus(clusterId, sessionId, phases.primary, data, members, tryMs);
  } catch {}
  if (result && result.success) return result;
  return node.p2p.runConsensus(clusterId, sessionId, phases.fallback, data, members, timeoutMs);
}
export function deriveRound(tag, key) {
  const input = `${String(tag || '')}:${String(key || '').toLowerCase()}`;
  let h;
  try {
    h = ethers.keccak256(ethers.toUtf8Bytes(input));
  } catch {
    return 0;
  }
  const n = BigInt(h);
  const base = 120000n;
  const span = 600000n;
  return Number(base + (n % span));
}
export function scheduleClearWithdrawalRounds(node, withdrawalKey) {
  if (!node || !node.p2p || !node._activeClusterId) return;
  const delayMs = WITHDRAWAL_ROUND_CLEAR_DELAY_MS;
  if (!Number.isFinite(delayMs) || delayMs <= 0) return;
  const key = String(withdrawalKey || '').toLowerCase();
  if (!key) return;
  const pending = node._pendingWithdrawals && node._pendingWithdrawals instanceof Map ? node._pendingWithdrawals.get(key) : null;
  const sessionIds =
    pending && pending.multisigSyncSessionIds && typeof pending.multisigSyncSessionIds === 'object'
      ? pending.multisigSyncSessionIds
      : null;
  const rounds = [];
  const pushRound = (r) => {
    if (!r || !Number.isFinite(r) || r <= 0 || rounds.includes(r)) return;
    rounds.push(r);
  };
  pushRound(WITHDRAWAL_SIGN_STEP_RESPONSE_ROUND);
  pushRound(WITHDRAWAL_UNSIGNED_INFO_ROUND);
  pushRound(WITHDRAWAL_SIGNED_INFO_ROUND);
  const maxAttempts = Number.isFinite(WITHDRAWAL_MAX_ROUND_ATTEMPTS) && WITHDRAWAL_MAX_ROUND_ATTEMPTS > 0
    ? Math.floor(WITHDRAWAL_MAX_ROUND_ATTEMPTS)
    : 1;
  for (let i = 0; i < maxAttempts; i += 1) {
    pushRound(MULTISIG_SYNC_ROUND + i);
    pushRound(deriveRound('withdrawal-unsigned-proposal', `${key}:${i}`));
    pushRound(deriveRound('withdrawal-batch-manifest', `${key}:${i}`));
    pushRound(deriveRound('withdrawal-signer-set', `${key}:${i}`));
    pushRound(deriveRound('multisig-sync-announce', `${key}:${i}`));
    const sid = sessionIds ? sessionIds[String(i)] : null;
    const syncSession = WITHDRAWAL_ROUND_V2 && sid ? `${key}:sync:${i}:${sid}` : `${key}:sync:${i}`;
    pushRound(deriveRound('multisig-sync', syncSession));
    pushRound(deriveRound('multisig-sync-ready', syncSession));
    pushRound(deriveRound('multisig-sync-digest', syncSession));
    for (let j = 0; j < 64; j += 1) pushRound(deriveRound('multisig-sync-chunk', `${syncSession}:${j}`));
  }
  const postMax =
    Number.isFinite(WITHDRAWAL_POSTSUBMIT_SYNC_MAX_ATTEMPTS) && WITHDRAWAL_POSTSUBMIT_SYNC_MAX_ATTEMPTS > 0
      ? Math.floor(WITHDRAWAL_POSTSUBMIT_SYNC_MAX_ATTEMPTS)
      : 0;
  for (let i = 0; i < postMax; i += 1) {
    pushRound(deriveRound('multisig-sync-postsubmit-announce', `${key}:${i}`));
  }
  if (WITHDRAWAL_ROUND_V2) {
    pushRound(deriveRound('withdrawal-sign-step-response', key));
    pushRound(deriveRound('withdrawal-signed-info', key));
    pushRound(deriveRound('multisig-sync', key));
  }
  if (rounds.length === 0) return;
  const clusterId = node._activeClusterId;
  const sessionId = node._sessionId || 'bridge';
  setTimeout(() => {
    try {
      node.p2p.clearClusterRounds(clusterId, sessionId, rounds);
    } catch {}
  }, delayMs);
}

export function scheduleClearMaintenanceMultisigSyncRounds(node, attempt) {
  if (!node || !node.p2p || !node._activeClusterId) return;
  const delayMs = MULTISIG_MAINTENANCE_CLEAR_DELAY_MS;
  if (!Number.isFinite(delayMs) || delayMs <= 0) return;
  const n = Number(attempt);
  const i = Number.isFinite(n) && n >= 0 ? Math.floor(n) : null;
  if (i === null) return;
  const syncSession = `maintenance:sync:${i}`;
  const rounds = [];
  const pushRound = (r) => {
    if (!r || !Number.isFinite(r) || r <= 0 || rounds.includes(r)) return;
    rounds.push(r);
  };
  pushRound(deriveRound('multisig-sync', syncSession));
  pushRound(deriveRound('multisig-sync-ready', syncSession));
  pushRound(deriveRound('multisig-sync-digest', syncSession));
  for (let j = 0; j < 64; j += 1) pushRound(deriveRound('multisig-sync-chunk', `${syncSession}:${j}`));
  if (rounds.length === 0) return;
  const clusterId = node._activeClusterId;
  const sessionId = node._sessionId || 'bridge';
  setTimeout(() => {
    try {
      node.p2p.clearClusterRounds(clusterId, sessionId, rounds);
    } catch {}
  }, delayMs);
}
