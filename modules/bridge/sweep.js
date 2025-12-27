import { saveBridgeState } from './bridge-state.js';
import { recordAuthorizedTxids } from '../utils/authorized-txids.js';
import { logSwallowedError } from '../utils/rate-limited-log.js';
import { withMoneroLock } from './monero-lock.js';
import {
  SWEEP_ENABLED,
  SWEEP_MIN_CONFIRMATIONS,
  SWEEP_MIN_AMOUNT_ATOMIC,
  SWEEP_MAX_INPUTS_PER_TX,
  SWEEP_DELAY_AFTER_WITHDRAWAL_MS,
  SWEEP_PRIORITY_BY_AMOUNT,
  SWEEP_SYNC_ROUND,
  REQUIRED_WITHDRAWAL_SIGNATURES,
  WITHDRAWAL_EPOCH_SECONDS,
} from './withdrawals/config.js';
const SWEEP_EPOCH_CUTOFF_MS = Number(process.env.SWEEP_EPOCH_CUTOFF_MS || 300000);
const SWEEP_SIGN_RESPONSE_CACHE_MAX = Number(process.env.SWEEP_SIGN_RESPONSE_CACHE_MAX || 5000);
export function isSweepEnabled() {
  return SWEEP_ENABLED;
}
export function isSweepSafe(node, skipLockCheck = false) {
  if (!node) return false;
  if (!node._clusterFinalized || !node._clusterFinalAddress) return false;
  if (!node.monero) return false;
  if (!skipLockCheck && node._withdrawalMultisigLock) return false;
  if (!skipLockCheck && node._sweepInProgress) return false;
  if (!skipLockCheck && node._moneroSigningInProgress) return false;
  if (node._pendingWithdrawals && node._pendingWithdrawals.size > 0) {
    for (const [, data] of node._pendingWithdrawals) {
      if (data && data.status === 'pending') return false;
      if (data && data.status === 'signing') return false;
    }
  }
  const epoch = node._withdrawalEpoch;
  if (epoch && typeof epoch.startSec === "number") {
    const epochEndSec =
      typeof epoch.endSec === 'number'
        ? epoch.endSec
        : epoch.startSec + (typeof epoch.durationSec === 'number' ? epoch.durationSec : WITHDRAWAL_EPOCH_SECONDS);
    const nowSec = Math.floor(Date.now() / 1000);
    const secUntilNextEpoch = Math.max(0, epochEndSec - nowSec);
    if (secUntilNextEpoch * 1000 < SWEEP_EPOCH_CUTOFF_MS) {
      console.log(`[Sweep] Too close to next epoch (${secUntilNextEpoch}s), skipping`);
      return false;
    }
  }
  return true;
}
function getUtxoKey(t) {
  if (t.keyImage) return t.keyImage;
  const txHash = t.txHash || '';
  const outputIndex = typeof t.outputIndex === 'number' ? t.outputIndex : (typeof t.subaddrIndex === 'number' ? t.subaddrIndex : 0);
  return `${txHash}:${outputIndex}`;
}
export async function getUnsweptDeposits(node, minConfirmations) {
  if (!node || !node.monero) return [];
  const conf =
    typeof minConfirmations === 'number' && Number.isFinite(minConfirmations) && minConfirmations > 0
      ? Math.floor(minConfirmations)
      : SWEEP_MIN_CONFIRMATIONS;
  let chainHeight = null;
  if (conf > 0) {
    try {
      chainHeight = await node.monero.getHeight();
    } catch (e) {
      console.log('[Sweep] Failed to get height:', e.message || String(e));
      chainHeight = null;
    }
  }
  let transfers;
  try {
    transfers = await node.monero.getIncomingTransfers('available');
  } catch (e) {
    console.log('[Sweep] Failed to get incoming transfers:', e.message || String(e));
    return [];
  }
  if (!Array.isArray(transfers) || transfers.length === 0) return [];
  node._sweptDeposits = node._sweptDeposits || new Set();
  const mainSubaddrIndex = 0;
  const unswept = [];
  for (const t of transfers) {
    if (t.spent) continue;
    if (!t.unlocked) continue;
    if (t.subaddrIndex === mainSubaddrIndex) continue;
    if (typeof t.amount !== 'number' && typeof t.amount !== 'bigint') continue;
    const bh = typeof t.blockHeight === 'number' && Number.isFinite(t.blockHeight) ? t.blockHeight : 0;
    if (conf > 0 && typeof chainHeight === 'number' && chainHeight > 0) {
      if (bh <= 0) continue;
      const confirmations = chainHeight - bh;
      if (confirmations < conf) continue;
    }
    const amt = typeof t.amount === 'bigint' ? t.amount : BigInt(t.amount);
    if (amt < SWEEP_MIN_AMOUNT_ATOMIC) continue;
    const key = getUtxoKey(t);
    if (node._sweptDeposits.has(key)) continue;
    const firstSeenAtMs = node._sweptDepositsMeta && node._sweptDepositsMeta.get(key)
      ? node._sweptDepositsMeta.get(key).firstSeenAtMs
      : null;
    unswept.push({
      amount: amt,
      txHash: t.txHash,
      subaddrIndex: t.subaddrIndex,
      outputIndex: t.outputIndex,
      keyImage: t.keyImage,
      blockHeight: bh,
      key,
      firstSeenAtMs: firstSeenAtMs || Date.now(),
    });
  }
  return unswept;
}
export function prioritizeByAmount(deposits, descending = true) {
  if (!Array.isArray(deposits)) return [];
  const sorted = [...deposits];
  sorted.sort((a, b) => {
    const amtA = typeof a.amount === 'bigint' ? a.amount : BigInt(a.amount || 0);
    const amtB = typeof b.amount === 'bigint' ? b.amount : BigInt(b.amount || 0);
    const cmpAmt = descending ? (amtB > amtA ? 1 : amtB < amtA ? -1 : 0) : (amtA > amtB ? 1 : amtA < amtB ? -1 : 0);
    if (cmpAmt !== 0) return cmpAmt;
    const seenA = typeof a.firstSeenAtMs === 'number' ? a.firstSeenAtMs : Number.MAX_SAFE_INTEGER;
    const seenB = typeof b.firstSeenAtMs === 'number' ? b.firstSeenAtMs : Number.MAX_SAFE_INTEGER;
    return seenA - seenB;
  });
  return sorted;
}
export function createSweepBatches(deposits, maxInputs) {
  if (!Array.isArray(deposits) || deposits.length === 0) return [];
  const max = typeof maxInputs === 'number' && maxInputs > 0 ? maxInputs : SWEEP_MAX_INPUTS_PER_TX;
  const batches = [];
  for (let i = 0; i < deposits.length; i += max) {
    batches.push(deposits.slice(i, i + max));
  }
  return batches;
}
export async function runSweepMultisigSync(node, sweepSession) {
  if (!node || !node.monero || !node.p2p || !node._activeClusterId) {
    return { success: false, reason: 'no_cluster_or_monero' };
  }
  if (!Array.isArray(node._clusterMembers) || node._clusterMembers.length === 0) {
    return { success: false, reason: 'no_cluster_members' };
  }
  const clusterId = node._activeClusterId;
  const sessionId = node._sessionId || 'bridge';
  const minSigners = REQUIRED_WITHDRAWAL_SIGNATURES;
  const windowMs = 15000;
  let localInfo;
  try {
    localInfo = await withMoneroLock(node, { key: `sweep:${sweepSession}`, op: 'sweep_sync_export' }, async () => {
      return await node.monero.exportMultisigInfo();
    });
  } catch (e) {
    console.log('[Sweep] export_multisig_info failed:', e.message || String(e));
    return { success: false, reason: 'export_failed' };
  }
  const selfAddress = node.wallet && node.wallet.address ? node.wallet.address.toLowerCase() : null;
  const responders = new Set();
  if (selfAddress) responders.add(selfAddress);
  const seenInfosBySender = new Map();
  if (selfAddress && localInfo) {
    seenInfosBySender.set(selfAddress, localInfo);
  }
  const requestPayload = JSON.stringify({
    type: 'sweep-sync-request',
    clusterId,
    sweepSession,
    info: localInfo,
    sender: selfAddress,
  });
  try {
    await node.p2p.broadcastRoundData(clusterId, sessionId, SWEEP_SYNC_ROUND, requestPayload);
  } catch (e) {
    console.log('[Sweep] Failed to broadcast sync request:', e.message || String(e));
  }
  const start = Date.now();
  while (Date.now() - start < windowMs) {
    try {
      const memberSet = new Set(node._clusterMembers.map((a) => String(a || '').toLowerCase()));
      const roundData = await node.p2p.getRoundData(clusterId, sessionId, SWEEP_SYNC_ROUND);
      const entries = roundData && typeof roundData === 'object' ? Object.entries(roundData) : [];
      for (const [sender, raw] of entries) {
        const senderLc = sender ? String(sender).toLowerCase() : '';
        if (!senderLc || !memberSet.has(senderLc)) continue;
        try {
          const data = typeof raw === 'string' ? JSON.parse(raw) : raw;
          if (!data || data.sweepSession !== sweepSession) continue;
          if ((data.type === 'sweep-sync-response' || data.type === 'sweep-sync-request') &&
              typeof data.info === 'string' && data.info.length > 0) {
            if (!seenInfosBySender.has(senderLc)) {
              responders.add(senderLc);
              seenInfosBySender.set(senderLc, data.info);
            }
          }
        } catch (e) { logSwallowedError('sweep_sync_entry_parse', e); }
      }
    } catch (e) { logSwallowedError('sweep_sync_poll_error', e); }
    if (responders.size >= minSigners) break;
    await new Promise((r) => setTimeout(r, 500));
  }
  if (responders.size < minSigners) {
    console.log(`[Sweep] Not enough responders: ${responders.size}/${minSigners}`);
    return { success: false, reason: 'not_enough_participants', responderCount: responders.size };
  }
  const infos = Array.from(seenInfosBySender.values());
  if (infos.length === 0) {
    return { success: false, reason: 'no_valid_infos' };
  }
  let importedOutputs = 0;
  try {
    const n = await withMoneroLock(node, { key: `sweep:${sweepSession}`, op: 'sweep_sync_import' }, async () => {
      return await node.monero.importMultisigInfo(infos);
    });
    if (typeof n === 'number') importedOutputs = n;
  } catch (e) {
    console.log('[Sweep] import_multisig_info failed:', e.message || String(e));
    return { success: false, reason: 'import_failed' };
  }
  const signerSet = [];
  const responderSet = new Set(Array.from(responders));
  for (const m of node._clusterMembers) {
    const mLc = String(m || '').toLowerCase();
    if (responderSet.has(mLc)) signerSet.push(mLc);
  }
  return { success: true, signerSet, importedOutputs, responderCount: responders.size };
}
export async function executeSweepBatch(node, batch, destinationAddress) {
  if (!node || !node.monero || !batch || batch.length === 0) {
    return { success: false, reason: 'invalid_args' };
  }
  if (!destinationAddress) {
    return { success: false, reason: 'no_destination' };
  }
  const subaddrIndices = [...new Set(batch.map((d) => d.subaddrIndex).filter((i) => typeof i === 'number'))];
  if (subaddrIndices.length === 0) {
    return { success: false, reason: 'no_subaddresses' };
  }
  const sweepSession = `sweep-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
  const lockKey = `sweep:${sweepSession}`;
  if (node._withdrawalMultisigLock) {
    return { success: false, reason: 'withdrawal_lock_active' };
  }
  if (node._moneroSigningInProgress) {
    return { success: false, reason: 'monero_busy' };
  }
  node._withdrawalMultisigLock = lockKey;
  node._sweepInProgress = true;
  try {
    if (!isSweepSafe(node, true)) {
      console.log('[Sweep] Sweep conditions became unsafe after acquiring lock');
      return { success: false, reason: 'conditions_unsafe' };
    }
    console.log(`[Sweep] Starting sweep of ${batch.length} deposits from ${subaddrIndices.length} subaddresses`);
    const syncResult = await runSweepMultisigSync(node, sweepSession);
    if (!syncResult || !syncResult.success) {
      console.log('[Sweep] Multisig sync failed:', syncResult?.reason || 'unknown');
      return { success: false, reason: syncResult?.reason || 'sync_failed' };
    }
    console.log(`[Sweep] Multisig sync completed, importedOutputs=${syncResult.importedOutputs}`);
    let sweepResult;
    try {
      sweepResult = await withMoneroLock(node, { key: lockKey, op: 'sweep_all' }, async () => {
        return await node.monero.sweepAll(destinationAddress, {
        subaddrIndices,
        doNotRelay: true,
        });
      });
    } catch (e) {
      console.log('[Sweep] sweep_all failed:', e.message || String(e));
      return { success: false, reason: 'sweep_call_failed', error: e.message };
    }
    if (!sweepResult || !sweepResult.txDataHex) {
      console.log('[Sweep] No transaction data returned from sweep');
      return { success: false, reason: 'no_tx_data' };
    }
    const signerSet = syncResult.signerSet || [];
    if (signerSet.length < REQUIRED_WITHDRAWAL_SIGNATURES) {
      console.log('[Sweep] Not enough signers for sweep');
      return { success: false, reason: 'not_enough_signers' };
    }
    let currentTxDataHex = sweepResult.txDataHex;
    const selfLc = node.wallet && node.wallet.address ? node.wallet.address.toLowerCase() : '';
    let signedSteps = 1;
    for (let i = 0; i < signerSet.length && signedSteps < REQUIRED_WITHDRAWAL_SIGNATURES; i++) {
      const signer = signerSet[i];
      if (!signer || signer === selfLc) continue;
      try {
        await node.p2p.broadcastRoundData(
          node._activeClusterId,
          node._sessionId || 'bridge',
          SWEEP_SYNC_ROUND + 1,
          JSON.stringify({
            type: 'sweep-sign-request',
            sweepSession,
            stepIndex: i,
            signer,
            txDataHex: currentTxDataHex,
          }),
        );
      } catch (e) { logSwallowedError('sweep_sign_request_broadcast', e); }
      const response = await waitForSweepSignResponse(node, sweepSession, i, signer, 20000);
      if (!response || !response.txDataHex) {
        console.log(`[Sweep] Sign step ${i} timeout or invalid response`);
        continue;
      }
      currentTxDataHex = response.txDataHex;
      signedSteps++;
    }
    if (signedSteps < REQUIRED_WITHDRAWAL_SIGNATURES) {
      console.log(`[Sweep] Not enough sign steps completed: ${signedSteps}/${REQUIRED_WITHDRAWAL_SIGNATURES}`);
      return { success: false, reason: 'signing_incomplete' };
    }
    console.log('[Sweep] Submitting sweep transaction...');
    let submitResult;
    try {
      submitResult = await withMoneroLock(node, { key: lockKey, op: 'sweep_submit' }, async () => {
        return await node.monero.call('submit_multisig', { tx_data_hex: currentTxDataHex }, 180000);
      });
    } catch (e) {
      console.log('[Sweep] submit_multisig failed:', e.message || String(e));
      return { success: false, reason: 'submit_failed', error: e.message };
    }
    const txHashes = submitResult?.tx_hash_list || [];
    if (Array.isArray(txHashes) && txHashes.length > 0) {
      recordAuthorizedTxids(node, txHashes);
      if (node.p2p && node._activeClusterId) {
        try {
          await node.p2p.broadcastRoundData(
            node._activeClusterId,
            node._sessionId || 'bridge',
            SWEEP_SYNC_ROUND + 3,
            JSON.stringify({ type: "sweep-txhashes", sweepSession, txHashList: txHashes }),
          );
        } catch (e) { console.log('[Sweep] Failed to broadcast txhashes:', e.message || String(e)); }
      }
    }
    console.log(`[Sweep] Sweep submitted successfully: ${txHashes.join(', ')}`);
    for (const dep of batch) {
      if (dep.key) {
        node._sweptDeposits = node._sweptDeposits || new Set();
        node._sweptDeposits.add(dep.key);
        node._sweptDepositsMeta = node._sweptDepositsMeta || new Map();
        node._sweptDepositsMeta.set(dep.key, { sweptAt: Date.now(), txHashes });
      }
    }
    node._lastSweepCompletedAt = Date.now();
    saveBridgeState(node);
    return { success: true, txHashes, sweepedCount: batch.length };
  } finally {
    node._sweepInProgress = false;
    if (node._withdrawalMultisigLock === lockKey) {
      node._withdrawalMultisigLock = null;
    }
  }
}
async function waitForSweepSignResponse(node, sweepSession, stepIndex, signer, timeoutMs) {
  const start = Date.now();
  const signerLc = signer.toLowerCase();
  while (Date.now() - start < timeoutMs) {
    try {
      const roundData = await node.p2p.getRoundData(
        node._activeClusterId,
        node._sessionId || 'bridge',
        SWEEP_SYNC_ROUND + 2,
      );
      if (roundData && typeof roundData === 'object') {
        for (const [sender, raw] of Object.entries(roundData)) {
          if (sender.toLowerCase() !== signerLc) continue;
          try {
            const data = typeof raw === 'string' ? JSON.parse(raw) : raw;
            if (data.type === 'sweep-sign-response' &&
                data.sweepSession === sweepSession &&
                data.stepIndex === stepIndex &&
                data.txDataHex) {
              return { txDataHex: data.txDataHex };
            }
          } catch (e) { logSwallowedError('sweep_sign_response_parse', e); }
        }
      }
    } catch (e) { logSwallowedError('sweep_sign_response_rounddata', e); }
    await new Promise((r) => setTimeout(r, 500));
  }
  return null;
}
export async function scheduleSweepAfterWithdrawals(node) {
  if (!isSweepEnabled()) return;
  if (!node || !node._clusterFinalAddress) return;
  const delayMs = SWEEP_DELAY_AFTER_WITHDRAWAL_MS;
  console.log(`[Sweep] Scheduling sweep check in ${delayMs / 1000}s`);
  setTimeout(async () => {
    try {
      await runScheduledSweep(node);
    } catch (e) {
      console.log('[Sweep] Scheduled sweep error:', e.message || String(e));
    }
  }, delayMs);
}
export async function runScheduledSweep(node) {
  if (!isSweepEnabled()) return;
  const lockKey = `sweep:scheduled-${Date.now()}`;
  if (node._withdrawalMultisigLock) {
    console.log('[Sweep] Lock active, skipping scheduled sweep');
    return;
  }
  if (node._moneroSigningInProgress) {
    console.log('[Sweep] Monero signing in progress, skipping scheduled sweep');
    return;
  }
  node._withdrawalMultisigLock = lockKey;
  try {
    if (!isSweepSafe(node, true)) {
      console.log('[Sweep] Sweep not safe at this time, skipping');
      return;
    }
    const unswept = await getUnsweptDeposits(node, SWEEP_MIN_CONFIRMATIONS);
    if (unswept.length === 0) {
      console.log('[Sweep] No deposits to sweep');
      return;
    }
    node._sweptDepositsMeta = node._sweptDepositsMeta || new Map();
    const now = Date.now();
    for (const dep of unswept) {
      if (!node._sweptDepositsMeta.has(dep.key)) {
        node._sweptDepositsMeta.set(dep.key, { firstSeenAtMs: now });
      }
    }
    const sorted = SWEEP_PRIORITY_BY_AMOUNT ? prioritizeByAmount(unswept, true) : unswept;
    const batches = createSweepBatches(sorted, SWEEP_MAX_INPUTS_PER_TX);
    console.log(`[Sweep] Found ${unswept.length} deposits to sweep in ${batches.length} batch(es)`);
    const destinationAddress = node._clusterFinalAddress;
    node._withdrawalMultisigLock = null;
    for (let i = 0; i < batches.length; i++) {
      if (node._withdrawalMultisigLock || node._moneroSigningInProgress) {
        console.log('[Sweep] Sweep interrupted by another operation, stopping');
        break;
      }
      const batch = batches[i];
      console.log(`[Sweep] Executing batch ${i + 1}/${batches.length} with ${batch.length} deposits`);
      const result = await executeSweepBatch(node, batch, destinationAddress);
      if (!result.success) {
        console.log(`[Sweep] Batch ${i + 1} failed: ${result.reason}`);
        break;
      }
      if (i < batches.length - 1) {
        await new Promise((r) => setTimeout(r, 5000));
      }
    }
  } finally {
    if (node._withdrawalMultisigLock === lockKey) {
      node._withdrawalMultisigLock = null;
    }
  }
}
export function setupSweepSignResponder(node) {
  if (!node || !node.p2p || node._sweepSignResponderSetup) return;
  node._sweepSignResponderSetup = true;
  const pollInterval = setInterval(async () => {
    if (!node._clusterFinalized || !node.monero) return;
    try {
      const roundData = await node.p2p.getRoundData(
        node._activeClusterId,
        node._sessionId || 'bridge',
        SWEEP_SYNC_ROUND + 1,
      );
      if (!roundData || typeof roundData !== 'object') return;
      const selfLc = node.wallet && node.wallet.address ? node.wallet.address.toLowerCase() : '';
      for (const [, raw] of Object.entries(roundData)) {
        try {
          const data = typeof raw === 'string' ? JSON.parse(raw) : raw;
          if (data.type !== 'sweep-sign-request') continue;
          if (!data.signer || data.signer.toLowerCase() !== selfLc) continue;
          if (!data.txDataHex || !data.sweepSession) continue;
          const responseKey = `${data.sweepSession}:${data.stepIndex}`;
          if (node._sweepSignResponses && node._sweepSignResponses.has(responseKey)) continue;
          const lockKey = `sweep:${data.sweepSession}`;
          const existingLock = node._withdrawalMultisigLock;
          if (existingLock && existingLock !== lockKey) continue;
          const lockAcquired = !existingLock;
          if (lockAcquired) node._withdrawalMultisigLock = lockKey;
                  node._sweepSignResponses = node._sweepSignResponses || new Set();
          node._sweepSignResponses.add(responseKey);
          const max = SWEEP_SIGN_RESPONSE_CACHE_MAX;
          if (Number.isFinite(max) && max > 0) {
            while (node._sweepSignResponses.size > max) {
              const oldest = node._sweepSignResponses.values().next().value;
              if (!oldest) break;
              node._sweepSignResponses.delete(oldest);
            }
          }
          try {
            const signed = await withMoneroLock(node, { key: lockKey, op: 'sweep_sign_step' }, async () => {
              return await node.monero.signMultisig(data.txDataHex);
            });
            if (signed && signed.txDataHex) {
              await node.p2p.broadcastRoundData(
                node._activeClusterId,
                node._sessionId || 'bridge',
                SWEEP_SYNC_ROUND + 2,
                JSON.stringify({
                  type: 'sweep-sign-response',
                  sweepSession: data.sweepSession,
                  stepIndex: data.stepIndex,
                  txDataHex: signed.txDataHex,
                }),
              );
            }
          } finally {
                    if (lockAcquired && node._withdrawalMultisigLock === lockKey) {
              node._withdrawalMultisigLock = null;
            }
          }
        } catch (e) { logSwallowedError('sweep_sign_responder_entry_error', e); }
      }
    } catch (e) { logSwallowedError('sweep_sign_responder_poll_error', e); }
  }, 2000);
  node._sweepSignResponderInterval = pollInterval;
}
