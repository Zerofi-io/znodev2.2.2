import { recordAuthorizedTxids } from '../../../utils/authorized-txids.js';
import { ethers } from 'ethers';
import { saveBridgeState } from '../../bridge-state.js';
import { getDefaultTracker, withIdempotency } from '../../idempotency.js';
import { markWithdrawalCompleted } from '../processed-tracking.js';
import { withMoneroLock } from '../../monero-lock.js';
import { computeTxsetHash, getTxset, putTxset, setExpectedSignStep } from '../txset-store.js';
import {
  deriveRound,
  getPBFTPhases,
  runConsensusWithFallback,
  scheduleClearWithdrawalRounds,
} from '../pbft-utils.js';
import { clearLocalSignStepCacheForWithdrawal } from '../sign-step-cache.js';
import { getCanonicalSignerSet } from '../signer-set.js';
import { waitForSignStepResponse } from './waiters.js';
import { runPostSubmitMultisigSync } from './multisig-sync.js';
import {
  REQUIRED_WITHDRAWAL_SIGNATURES,
  WITHDRAWAL_ROUND_V2_DUALWRITE,
  WITHDRAWAL_SIGNED_INFO_ROUND,
  WITHDRAWAL_SIGN_STEP_TIMEOUT_MS,
} from '../config.js';

const COORDINATOR_LIVENESS_TIMEOUT_MS = Number(process.env.COORDINATOR_LIVENESS_TIMEOUT_MS || 5000);
function getMode(mode) {
  return mode === 'batch' ? 'batch' : 'single';
}

function getNotEnoughSignersMessage(mode) {
  if (mode === 'batch') return '[Withdrawal] Not enough signers for sequential batch signing';
  return '[Withdrawal] Not enough signers available for sequential signing';
}

function getStaleSignStepMessage(mode, signer) {
  const s = signer ? String(signer).slice(0, 10) : '';
  const p = s ? ` ${s}...` : '';
  return `[Withdrawal] Sign step reported stale multisig state${p}`;
}

function getNotEnoughStepsMessage(mode) {
  if (mode === 'batch') return '[Withdrawal] Not enough sign steps completed for batch';
  return '[Withdrawal] Not enough sequential sign steps completed';
}

function getSubmittingMessage(mode) {
  if (mode === 'batch') return '[Withdrawal] Submitting signed batch transaction...';
  return '[Withdrawal] Submitting sequentially signed transaction...';
}

function getSubmittedMessage(mode) {
  if (mode === 'batch') return '[Withdrawal] Batch transaction submitted successfully';
  return '[Withdrawal] Transaction submitted successfully';
}

function getSubmitFailedMessage(mode) {
  if (mode === 'batch') return '[Withdrawal] Failed to submit batch transaction:';
  return '[Withdrawal] Failed to submit transaction:';
}

function getPbftErrorMessage(mode) {
  if (mode === 'batch') return '[Withdrawal] PBFT error for signed batch tx:';
  return '[Withdrawal] PBFT error for signed tx:';
}

function isLikelyDuplicateSubmitError(msg) {
  const m = String(msg || '').toLowerCase();
  if (!m) return false;
  if (m.includes('already') && (m.includes('submitted') || m.includes('in pool') || m.includes('in blockchain') || m.includes('known') || m.includes('exists'))) return true;
  if (m.includes('key image') && m.includes('spent')) return true;
  if (m.includes('double spend')) return true;
  return false;
}

async function checkSignerLiveness(node, signer) {
  if (!node || !node.p2p || !signer) return true;
  try {
    const peerConnected = await Promise.race([
      node.p2p.isPeerConnected ? node.p2p.isPeerConnected(signer) : Promise.resolve(true),
      new Promise((resolve) => setTimeout(() => resolve(null), COORDINATOR_LIVENESS_TIMEOUT_MS)),
    ]);
    if (peerConnected === null) return true;
    return peerConnected !== false;
  } catch {
    return true;
  }
}

export async function runCoordinatorSignAndSubmit(node, { key, txData, selfAddress, mode } = {}) {
  if (!node || !key) return;
  const kind = getMode(mode);
  const withdrawalKey = String(key || '').toLowerCase();
  let txDataHex0 = txData && txData.txDataHex ? String(txData.txDataHex) : '';
  if (!txDataHex0) {
    console.log('[Withdrawal] No transaction data for signing phase');
    const pending = node._pendingWithdrawals && node._pendingWithdrawals.get(withdrawalKey);
    if (pending) {
      pending.status = 'failed';
      pending.error = 'no_tx_data';
    }
    return;
  }
  const initialTxsetHash = computeTxsetHash(txDataHex0);
  if (!initialTxsetHash) {
    const pending = node._pendingWithdrawals && node._pendingWithdrawals.get(withdrawalKey);
    if (pending) {
      pending.status = 'failed';
      pending.error = 'txset_hash_failed';
    }
    return;
  }
  const stored0 = putTxset(node, initialTxsetHash, txDataHex0);
  txDataHex0 = '';
  if (!stored0.ok) {
    const pending = node._pendingWithdrawals && node._pendingWithdrawals.get(withdrawalKey);
    if (pending) {
      pending.status = 'failed';
      pending.error = stored0.reason ? `txset_store_failed:${stored0.reason}` : 'txset_store_failed';
    }
    return;
  }
  const pendingSigner = node._pendingWithdrawals && node._pendingWithdrawals.get(withdrawalKey);
  const canonicalSignerSet = getCanonicalSignerSet(node);
  const signerSetForSeq =
    pendingSigner && Array.isArray(pendingSigner.signerSet) && pendingSigner.signerSet.length > 0
      ? pendingSigner.signerSet.map((a) => String(a || '').toLowerCase())
      : canonicalSignerSet;
  if (!signerSetForSeq || signerSetForSeq.length < REQUIRED_WITHDRAWAL_SIGNATURES) {
    console.log(getNotEnoughSignersMessage(kind));
    const pending = node._pendingWithdrawals && node._pendingWithdrawals.get(withdrawalKey);
    if (pending) pending.status = 'signing_failed';
    return;
  }
  const stepTimeoutMs = WITHDRAWAL_SIGN_STEP_TIMEOUT_MS;
  const selfLc = selfAddress ? String(selfAddress).toLowerCase() : '';
  const moneroSignersSet = new Set();
  if (selfLc) moneroSignersSet.add(selfLc);
  if (pendingSigner && Array.isArray(pendingSigner.moneroSigners)) {
    for (const a of pendingSigner.moneroSigners) {
      const lc = a ? String(a).toLowerCase() : '';
      if (lc) moneroSignersSet.add(lc);
    }
  }
  let signedSteps = Math.max(1, moneroSignersSet.size);
  let currentTxsetHash = initialTxsetHash;
  let startIndex = 0;
  const resumeHashRaw = pendingSigner && typeof pendingSigner.currentTxsetHash === 'string' ? String(pendingSigner.currentTxsetHash).toLowerCase() : '';
  const resumeIdxRaw = pendingSigner && typeof pendingSigner.currentStepIndex !== 'undefined' ? Number(pendingSigner.currentStepIndex) : NaN;
  const resumeIdx = Number.isFinite(resumeIdxRaw) && resumeIdxRaw >= -1 ? Math.floor(resumeIdxRaw) : -1;
  if (resumeHashRaw && resumeHashRaw !== initialTxsetHash && getTxset(node, resumeHashRaw)) {
    currentTxsetHash = resumeHashRaw;
    startIndex = Math.min(signerSetForSeq.length, Math.max(0, resumeIdx + 1));
  } else if (resumeIdx >= 0 && signedSteps > 1) {
    startIndex = Math.min(signerSetForSeq.length, Math.max(0, resumeIdx + 1));
  }
  const startSigningAt = Date.now();
  const totalSigningTimeoutMs = Number(process.env.WITHDRAWAL_SIGN_TOTAL_TIMEOUT_MS || (stepTimeoutMs * Math.max(1, signerSetForSeq.length)));
  const extraSignWindowMsRaw = Number(process.env.WITHDRAWAL_EXTRA_SIGN_WINDOW_MS || 10000);
  const extraSignWindowMs = Number.isFinite(extraSignWindowMsRaw) && extraSignWindowMsRaw > 0 ? Math.floor(extraSignWindowMsRaw) : 0;
  let extraUntilMs = 0;
  const p0 = node._pendingWithdrawals && node._pendingWithdrawals.get(withdrawalKey);
  if (p0) {
    p0.currentStepIndex = startIndex > 0 ? startIndex - 1 : -1;
    p0.currentTxDataHex = null;
    p0.currentTxsetHash = currentTxsetHash;
    p0.moneroSigners = Array.from(moneroSignersSet);
  }
  for (let i = startIndex; i < signerSetForSeq.length; i += 1) {
      const now = Date.now();
      const elapsedMs = now - startSigningAt;
      if (elapsedMs > totalSigningTimeoutMs) {
        console.log(`[Withdrawal] Signing timeout after ${Math.round(elapsedMs / 1000)}s; aborting`);
        break;
      }
      if (extraUntilMs && now > extraUntilMs) {
        break;
      }
      const needed = REQUIRED_WITHDRAWAL_SIGNATURES - signedSteps;
      let remainingPotential = 0;
      for (let j = i; j < signerSetForSeq.length; j += 1) {
        const s2 = signerSetForSeq[j];
        if (!s2) continue;
        const s2lc = String(s2).toLowerCase();
        if (selfLc && s2lc === selfLc) continue;
        if (moneroSignersSet.has(s2lc)) continue;
        remainingPotential += 1;
      }
      if (remainingPotential < needed) {
        console.log('[Withdrawal] Not enough remaining signers to reach threshold, aborting');
        break;
      }
      const signer = signerSetForSeq[i];
      if (!signer) continue;
      const signerLc = String(signer).toLowerCase();
      if (selfLc && signerLc === selfLc) continue;
      if (moneroSignersSet.has(signerLc)) continue;
      const isLive = await checkSignerLiveness(node, signer);
      if (!isLive) {
        console.log(`[Withdrawal] Signer ${signer.slice(0, 10)}... appears offline, skipping`);
        continue;
      }
      setExpectedSignStep(node, { withdrawalKey, stepIndex: i, signer, txsetHashIn: currentTxsetHash });
      const pbftKey = pendingSigner && pendingSigner.pbftUnsignedHash ? String(pendingSigner.pbftUnsignedHash).toLowerCase() : '';
      const attemptRaw = pendingSigner && typeof pendingSigner.attempt !== 'undefined' ? Number(pendingSigner.attempt) : NaN;
      const attempt = Number.isFinite(attemptRaw) && attemptRaw >= 0 ? Math.floor(attemptRaw) : 0;
      const syncDigest = pendingSigner && typeof pendingSigner.multisigSyncDigest === 'string' ? String(pendingSigner.multisigSyncDigest).toLowerCase() : null;
      const signerSetHash = pendingSigner && typeof pendingSigner.signerSetHash === 'string' ? String(pendingSigner.signerSetHash).toLowerCase() : null;
      const requestRound = deriveRound('withdrawal-sign-step-request', `${withdrawalKey}:${attempt}:${pbftKey}:${signer}`);
      try {
        await node.p2p.broadcastRoundData(
          node._activeClusterId,
          node._sessionId || 'bridge',
          requestRound,
          JSON.stringify({
            type: 'withdrawal-sign-step-request',
            withdrawalTxHash: withdrawalKey,
            stepIndex: i,
            signer,
            attempt,
            syncDigest,
            signerSetHash,
            txsetHashIn: currentTxsetHash,
            pbftUnsignedHash: pendingSigner && pendingSigner.pbftUnsignedHash ? pendingSigner.pbftUnsignedHash : null,
            manifestHash: pendingSigner && pendingSigner.manifestHash ? pendingSigner.manifestHash : null,
            manifestAttempt: pendingSigner && typeof pendingSigner.manifestAttempt !== 'undefined' ? pendingSigner.manifestAttempt : attempt,
          }),
        );
      } catch (e) {
        const msg = e && e.message ? e.message : String(e);
        if (!msg.includes('round payload overwrite rejected')) continue;
      }
      const responseTimeoutMs = extraUntilMs ? Math.max(1, Math.min(stepTimeoutMs, extraUntilMs - Date.now())) : stepTimeoutMs;
      const response = await waitForSignStepResponse(node, withdrawalKey, i, signer, responseTimeoutMs, {
        txsetHashIn: currentTxsetHash,
        pbftUnsignedHash: pbftKey || null,
        attempt,
      });
      if (!response || (!response.txsetHashOut && !response.stale && !response.busy && !response.error)) {
        console.log('[Withdrawal] Sign step timeout or invalid response, skipping signer');
        continue;
      }
      if (response.busy) {
        console.log('[Withdrawal] Signer busy, skipping signer');
        continue;
      }
      if (response.error) {
        console.log(`[Withdrawal] Sign step rejected by signer: ${response.error}`);
        continue;
      }
      if (response.stale) {
        console.log(getStaleSignStepMessage(kind, signer));
        const pending = node._pendingWithdrawals && node._pendingWithdrawals.get(withdrawalKey);
        if (pending) {
          const curAttempt = typeof pending.attempt !== 'undefined' ? Number(pending.attempt) : 0;
          const normalizedAttempt = Number.isFinite(curAttempt) && curAttempt >= 0 ? Math.floor(curAttempt) : 0;
          const staleSigner = signer ? String(signer).toLowerCase() : '';
          if (staleSigner) {
            const prevStaleAttempt = typeof pending.staleSignersAttempt === 'number' ? Number(pending.staleSignersAttempt) : null;
            const prevNormalized =
              prevStaleAttempt !== null && Number.isFinite(prevStaleAttempt) && prevStaleAttempt >= 0 ? Math.floor(prevStaleAttempt) : null;
            if (prevNormalized !== normalizedAttempt) {
              pending.staleSignersAttempt = normalizedAttempt;
              pending.staleSigners = [staleSigner];
            } else {
              const arr = Array.isArray(pending.staleSigners) ? pending.staleSigners : [];
              const set = new Set(arr.map((a) => (a ? String(a).toLowerCase() : '')).filter(Boolean));
              set.add(staleSigner);
              pending.staleSigners = Array.from(set);
            }
          }
        }
        continue;
      }
      const nextHash = typeof response.txsetHashOut === 'string' ? response.txsetHashOut.toLowerCase() : '';
      if (!nextHash) {
        continue;
      }
      currentTxsetHash = nextHash;
      moneroSignersSet.add(signerLc);
      signedSteps = moneroSignersSet.size;
      if (!extraUntilMs && signedSteps >= REQUIRED_WITHDRAWAL_SIGNATURES) {
        if (extraSignWindowMs > 0) extraUntilMs = Date.now() + extraSignWindowMs;
        else break;
      }
      const p2 = node._pendingWithdrawals && node._pendingWithdrawals.get(withdrawalKey);
      if (p2) {
        p2.currentStepIndex = i;
        p2.currentTxDataHex = null;
        p2.currentTxsetHash = currentTxsetHash;
        p2.moneroSigners = Array.from(moneroSignersSet);
      }
    }
    if (signedSteps < REQUIRED_WITHDRAWAL_SIGNATURES) {
      console.log(getNotEnoughStepsMessage(kind));
      const p = node._pendingWithdrawals && node._pendingWithdrawals.get(withdrawalKey);
      if (p) {
        const curAttempt = typeof p.attempt !== 'undefined' ? Number(p.attempt) : 0;
        const normalizedAttempt = Number.isFinite(curAttempt) && curAttempt >= 0 ? Math.floor(curAttempt) : 0;
        const nextAttempt = normalizedAttempt + 1;
        const staleAttemptRaw = typeof p.staleSignersAttempt === 'number' ? Number(p.staleSignersAttempt) : null;
        const staleAttempt =
          staleAttemptRaw !== null && Number.isFinite(staleAttemptRaw) && staleAttemptRaw >= 0
            ? Math.floor(staleAttemptRaw)
            : null;
        const staleArr = Array.isArray(p.staleSigners) ? p.staleSigners : [];
        const staleSet = new Set(staleArr.map((a) => (a ? String(a).toLowerCase() : '')).filter(Boolean));
        const staleImpacted = staleAttempt !== null && staleAttempt === normalizedAttempt && staleSet.size > 0;
        if (staleImpacted) {
          p.error = 'stale_quorum_failed';
          p.staleRecoveryAttempt = normalizedAttempt;
          const c0 = typeof p.staleRecoveryCount === 'number' ? Number(p.staleRecoveryCount) : 0;
          p.staleRecoveryCount = Number.isFinite(c0) && c0 >= 0 ? Math.floor(c0) + 1 : 1;
        }
        p.attempt = nextAttempt;
        p.status = 'signing_failed';
      }
      scheduleClearWithdrawalRounds(node, withdrawalKey);
      return;
    }
    console.log(getSubmittingMessage(kind));
    const finalTxHex = getTxset(node, currentTxsetHash);
    if (!finalTxHex) {
      const pending = node._pendingWithdrawals && node._pendingWithdrawals.get(withdrawalKey);
      if (pending) {
        pending.status = 'failed';
        pending.error = 'final_txset_missing';
      }
      scheduleClearWithdrawalRounds(node, withdrawalKey);
      return;
    }
    const pendingBeforeSubmit = node._pendingWithdrawals && node._pendingWithdrawals.get(withdrawalKey);
    const attemptRaw = pendingBeforeSubmit && typeof pendingBeforeSubmit.attempt !== 'undefined' ? Number(pendingBeforeSubmit.attempt) : NaN;
    const attempt = Number.isFinite(attemptRaw) && attemptRaw >= 0 ? Math.floor(attemptRaw) : 0;
    const tracker = getDefaultTracker();
    const submitIdemKey = tracker.generateKey('withdrawal_submit_multisig', withdrawalKey, currentTxsetHash);
    if (pendingBeforeSubmit) {
      pendingBeforeSubmit.status = 'submitting';
      pendingBeforeSubmit.txDataHexSigned = null;
      pendingBeforeSubmit.txsetHashSigned = currentTxsetHash;
      pendingBeforeSubmit.txsetSizeSigned = finalTxHex.length;
      pendingBeforeSubmit.submitIdemKey = submitIdemKey;
      pendingBeforeSubmit.submitPreAt = Date.now();
      pendingBeforeSubmit.submitPreAttempt = attempt;
      pendingBeforeSubmit.submitPreTxsetHash = currentTxsetHash;
      pendingBeforeSubmit.submitPreTxsetSize = finalTxHex.length;
    }
    const preSaved = saveBridgeState(node);
    if (!preSaved) {
      const pending = node._pendingWithdrawals && node._pendingWithdrawals.get(withdrawalKey);
      if (pending) {
        pending.status = 'failed';
        pending.error = 'bridge_state_persist_failed_before_submit';
      }
      scheduleClearWithdrawalRounds(node, withdrawalKey);
      return;
    }
    let submitResult = null;
    try {
      submitResult = await withIdempotency(
        tracker,
        submitIdemKey,
        async () => {
          return await withMoneroLock(node, { key: withdrawalKey, op: 'withdrawal_submit' }, async () => {
            return await node.monero.call('submit_multisig', { tx_data_hex: finalTxHex }, 180000);
          });
        },
        { metadata: { withdrawalKey, txsetHash: currentTxsetHash } },
      );
      if (!submitResult) return;
    } catch (e) {
      const msg = e && e.message ? e.message : String(e);
      console.log(getSubmitFailedMessage(kind), msg);
      const pending = node._pendingWithdrawals && node._pendingWithdrawals.get(withdrawalKey);
      if (pending) {
        if (isLikelyDuplicateSubmitError(msg)) {
          pending.status = 'completed';
          pending.submitAt = Date.now();
          pending.submitTxsetHash = currentTxsetHash;
          pending.submitTxsetSize = finalTxHex.length;
          pending.submitDuplicateError = msg;
          markWithdrawalCompleted(node, withdrawalKey, pending, pending.xmrTxHashes);
          saveBridgeState(node);
        } else {
          pending.status = 'submit_failed';
          pending.error = msg;
        }
      }
      return;
    }
    if (Array.isArray(submitResult.tx_hash_list) && submitResult.tx_hash_list.length > 0) {
      recordAuthorizedTxids(node, submitResult.tx_hash_list);
    }
    console.log(getSubmittedMessage(kind));
    console.log(`  TX Hash(es): ${submitResult.tx_hash_list ? submitResult.tx_hash_list.join(', ') : 'unknown'}`);
    const pending = node._pendingWithdrawals && node._pendingWithdrawals.get(withdrawalKey);
    if (pending) {
      pending.status = 'completed';
      pending.xmrTxHashes = submitResult.tx_hash_list;
      pending.submitAt = Date.now();
      pending.submitTxsetHash = currentTxsetHash;
      pending.submitTxsetSize = finalTxHex.length;
      pending.submitDuplicateError = null;
    }
    const committed = node._pendingWithdrawals && node._pendingWithdrawals.get(withdrawalKey);
    if (committed && committed.status === 'completed') {
      markWithdrawalCompleted(node, withdrawalKey, committed, submitResult.tx_hash_list);
      const savedAfterSubmit = saveBridgeState(node);
      if (!savedAfterSubmit) {
        committed.error = 'bridge_state_persist_failed_after_submit';
        return;
      }
    }
    let postSync = null;
    try {
      postSync = await runPostSubmitMultisigSync(node, withdrawalKey);
    } catch (e) {
      const msg = e && e.message ? e.message : String(e);
      console.log(`[Withdrawal] Post-submit multisig sync failed: ${msg}`);
      postSync = null;
    }
    if (postSync && postSync.success && !postSync.skipped) {
      console.log(
        `[Withdrawal] Post-submit multisig sync completed; importedOutputs=${postSync.importedOutputs || 0} ready=${postSync.readyCount || 0}`,
      );
    } else if (postSync && !postSync.skipped) {
      console.log(`[Withdrawal] Post-submit multisig sync failed: ${postSync.reason || 'unknown'}`);
    }
    if (node.p2p && node._activeClusterId) {
      const signedInfo = JSON.stringify({
        type: 'withdrawal-signed-info',
        batchId: withdrawalKey,
        withdrawalTxHash: withdrawalKey,
        txHashList: submitResult.tx_hash_list || [],
        txKey: committed && committed.xmrTxKey ? committed.xmrTxKey : null,
        moneroSigners: Array.from(moneroSignersSet),
      });
      const clusterId = node._activeClusterId;
      const sessionId = node._sessionId || 'bridge';
      const r2 = deriveRound('withdrawal-signed-info', withdrawalKey);
      if (r2 && Number.isFinite(r2) && r2 > 0) {
        try { await node.p2p.broadcastRoundData(clusterId, sessionId, r2, signedInfo); } catch {}
      }
      if (WITHDRAWAL_ROUND_V2_DUALWRITE) {
        try { await node.p2p.broadcastRoundData(clusterId, sessionId, WITHDRAWAL_SIGNED_INFO_ROUND, signedInfo); } catch {}
      }
    }
    const signedPayload = JSON.stringify({ txsetHash: currentTxsetHash, txHashList: submitResult.tx_hash_list || [] });
    const signedHash = ethers.keccak256(ethers.toUtf8Bytes(signedPayload));
    const txPbftTimeoutMs = Number(process.env.WITHDRAWAL_TX_PBFT_TIMEOUT_MS || process.env.WITHDRAWAL_SIGN_TIMEOUT_MS || 300000);
    const signedPhases = getPBFTPhases('withdrawal-signed', withdrawalKey);
    let signedConsensus = null;
    try {
      signedConsensus = await runConsensusWithFallback(node, signedPhases, signedHash, node._clusterMembers, txPbftTimeoutMs);
    } catch (err) {
      console.log(getPbftErrorMessage(kind), err.message || String(err));
    }
    const pending2 = node._pendingWithdrawals && node._pendingWithdrawals.get(withdrawalKey);
    if (pending2) {
      pending2.pbftSignedHash = signedHash;
      pending2.signedPbftSuccess = !!(signedConsensus && signedConsensus.success);
    }
    const finalPending = node._pendingWithdrawals && node._pendingWithdrawals.get(withdrawalKey);
    if (finalPending && finalPending.status === 'completed') {
      clearLocalSignStepCacheForWithdrawal(node, withdrawalKey);
      scheduleClearWithdrawalRounds(node, withdrawalKey);
      node._pendingWithdrawals.delete(withdrawalKey);
      saveBridgeState(node);
    }
}
