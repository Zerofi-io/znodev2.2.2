import { tryParseJSON } from '../json.js';
import { deriveRound } from '../pbft-utils.js';
import { takeSignStepResult } from '../txset-store.js';
import { WITHDRAWAL_ROUND_V2, WITHDRAWAL_SIGN_STEP_RESPONSE_ROUND, WITHDRAWAL_SIGNED_INFO_ROUND } from '../config.js';
export async function waitForSignStepResponse(node, withdrawalTxHash, stepIndex, signer, timeoutMs, expected = {}) {
  if (!node.p2p || !node._activeClusterId || !Array.isArray(node._clusterMembers) || node._clusterMembers.length === 0) {
    return null;
  }
  const clusterId = node._activeClusterId;
  const sessionId = node._sessionId || 'bridge';
  const key = String(withdrawalTxHash || '').toLowerCase();
  const signerLc = String(signer || '').toLowerCase();
  const expectedTxsetHashIn =
    expected && typeof expected.txsetHashIn === 'string' ? String(expected.txsetHashIn).toLowerCase() : '';
  const expectedPbftUnsignedHash =
    expected && typeof expected.pbftUnsignedHash === 'string' ? String(expected.pbftUnsignedHash).toLowerCase() : '';
  const expectedAttemptRaw = expected && typeof expected.attempt !== 'undefined' ? Number(expected.attempt) : NaN;
  const expectedAttempt = Number.isFinite(expectedAttemptRaw) && expectedAttemptRaw >= 0 ? Math.floor(expectedAttemptRaw) : null;
  const start = Date.now();
  const rounds = [];
  const memberSet = new Set(node._clusterMembers.map((a) => String(a || '').toLowerCase()));
  if (WITHDRAWAL_ROUND_V2) {
    const rKey = expectedAttempt !== null ? `${key}:${expectedAttempt}` : key;
    const r = deriveRound('withdrawal-sign-step-response', rKey);
    if (r && Number.isFinite(r) && r > 0) rounds.push(r);
  }
  if (Number.isFinite(WITHDRAWAL_SIGN_STEP_RESPONSE_ROUND) && WITHDRAWAL_SIGN_STEP_RESPONSE_ROUND > 0) rounds.push(WITHDRAWAL_SIGN_STEP_RESPONSE_ROUND);
  while (Date.now() - start < timeoutMs) {
    const res = takeSignStepResult(node, { withdrawalKey: key, stepIndex, signer: signerLc });
    if (res) {
      if (res.stale) return { stale: true };
      if (res.busy) { continue; }
      if (res.txsetHashOut) return { txsetHashOut: res.txsetHashOut };
    }
    for (const round of rounds) {
      let roundData = null;
      try {
        roundData = await node.p2p.getRoundData(clusterId, sessionId, round);
      } catch {}
      const entries = roundData && typeof roundData === 'object' ? Object.entries(roundData) : [];
      for (const [sender, raw] of entries) {
        const senderLc = sender ? String(sender).toLowerCase() : '';
        if (!senderLc || !memberSet.has(senderLc)) continue;
        if (signerLc && senderLc !== signerLc) continue;
        try {
          const data = tryParseJSON(raw, `sign-step-response from ${senderLc}`);
          if (!data) continue;
          const t = data.type;
          const isStale = t === 'withdrawal-sign-step-stale';
          const isBusy = t === 'withdrawal-sign-step-busy';
          const isError = t === 'withdrawal-sign-step-error';
          if (!isStale && !isBusy && !isError) continue;
          if (!data.withdrawalTxHash) continue;
          if (String(data.withdrawalTxHash).toLowerCase() !== key) continue;
          const idx = Number(data.stepIndex);
          if (!Number.isFinite(idx) || idx !== stepIndex) continue;
          if (data.signer && signerLc && String(data.signer).toLowerCase() !== signerLc) continue;
          if (expectedTxsetHashIn) {
            if (!data.txsetHashIn) continue;
            if (String(data.txsetHashIn).toLowerCase() !== expectedTxsetHashIn) continue;
          }
          if (expectedPbftUnsignedHash) {
            if (!data.pbftUnsignedHash) continue;
            if (String(data.pbftUnsignedHash).toLowerCase() !== expectedPbftUnsignedHash) continue;
          }
          if (expectedAttempt !== null) {
            const a0 = Number(data.attempt);
            const a = Number.isFinite(a0) && a0 >= 0 ? Math.floor(a0) : null;
            if (a === null || a !== expectedAttempt) continue;
          }
          if (isBusy) { continue; }
          if (isError) {
            const reason = data.reason != null ? String(data.reason) : 'sign_step_error';
            return { error: reason };
          }
          return { stale: true };
        } catch {}
      }
    }
    await new Promise((resolve) => setTimeout(resolve, 1000));
  }
  return null;
}
export async function waitForSignedWithdrawalInfo(node, withdrawalKey, expectedSender, timeoutMs) {
  if (!node.p2p || !node._activeClusterId || !Array.isArray(node._clusterMembers) || node._clusterMembers.length === 0) {
    return null;
  }
  const clusterId = node._activeClusterId;
  const sessionId = node._sessionId || 'bridge';
  const key = String(withdrawalKey || '').toLowerCase();
  const expectedLc = expectedSender ? String(expectedSender).toLowerCase() : '';
  const start = Date.now();
  const rounds = [];
  const memberSet = new Set(node._clusterMembers.map((a) => String(a || '').toLowerCase()));
  const r = deriveRound('withdrawal-signed-info', key);
  if (r && Number.isFinite(r) && r > 0) rounds.push(r);
  if (WITHDRAWAL_SIGNED_INFO_ROUND && Number.isFinite(WITHDRAWAL_SIGNED_INFO_ROUND)) rounds.push(WITHDRAWAL_SIGNED_INFO_ROUND);
  while (Date.now() - start < timeoutMs) {
    for (const round of rounds) {
      let roundData = null;
      try {
        roundData = await node.p2p.getRoundData(clusterId, sessionId, round);
      } catch {}
      const entries = roundData && typeof roundData === 'object' ? Object.entries(roundData) : [];
      for (const [sender, raw] of entries) {
        const senderLc = sender ? String(sender).toLowerCase() : '';
        if (!senderLc || !memberSet.has(senderLc)) continue;
        if (expectedLc && senderLc !== expectedLc) continue;
        try {
          const data = tryParseJSON(raw, `signed-info from ${senderLc}`);
          if (!data) continue;
          const id = data.batchId || data.withdrawalTxHash || data.txHash;
          if (!id || String(id).toLowerCase() !== key) continue;
          const txHashList = Array.isArray(data.txHashList) ? data.txHashList : Array.isArray(data.xmrTxHashes) ? data.xmrTxHashes : [];
          const txKey = typeof data.txKey === 'string' ? data.txKey : typeof data.xmrTxKey === 'string' ? data.xmrTxKey : null;
          const moneroSigners = Array.isArray(data.moneroSigners) ? data.moneroSigners : Array.isArray(data.signers) ? data.signers : [];
          const normalizedSigners = Array.isArray(moneroSigners)
            ? moneroSigners
              .map((v) => (typeof v === 'string' ? v.toLowerCase() : ''))
              .filter((v) => /^0x[0-9a-f]{40}$/.test(v))
            : [];
          return {
            txHashList: Array.isArray(txHashList) ? txHashList.map((v) => String(v || '')).filter(Boolean) : [],
            txKey: txKey ? String(txKey) : null,
            moneroSigners: normalizedSigners,
          };
        } catch {}
      }
    }
    await new Promise((resolve) => setTimeout(resolve, 1000));
  }
  return null;
}
