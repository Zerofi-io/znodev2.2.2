import { ethers } from 'ethers';
import { tryParseJSON } from '../json.js';
import { deriveRound } from '../pbft-utils.js';
import { WITHDRAWAL_SECURITY_HARDEN, WITHDRAWAL_MAX_ROUND_ATTEMPTS } from '../config.js';

function normalizeMaxAttempts() {
  const n = Number(WITHDRAWAL_MAX_ROUND_ATTEMPTS);
  if (!Number.isFinite(n) || n <= 0) return 1;
  return Math.max(1, Math.floor(n));
}

function getUnsignedPbftBackoffMs(attempts) {
  const n = Number(attempts);
  if (!Number.isFinite(n) || n <= 0) return 60000;
  const base = 60000;
  const max = 300000;
  // 60s, 120s, 240s, then cap at 300s
  const pow = Math.min(3, Math.max(0, n - 1));
  return Math.min(max, base * (2 ** pow));
}

export function setupWithdrawalUnsignedProposalListener(node, { runUnsignedWithdrawalTxConsensus } = {}) {
  if (!node || !node.p2p || node._withdrawalUnsignedProposalListenerSetup) return;
  if (typeof runUnsignedWithdrawalTxConsensus !== 'function') {
    throw new TypeError('setupWithdrawalUnsignedProposalListener requires runUnsignedWithdrawalTxConsensus');
  }
  node._withdrawalUnsignedProposalListenerSetup = true;
  const pollUnsignedProposals = async () => {
    if (!node._withdrawalMonitorRunning) return;
    if (!node._activeClusterId || !Array.isArray(node._clusterMembers) || node._clusterMembers.length === 0) {
      if (node._withdrawalMonitorRunning) setTimeout(pollUnsignedProposals, 3000);
      return;
    }
    const now = Date.now();
    try {
      if (!node._pendingWithdrawals || typeof node._pendingWithdrawals.entries !== 'function') {
        if (node._withdrawalMonitorRunning) setTimeout(pollUnsignedProposals, 3000);
        return;
      }
      const maxAttempts = normalizeMaxAttempts();
      const memberSet = new Set(node._clusterMembers.map((a) => String(a || '').toLowerCase()));
      const handledKeys = new Set();
      for (const [k0, pending0] of node._pendingWithdrawals.entries()) {
        const keyLc = k0 ? String(k0).toLowerCase() : '';
        if (!keyLc || handledKeys.has(keyLc)) continue;

        // Do not spam PBFT on terminal entries.
        const status0 = pending0 && typeof pending0 === 'object' ? String(pending0.status || '') : '';
        if (status0 === 'completed') {
          handledKeys.add(keyLc);
          continue;
        }
        if (pending0 && typeof pending0 === 'object' && (pending0.permanentFailure || status0 === 'failed_permanent')) {
          handledKeys.add(keyLc);
          continue;
        }

        for (let attempt = maxAttempts - 1; attempt >= 0; attempt -= 1) {
          const round = deriveRound('withdrawal-unsigned-proposal', `${keyLc}:${attempt}`);
          let roundData = null;
          try {
            roundData = await node.p2p.getRoundData(node._activeClusterId, node._sessionId || 'bridge', round);
          } catch {
            continue;
          }
          const entries = roundData && typeof roundData === 'object' ? Object.entries(roundData) : [];
          if (entries.length === 0) continue;
          for (const [sender, raw] of entries) {
            const senderLc = sender ? String(sender).toLowerCase() : '';
            if (!senderLc || !memberSet.has(senderLc)) continue;
            const data = tryParseJSON(raw, `from sender ${senderLc}`);
            if (!data || !data.type) continue;
            const keyRaw =
              data.type === 'withdrawal-unsigned-proposal'
                ? data.txHash
                : data.type === 'withdrawal-batch-unsigned-proposal'
                  ? data.batchId
                  : null;
            if (!keyRaw) continue;
            const key = String(keyRaw);
            if (!key || key.toLowerCase() !== keyLc) continue;
            if (WITHDRAWAL_SECURITY_HARDEN) {
              const pending = node._pendingWithdrawals && node._pendingWithdrawals.get(keyLc);
              const coordinator = pending && pending.coordinator ? String(pending.coordinator).toLowerCase() : '';
              if (coordinator && senderLc !== coordinator) continue;
            }
            let unsignedHash;
            try {
              unsignedHash = ethers.keccak256(ethers.toUtf8Bytes(raw));
            } catch {
              continue;
            }
            const unsignedHashLc = unsignedHash ? String(unsignedHash).toLowerCase() : '';
            const pendingUnsigned = node._pendingWithdrawals && node._pendingWithdrawals.get(keyLc);
            let shouldRunConsensus = !!pendingUnsigned;
            if (pendingUnsigned) {
              pendingUnsigned.attempt = attempt;
              if (Array.isArray(data.signerSet)) {
                pendingUnsigned.signerSet = data.signerSet.map((a) => String(a || '').toLowerCase());
              }
              pendingUnsigned.txDataHexUnsigned = null;
              pendingUnsigned.txsetHashUnsigned = typeof data.txsetHash === 'string' ? data.txsetHash.toLowerCase() : null;
              pendingUnsigned.txsetSizeUnsigned =
                typeof data.txsetSize === 'number' && Number.isFinite(data.txsetSize) && data.txsetSize > 0
                  ? data.txsetSize
                  : null;
              pendingUnsigned.pbftUnsignedHash = unsignedHash;
              const mh = typeof data.manifestHash === 'string' ? data.manifestHash.toLowerCase() : '';
              if (mh && /^0x[0-9a-f]{64}$/.test(mh)) pendingUnsigned.manifestHash = mh;
              const mar = typeof data.manifestAttempt !== 'undefined' ? Number(data.manifestAttempt) : NaN;
              const ma = Number.isFinite(mar) && mar >= 0 ? Math.floor(mar) : null;
              if (ma !== null) pendingUnsigned.manifestAttempt = ma;

              const status = String(pendingUnsigned.status || '');
              const isPermanent = pendingUnsigned.permanentFailure || status === 'failed_permanent';
              if (isPermanent) {
                shouldRunConsensus = false;
              } else {
                const prevHash =
                  typeof pendingUnsigned.pbftUnsignedConsensusHash === 'string'
                    ? pendingUnsigned.pbftUnsignedConsensusHash.toLowerCase()
                    : '';
                const inFlight = pendingUnsigned.pbftUnsignedConsensusInFlight === true;
                const prevSuccess = pendingUnsigned.pbftUnsignedConsensusSuccess === true;
                const nextAt =
                  typeof pendingUnsigned.pbftUnsignedConsensusNextAt === 'number'
                    ? pendingUnsigned.pbftUnsignedConsensusNextAt
                    : 0;

                // New unsigned proposal => reset backoff/attempt tracking.
                if (unsignedHashLc && prevHash && prevHash !== unsignedHashLc) {
                  pendingUnsigned.pbftUnsignedConsensusAttempts = 0;
                  pendingUnsigned.pbftUnsignedConsensusSuccess = false;
                  pendingUnsigned.pbftUnsignedConsensusNextAt = 0;
                  pendingUnsigned.pbftUnsignedConsensusInFlight = false;
                }
                if (unsignedHashLc) pendingUnsigned.pbftUnsignedConsensusHash = unsignedHashLc;

                if (inFlight && prevHash === unsignedHashLc) {
                  shouldRunConsensus = false;
                } else if (prevSuccess && prevHash === unsignedHashLc) {
                  shouldRunConsensus = false;
                } else if (nextAt && prevHash === unsignedHashLc && now < nextAt) {
                  shouldRunConsensus = false;
                }

                if (shouldRunConsensus) {
                  const attempts =
                    typeof pendingUnsigned.pbftUnsignedConsensusAttempts === 'number'
                      ? pendingUnsigned.pbftUnsignedConsensusAttempts
                      : 0;
                  pendingUnsigned.pbftUnsignedConsensusAttempts = attempts + 1;
                  pendingUnsigned.pbftUnsignedConsensusInFlight = true;
                  pendingUnsigned.pbftUnsignedConsensusAt = now;
                  pendingUnsigned.pbftUnsignedConsensusNextAt = 0;
                  pendingUnsigned.pbftUnsignedConsensusSuccess = false;
                }
              }
            }

            handledKeys.add(keyLc);
            if (shouldRunConsensus) {
              (async () => {
                try {
                  const result = await runUnsignedWithdrawalTxConsensus(node, key, unsignedHash);
                  const ok = !!(result && result.success);
                  const pending = node._pendingWithdrawals && node._pendingWithdrawals.get(keyLc);
                  if (pending) {
                    pending.pbftUnsignedConsensusAt = Date.now();
                    pending.pbftUnsignedConsensusSuccess = ok;
                    pending.pbftUnsignedConsensusInFlight = false;
                    pending.pbftUnsignedConsensusReason =
                      !ok && result && result.reason ? String(result.reason) : null;
                    if (!ok) {
                      const attempts =
                        typeof pending.pbftUnsignedConsensusAttempts === 'number'
                          ? pending.pbftUnsignedConsensusAttempts
                          : 1;
                      const backoffMs = getUnsignedPbftBackoffMs(attempts);
                      pending.pbftUnsignedConsensusNextAt = Date.now() + backoffMs;
                    } else {
                      pending.pbftUnsignedConsensusNextAt = 0;
                    }
                  }
                } catch (e) {
                  const pending = node._pendingWithdrawals && node._pendingWithdrawals.get(keyLc);
                  if (pending) {
                    pending.pbftUnsignedConsensusAt = Date.now();
                    pending.pbftUnsignedConsensusSuccess = false;
                    pending.pbftUnsignedConsensusInFlight = false;
                    pending.pbftUnsignedConsensusReason = e && e.message ? String(e.message) : String(e);
                    const attempts =
                      typeof pending.pbftUnsignedConsensusAttempts === 'number'
                        ? pending.pbftUnsignedConsensusAttempts
                        : 1;
                    const backoffMs = getUnsignedPbftBackoffMs(attempts);
                    pending.pbftUnsignedConsensusNextAt = Date.now() + backoffMs;
                  }
                  console.log('[Withdrawal] Unsigned-tx PBFT error (listener):', e.message || String(e));
                } finally {
                  const pending = node._pendingWithdrawals && node._pendingWithdrawals.get(keyLc);
                  if (pending) pending.pbftUnsignedConsensusInFlight = false;
                }
              })();
            }
            break;
          }
          if (handledKeys.has(keyLc)) break;
        }
      }
    } catch {}
    if (node._withdrawalMonitorRunning) setTimeout(pollUnsignedProposals, 3000);
  };
  pollUnsignedProposals();
}
