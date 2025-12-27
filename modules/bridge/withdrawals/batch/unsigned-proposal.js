import { ethers } from 'ethers';
import { scheduleWithdrawalStateSave } from '../pending-store.js';
import { tryParseJSON } from '../json.js';
import { getCanonicalSignerSet } from '../signer-set.js';
import { computeTxsetHash, getTxset, putTxset } from '../txset-store.js';
import { withMoneroLock } from '../../monero-lock.js';
import {
  REQUIRED_WITHDRAWAL_SIGNATURES,
  WITHDRAWAL_SECURITY_HARDEN,
  WITHDRAWAL_MAX_ROUND_ATTEMPTS,
} from '../config.js';
import { deriveRound } from '../pbft-utils.js';

function normalizeAttempt(v) {
  const n = Number(v);
  if (!Number.isFinite(n) || n < 0) return 0;
  return Math.floor(n);
}

function normalizeMaxAttempts() {
  const n = normalizeAttempt(WITHDRAWAL_MAX_ROUND_ATTEMPTS);
  return Math.max(1, n || 1);
}

export async function resolveUnsignedBatchProposal(
  node,
  {
    key,
    isCoordinator,
    destinations,
    pendingAfterSync,
  },
) {
  if (!node || !node.p2p || !node._activeClusterId || !key) return null;
  let txData = null;
  let unsignedHash = null;
  const batchKey = String(key || '').toLowerCase();
  if (!batchKey) return null;
  if (isCoordinator) {
    const existing = pendingAfterSync && typeof pendingAfterSync === 'object' ? pendingAfterSync : null;
    const existingTxsetHash = existing && typeof existing.txsetHashUnsigned === 'string'
      ? String(existing.txsetHashUnsigned).toLowerCase()
      : '';
    const existingPbft = existing && typeof existing.pbftUnsignedHash === 'string'
      ? String(existing.pbftUnsignedHash).toLowerCase()
      : '';
    const attempt = existing && typeof existing.attempt !== 'undefined' ? normalizeAttempt(existing.attempt) : 0;
    const proposalRound = deriveRound('withdrawal-unsigned-proposal', `${batchKey}:${attempt}`);

    if (existing && existingTxsetHash && existingPbft && existing.error !== 'stale_multisig_tx') {
      const txDataHexExisting = getTxset(node, existingTxsetHash);
      if (!txDataHexExisting) {
        console.log('[Withdrawal] Unsigned txset missing locally; cannot retry without clearing stale round data');
        const pending = node._pendingWithdrawals && node._pendingWithdrawals.get(batchKey);
        if (pending) {
          pending.status = 'failed';
          pending.error = 'txset_missing_for_retry';
        }
        return null;
      }
      const signerSet = Array.isArray(existing.signerSet) && existing.signerSet.length > 0
        ? existing.signerSet.map((a) => String(a || '').toLowerCase())
        : getCanonicalSignerSet(node);
      if (!signerSet || signerSet.length < REQUIRED_WITHDRAWAL_SIGNATURES) {
        console.log('[Withdrawal] Not enough signers available for PBFT batch tx commit');
        const pending = node._pendingWithdrawals && node._pendingWithdrawals.get(batchKey);
        if (pending) {
          pending.status = 'failed';
          pending.error = 'not_enough_signers_for_tx_pbft';
        }
        return null;
      }
      const manifestHashRaw = existing && typeof existing.manifestHash === 'string'
        ? String(existing.manifestHash).toLowerCase()
        : '';
      const manifestHash = /^0x[0-9a-f]{64}$/.test(manifestHashRaw) ? manifestHashRaw : null;
      const manifestAttempt = existing && typeof existing.manifestAttempt !== 'undefined'
        ? normalizeAttempt(existing.manifestAttempt)
        : attempt;
      const proposalBase = {
        type: 'withdrawal-batch-unsigned-proposal',
        batchId: batchKey,
        txsetHash: existingTxsetHash,
        txsetSize: txDataHexExisting.length,
        signerSet,
        threshold: REQUIRED_WITHDRAWAL_SIGNATURES,
      };
      const proposalV2 = manifestHash ? { ...proposalBase, manifestHash, manifestAttempt } : proposalBase;
      const unsignedPayloadV2 = JSON.stringify(
        proposalV2,
        (_, v) => (typeof v === 'bigint' ? v.toString() : v),
      );
      let unsignedPayload = unsignedPayloadV2;
      let computedHash = ethers.keccak256(ethers.toUtf8Bytes(unsignedPayloadV2));
      if (computedHash.toLowerCase() !== existingPbft) {
        const unsignedPayloadV1 = JSON.stringify(
          proposalBase,
          (_, v) => (typeof v === 'bigint' ? v.toString() : v),
        );
        const computedHashV1 = ethers.keccak256(ethers.toUtf8Bytes(unsignedPayloadV1));
        if (computedHashV1.toLowerCase() !== existingPbft) {
          console.log('[Withdrawal] Existing unsigned PBFT hash mismatch; refusing to retry without clearing rounds');
          const pending = node._pendingWithdrawals && node._pendingWithdrawals.get(batchKey);
          if (pending) {
            pending.status = 'failed';
            pending.error = 'pbft_unsigned_hash_mismatch';
          }
          return null;
        }
        unsignedPayload = unsignedPayloadV1;
        computedHash = computedHashV1;
      }
      const clusterId = node._activeClusterId;
      const sessionId = node._sessionId || 'bridge';
      try {
        await node.p2p.broadcastRoundData(clusterId, sessionId, proposalRound, unsignedPayload);
      } catch (e) {
        const msg = e && e.message ? e.message : String(e);
        if (!msg.includes('round payload overwrite rejected')) {
          console.log('[Withdrawal] Failed to broadcast unsigned proposal:', msg);
          const pending = node._pendingWithdrawals && node._pendingWithdrawals.get(batchKey);
          if (pending) {
            pending.status = 'failed';
            pending.error = `unsigned_proposal_broadcast_failed:${msg}`;
          }
          return null;
        }
      }
      const pendingUnsigned = node._pendingWithdrawals && node._pendingWithdrawals.get(batchKey);
      if (pendingUnsigned) {
        pendingUnsigned.attempt = attempt;
        pendingUnsigned.signerSet = signerSet;
        pendingUnsigned.txDataHexUnsigned = null;
        pendingUnsigned.txsetHashUnsigned = existingTxsetHash;
        pendingUnsigned.txsetSizeUnsigned = txDataHexExisting.length;
        pendingUnsigned.pbftUnsignedHash = computedHash;
      }
      return { txData: { txDataHex: txDataHexExisting }, unsignedHash: computedHash, signerSet };
    }

    console.log('[Withdrawal] Creating multisig batch transfer transaction (coordinator)...');
    try {
      txData = await withMoneroLock(node, { key: batchKey, op: 'withdrawal_transfer' }, async () => {
        return await node.monero.transfer(destinations);
      });
      console.log('[Withdrawal] Batch transfer transaction created');
      const pendingAfterTransfer = node._pendingWithdrawals && node._pendingWithdrawals.get(batchKey);
      if (pendingAfterTransfer) {
        const txKey = txData && txData.txKey ? String(txData.txKey) : '';
        pendingAfterTransfer.xmrTxKey = txKey || null;
        scheduleWithdrawalStateSave(node);
      }
    } catch (e) {
      console.log('[Withdrawal] Failed to create batch transfer:', e.message || String(e));
      const pending = node._pendingWithdrawals && node._pendingWithdrawals.get(batchKey);
      if (pending) {
        pending.status = 'failed';
        pending.error = e.message;
      }
      return null;
    }
    const baseSignerSet =
      pendingAfterSync && Array.isArray(pendingAfterSync.signerSet) && pendingAfterSync.signerSet.length > 0
        ? pendingAfterSync.signerSet.map((a) => String(a || '').toLowerCase())
        : getCanonicalSignerSet(node);
    const signerSet = baseSignerSet;
    if (!signerSet || signerSet.length < REQUIRED_WITHDRAWAL_SIGNATURES) {
      console.log('[Withdrawal] Not enough signers available for PBFT batch tx commit');
      const pending = node._pendingWithdrawals && node._pendingWithdrawals.get(batchKey);
      if (pending) {
        pending.status = 'failed';
        pending.error = 'not_enough_signers_for_tx_pbft';
      }
      return null;
    }
    const txDataHex = txData && txData.txDataHex ? String(txData.txDataHex) : '';
    const txsetHash = computeTxsetHash(txDataHex);
    if (!txsetHash) {
      const pending = node._pendingWithdrawals && node._pendingWithdrawals.get(batchKey);
      if (pending) {
        pending.status = 'failed';
        pending.error = 'txset_hash_failed';
      }
      return null;
    }
    const stored = putTxset(node, txsetHash, txDataHex);
    if (!stored.ok) {
      const pending = node._pendingWithdrawals && node._pendingWithdrawals.get(batchKey);
      if (pending) {
        pending.status = 'failed';
        pending.error = stored.reason ? `txset_store_failed:${stored.reason}` : 'txset_store_failed';
      }
      return null;
    }
    const pendingForManifest = node._pendingWithdrawals && node._pendingWithdrawals.get(batchKey);
    const manifestHashRaw = pendingForManifest && typeof pendingForManifest.manifestHash === 'string'
      ? String(pendingForManifest.manifestHash).toLowerCase()
      : '';
    const manifestHash = /^0x[0-9a-f]{64}$/.test(manifestHashRaw) ? manifestHashRaw : null;
    const manifestAttempt = pendingForManifest && typeof pendingForManifest.manifestAttempt !== 'undefined'
      ? normalizeAttempt(pendingForManifest.manifestAttempt)
      : attempt;
    const proposal = {
      type: 'withdrawal-batch-unsigned-proposal',
      batchId: batchKey,
      txsetHash,
      txsetSize: txDataHex.length,
      signerSet,
      threshold: REQUIRED_WITHDRAWAL_SIGNATURES,
    };
    if (manifestHash) {
      proposal.manifestHash = manifestHash;
      proposal.manifestAttempt = manifestAttempt;
    }
    const unsignedPayload = JSON.stringify(
      proposal,
      (_, v) => (typeof v === 'bigint' ? v.toString() : v),
    );
    const clusterId = node._activeClusterId;
    const sessionId = node._sessionId || 'bridge';
    unsignedHash = ethers.keccak256(ethers.toUtf8Bytes(unsignedPayload));
    const pendingUnsigned = node._pendingWithdrawals && node._pendingWithdrawals.get(batchKey);
    if (pendingUnsigned) {
      pendingUnsigned.attempt = attempt;
      pendingUnsigned.signerSet = signerSet;
      pendingUnsigned.txDataHexUnsigned = null;
      pendingUnsigned.txsetHashUnsigned = txsetHash;
      pendingUnsigned.txsetSizeUnsigned = txDataHex.length;
      pendingUnsigned.pbftUnsignedHash = unsignedHash;
    }
    try {
      await node.p2p.broadcastRoundData(clusterId, sessionId, proposalRound, unsignedPayload);
    } catch (e) {
      const msg = e && e.message ? e.message : String(e);
      console.log('[Withdrawal] Failed to broadcast unsigned proposal:', msg);
      const pending = node._pendingWithdrawals && node._pendingWithdrawals.get(batchKey);
      if (pending) {
        pending.status = 'failed';
        pending.error = `unsigned_proposal_broadcast_failed:${msg}`;
        pending.attempt = attempt + 1;
        pending.txDataHexUnsigned = null;
        pending.txsetHashUnsigned = null;
        pending.txsetSizeUnsigned = null;
        pending.pbftUnsignedHash = null;
      }
      return null;
    }
    return { txData, unsignedHash, signerSet };
  }
  console.log('[Withdrawal] Waiting for unsigned proposal from coordinator (non-coordinator)...');
  const proposalWaitMs = Number(process.env.WITHDRAWAL_PROPOSAL_WAIT_MS || 60000);
  const startWait = Date.now();
  const maxAttempts = normalizeMaxAttempts();
  while (Date.now() - startWait < proposalWaitMs) {
    try {
      for (let attempt = maxAttempts - 1; attempt >= 0; attempt -= 1) {
        const round = deriveRound('withdrawal-unsigned-proposal', `${batchKey}:${attempt}`);
        const roundData = await node.p2p.getRoundData(
          node._activeClusterId,
          node._sessionId || 'bridge',
          round,
        );
        const entries = roundData && typeof roundData === 'object' ? Object.entries(roundData) : [];
        if (entries.length === 0) continue;
        const pendingUnsigned = node._pendingWithdrawals && node._pendingWithdrawals.get(batchKey);
        const coordinator =
          WITHDRAWAL_SECURITY_HARDEN && pendingUnsigned && pendingUnsigned.coordinator
            ? String(pendingUnsigned.coordinator).toLowerCase()
            : '';
        const memberSet = new Set(
          Array.isArray(node._clusterMembers)
            ? node._clusterMembers.map((a) => String(a || '').toLowerCase())
            : [],
        );
        for (const [sender, raw] of entries) {
          const senderLc = sender ? String(sender).toLowerCase() : '';
          if (!senderLc || (memberSet.size > 0 && !memberSet.has(senderLc))) continue;
          if (coordinator && senderLc !== coordinator) continue;
          const data = tryParseJSON(raw, `batch-unsigned from round ${round}`);
          if (!data) continue;
          if (!data || data.type !== 'withdrawal-batch-unsigned-proposal') continue;
          if (String(data.batchId || '').toLowerCase() !== batchKey) continue;
          const txsetHash = typeof data.txsetHash === 'string' ? data.txsetHash.toLowerCase() : '';
          if (!txsetHash) continue;
          unsignedHash = ethers.keccak256(ethers.toUtf8Bytes(raw));
          const signerSet = Array.isArray(data.signerSet) ? data.signerSet : null;
          const txsetSize = typeof data.txsetSize === 'number' && Number.isFinite(data.txsetSize) && data.txsetSize > 0
            ? data.txsetSize
            : null;
          if (pendingUnsigned) {
            pendingUnsigned.attempt = attempt;
            pendingUnsigned.signerSet = signerSet;
            pendingUnsigned.txDataHexUnsigned = null;
            pendingUnsigned.txsetHashUnsigned = txsetHash;
            pendingUnsigned.txsetSizeUnsigned = txsetSize;
            pendingUnsigned.pbftUnsignedHash = unsignedHash;
            const mh = typeof data.manifestHash === 'string' ? data.manifestHash.toLowerCase() : '';
            if (mh && /^0x[0-9a-f]{64}$/.test(mh)) pendingUnsigned.manifestHash = mh;
            const mar = typeof data.manifestAttempt !== 'undefined' ? Number(data.manifestAttempt) : NaN;
            const ma = Number.isFinite(mar) && mar >= 0 ? Math.floor(mar) : null;
            if (ma !== null) pendingUnsigned.manifestAttempt = ma;
          }
          break;
        }
        if (unsignedHash) break;
      }
    } catch {}
    if (unsignedHash) break;
    await new Promise((r) => setTimeout(r, 1000));
  }
  if (!unsignedHash) {
    console.log('[Withdrawal] Timeout waiting for unsigned proposal from coordinator');
    const pending = node._pendingWithdrawals && node._pendingWithdrawals.get(batchKey);
    if (pending) {
      pending.status = 'failed';
      pending.error = 'unsigned_proposal_timeout';
    }
    return null;
  }
  return { txData: null, unsignedHash, signerSet: null };
}
