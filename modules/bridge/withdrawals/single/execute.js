import { ethers } from 'ethers';
import { scheduleWithdrawalStateSave } from '../pending-store.js';
import { formatXmrAtomic } from '../amounts.js';
import { clearLocalSignStepCacheForWithdrawal } from '../sign-step-cache.js';
import { computeTxsetHash, putTxset } from '../txset-store.js';
import {
  REQUIRED_WITHDRAWAL_SIGNATURES,
  WITHDRAWAL_MAX_XMR,
  WITHDRAWAL_MAX_XMR_ATOMIC,
} from '../config.js';
import { runUnsignedWithdrawalTxConsensus } from '../consensus.js';
import { deriveRound } from '../pbft-utils.js';
import { getCanonicalSignerSet } from '../signer-set.js';
import { runWithdrawalMultisigSync } from '../signing/multisig-sync.js';
import { runSingleCoordinatorSignAndSubmit } from './coordinator-sign-submit.js';
import { withMoneroLock } from '../../monero-lock.js';

function normalizeAttempt(v) {
  const n = Number(v);
  if (!Number.isFinite(n) || n < 0) return 0;
  return Math.floor(n);
}

export async function executeWithdrawal(node, withdrawal) {
  console.log(`[Withdrawal] Executing withdrawal to ${withdrawal.xmrAddress}`);
  console.log(`  Amount: ${formatXmrAtomic(withdrawal.xmrAmountAtomic != null ? withdrawal.xmrAmountAtomic : withdrawal.xmrAmount)} XMR`);
  node._pendingWithdrawals = node._pendingWithdrawals || new Map();
  const key = withdrawal.txHash.toLowerCase();
  const existing0 = node._pendingWithdrawals.get(key);
  const firstAttemptAt =
    existing0 && typeof existing0.firstAttemptAt === 'number'
      ? existing0.firstAttemptAt
      : existing0 && typeof existing0.startedAt === 'number'
        ? existing0.startedAt
        : Date.now();
  const prevAttempt = existing0 && typeof existing0.attempt !== 'undefined' ? normalizeAttempt(existing0.attempt) : 0;
  node._pendingWithdrawals.set(key, {
    ...(existing0 && typeof existing0 === 'object' ? existing0 : {}),
    ...withdrawal,
    status: 'pending',
    startedAt: Date.now(),
    firstAttemptAt,
    attempt: prevAttempt,
  });
  const existingLock = node._withdrawalMultisigLock;
  if (existingLock && existingLock !== key) {
    const pending = node._pendingWithdrawals.get(key);
    if (pending) {
      pending.status = 'failed';
      pending.error = 'withdrawal_lock_active';
    }
    scheduleWithdrawalStateSave(node);
    return;
  }
  const lockAcquired = !existingLock;
  if (lockAcquired) node._withdrawalMultisigLock = key;
  try {
    clearLocalSignStepCacheForWithdrawal(node, key);
    const pending0 = node._pendingWithdrawals.get(key);
    if (pending0) {
      pending0.signerSet = null;
      pending0.txDataHexUnsigned = null;
      pending0.txsetHashUnsigned = null;
      pending0.txsetSizeUnsigned = null;
      pending0.txDataHexSigned = null;
      pending0.txsetHashSigned = null;
      pending0.txsetSizeSigned = null;
      pending0.pbftUnsignedHash = null;
      pending0.pbftSignedHash = null;
    }
    if (WITHDRAWAL_MAX_XMR_ATOMIC > 0n) {
      let amt = withdrawal.xmrAmountAtomic;
      if (typeof amt === 'string') {
        try { amt = BigInt(amt); } catch { amt = 0n; }
      }
      if (typeof amt !== 'bigint') {
        try { amt = BigInt(withdrawal.xmrAmount || '0'); } catch { amt = 0n; }
      }
      if (amt > WITHDRAWAL_MAX_XMR_ATOMIC) {
        const pending = node._pendingWithdrawals.get(key);
        if (pending) {
          pending.status = 'failed_permanent';
          pending.permanentFailure = true;
          pending.error = 'withdrawal_amount_exceeds_limit';
          pending.limitXmr = WITHDRAWAL_MAX_XMR;
        }
        scheduleWithdrawalStateSave(node);
        return;
      }
    }
    const syncResult = await runWithdrawalMultisigSync(node, withdrawal.txHash);
    if (!syncResult || !syncResult.success) {
      console.log(
        '[Withdrawal] Withdrawal multisig sync failed (PBFT-coordinated multisig-info sync):',
        syncResult && syncResult.reason ? syncResult.reason : 'unknown',
      );
      const pending = node._pendingWithdrawals.get(key);
      if (pending) {
        pending.status = 'failed';
        pending.error = `multisig_sync_failed:${(syncResult && syncResult.reason) || 'unknown'}`;
      }
      return;
    }
    console.log(
      `[Withdrawal] Withdrawal multisig sync completed before transfer; importedOutputs=${
        typeof syncResult.importedOutputs === 'number' ? syncResult.importedOutputs : 'unknown'
      }`,
    );
    const pendingAfterSync = node._pendingWithdrawals.get(key);
    if (pendingAfterSync && Array.isArray(syncResult.signerSet) && syncResult.signerSet.length > 0) {
      pendingAfterSync.signerSet = syncResult.signerSet.map((a) => String(a || '').toLowerCase());
    }
    console.log('[Withdrawal] Creating multisig transfer transaction...');
    const destinations = [{ address: withdrawal.xmrAddress, amount: withdrawal.xmrAmountAtomic }];
    let txData;
    try {
      txData = await withMoneroLock(node, { key, op: 'withdrawal_transfer' }, async () => {
        return await node.monero.transfer(destinations);
      });
      console.log('[Withdrawal] Transfer transaction created');
      const pendingAfterTransfer = node._pendingWithdrawals.get(key);
      if (pendingAfterTransfer) {
        const txKey = txData && txData.txKey ? String(txData.txKey) : '';
        pendingAfterTransfer.xmrTxKey = txKey || null;
        scheduleWithdrawalStateSave(node);
      }
      const baseSignerSet =
        pendingAfterSync && Array.isArray(pendingAfterSync.signerSet) && pendingAfterSync.signerSet.length > 0
          ? pendingAfterSync.signerSet.map((a) => String(a || '').toLowerCase())
          : getCanonicalSignerSet(node);
      const signerSet = baseSignerSet;
      if (!signerSet || signerSet.length < REQUIRED_WITHDRAWAL_SIGNATURES) {
        console.log('[Withdrawal] Not enough signers available for PBFT tx commit');
        const pending = node._pendingWithdrawals.get(key);
        if (pending) {
          pending.status = 'failed';
          pending.error = 'not_enough_signers_for_tx_pbft';
        }
        return;
      }
      const txDataHex = txData && txData.txDataHex ? String(txData.txDataHex) : '';
      if (!txDataHex) {
        const pending = node._pendingWithdrawals.get(key);
        if (pending) {
          pending.status = 'failed';
          pending.error = 'no_tx_data_hex';
        }
        return;
      }
      const txsetHash = computeTxsetHash(txDataHex);
      if (!txsetHash) {
        const pending = node._pendingWithdrawals.get(key);
        if (pending) {
          pending.status = 'failed';
          pending.error = 'txset_hash_failed';
        }
        return;
      }
      const stored = putTxset(node, txsetHash, txDataHex);
      if (!stored.ok) {
        const pending = node._pendingWithdrawals.get(key);
        if (pending) {
          pending.status = 'failed';
          pending.error = stored.reason ? `txset_store_failed:${stored.reason}` : 'txset_store_failed';
        }
        return;
      }
      const unsignedPayload = JSON.stringify({
        type: 'withdrawal-unsigned-proposal',
        txHash: key,
        txsetHash,
        txsetSize: txDataHex.length,
        signerSet,
        threshold: REQUIRED_WITHDRAWAL_SIGNATURES,
      });
      const clusterId = node._activeClusterId;
      const sessionId = node._sessionId || 'bridge';
      const unsignedHash = ethers.keccak256(ethers.toUtf8Bytes(unsignedPayload));
      const pendingUnsigned = node._pendingWithdrawals.get(key);
      const attempt = pendingUnsigned && typeof pendingUnsigned.attempt !== 'undefined' ? normalizeAttempt(pendingUnsigned.attempt) : 0;
      const proposalRound = deriveRound('withdrawal-unsigned-proposal', `${key}:${attempt}`);
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
        const pending = node._pendingWithdrawals.get(key);
        if (pending) {
          pending.status = 'failed';
          pending.error = `unsigned_proposal_broadcast_failed:${msg}`;
          pending.attempt = attempt + 1;
          pending.txDataHexUnsigned = null;
          pending.txsetHashUnsigned = null;
          pending.txsetSizeUnsigned = null;
          pending.pbftUnsignedHash = null;
        }
        return;
      }
      const txConsensus = await runUnsignedWithdrawalTxConsensus(node, key, unsignedHash);
      if (!txConsensus || !txConsensus.success) {
        const reason = txConsensus && txConsensus.reason ? txConsensus.reason : 'tx_pbft_failed';
        const pending = node._pendingWithdrawals.get(key);
        if (pending) {
          pending.status = 'failed';
          pending.error = reason;
        }
        return;
      }
    } catch (e) {
      console.log('[Withdrawal] Failed to create transfer:', e.message || String(e));
      const pending = node._pendingWithdrawals.get(key);
      if (pending) {
        pending.status = 'failed';
        pending.error = e.message;
      }
      return;
    }
    const selfAddress = node.wallet && node.wallet.address ? String(node.wallet.address).toLowerCase() : '';
    await runSingleCoordinatorSignAndSubmit(node, { key, txData, selfAddress });
  } catch (e) {
    console.log('[Withdrawal] Withdrawal execution error:', e.message || String(e));
    if (node._pendingWithdrawals) {
      const pending = node._pendingWithdrawals.get(key);
      if (pending) {
        pending.status = 'error';
        pending.error = e.message;
      }
    }
  } finally {
    if (lockAcquired && node._withdrawalMultisigLock === key) {
      node._withdrawalMultisigLock = null;
    }
  }
}
