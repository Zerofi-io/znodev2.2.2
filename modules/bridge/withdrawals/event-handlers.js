import { ethers } from 'ethers';
import { isValidMoneroAddress } from '../bridge-service.js';
import { normalizeEthersEvent, describeEventForDebug } from './event-utils.js';
import { scheduleWithdrawalStateSave } from './pending-store.js';
import { markWithdrawalProcessedInMemory } from './processed-tracking.js';
import { parseAtomicAmount } from './amounts.js';
import { getBridgeAddressLower } from './bridge-utils.js';
import { onSlotReservedEvent } from './bribe-tracking.js';
import { CARRYOVER_STATUS, addToCarryover, PRIORITY_CLASS } from './carryover.js';
import { isClusterOperational } from '../../core/health.js';
import { withMoneroLock } from '../monero-lock.js';
import {
  WITHDRAWAL_MAX_XMR,
  WITHDRAWAL_MAX_XMR_ATOMIC,
  WITHDRAWAL_STRICT_XMR_ADDRESS,
  ZXMR_DECIMALS,
} from './config.js';
export function createWithdrawalEventHandlers({ queueWithdrawalForEpoch } = {}) {
  if (typeof queueWithdrawalForEpoch !== 'function') {
    throw new TypeError('createWithdrawalEventHandlers requires queueWithdrawalForEpoch');
  }
  async function handleBurnEvent(node, event) {
    try {
      const normalized = normalizeEthersEvent(event);
      if (!normalized) {
        console.log('[Withdrawal] Burn event missing expected fields after normalization, skipping');
        console.log('[Withdrawal] Burn event debug:', describeEventForDebug(event));
        return;
      }
      const { args, txHash, blockNumber } = normalized;
      node._processedWithdrawals = node._processedWithdrawals || new Set();
      const key = txHash.toLowerCase();
      if (node._processedWithdrawals.has(key)) return;
      node._pendingWithdrawals = node._pendingWithdrawals || new Map();
      if (node._pendingWithdrawals.has(key)) return;
      for (const [, data] of node._pendingWithdrawals) {
        if (!data || !data.isBatch) continue;
        const hashes = Array.isArray(data.ethTxHashes) ? data.ethTxHashes : null;
        if (hashes && hashes.includes(key)) return;
      }
      const activeClusterId = node._activeClusterId;
      if (!activeClusterId) {
        console.log('[Withdrawal] No activeClusterId set, skipping burn event');
        return;
      }
      const user = args[0] ?? args.user;
      const clusterIdFromEvent = args[1] ?? args.clusterId;
      const xmrAddress = args[2] ?? args.xmrAddress;
      const amount = args[3] ?? args.burnAmount ?? args.amount;
      const fee = args[4] ?? args.fee ?? 0n;
      const usedReservation = args[5] ?? args.usedReservation ?? false;
      if (amount == null) {
        console.log('[Withdrawal] Burn event missing amount, skipping');
        markWithdrawalProcessedInMemory(node, key);
        return;
      }
      console.log('[Withdrawal] TokensBurned event detected:');
      console.log(`  TX: ${txHash}`);
      console.log(`  User: ${user}`);
      console.log(`  Cluster ID: ${clusterIdFromEvent}`);
      console.log(`  XMR Address: ${xmrAddress}`);
      try {
        console.log(`  Amount: ${ethers.formatUnits(amount, ZXMR_DECIMALS)} zXMR`);
      } catch (e) {
        console.log('[Withdrawal] Failed to format amount for logging:', e.message || String(e));
      }
      try {
        console.log(`  Fee: ${ethers.formatUnits(fee, ZXMR_DECIMALS)} zXMR`);
      } catch (e) {
        console.log('[Withdrawal] Failed to format fee for logging:', e.message || String(e));
      }
      if (
        typeof clusterIdFromEvent === 'string' &&
        typeof activeClusterId === 'string' &&
        clusterIdFromEvent.toLowerCase() !== activeClusterId.toLowerCase()
      ) {
        console.log(
          `[Withdrawal] Burn event clusterId mismatch (event=${clusterIdFromEvent}, local=${activeClusterId}), skipping`,
        );
        markWithdrawalProcessedInMemory(node, key);
        return;
      }
      if (!isValidMoneroAddress(xmrAddress)) {
        console.log(`[Withdrawal] Invalid Monero address, skipping: ${xmrAddress}`);
        markWithdrawalProcessedInMemory(node, key);
        return;
      }
      if (WITHDRAWAL_STRICT_XMR_ADDRESS && node.monero) {
        try {
          const result = await withMoneroLock(node, { key: node._withdrawalMultisigLock, op: 'withdrawal_validate_address', timeoutMs: 10000 }, async () => {
            return await node.monero.call('validate_address', { address: xmrAddress, any_net_type: true }, 10000);
          });
          if (result && result.valid === false) {
            console.log(`[Withdrawal] validate_address rejected address, skipping: ${xmrAddress}`);
            markWithdrawalProcessedInMemory(node, key);
            return;
          }
        } catch (e) {
          console.log('[Withdrawal] validate_address failed:', e.message || String(e));
        }
      }
      const burnAmountAtomic = parseAtomicAmount(amount);
      if (burnAmountAtomic == null || burnAmountAtomic <= 0n) {
        console.log(`[Withdrawal] Burn amount ${amount.toString()} is invalid; skipping`);
        markWithdrawalProcessedInMemory(node, key);
        return;
      }
      const xmrAtomicAmount = burnAmountAtomic;
      if (xmrAtomicAmount <= 0n) {
        console.log(
          `[Withdrawal] Burn amount ${amount.toString()} is too small to produce a positive XMR amount; skipping`,
        );
        markWithdrawalProcessedInMemory(node, key);
        return;
      }
      const withdrawal = {
        txHash,
        user,
        xmrAddress,
        zxmrAmount: amount.toString(),
        xmrAmount: xmrAtomicAmount.toString(),
        xmrAmountAtomic: xmrAtomicAmount,
        priorityClass: usedReservation ? PRIORITY_CLASS.BRIBE : PRIORITY_CLASS.STANDARD,
        blockNumber,
        timestamp: Date.now(),
      };
      if (WITHDRAWAL_MAX_XMR_ATOMIC > 0n && xmrAtomicAmount > WITHDRAWAL_MAX_XMR_ATOMIC) {
        node._pendingWithdrawals.set(key, {
          ...withdrawal,
          status: 'failed_permanent',
          permanentFailure: true,
          error: 'withdrawal_amount_exceeds_limit',
          limitXmr: WITHDRAWAL_MAX_XMR,
        });
        scheduleWithdrawalStateSave(node);
        return;
      }
      const clusterStatus = await isClusterOperational(node);
      if (!clusterStatus.operational) {
        console.log(`[Withdrawal] Cluster not operational: ${clusterStatus.reason}, deferring withdrawal to carryover`);
        addToCarryover(node, withdrawal, CARRYOVER_STATUS.WAITING_CLUSTER, node._withdrawalEpoch ? node._withdrawalEpoch.epochIndex : null);
        return;
      }
      await queueWithdrawalForEpoch(node, withdrawal);
    } catch (e) {
      console.log('[Withdrawal] Error handling burn event:', e.message || String(e));
    }
  }
  async function handleSlotReservedEvent(node, event) {
    try {
      const normalized = normalizeEthersEvent(event);
      if (!normalized) {
        console.log('[Bribe] SlotReserved event missing expected fields, skipping');
        return;
      }
      const { args, txHash } = normalized;
      const targetEpoch = args[0] ?? args.targetEpoch;
      const user = args[1] ?? args.user;
      const slotIndex = args[2] ?? args.slotIndex;
      const zfiCost = args[3] ?? args.zfiCost;
      if (targetEpoch == null || !user) {
        console.log('[Bribe] SlotReserved event missing required fields, skipping');
        return;
      }
      const epochNum = typeof targetEpoch === 'bigint' ? Number(targetEpoch) : Number(targetEpoch);
      if (!Number.isFinite(epochNum) || epochNum < 0 || !Number.isInteger(epochNum)) {
        console.log(`[Bribe] SlotReserved invalid epoch (${String(targetEpoch)}), skipping`);
        return;
      }
      const tracked = await onSlotReservedEvent(node, epochNum, user, slotIndex, txHash);
      if (tracked) {
        console.log(`[Bribe] SlotReserved: epoch=${epochNum} user=${user} slot=${slotIndex} cost=${zfiCost} tx=${txHash}`);
      }
    } catch (e) {
      console.log('[Bribe] Error handling SlotReserved event:', e.message || String(e));
    }
  }
  async function handleWithdrawalRequeuedEvent(node, event) {
    try {
      if (!node || !node.provider || !node.bridge) return;
      const normalized = normalizeEthersEvent(event);
      if (!normalized) {
        console.log('[Withdrawal] WithdrawalRequeued event missing expected fields after normalization, skipping');
        console.log('[Withdrawal] WithdrawalRequeued event debug:', describeEventForDebug(event));
        return;
      }
      const { args } = normalized;
      const burnTxHash = args[0] ?? args.burnTxHash;
      const caller = args[1] ?? args.caller;
      const burnHash = burnTxHash ? String(burnTxHash).toLowerCase() : '';
      const callerLc = caller ? String(caller).toLowerCase() : '';
      if (!burnHash || !/^0x[0-9a-f]{64}$/.test(burnHash) || !callerLc) return;
      node._processedWithdrawals = node._processedWithdrawals || new Set();
      if (node._processedWithdrawals.has(burnHash)) {
        console.log(`[Withdrawal] Requeue rejected: ${burnHash.slice(0, 18)} already processed`);
        return;
      }
      node._pendingWithdrawals = node._pendingWithdrawals || new Map();
      const existingPending = node._pendingWithdrawals.get(burnHash);
      if (existingPending && existingPending.status !== CARRYOVER_STATUS.ORPHANED) {
        console.log(`[Withdrawal] Requeue rejected: ${burnHash.slice(0, 18)} status is ${existingPending.status}, not orphaned`);
        return;
      }
      for (const [, data] of node._pendingWithdrawals) {
        if (!data || !data.isBatch) continue;
        const hashes = Array.isArray(data.ethTxHashes) ? data.ethTxHashes : null;
        if (hashes && hashes.includes(burnHash)) {
          console.log(`[Withdrawal] Requeue rejected: ${burnHash.slice(0, 18)} is part of active batch`);
          return;
        }
      }
      let receipt = null;
      try {
        receipt = await node.provider.getTransactionReceipt(burnHash);
      } catch {
        return;
      }
      const logs = receipt && Array.isArray(receipt.logs) ? receipt.logs : [];
      if (logs.length === 0) return;
      const bridgeAddr = getBridgeAddressLower(node);
      let burn = null;
      for (const log of logs) {
        if (bridgeAddr) {
          const addr = log && log.address ? String(log.address).toLowerCase() : '';
          if (!addr || addr !== bridgeAddr) continue;
        }
        let parsed;
        try {
          parsed = node.bridge.interface.parseLog(log);
        } catch {
          continue;
        }
        if (!parsed || parsed.name !== 'TokensBurned') continue;
        const bargs = parsed.args || [];
        const user = bargs[0] ?? bargs.user;
        const clusterId = bargs[1] ?? bargs.clusterId;
        const xmrAddress = bargs[2] ?? bargs.xmrAddress;
        const amount = bargs[3] ?? bargs.burnAmount ?? bargs.amount;
        const fee = bargs[4] ?? bargs.fee ?? 0n;
        const usedReservation = bargs[5] ?? bargs.usedReservation ?? false;
        const userLc = user ? String(user).toLowerCase() : '';
        if (!userLc || userLc !== callerLc) continue;
        burn = { user, clusterId, xmrAddress, amount, fee, usedReservation };
        break;
      }
      if (!burn) {
        console.log(`[Withdrawal] Requeue rejected: ${burnHash.slice(0, 18)} no matching TokensBurned event for caller`);
        return;
      }
      if (existingPending) {
        console.log(`[Withdrawal] Reprocessing orphaned withdrawal ${burnHash.slice(0, 18)}`);
        node._pendingWithdrawals.delete(burnHash);
        scheduleWithdrawalStateSave(node);
      }
      await handleBurnEvent(node, {
        transactionHash: burnHash,
        blockNumber: receipt && receipt.blockNumber != null ? receipt.blockNumber : null,
        args: [burn.user, burn.clusterId, burn.xmrAddress, burn.amount, burn.fee, burn.usedReservation],
      });
    } catch (e) {
      console.log('[Withdrawal] Error handling WithdrawalRequeued event:', e.message || String(e));
    }
  }
  return {
    handleBurnEvent,
    handleSlotReservedEvent,
    handleWithdrawalRequeuedEvent,
  };
}
