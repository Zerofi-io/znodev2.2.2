import { scheduleWithdrawalStateSave } from './pending-store.js';
import {
  WITHDRAWAL_EVENT_QUERY_CHUNK_BLOCKS,
  WITHDRAWAL_EVENT_REORG_BUFFER_BLOCKS,
  WITHDRAWAL_MIN_CONFIRMATIONS,
} from './config.js';
export async function pollWithdrawalEvents(
  node,
  {
    handleBurnEvent,
    handleSlotReservedEvent,
    handleWithdrawalRequeuedEvent,
  } = {},
) {
  if (typeof handleBurnEvent !== 'function') {
    throw new TypeError('pollWithdrawalEvents requires handleBurnEvent');
  }
  if (typeof handleWithdrawalRequeuedEvent !== 'function') {
    throw new TypeError('pollWithdrawalEvents requires handleWithdrawalRequeuedEvent');
  }
  if (!node || !node.bridge || !node.provider) return;
  if (!node._activeClusterId) return;
  let head;
  try {
    head = await node.provider.getBlockNumber();
  } catch (e) {
    console.log('[Withdrawal] Failed to fetch current block for TokensBurned poll:', e.message || String(e));
    return;
  }
  if (typeof head !== 'number' || !Number.isFinite(head) || head < 0) return;
  const safeHead = WITHDRAWAL_MIN_CONFIRMATIONS > 0 ? head - WITHDRAWAL_MIN_CONFIRMATIONS : head;
  if (safeHead < 0) return;
  let last =
    typeof node._withdrawalLastScanBlock === 'number' && Number.isFinite(node._withdrawalLastScanBlock)
      ? node._withdrawalLastScanBlock
      : safeHead;
  if (last > safeHead) last = safeHead;
  let fromBlock = last - WITHDRAWAL_EVENT_REORG_BUFFER_BLOCKS;
  if (!Number.isFinite(fromBlock) || fromBlock < 0) fromBlock = 0;
  const toBlock = safeHead;
  if (toBlock < fromBlock) return;
  const chunkSize =
    Number.isFinite(WITHDRAWAL_EVENT_QUERY_CHUNK_BLOCKS) && WITHDRAWAL_EVENT_QUERY_CHUNK_BLOCKS > 0
      ? Math.floor(WITHDRAWAL_EVENT_QUERY_CHUNK_BLOCKS)
      : 500;
  const burnFilter = node.bridge.filters.TokensBurned();
  const requeueFilter =
    node.bridge.filters && typeof node.bridge.filters.WithdrawalRequeued === 'function'
      ? node.bridge.filters.WithdrawalRequeued()
      : null;
  const slotReservedFilter =
    node.bridge.filters && typeof node.bridge.filters.SlotReserved === 'function'
      ? node.bridge.filters.SlotReserved()
      : null;
  for (let startBlock = fromBlock; startBlock <= toBlock; startBlock += chunkSize) {
    const endBlock = Math.min(toBlock, startBlock + chunkSize - 1);
    let burnEvents;
    try {
      burnEvents = await node.bridge.queryFilter(burnFilter, startBlock, endBlock);
    } catch (e) {
      const msg = e && e.message ? e.message : String(e);
      const code = e && (e.code || (e.error && e.error.code));
      if (code == -32000 || msg.includes('could not coalesce error')) {
        console.log('[Withdrawal] Provider log query transient error while polling TokensBurned:', msg);
        return;
      }
      console.log('[Withdrawal] Failed to poll TokensBurned events:', msg);
      return;
    }
    let requeueEvents = [];
    if (requeueFilter) {
      try {
        requeueEvents = await node.bridge.queryFilter(requeueFilter, startBlock, endBlock);
      } catch (e) {
        const msg = e && e.message ? e.message : String(e);
        const code = e && (e.code || (e.error && e.error.code));
        if (code == -32000 || msg.includes('could not coalesce error')) {
          console.log('[Withdrawal] Provider log query transient error while polling WithdrawalRequeued:', msg);
          return;
        }
        console.log('[Withdrawal] Failed to poll WithdrawalRequeued events:', msg);
        return;
      }
    }
    let slotReservedEvents = [];
    if (slotReservedFilter) {
      try {
        slotReservedEvents = await node.bridge.queryFilter(slotReservedFilter, startBlock, endBlock);
      } catch (e) {
        const msg = e && e.message ? e.message : String(e);
        const code = e && (e.code || (e.error && e.error.code));
        if (code == -32000 || msg.includes('could not coalesce error')) {
          console.log('[Bribe] Provider log query transient error while polling SlotReserved:', msg);
          return;
        }
        console.log('[Bribe] Failed to poll SlotReserved events:', msg);
        return;
      }
    }
    if (burnEvents && burnEvents.length) {
      for (const event of burnEvents) {
        try {
          await handleBurnEvent(node, event);
        } catch (e) {
          console.log('[Withdrawal] Error handling TokensBurned event (poll):', e.message || String(e));
        }
      }
    }
    if (requeueEvents && requeueEvents.length) {
      for (const event of requeueEvents) {
        try {
          await handleWithdrawalRequeuedEvent(node, event);
        } catch (e) {
          console.log('[Withdrawal] Error handling WithdrawalRequeued event (poll):', e.message || String(e));
        }
      }
    }
    if (slotReservedEvents && slotReservedEvents.length && typeof handleSlotReservedEvent === 'function') {
      for (const event of slotReservedEvents) {
        try {
          await handleSlotReservedEvent(node, event);
        } catch (e) {
          console.log('[Bribe] Error handling SlotReserved event (poll):', e.message || String(e));
        }
      }
    }
    node._withdrawalLastScanBlock = endBlock;
    scheduleWithdrawalStateSave(node);
  }
}
