export function normalizeEthersEvent(rawEvent) {
  if (!rawEvent) return null;
  const eventLike = rawEvent;
  const maybeLog = eventLike.log;
  const log = maybeLog && typeof maybeLog === 'object' && 'transactionHash' in maybeLog
    ? maybeLog
    : eventLike;
  const args = eventLike.args ?? log.args ?? null;
  const txHash = log && log.transactionHash ? log.transactionHash : null;
  const blockNumber =
    log && (typeof log.blockNumber === 'number' || typeof log.blockNumber === 'bigint')
      ? log.blockNumber
      : null;
  if (!txHash || !args) {
    return null;
  }
  return { log, args, txHash, blockNumber };
}
export function describeEventForDebug(rawEvent) {
  try {
    if (!rawEvent || typeof rawEvent !== 'object') return String(rawEvent);
    const summary = {
      keys: Object.keys(rawEvent),
      hasLog: !!rawEvent.log,
      hasArgs: !!rawEvent.args,
    };
    if (rawEvent.log && typeof rawEvent.log === 'object') {
      summary.logKeys = Object.keys(rawEvent.log);
    }
    return JSON.stringify(summary);
  } catch {
    return '[unserializable event]';
  }
}
