export function onTxManagerPendingChange(node, pendingSize) {
  const intervalMs = node._txManagerStuckIntervalMs;
  if (!Number.isFinite(intervalMs) || intervalMs <= 0) {
    return;
  }
  if (pendingSize > 0) {
    if (!node._txManagerTimer) {
      node._txManagerTimer = setInterval(() => {
        if (!node.txManager || !node.txManager.pending || node.txManager.pending.size === 0) {
          return;
        }
        node.txManager
          .detectAndClearStuckTransactions()
          .catch((e) =>
            console.warn(
              '[TxManager] detectAndClearStuckTransactions error:',
              e.message || String(e),
            ),
          );
      }, intervalMs);
    }
  } else if (node._txManagerTimer) {
    clearInterval(node._txManagerTimer);
    node._txManagerTimer = null;
  }
}
