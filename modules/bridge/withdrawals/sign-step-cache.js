export function clearLocalSignStepCacheForWithdrawal(node, withdrawalTxHash) {
  if (!node || !withdrawalTxHash) return;
  const prefix = `${String(withdrawalTxHash || '').toLowerCase()}:`;
  if (node._signStepPostCache) {
    for (const k of node._signStepPostCache.keys()) {
      if (typeof k === 'string' && k.startsWith(prefix)) {
        node._signStepPostCache.delete(k);
      }
    }
  }
  if (node._handledSignSteps) {
    for (const k of Array.from(node._handledSignSteps)) {
      if (typeof k === 'string' && k.startsWith(prefix)) {
        node._handledSignSteps.delete(k);
      }
    }
  }
  if (node._withdrawalExpectedSignSteps) {
    for (const k of node._withdrawalExpectedSignSteps.keys()) {
      if (typeof k === 'string' && k.startsWith(prefix)) {
        node._withdrawalExpectedSignSteps.delete(k);
      }
    }
  }
  if (node._withdrawalSignStepResults) {
    for (const k of node._withdrawalSignStepResults.keys()) {
      if (typeof k === 'string' && k.startsWith(prefix)) {
        node._withdrawalSignStepResults.delete(k);
      }
    }
  }
}
