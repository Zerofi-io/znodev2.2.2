import { runCoordinatorSignAndSubmit } from '../signing/coordinator-sign-submit.js';
export async function runBatchCoordinatorSignAndSubmit(node, { key, txData, selfAddress } = {}) {
  return runCoordinatorSignAndSubmit(node, { key, txData, selfAddress, mode: 'batch' });
}
