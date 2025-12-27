import { runCoordinatorSignAndSubmit } from '../signing/coordinator-sign-submit.js';
export async function runSingleCoordinatorSignAndSubmit(node, { key, txData, selfAddress } = {}) {
  return runCoordinatorSignAndSubmit(node, { key, txData, selfAddress, mode: 'single' });
}
