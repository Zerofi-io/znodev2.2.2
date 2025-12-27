import { getMetrics } from '../../core/p2p-metrics.js';
export function tryParseJSON(jsonString, context = '') {
  try {
    return JSON.parse(jsonString);
  } catch (e) {
    getMetrics().incrementJsonParseFailures();
    console.log(`[Withdrawal] JSON parse failure ${context}: ${e.message}`);
    return null;
  }
}
