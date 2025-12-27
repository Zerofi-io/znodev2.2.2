import { runtimeConfig } from '../../core/runtime-config.js';
import { withRetry } from '../../core/retry.js';
export async function withRetryForNode(node, fn, options = {}) {
  const retryConfig = runtimeConfig.getRetryConfig(options);
  const result = await withRetry(fn, {
    ...retryConfig,
    ...options,
    cleanup: async (err, attempt) => {
      if (node._activeClusterId) {
        await node._cleanupClusterAttempt(node._activeClusterId);
      }
      if (options.cleanup) await options.cleanup(err, attempt);
    },
  });
  if (result.success) return result.value;
  throw result.error || new Error('Retry exhausted');
}
