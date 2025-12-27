import ConfigValidator from '../../core/config-validator.js';
import { TEST_MODE, DRY_RUN, EFFECTIVE_RPC_URL, usedDemoRpc } from '../runtime.js';
export function preflight() {
  const validator = new ConfigValidator();
  const validationResult = validator.validate();
  if (!validationResult.valid) {
    validator.printResults();
    console.error(
      '\n[ERROR] Configuration validation failed. Please fix the errors above and try again.',
    );
    console.error('See config/.env.example for configuration documentation.\n');
    process.exit(1);
  }
  if (validationResult.warnings.length > 0) {
    validator.printResults();
  }
  if (TEST_MODE) {
    console.warn('[WARN]  WARNING: TEST_MODE is ENABLED');
    console.warn('[WARN]  This mode uses insecure defaults and should NEVER be used in production!');
    console.warn('[WARN]  Set TEST_MODE=0 for production use.');
  }
  if (DRY_RUN) {
    console.log(
      '[INFO]  DRY_RUN mode enabled: on-chain transactions will be simulated but not sent.',
    );
    console.log('[INFO]  Set DRY_RUN=0 to send real transactions.');
    if (!process.env.DRY_RUN) {
      console.warn(
        '[WARN]  DRY_RUN defaulted to enabled (not explicitly set). Set DRY_RUN=0 for production.',
      );
    }
  }
  if (!EFFECTIVE_RPC_URL) {
    console.error('ERROR: RPC_URL or ETH_RPC_URL is required.');
    console.error('Set RPC_URL=<your-rpc-url> or enable TEST_MODE=1 for testing.');
    process.exit(1);
  }
  if (usedDemoRpc) {
    console.warn('[WARN]  TEST_MODE: Using demo RPC URL (rate-limited, for testing only)');
  }
  if (!process.env.PRIVATE_KEY) {
    console.error('Environment variable PRIVATE_KEY is required to run znode.');
    process.exit(1);
  }
  if (!process.env.MONERO_WALLET_PASSWORD) {
    console.error('ERROR: MONERO_WALLET_PASSWORD is required.');
    console.error('Set MONERO_WALLET_PASSWORD=<password> in your .env file.');
    console.error(
      'SECURITY: Never derive passwords from private keys. Use unique, random passwords.',
    );
    process.exit(1);
  }
  console.log('Using MONERO_WALLET_PASSWORD from environment for Monero wallet.');
}
export default preflight;
