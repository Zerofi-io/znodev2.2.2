import { resolveRuntimeConfig } from '../core/runtime-config.js';
const resolved = resolveRuntimeConfig(process.env);
export const TEST_MODE = resolved.TEST_MODE;
export const DRY_RUN = resolved.DRY_RUN;
export const EFFECTIVE_RPC_URL = resolved.EFFECTIVE_RPC_URL;
export const usedDemoRpc = resolved.usedDemoRpc;
export default resolved;
