import './modules/core/load-env.js';
import './modules/core/console-shim.js';

import preflight from './modules/app/bootstrap/preflight.js';

import ZNode from './modules/app/znode.js';

const isMainModule = import.meta.url === `file://${process.argv[1]}`;

if (isMainModule) {
  preflight();

  const node = new ZNode();

  let shuttingDown = false;
  let restarting = false;
  let startupAttempts = 0;
  let recoveryAttempts = 0;
  let unhandledRejectionCount = 0;
  let unhandledRejectionWindowStart = 0;

  const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
  const backoffMs = (attempt) => {
    const base = 1000;
    const max = 60000;
    const delay = Math.min(max, base * Math.pow(2, Math.min(attempt - 1, 6)));
    const jitter = Math.floor(Math.random() * 1000);
    return delay + jitter;
  };

  const startWithRetry = async () => {
    while (!shuttingDown) {
      startupAttempts += 1;
      try {
        await node.start();
        startupAttempts = 0;
        recoveryAttempts = 0;
        return true;
      } catch (error) {
        const delay = backoffMs(startupAttempts);
        console.error(`[ERROR] Startup attempt ${startupAttempts} failed:`, error?.message || String(error));
        console.log(`[INFO] Retrying startup in ${delay}ms...`);
        await sleep(delay);
      }
    }
    return false;
  };

  const recoverFromFatal = async (source, err) => {
    if (shuttingDown) return;
    const errMsg = err instanceof Error ? err.message : String(err);
    const errStack = err instanceof Error ? err.stack : '';
    const errName = err instanceof Error ? err.name : typeof err;
    console.error(`\n[ERROR] ${source} [${errName}]: ${errMsg}`);
    if (errStack) console.error(errStack);
    if (restarting) return;
    restarting = true;
    try {
      recoveryAttempts += 1;
      const delay = backoffMs(recoveryAttempts);
      try {
        await node.stop();
      } catch (e) {
        console.error('Recovery stop error:', e?.message || String(e));
      }
      if (shuttingDown) return;
      console.log(`[INFO] Restarting in ${delay}ms...`);
      await sleep(delay);
      await startWithRetry();
    } finally {
      restarting = false;
    }
  };

  const shutdown = async (signal) => {
    if (shuttingDown) return;
    shuttingDown = true;
    console.log(`\n[signal] ${signal} received, shutting down...`);
    try {
      await node.stop();
    } catch (e) {
      console.error('Shutdown error:', e?.message || String(e));
    } finally {
      process.exit(0);
    }
  };

  process.on('uncaughtException', (err) => {
    if (err && (err.name === 'StreamStateError' || err.message?.includes('stream that is closed'))) {
      console.warn('[P2P] Stream closed during write (transient, ignoring):', err.message);
      return;
    }
    recoverFromFatal('uncaughtException', err).catch((e) => {
      console.error('Fatal recovery error:', e?.message || String(e));
      process.exit(1);
    });
  });

  process.on('unhandledRejection', (reason, promise) => {
    const reasonMsg = reason instanceof Error ? reason.message : String(reason);
    const reasonStack = reason instanceof Error ? reason.stack : '';
    const reasonName = reason instanceof Error ? reason.name : typeof reason;
    console.error(`Unhandled Promise Rejection [${reasonName}]: ${reasonMsg}`);
    if (reasonStack) console.error(reasonStack);
    console.error('Promise:', promise);

    const now = Date.now();
    if (!unhandledRejectionWindowStart || now - unhandledRejectionWindowStart > 60000) {
      unhandledRejectionWindowStart = now;
      unhandledRejectionCount = 0;
    }
    unhandledRejectionCount += 1;
    if (unhandledRejectionCount >= 3) {
      unhandledRejectionCount = 0;
      recoverFromFatal('unhandledRejection', reason).catch((e) => {
        console.error('Fatal recovery error:', e?.message || String(e));
      });
    }
  });

  process.on('SIGINT', () => shutdown('SIGINT'));
  process.on('SIGTERM', () => shutdown('SIGTERM'));

  startWithRetry().catch((error) => {
    console.error('[ERROR] Fatal error:', error?.message || String(error));
    process.exit(1);
  });
}

export default ZNode;
