import { spawn } from 'child_process';
import crypto from 'crypto';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { getMoneroWalletDir } from '../wallet-dir.js';
async function _spawnAndCapture(cmd, args, { cwd, env, stdin, timeoutMs } = {}) {
  return await new Promise((resolve) => {
    const child = spawn(cmd, args, {
      cwd,
      env,
      stdio: ['pipe', 'pipe', 'pipe'],
    });
    let stdout = '';
    let stderr = '';
    let settled = false;
    const finish = (exitCode) => {
      if (settled) return;
      settled = true;
      resolve({ stdout, stderr, exitCode });
    };
    const timer = timeoutMs
      ? setTimeout(() => {
          try {
            child.kill('SIGKILL');
          } catch {}
        }, timeoutMs)
      : null;
    child.stdout.on('data', (d) => {
      stdout += d.toString();
    });
    child.stderr.on('data', (d) => {
      stderr += d.toString();
    });
    child.on('error', (err) => {
      if (timer) clearTimeout(timer);
      stderr += (stderr ? '\n' : '') + (err && err.message ? err.message : String(err));
      finish(-1);
    });
    child.on('close', (code) => {
      if (timer) clearTimeout(timer);
      finish(typeof code === 'number' ? code : -1);
    });
    if (typeof stdin === 'string' && child.stdin) {
      child.stdin.write(stdin);
      child.stdin.end();
    } else {
      try {
        child.stdin.end();
      } catch {}
    }
  });
}
async function _enableWithMoneroWalletCliPty({
  cliPath,
  walletDir,
  walletFile,
  passwordFile,
  daemonAddress,
  daemonLogin,
  timeoutMs,
}) {
  const scriptPath = path.join(
    path.dirname(fileURLToPath(import.meta.url)),
    'multisig-experimental-pty.py',
  );
  const innerTimeoutMs = Number.isFinite(timeoutMs) && timeoutMs > 0 ? timeoutMs : 30000;
  const outerTimeoutMs = innerTimeoutMs + 15000;
  const env = {
    ...process.env,
    ZNODE_MWCLI_PATH: cliPath,
    ZNODE_MWCLI_WALLET_FILE: walletFile,
    ZNODE_MWCLI_PASSWORD_FILE: passwordFile,
    ZNODE_MWCLI_DAEMON_ADDRESS: daemonAddress || '',
    ZNODE_MWCLI_DAEMON_LOGIN: daemonLogin || '',
    ZNODE_MWCLI_TIMEOUT_SEC: String(Math.max(5, Math.floor(innerTimeoutMs / 1000))),
  };
  return await _spawnAndCapture('python3', [scriptPath], { cwd: walletDir, env, timeoutMs: outerTimeoutMs });
}
export async function enableMoneroMultisigExperimentalForWallet(node, walletFile, label) {
  const walletDir = getMoneroWalletDir();
  let cliPath = process.env.MONERO_WALLET_CLI || 'monero-wallet-cli';
  if (cliPath && path.isAbsolute(cliPath) && !fs.existsSync(cliPath)) {
    const fallback = 'monero-wallet-cli';
    if (cliPath !== fallback) cliPath = fallback;
  }
  if (cliPath && path.isAbsolute(cliPath)) {
    try {
      const real = fs.realpathSync(cliPath);
      fs.accessSync(real, fs.constants.X_OK);
    } catch {
      const fallback = 'monero-wallet-cli';
      if (cliPath !== fallback) cliPath = fallback;
    }
  }
  if (!walletFile || !node.moneroPassword) {
    return false;
  }
  const display = label || `wallet ${walletFile}`;
  console.log(`  Ensuring Monero multisig experimental mode is enabled for ${display}...`);
  try {
    await node._walletMutex.withLock(async () => {
      try {
        await node._moneroRawCall('close_wallet', {}, 180000);
      } catch {}
    });
  } catch {}
  const passwordFile = path.join(
    walletDir,
    `.znode_msig_pw_${process.pid}_${Date.now()}_${crypto.randomBytes(8).toString('hex')}`,
  );
  try {
    fs.writeFileSync(passwordFile, `${node.moneroPassword}\n`, { mode: 0o600 });
  } catch (e) {
    console.log(
      '  [WARN] Failed to create temporary password file for monero-wallet-cli:',
      e.message || String(e),
    );
    return false;
  }
  let ok = true;
  try {
    const daemonAddress = process.env.MONERO_DAEMON_ADDRESS || '';
    const daemonLogin = process.env.MONERO_DAEMON_LOGIN || '';
    const timeoutMs = Number(process.env.MONERO_WALLET_CLI_TIMEOUT_MS || 45000);

    const { stdout, stderr, exitCode } = await _enableWithMoneroWalletCliPty({
      cliPath,
      walletDir,
      walletFile,
      passwordFile,
      daemonAddress,
      daemonLogin,
      timeoutMs,
    });

    const transcript = (stdout || '') + (stderr || '');
    if (exitCode !== 0) {
      ok = false;
      console.log(
        '  [WARN] monero-wallet-cli failed while enabling multisig experimental mode (PTY):',
      );
      console.log(transcript || `(no output, exit code ${exitCode})`);
    } else {
      const checkArgs = [
        '--wallet-file',
        walletFile,
        '--password-file',
        passwordFile,
        '--offline',
        '--command',
        'set',
      ];
      const check = await _spawnAndCapture(cliPath, checkArgs, {
        cwd: walletDir,
        env: process.env,
        timeoutMs: Math.max(15000, timeoutMs),
      });
      const checkOut = (check.stdout || '') + (check.stderr || '');
      const enabled = /enable-multisig-experimental\s*=\s*(1|true)/i.test(checkOut);
      if (!enabled || check.exitCode !== 0) {
        ok = false;
        console.log(
          '  [WARN] monero-wallet-cli did not confirm enable-multisig-experimental=1 after attempt:',
        );
        console.log(checkOut || `(no output, exit code ${check.exitCode})`);
      } else {
        console.log(
          `  [OK] Enabled multisig experimental mode for wallet ${walletFile} using ${cliPath}`,
        );
      }
    }
  } catch (e) {
    ok = false;
    console.log(
      '  [WARN] Failed to auto-enable multisig experimental mode via monero-wallet-cli:',
      e.message || String(e),
    );
  } finally {
    try {
      fs.unlinkSync(passwordFile);
    } catch {}
  }
  try {
    await node._walletMutex.withLock(async () => {
      try {
        await node._moneroRawCall(
          'open_wallet',
          { filename: walletFile, password: node.moneroPassword },
          180000,
        );
      } catch (e) {
        console.log(
          '  [WARN] Failed to reopen wallet after multisig flag attempt:',
          e.message || String(e),
        );
      }
    });
  } catch (e) {
    console.log(
      '  [WARN] Wallet mutex error while reopening wallet after multisig flag attempt:',
      e.message || String(e),
    );
  }
  return ok;
}
export async function autoEnableMoneroMultisigExperimental(node) {
  const walletFile = node.baseWalletName;
  if (!walletFile || !node.moneroPassword) {
    return;
  }
  await enableMoneroMultisigExperimentalForWallet(node, walletFile, `base wallet ${walletFile}`);
}
