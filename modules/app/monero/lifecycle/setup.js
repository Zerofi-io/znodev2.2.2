import { execFile } from 'child_process';
import crypto from 'crypto';
import fs from 'fs';
import path from 'path';
import { promisify } from 'util';
import { autoEnableMoneroMultisigExperimental } from './multisig-experimental.js';
const execFileAsync = promisify(execFile);
export async function setupMonero(node, projectRoot) {
  console.log('[INFO] Setting up Monero with multisig support...');
  try {
    const restoreScript = path.join(projectRoot, 'scripts', 'auto-restore-wallet.sh');
    if (fs.existsSync(restoreScript)) {
      try {
        fs.accessSync(restoreScript, fs.constants.X_OK);
      } catch {
        console.warn('[WARN]  auto-restore-wallet.sh exists but is not executable');
      }
      await execFileAsync(restoreScript, [], {
        cwd: projectRoot,
        timeout: Number(process.env.WALLET_BACKUP_TIMEOUT_MS || 30000),
      });
    }
  } catch (restoreErr) {
    const msg = restoreErr.message || String(restoreErr);
    if (!/exit code 0/i.test(msg)) {
      console.warn('[WARN]  Wallet restore script failed:', msg);
    }
  }
  for (let i = 1; i <= 10; i++) {
    try {
      await node.monero.openWallet(node.baseWalletName, node.moneroPassword);
      console.log(`[OK] Base wallet opened: ${node.baseWalletName}`);
      break;
    } catch (error) {
      if (error.code === 'ECONNREFUSED' && i < 10) {
        console.log(`  Waiting for Monero RPC (attempt ${i}/10)...`);
        await new Promise((r) => setTimeout(r, 3000));
        continue;
      }
      if (error.code === 'ECONNREFUSED') {
        throw new Error('Monero RPC not available');
      }
      console.log('  Creating wallet with password...');
      try {
        await node.monero.createWallet(node.baseWalletName, node.moneroPassword);
        console.log(`[OK] Base wallet created: ${node.baseWalletName}`);
        try {
          const backupScript = path.join(projectRoot, 'scripts', 'auto-backup-wallet.sh');
          if (fs.existsSync(backupScript)) {
            try {
              fs.accessSync(backupScript, fs.constants.X_OK);
            } catch {
              throw new Error('auto-backup-wallet.sh exists but is not executable');
            }
            let backupPass = process.env.WALLET_BACKUP_PASSPHRASE;
            try {
              const homeDir = process.env.HOME || process.cwd();
              const defaultPassFile = path.join(
                homeDir,
                '.znode-backup',
                'wallet_backup_passphrase.txt',
              );
              const passFile = process.env.WALLET_BACKUP_PASSPHRASE_FILE || defaultPassFile;
              const passDir = path.dirname(passFile);
              try {
                fs.mkdirSync(passDir, { recursive: true, mode: 0o700 });
              } catch {}
              if (!backupPass) {
                if (fs.existsSync(passFile)) {
                  backupPass = fs.readFileSync(passFile, 'utf8').trim();
                  if (!backupPass) {
                    console.warn(
                      '[WARN]  Wallet backup passphrase file is empty, regenerating a new one',
                    );
                  } else {
                    console.log('[OK] Loaded existing wallet backup passphrase from disk');
                  }
                }
              }
              if (!backupPass) {
                backupPass = crypto.randomBytes(32).toString('hex');
                fs.writeFileSync(passFile, backupPass + '\n', {
                  mode: 0o600,
                });
                console.log(
                  `[OK] Generated new wallet backup passphrase and saved to ${passFile}`,
                );
              }
            } catch (passErr) {
              console.warn(
                '[WARN]  Failed to initialize wallet backup passphrase:',
                passErr.message || String(passErr),
              );
            }
            const env = { ...process.env };
            if (backupPass) {
              env.WALLET_BACKUP_PASSPHRASE = backupPass;
            }
            await execFileAsync(backupScript, [], {
              cwd: projectRoot,
              timeout: Number(process.env.WALLET_BACKUP_TIMEOUT_MS || 30000),
              env,
            });
            console.log('[OK] Base wallet backed up using auto-backup-wallet.sh');
          } else {
            console.log('[WARN]  auto-backup-wallet.sh not found, skipping automatic backup');
          }
        } catch (backupErr) {
          console.log(
            '[WARN]  Base wallet backup failed:',
            backupErr.message || String(backupErr),
          );
        }
      } catch (e2) {
        const msg = e2 && e2.message ? e2.message : String(e2);
        if (/already exists/i.test(msg) || /timeout/i.test(msg)) {
          console.log('  Create failed or timed out; attempting to open...');
          try {
            await node.monero.openWallet(node.baseWalletName, node.moneroPassword);
            console.log(`[OK] Base wallet opened: ${node.baseWalletName}`);
          } catch (e3) {
            const openMsg = e3 && e3.message ? e3.message : String(e3);
            console.error(`  [ERROR] Wallet exists but cannot open: ${openMsg}`);
            console.error(`  [INFO]  Run ./clean-restart to delete old wallet files and retry`);
            throw new Error(
              `Wallet ${node.baseWalletName} exists but cannot be opened. Run ./clean-restart to reset.`,
            );
          }
        } else {
          throw e2;
        }
      }
      break;
    }
  }
  await autoEnableMoneroMultisigExperimental(node);
}
