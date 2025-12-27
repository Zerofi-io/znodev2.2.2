import { execFile } from 'child_process';
import fs from 'fs';
import os from 'os';
import path from 'path';
import { promisify } from 'util';
const execFileAsync = promisify(execFile);
export async function backupMultisigWallet(projectRoot) {
  try {
    const backupScript = path.join(projectRoot, 'scripts', 'auto-backup-wallet.sh');
    if (fs.existsSync(backupScript)) {
      let backupPass = process.env.WALLET_BACKUP_PASSPHRASE;
      if (!backupPass) {
        const homeDir = process.env.HOME || process.cwd();
        const defaultPassFile = path.join(homeDir, '.znode-backup', 'wallet_backup_passphrase.txt');
        const passFile = process.env.WALLET_BACKUP_PASSPHRASE_FILE || defaultPassFile;
        if (fs.existsSync(passFile)) {
          backupPass = fs.readFileSync(passFile, 'utf8').trim();
        } else {
          const altPassFile = path.join(os.homedir(), '.znode-backup', 'wallet_backup_passphrase.txt');
          if (altPassFile !== passFile && fs.existsSync(altPassFile)) {
            backupPass = fs.readFileSync(altPassFile, 'utf8').trim();
          } else {
            const localPassFile = path.join(projectRoot, '.znode-backup', 'wallet_backup_passphrase.txt');
            if (
              localPassFile !== passFile &&
              localPassFile !== altPassFile &&
              fs.existsSync(localPassFile)
            ) {
              backupPass = fs.readFileSync(localPassFile, 'utf8').trim();
            }
          }
        }
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
      console.log('[OK] Multisig wallet backed up automatically using auto-backup-wallet.sh');
    } else {
      console.log('[WARN]  auto-backup-wallet.sh not found, skipping automatic backup');
    }
  } catch (backupErr) {
    console.log('[WARN]  Wallet backup failed:', backupErr.message || String(backupErr));
  }
}
