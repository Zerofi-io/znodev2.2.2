import { execFile } from 'child_process';
import crypto from 'crypto';
import fs from 'fs';
import path from 'path';
import { promisify } from 'util';
const execFileAsync = promisify(execFile);
export async function prepareMultisig(node, projectRoot) {
  console.log('\n' + '[INFO] Preparing multisig...');
  try {
    const info = await node.monero.prepareMultisig();
    node.multisigInfo = info;
    console.log('[OK] Multisig info generated');
    const infoHash = crypto
      .createHash('sha256')
      .update(node.multisigInfo)
      .digest('hex')
      .slice(0, 10);
    console.log(`  Info hash: ${infoHash}... (length: ${node.multisigInfo.length})`);
    return node.multisigInfo;
  } catch (error) {
    if (error.message && error.message.toLowerCase().includes('already multisig')) {
      console.log(
        '  Wallet is already multisig from old deployment. Creating a fresh base wallet...',
      );
      try {
        try {
          await node.monero.closeWallet();
        } catch {}
        await new Promise((r) => setTimeout(r, 500));
        const canonicalBase =
          process.env.MONERO_WALLET_NAME || `znode_${node.wallet.address.slice(2, 10)}`;
        const suffix = Math.floor(Date.now() / 1000)
          .toString(36)
          .slice(-4);
        node.baseWalletName = `${canonicalBase}_b${suffix}`;
        await node.monero.createWallet(node.baseWalletName, node.moneroPassword);
        console.log(`  [OK] New base wallet created: ${node.baseWalletName}`);
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
          console.log('[WARN]  Base wallet backup failed:', backupErr.message || String(backupErr));
        }
        const info2 = await node.monero.prepareMultisig();
        node.multisigInfo = info2;
        console.log('[OK] Multisig info generated');
        const infoHash2 = crypto
          .createHash('sha256')
          .update(node.multisigInfo)
          .digest('hex')
          .slice(0, 10);
        console.log(`  Info hash: ${infoHash2}... (length: ${node.multisigInfo.length})`);
        return node.multisigInfo;
      } catch (e) {
        console.error('[ERROR] Failed to create fresh base wallet:', e.message);
        throw e;
      }
    }
    console.error('[ERROR] prepare_multisig failed:', error.message);
    throw error;
  }
}
export async function makeMultisig(node, multisigInfos, threshold) {
  console.log(
    `\n[INFO] Stage 1: make_multisig for ${threshold}-of-${multisigInfos.length + 1} multisig (intermediate wallet, finalization pending)...`,
  );
  try {
    const result = await node.monero.call(
      'make_multisig',
      {
        multisig_info: multisigInfos,
        threshold: threshold,
        password: node.moneroPassword,
      },
      180000,
    );
    console.log('[OK] make_multisig RPC succeeded (intermediate multisig wallet; not final yet)');
    console.log(`  Intermediate address (not final): ${result.address}`);
    return result;
  } catch (error) {
    console.error('[ERROR] make_multisig failed:', error.message);
    if (error.message && error.message.toLowerCase().includes('already multisig')) {
      try {
        console.log(
          '  Wallet is already multisig from old deployment. Rotating base wallet for next attempt...',
        );
        try {
          await node.monero.closeWallet();
        } catch {}
        await new Promise((r) => setTimeout(r, 500));
        const suffix = Math.floor(Date.now() / 1000)
          .toString(36)
          .slice(-4);
        node.baseWalletName = `${node.baseWalletName}_b${suffix}`;
        await node.monero.createWallet(node.baseWalletName, node.moneroPassword);
        console.log(`  [OK] New base wallet created for next attempt: ${node.baseWalletName}`);
        try {
          await node._enableMoneroMultisigExperimentalForWallet(
            node.baseWalletName,
            `rotated base wallet ${node.baseWalletName}`,
          );
        } catch (e) {
          console.log(
            '  [WARN] Failed to enable multisig experimental mode on rotated base wallet:',
            e.message || String(e),
          );
        }
        node.multisigInfo = null;
      } catch (rotateErr) {
        console.log(
          '  [WARN] Failed to rotate base wallet after make_multisig already-multisig error:',
          rotateErr.message || String(rotateErr),
        );
      }
    }
    throw error;
  }
}
export default {
  prepareMultisig,
  makeMultisig,
};
