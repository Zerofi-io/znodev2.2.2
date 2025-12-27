import fs from 'fs';
import os from 'os';
import path from 'path';
let _moneroWalletDir = null;
export function getMoneroWalletDir() {
  if (_moneroWalletDir) return _moneroWalletDir;
  const raw =
    typeof process.env.MONERO_WALLET_DIR === 'string'
      ? process.env.MONERO_WALLET_DIR.trim()
      : '';
  const dir = raw ? raw : path.join(os.homedir(), '.monero-wallets');
  try {
    fs.mkdirSync(dir, { recursive: true, mode: 0o700 });
  } catch {}
  try {
    const st = fs.statSync(dir);
    if ((st.mode & 0o077) !== 0) {
      console.warn(`[Monero] Wallet dir permissions are too open: ${dir}`);
    }
  } catch {}
  _moneroWalletDir = dir;
  return dir;
}
export default getMoneroWalletDir;
