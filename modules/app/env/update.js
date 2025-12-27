import fs from 'fs';
import path from 'path';
export function updateEnv(projectRoot, key, value) {
  try {
    const envPath = path.join(projectRoot, '.env');
    let envContent = fs.existsSync(envPath) ? fs.readFileSync(envPath, 'utf8') : '';
    const regex = new RegExp(`^${key}=.*`, 'm');
    if (regex.test(envContent)) {
      envContent = envContent.replace(regex, `${key}=${value}`);
    } else {
      envContent += `\n${key}=${value}`;
    }
    fs.writeFileSync(envPath, envContent, { mode: 0o600 });
    process.env[key] = value;
    console.log(`[OK] Updated .env: ${key}=${value}`);
  } catch (e) {
    console.warn(`[WARN] Failed to update .env: ${e.message}`);
  }
}
