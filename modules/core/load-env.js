import dotenv from 'dotenv';
import { fileURLToPath } from 'url';
import { dirname } from 'path';
import path from 'path';
const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const projectRoot = path.resolve(__dirname, '..', '..');
const envPath = path.join(projectRoot, '.env');
const dotenvResult = dotenv.config({ path: envPath });
if (dotenvResult.error) {
  console.error('[ERROR] Failed to load .env file:', dotenvResult.error.message);
  console.error('Please ensure .env file exists. Run scripts/setup.sh to create it.\n');
  process.exit(1);
}
export { envPath };
export default dotenvResult;
