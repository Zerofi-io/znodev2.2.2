import { Buffer } from 'node:buffer';
import crypto from 'crypto';
class DigestAuth {
  constructor(username, password) {
    this.username = username;
    this.password = password;
    this._nonceCount = 0;
    this._cachedChallenge = null;
    this._cachedRealm = null;
  }
  _md5(str) {
    return crypto.createHash('md5').update(str).digest('hex');
  }
  _parseChallenge(header) {
    const prefix = /^digest\s+/i;
    const value = header.replace(prefix, '');
    const params = {};
    const regex = /([a-zA-Z0-9_]+)=((\"[^\"]+\")|([^,\s]+))/g;
    let match;
    while ((match = regex.exec(value)) !== null) {
      const key = match[1];
      let val = match[3] || match[4] || '';
      if (val.startsWith('"') && val.endsWith('"')) {
        val = val.slice(1, -1);
      }
      params[key] = val;
    }
    return params;
  }
  _buildDigestHeader(challenge, method, uri) {
    const realm = challenge.realm || '';
    const nonce = challenge.nonce || '';
    const qop = challenge.qop ? challenge.qop.split(',')[0].trim() : 'auth';
    const opaque = challenge.opaque;
    const algorithm = (challenge.algorithm || 'MD5').toUpperCase();
    this._nonceCount++;
    const nc = this._nonceCount.toString(16).padStart(8, '0');
    const cnonce = crypto.randomBytes(16).toString('hex');
    let ha1 = this._md5(`${this.username}:${realm}:${this.password}`);
    if (algorithm === 'MD5-SESS') {
      ha1 = this._md5(`${ha1}:${nonce}:${cnonce}`);
    }
    const ha2 = this._md5(`${method}:${uri}`);
    let response;
    if (qop) {
      response = this._md5(`${ha1}:${nonce}:${nc}:${cnonce}:${qop}:${ha2}`);
    } else {
      response = this._md5(`${ha1}:${nonce}:${ha2}`);
    }
    let header = `Digest username="${this.username}", realm="${realm}", nonce="${nonce}", uri="${uri}", response="${response}"`;
    if (qop) {
      header += `, qop=${qop}, nc=${nc}, cnonce="${cnonce}"`;
    }
    if (opaque) {
      header += `, opaque="${opaque}"`;
    }
    if (algorithm && algorithm !== 'MD5') {
      header += `, algorithm=${algorithm}`;
    }
    return header;
  }
  buildBasicHeader() {
    const credentials = Buffer.from(`${this.username}:${this.password}`).toString('base64');
    return `Basic ${credentials}`;
  }
  processChallenge(wwwAuthHeader) {
    if (!wwwAuthHeader) return null;
    if (/^basic\s+/i.test(wwwAuthHeader)) {
      return { scheme: 'basic', header: this.buildBasicHeader() };
    }
    if (/^digest\s+/i.test(wwwAuthHeader)) {
      const challenge = this._parseChallenge(wwwAuthHeader);
      this._cachedChallenge = challenge;
      this._cachedRealm = challenge.realm;
      this._nonceCount = 0;
      return { scheme: 'digest', challenge };
    }
    return null;
  }
  getAuthHeader(method, uri) {
    if (!this._cachedChallenge) return null;
    return this._buildDigestHeader(this._cachedChallenge, method, uri);
  }
  hasCachedAuth() {
    return this._cachedChallenge !== null;
  }
  invalidateCache() {
    this._cachedChallenge = null;
    this._cachedRealm = null;
    this._nonceCount = 0;
  }
  resetNonceCount() {
    this._nonceCount = 0;
  }
}
export default DigestAuth;
export { DigestAuth };
