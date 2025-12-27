import { execSync } from 'child_process';
import { ethers } from 'ethers';
class ConfigValidator {
  constructor() {
    this.errors = [];
    this.warnings = [];
  }
  validate() {
    this.validateRequiredConfig();
    this.validateSecurityConfig();
    this.validateP2PConfig();
    this.validateP2PClientConfig();
    this.validateMoneroConfig();
    this.validateMoneroRpcTimingConfig();
    this.validateMoneroRPCBinary();
    this.validateClusterConfig();
    this.validateContractAddresses();
    this.validateBridgeConfig();
    this.validateBackupConfig();
    this.validateEmergencySweepConfig();
    this.validateCircuitBreakerConfig();
    this.validatePortConflicts();
    this.validateNetworkConsistency();
    this.validateContractAddressChecksums();
    return {
      valid: this.errors.length === 0,
      errors: this.errors,
      warnings: this.warnings,
    };
  }
  validateRequiredConfig() {
    const TEST_MODE = process.env.TEST_MODE === '1';
    if (!process.env.PRIVATE_KEY) {
      this.errors.push('PRIVATE_KEY is required');
    } else if (!process.env.PRIVATE_KEY.startsWith('0x')) {
      this.warnings.push('PRIVATE_KEY should start with 0x');
    } else if (process.env.PRIVATE_KEY.length !== 66) {
      this.warnings.push('PRIVATE_KEY should be 66 characters (0x + 64 hex chars)');
    }
    if (!TEST_MODE && !process.env.RPC_URL && !process.env.ETH_RPC_URL) {
      this.errors.push('RPC_URL or ETH_RPC_URL is required in production mode');
    }
    if (!TEST_MODE && !process.env.MONERO_WALLET_PASSWORD) {
      this.errors.push('MONERO_WALLET_PASSWORD is required in production mode');
    }
  }
  validateSecurityConfig() {
    const TEST_MODE = process.env.TEST_MODE === '1';
    const DRY_RUN = TEST_MODE ? process.env.DRY_RUN !== '0' : process.env.DRY_RUN === '1';
    if (TEST_MODE) {
      this.warnings.push('TEST_MODE is enabled - this should NEVER be used in production!');
    }
    if (!DRY_RUN && TEST_MODE) {
      this.errors.push(
        'Invalid configuration: DRY_RUN=0 with TEST_MODE=1 is inconsistent. Set TEST_MODE=0 for production or DRY_RUN=1 for testing.',
      );
    }
    if (
      process.env.PRIVATE_KEY ===
      '0x0000000000000000000000000000000000000000000000000000000000000000'
    ) {
      this.errors.push('PRIVATE_KEY appears to be a placeholder - use a real private key');
    }
    if (process.env.MONERO_WALLET_PASSWORD === 'your_secure_password_here') {
      this.errors.push('MONERO_WALLET_PASSWORD appears to be a placeholder - use a real password');
    }
    if (process.env.CHAIN_ID) {
      const chainId = Number(process.env.CHAIN_ID);
      if (isNaN(chainId) || chainId < 1) {
        this.errors.push(`Invalid CHAIN_ID: ${process.env.CHAIN_ID} (must be a positive integer)`);
      }
    }
  }
  validateP2PConfig() {
    const P2P_IMPL = process.env.P2P_IMPL || 'libp2p';
    const TEST_MODE = process.env.TEST_MODE === '1';
    if (P2P_IMPL !== 'libp2p') {
      this.errors.push(
        `P2P_IMPL must be set to "libp2p" - other implementations are not supported (got: ${P2P_IMPL})`,
      );
    }
    if (P2P_IMPL === 'libp2p' && !process.env.P2P_BOOTSTRAP_PEERS && !TEST_MODE) {
      this.warnings.push(
        'P2P_BOOTSTRAP_PEERS not configured - will only discover peers via mDNS (local network)',
      );
    }
    if (process.env.P2P_BOOTSTRAP_PEERS) {
      const peers = process.env.P2P_BOOTSTRAP_PEERS.split(',').filter(Boolean);
      for (const peer of peers) {
        if (
          !peer.trim().startsWith('/ip4/') &&
          !peer.trim().startsWith('/ip6/') &&
          !peer.trim().startsWith('/dns')
        ) {
          this.warnings.push(`Bootstrap peer may be invalid multiaddr: ${peer.trim()}`);
        }
      }
    }
    if (process.env.P2P_PORT) {
      const port = Number(process.env.P2P_PORT);
      if (isNaN(port) || port < 0 || port > 65535) {
        this.errors.push(`Invalid P2P_PORT: ${process.env.P2P_PORT} (must be 0-65535)`);
      }
    }
  }
  validateMoneroConfig() {
    if (process.env.MONERO_RPC_URL) {
      try {
        new URL(process.env.MONERO_RPC_URL);
      } catch {
        this.errors.push(`Invalid MONERO_RPC_URL: ${process.env.MONERO_RPC_URL}`);
      }
    }
    const hasUser = !!process.env.MONERO_WALLET_RPC_USER;
    const hasPass = !!process.env.MONERO_WALLET_RPC_PASSWORD;
    if (hasUser && !hasPass) {
      this.errors.push('MONERO_WALLET_RPC_USER is set but MONERO_WALLET_RPC_PASSWORD is not');
    }
    if (!hasUser && hasPass) {
      this.errors.push('MONERO_WALLET_RPC_PASSWORD is set but MONERO_WALLET_RPC_USER is not');
    }
    if (process.env.MONERO_RPC_BIND_IP && process.env.MONERO_RPC_BIND_IP !== '127.0.0.1') {
      const TEST_MODE = process.env.TEST_MODE === '1';
      if (!TEST_MODE) {
        this.errors.push(
          'MONERO_RPC_BIND_IP is not 127.0.0.1 in production mode - this exposes RPC to network without HTTPS/mTLS. Set TEST_MODE=1 to acknowledge risk or use 127.0.0.1.',
        );
      } else {
        this.warnings.push(
          'MONERO_RPC_BIND_IP is not 127.0.0.1 - ensure firewall is configured correctly',
        );
      }
    }
    if (process.env.MONERO_TRUST_DAEMON === '1') {
      this.warnings.push(
        'MONERO_TRUST_DAEMON is enabled - only use this if you control the daemon',
      );
    }
  }
  validateMoneroRpcTimingConfig() {
    const checks = [
      { key: 'RPC_READY_RETRIES', label: 'RPC_READY_RETRIES' },
      { key: 'RPC_READY_INTERVAL_MS', label: 'RPC_READY_INTERVAL_MS' },
      { key: 'RPC_HEALTH_INTERVAL_MS', label: 'RPC_HEALTH_INTERVAL_MS' },
      { key: 'RPC_HEALTH_TIMEOUT_MS', label: 'RPC_HEALTH_TIMEOUT_MS' },
    ];
    for (const { key, label } of checks) {
      const raw = process.env[key];
      if (raw === undefined) {
        continue;
      }
      const value = Number(raw);
      if (!Number.isFinite(value) || value <= 0) {
        this.errors.push(`Invalid ${label}: ${raw} (must be a positive number)`);
      }
    }
  }
  validateMoneroRPCBinary() {
    try {
      execSync('which monero-wallet-rpc', { stdio: 'ignore' });
    } catch {
      this.warnings.push(
        'monero-wallet-rpc binary not found in PATH - ensure Monero wallet RPC is installed',
      );
    }
  }
  validateClusterConfig() {
    const clusterSizeRaw = process.env.CLUSTER_SIZE;
    const clusterSize = 11;
    if (clusterSizeRaw !== undefined && String(clusterSizeRaw) !== '11') {
      this.errors.push(`CLUSTER_SIZE must be 11 (got ${clusterSizeRaw})`);
    }
    const clusterThreshold = Number(process.env.CLUSTER_THRESHOLD || 7);
    const livenessQuorum = Number(process.env.LIVENESS_QUORUM || clusterSize);
    if (isNaN(clusterThreshold) || clusterThreshold < 1) {
      this.errors.push(
        `Invalid CLUSTER_THRESHOLD: ${process.env.CLUSTER_THRESHOLD} (must be >= 1)`,
      );
    }
    if (clusterThreshold > clusterSize) {
      this.errors.push(
        `CLUSTER_THRESHOLD (${clusterThreshold}) cannot be greater than CLUSTER_SIZE (${clusterSize})`,
      );
    }
    if (isNaN(livenessQuorum) || livenessQuorum < 1) {
      this.errors.push(`Invalid LIVENESS_QUORUM: ${process.env.LIVENESS_QUORUM} (must be >= 1)`);
    }
    if (livenessQuorum > clusterSize) {
      this.errors.push(
        `LIVENESS_QUORUM (${livenessQuorum}) cannot be greater than CLUSTER_SIZE (${clusterSize})`,
      );
    }
    const pbftQuorumRaw = process.env.PBFT_QUORUM;
    if (pbftQuorumRaw !== undefined) {
      const pbftQuorum = Number(pbftQuorumRaw);
      if (!Number.isFinite(pbftQuorum) || pbftQuorum < 1 || !Number.isInteger(pbftQuorum)) {
        this.errors.push(`Invalid PBFT_QUORUM: ${pbftQuorumRaw} (must be a positive integer)`);
      } else if (pbftQuorum > clusterSize) {
        this.errors.push(
          `PBFT_QUORUM (${pbftQuorum}) cannot be greater than CLUSTER_SIZE (${clusterSize})`,
        );
      }
    }
    const f = clusterSize > 1 ? Math.floor((clusterSize - 1) / 3) : 0;
    const pbftRecommendedQuorum = 2 * f + 1;
    if (
      pbftQuorumRaw === undefined &&
      Number.isFinite(clusterThreshold) &&
      clusterSize > 1 &&
      clusterThreshold !== pbftRecommendedQuorum
    ) {
      this.warnings.push(
        `CLUSTER_THRESHOLD (${clusterThreshold}) differs from recommended PBFT quorum (${pbftRecommendedQuorum}) for CLUSTER_SIZE (${clusterSize}). Consider setting PBFT_QUORUM=${pbftRecommendedQuorum} to keep PBFT safety if you change wallet threshold.`,
      );
    }
    const requiredMintSignatures = 7;
    if (Number.isFinite(clusterSize) && clusterSize > 0 && clusterSize < requiredMintSignatures) {
      this.errors.push(`CLUSTER_SIZE (${clusterSize}) cannot be less than required mint signatures (${requiredMintSignatures})`);
    }
    const pbftQuorumEffective = pbftQuorumRaw !== undefined ? Number(pbftQuorumRaw) : pbftRecommendedQuorum;
    if (Number.isFinite(pbftQuorumEffective) && pbftQuorumEffective > 0 && requiredMintSignatures < pbftQuorumEffective) {
      this.errors.push(`REQUIRED_MINT_SIGNATURES (${requiredMintSignatures}) cannot be less than PBFT quorum (${pbftQuorumEffective})`);
    }
    const boolFlags = [
      'ENABLE_MULTI_CLUSTER_FORMATION',
      'ENABLE_HEARTBEAT_FILTERED_SELECTION',
      'ENFORCE_SELECTED_MEMBERS_P2P_VISIBILITY',
      'COOLDOWN_TO_NEXT_EPOCH_WINDOW',
    ];
    for (const name of boolFlags) {
      const v = process.env[name];
      if (v !== undefined && v !== '0' && v !== '1') {
        this.errors.push(`Invalid ${name}: ${v} (must be '0' or '1')`);
      }
    }
    const enableMulti = process.env.ENABLE_MULTI_CLUSTER_FORMATION !== '0';
    const enableHeartbeatSelection = process.env.ENABLE_HEARTBEAT_FILTERED_SELECTION !== '0';
    if (enableMulti && !enableHeartbeatSelection) {
      this.warnings.push(
        'ENABLE_MULTI_CLUSTER_FORMATION=1 without ENABLE_HEARTBEAT_FILTERED_SELECTION=1 may select offline nodes.',
      );
    }
    const enforceSelectedVisibility = process.env.ENFORCE_SELECTED_MEMBERS_P2P_VISIBILITY !== '0';
    const isTestMode = process.env.TEST_MODE === '1';
    if (!isTestMode && (enableMulti || enableHeartbeatSelection) && !enforceSelectedVisibility) {
      this.warnings.push(
        'ENFORCE_SELECTED_MEMBERS_P2P_VISIBILITY is disabled; consider enabling it in production to reduce split-brain risk.',
      );
    }
    const ttlRaw = process.env.HEARTBEAT_ONLINE_TTL_MS;
    if (ttlRaw !== undefined) {
      const ttlMs = Number(ttlRaw);
      if (!Number.isFinite(ttlMs) || ttlMs <= 0) {
        this.errors.push(`Invalid HEARTBEAT_ONLINE_TTL_MS: ${ttlRaw} (must be > 0)`);
      } else {
        const hbIntervalSec = Number(process.env.HEARTBEAT_INTERVAL || 30);
        const hbIntervalMs = Number.isFinite(hbIntervalSec) && hbIntervalSec > 0 ? hbIntervalSec * 1000 : 30000;
        if (ttlMs < hbIntervalMs * 2) {
          this.warnings.push(
            `HEARTBEAT_ONLINE_TTL_MS (${ttlMs}) is small relative to HEARTBEAT_INTERVAL (${hbIntervalMs}) and may exclude healthy nodes.`,
          );
        }
        if (ttlMs > hbIntervalMs * 120) {
          this.warnings.push(
            `HEARTBEAT_ONLINE_TTL_MS (${ttlMs}) is large relative to HEARTBEAT_INTERVAL (${hbIntervalMs}) and may include offline nodes.`,
          );
        }
      }
    }
    const epochSecRaw = process.env.FORMATION_EPOCH_SECONDS;
    const epochSec = Number(epochSecRaw || 15 * 60);
    if (epochSecRaw !== undefined && (!Number.isFinite(epochSec) || epochSec <= 0)) {
      this.errors.push(`Invalid FORMATION_EPOCH_SECONDS: ${epochSecRaw} (must be > 0)`);
    }
    const windowSecRaw = process.env.FORMATION_WINDOW_SECONDS;
    const windowSec = Number(windowSecRaw || 180);
    if (windowSecRaw !== undefined && (!Number.isFinite(windowSec) || windowSec <= 0)) {
      this.errors.push(`Invalid FORMATION_WINDOW_SECONDS: ${windowSecRaw} (must be > 0)`);
    }
    if (Number.isFinite(epochSec) && epochSec > 0 && Number.isFinite(windowSec) && windowSec > epochSec) {
      this.warnings.push(
        `FORMATION_WINDOW_SECONDS (${windowSec}) is greater than FORMATION_EPOCH_SECONDS (${epochSec}) - formation gate may behave unexpectedly.`,
      );
    }
    const maxPagesRaw = process.env.MAX_REGISTERED_PAGES;
    if (maxPagesRaw !== undefined) {
      const maxPages = Number(maxPagesRaw);
      if (!Number.isFinite(maxPages) || maxPages <= 0 || !Number.isInteger(maxPages)) {
        this.errors.push(`Invalid MAX_REGISTERED_PAGES: ${maxPagesRaw} (must be a positive integer)`);
      }
    }
    const timeouts = [
      { name: 'LIVENESS_TIMEOUT_MS', value: process.env.LIVENESS_TIMEOUT_MS },
      { name: 'ROUND_TIMEOUT_MS', value: process.env.ROUND_TIMEOUT_MS },
      { name: 'HEALTH_LOG_INTERVAL_MS', value: process.env.HEALTH_LOG_INTERVAL_MS },
      { name: 'FINALIZE_FAILOVER_MS', value: process.env.FINALIZE_FAILOVER_MS },
      { name: 'STALE_ROUND_MIN_AGE_MS', value: process.env.STALE_ROUND_MIN_AGE_MS },
      { name: 'P2P_MESSAGE_MAX_AGE_MS', value: process.env.P2P_MESSAGE_MAX_AGE_MS },
      { name: 'READY_BARRIER_TIMEOUT_MS', value: process.env.READY_BARRIER_TIMEOUT_MS },
      { name: 'P2P_WARMUP_MS', value: process.env.P2P_WARMUP_MS },
      { name: 'LIVENESS_ATTEMPT_INTERVAL_MS', value: process.env.LIVENESS_ATTEMPT_INTERVAL_MS },
    ];
    for (const { name, value } of timeouts) {
      if (value !== undefined) {
        const num = Number(value);
        if (isNaN(num) || num < 0) {
          this.errors.push(`Invalid ${name}: ${value} (must be >= 0)`);
        }
      }
    }
    if (process.env.LIVENESS_ATTEMPTS) {
      const val = Number(process.env.LIVENESS_ATTEMPTS);
      if (!Number.isFinite(val) || val <= 0 || !Number.isInteger(val)) {
        this.errors.push(
          `Invalid LIVENESS_ATTEMPTS: ${process.env.LIVENESS_ATTEMPTS} (must be a positive integer)`,
        );
      }
    }
    const intervals = [
      { name: 'HEARTBEAT_INTERVAL', value: process.env.HEARTBEAT_INTERVAL },
      { name: 'MAX_KEY_EXCHANGE_ROUNDS', value: process.env.MAX_KEY_EXCHANGE_ROUNDS },
      { name: 'MAX_REGISTERED_SCAN', value: process.env.MAX_REGISTERED_SCAN },
      { name: 'SELECTION_EPOCH_BLOCKS', value: process.env.SELECTION_EPOCH_BLOCKS },
      { name: 'FAILOVER_COORDINATOR_INDEX', value: process.env.FAILOVER_COORDINATOR_INDEX },
    ];
    for (const { name, value } of intervals) {
      if (value !== undefined) {
        const num = Number(value);
        if (isNaN(num) || num < 0) {
          this.errors.push(`Invalid ${name}: ${value} (must be >= 0)`);
        }
      }
    }
  }
  validateContractAddresses() {
    const addresses = [
      { name: 'REGISTRY_ADDR', value: process.env.REGISTRY_ADDR },
      { name: 'STAKING_ADDR', value: process.env.STAKING_ADDR },
      { name: 'ZFI_ADDR', value: process.env.ZFI_ADDR },
      { name: 'COORDINATOR_ADDR', value: process.env.COORDINATOR_ADDR },
      { name: 'BRIDGE_ADDR', value: process.env.BRIDGE_ADDR },
    ];
    for (const { name, value } of addresses) {
      if (value && !value.match(/^0x[a-fA-F0-9]{40}$/)) {
        this.errors.push(`Invalid ${name}: ${value} (must be a valid Ethereum address)`);
      }
    }
  }
  validateBridgeConfig() {
    const apiEnabled = process.env.BRIDGE_API_ENABLED;
    if (apiEnabled !== undefined && !['0', '1'].includes(apiEnabled)) {
      this.errors.push(`Invalid BRIDGE_API_ENABLED: ${apiEnabled} (must be '0' or '1')`);
    }
    const bridgeEnabled = process.env.BRIDGE_ENABLED;
    if (bridgeEnabled !== undefined && !['0', '1'].includes(bridgeEnabled)) {
      this.errors.push(`Invalid BRIDGE_ENABLED: ${bridgeEnabled} (must be '0' or '1')`);
    }
    if (bridgeEnabled === '1') {
      if (!process.env.MONERO_RPC_URL && !process.env.MONERO_WALLET_RPC_URL) {
        this.errors.push(
          'BRIDGE_ENABLED=1 requires MONERO_RPC_URL or MONERO_WALLET_RPC_URL to be set',
        );
      }
    }
    if (process.env.BRIDGE_API_PORT) {
      const port = Number(process.env.BRIDGE_API_PORT);
      if (!Number.isInteger(port) || port <= 0 || port > 65535) {
        this.errors.push(
          `Invalid BRIDGE_API_PORT: ${process.env.BRIDGE_API_PORT} (must be 1-65535)`,
        );
      }
    }
    const tlsEnabled = process.env.BRIDGE_API_TLS_ENABLED;
    if (tlsEnabled !== undefined && !['0', '1'].includes(tlsEnabled)) {
      this.errors.push(`Invalid BRIDGE_API_TLS_ENABLED: ${tlsEnabled} (must be '0' or '1')`);
    }
    if (tlsEnabled === '1') {
      if (!process.env.BRIDGE_API_TLS_KEY_PATH || !process.env.BRIDGE_API_TLS_CERT_PATH || !process.env.BRIDGE_API_TLS_CA_PATH) {
        this.errors.push(
          'BRIDGE_API_TLS_ENABLED=1 requires BRIDGE_API_TLS_KEY_PATH, BRIDGE_API_TLS_CERT_PATH, and BRIDGE_API_TLS_CA_PATH',
        );
      }
    }
  }
  validateBackupConfig() {
    const TEST_MODE = process.env.TEST_MODE === '1';
    if (!TEST_MODE && !process.env.WALLET_BACKUP_PASSPHRASE) {
      this.warnings.push(
        'WALLET_BACKUP_PASSPHRASE is not set; a random passphrase will be generated on first run and stored on disk. Back up this file if you rely on automatic wallet backups.',
      );
    }
    if (process.env.WALLET_BACKUP_PASSPHRASE) {
      const passphrase = process.env.WALLET_BACKUP_PASSPHRASE;
      if (passphrase.length < 12) {
        this.warnings.push('WALLET_BACKUP_PASSPHRASE is too short (recommended: 20+ characters)');
      }
      if (passphrase === 'your-strong-passphrase-here' || passphrase === 'changeme') {
        if (TEST_MODE) {
          this.warnings.push(
            'WALLET_BACKUP_PASSPHRASE appears to be a placeholder - use a real passphrase for production',
          );
        } else {
          this.errors.push(
            'WALLET_BACKUP_PASSPHRASE appears to be a placeholder - use a real passphrase',
          );
        }
      }
    }
    if (process.env.APPROVAL_MULTIPLIER && process.env.APPROVAL_MULTIPLIER !== '1' && !TEST_MODE) {
      this.warnings.push('APPROVAL_MULTIPLIER is ignored in production mode (always uses 1x)');
    }
    if (process.env.LIVENESS_QUORUM) {
      const clusterSize = Number(process.env.CLUSTER_SIZE || 11);
      const livenessQuorum = Number(process.env.LIVENESS_QUORUM);
      if (livenessQuorum === clusterSize) {
        this.warnings.push(
          `LIVENESS_QUORUM equals CLUSTER_SIZE (${clusterSize}) - consider setting to ${clusterSize - 1} to allow 1 transient failure`,
        );
      }
    }
  }
  validateEmergencySweepConfig() {
    const sweepCoordinatorUrl = process.env.SWEEP_COORDINATOR_URL;
    if (sweepCoordinatorUrl) {
      const url = sweepCoordinatorUrl;
      if (!url.startsWith('http://') && !url.startsWith('https://')) {
        this.errors.push(
          `Invalid SWEEP_COORDINATOR_URL: must start with http:// or https:// (got: ${url})`,
        );
      }
    }
    if (process.env.SWEEP_OFFLINE_MS) {
      const val = Number(process.env.SWEEP_OFFLINE_MS);
      if (isNaN(val) || val < 0) {
        this.errors.push(
          `Invalid SWEEP_OFFLINE_MS: must be a non-negative number (got: ${process.env.SWEEP_OFFLINE_MS})`,
        );
      } else if (val > 0 && val < 300000) {
        this.warnings.push(
          `SWEEP_OFFLINE_MS is very short (${val}ms) - nodes may be swept prematurely`,
        );
      }
    }
    if (process.env.MAX_AUTO_WALLET_RESETS) {
      const val = Number(process.env.MAX_AUTO_WALLET_RESETS);
      if (isNaN(val) || val < 0 || !Number.isInteger(val)) {
        this.errors.push(
          `Invalid MAX_AUTO_WALLET_RESETS: must be a non-negative integer (got: ${process.env.MAX_AUTO_WALLET_RESETS})`,
        );
      }
    }
  }
  validateP2PClientConfig() {
    const raw = process.env.PBFT_RPC_TIMEOUT_MS;
    if (raw !== undefined) {
      const value = Number(raw);
      if (!Number.isFinite(value) || value <= 0) {
        this.errors.push(`Invalid PBFT_RPC_TIMEOUT_MS: ${raw} (must be a positive number)`);
      }
    }
  }
  validateCircuitBreakerConfig() {
    const cbVars = [
      'RPC_CIRCUIT_BREAKER_COOLDOWN_MS',
      'RPC_CIRCUIT_BREAKER_WINDOW_MS',
      'RPC_CIRCUIT_BREAKER_THRESHOLD',
    ];
    for (const varName of cbVars) {
      if (process.env[varName]) {
        const val = Number(process.env[varName]);
        if (isNaN(val) || val < 0) {
          this.errors.push(
            `Invalid ${varName}: must be a non-negative number (got: ${process.env[varName]})`,
          );
        }
      }
    }
    if (
      process.env.RPC_CIRCUIT_BREAKER_WAIT_ON_CALL &&
      !['0', '1', 'true', 'false'].includes(process.env.RPC_CIRCUIT_BREAKER_WAIT_ON_CALL)
    ) {
      this.warnings.push(
        `RPC_CIRCUIT_BREAKER_WAIT_ON_CALL should be '0' or '1' (got: ${process.env.RPC_CIRCUIT_BREAKER_WAIT_ON_CALL})`,
      );
    }
  }
  validatePortConflicts() {
    const ports = [];
    const portConfigs = [
      { name: 'BRIDGE_API_PORT', value: process.env.BRIDGE_API_PORT, enabled: process.env.BRIDGE_API_ENABLED === '1' },
      { name: 'HEALTH_PORT', value: process.env.HEALTH_PORT || '3003', enabled: true },
      { name: 'CLUSTER_AGG_PORT', value: process.env.CLUSTER_AGG_PORT, enabled: true },
    ];
    for (const config of portConfigs) {
      if (!config.enabled || !config.value) continue;
      const port = Number(config.value);
      if (Number.isInteger(port) && port > 0 && port <= 65535) {
        const existing = ports.find((p) => p.port === port);
        if (existing) {
          this.errors.push(`Port conflict: ${config.name} and ${existing.name} both use port ${port}`);
        } else {
          ports.push({ name: config.name, port });
        }
      }
    }
  }
  validateNetworkConsistency() {
    const chainId = process.env.CHAIN_ID;
    const chainName = process.env.CHAIN_NAME;
    if (chainId && chainName) {
      const knownNetworks = {
        '1': 'mainnet',
        '11155111': 'sepolia',
        '17000': 'holesky',
        '5': 'goerli',
      };
      if (knownNetworks[chainId] && knownNetworks[chainId] !== chainName) {
        this.warnings.push(`CHAIN_ID ${chainId} typically corresponds to '${knownNetworks[chainId]}' but CHAIN_NAME is set to '${chainName}'`);
      }
    }
  }
  validateAddressChecksum(address) {
    if (!address || !address.match(/^0x[a-fA-F0-9]{40}$/)) {
      return false;
    }
    const addr = address.slice(2).toLowerCase();
    const hash = this.keccak256(addr);
    for (let i = 0; i < 40; i++) {
      const char = address[2 + i];
      const hashChar = hash[i];
      if (char >= 'A' && char <= 'F') {
        if (!(parseInt(hashChar, 16) >= 8)) {
          return false;
        }
      } else if (char >= 'a' && char <= 'f') {
        if (!(parseInt(hashChar, 16) < 8)) {
          return false;
        }
      }
    }
    return true;
  }
  keccak256(input) {
    try {
      return ethers.keccak256(ethers.toUtf8Bytes(input)).slice(2);
    } catch {
      return '';
    }
  }
  validateContractAddressChecksums() {
    const addresses = [
      { name: 'REGISTRY_ADDR', value: process.env.REGISTRY_ADDR },
      { name: 'STAKING_ADDR', value: process.env.STAKING_ADDR },
      { name: 'ZFI_ADDR', value: process.env.ZFI_ADDR },
      { name: 'COORDINATOR_ADDR', value: process.env.COORDINATOR_ADDR },
      { name: 'BRIDGE_ADDR', value: process.env.BRIDGE_ADDR },
      { name: 'ZXMR_ADDR', value: process.env.ZXMR_ADDR },
      { name: 'FEE_POOL_ADDR', value: process.env.FEE_POOL_ADDR },
      { name: 'CONFIG_ADDR', value: process.env.CONFIG_ADDR },
    ];
    for (const { name, value } of addresses) {
      if (value && value.match(/^0x[a-fA-F0-9]{40}$/)) {
        const hasUppercase = /[A-F]/.test(value.slice(2));
        if (hasUppercase && !this.validateAddressChecksum(value)) {
          this.warnings.push(`${name} has invalid EIP-55 checksum: ${value}`);
        }
      }
    }
  }
  printResults() {
    if (this.errors.length > 0) {
      console.error('\n[ERROR] Configuration Errors:');
      for (const error of this.errors) {
        console.error(`  - ${error}`);
      }
    }
    if (this.warnings.length > 0) {
      console.warn('\n[WARN] Configuration Warnings:');
      for (const warning of this.warnings) {
        console.warn(`  - ${warning}`);
      }
    }
    if (this.errors.length === 0 && this.warnings.length === 0) {
      console.log('\n[OK] Configuration validation passed');
    }
  }
}
export default ConfigValidator;
