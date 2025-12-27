import { ethers } from 'ethers';
import KNOWN_DEFAULTS from './defaults.js';
import {
  bridgeABI,
  configABI,
  exchangeCoordinatorABI,
  registryABI,
  stakingABI,
  zfiABI,
} from './abis.js';
export async function initContracts(node) {
  if (!node) throw new Error('initContracts: node is required');
  if (node.chainId === null) {
    const network = await node.provider.getNetwork();
    const chainIdBigInt = network.chainId;
    if (chainIdBigInt > Number.MAX_SAFE_INTEGER) {
      throw new Error(
        `Chain ID ${chainIdBigInt} exceeds Number.MAX_SAFE_INTEGER. This chain is not supported.`,
      );
    }
    node.chainId = Number(chainIdBigInt);
    node.networkName = network.name || 'unknown';
    process.env.CHAIN_ID = String(node.chainId);
    console.log(`[WARN] CHAIN_ID not set, fetched ${node.chainId} from provider\n`);
  }
  console.log(`Network: ${node.networkName} (chainId: ${node.chainId})`);
  const defaults = KNOWN_DEFAULTS[node.chainId];
  if (!process.env.REGISTRY_ADDR && !defaults) {
    throw new Error(
      `No default contract addresses for chainId ${node.chainId}. Please set REGISTRY_ADDR, STAKING_ADDR, ZFI_ADDR, and COORDINATOR_ADDR explicitly.`,
    );
  }
  node._chainIdVerified = true;
  const REGISTRY_ADDR = process.env.REGISTRY_ADDR || defaults.REGISTRY_ADDR;
  const STAKING_ADDR = process.env.STAKING_ADDR || defaults.STAKING_ADDR;
  const ZFI_ADDR = process.env.ZFI_ADDR || defaults.ZFI_ADDR;
  const COORDINATOR_ADDR = process.env.COORDINATOR_ADDR || defaults.COORDINATOR_ADDR;
  console.log('Contract Addresses:');
  console.log(`  Registry: ${REGISTRY_ADDR}`);
  console.log(`  Staking: ${STAKING_ADDR}`);
  console.log(`  ZFI: ${ZFI_ADDR}`);
  console.log(`  Coordinator: ${COORDINATOR_ADDR}\n`);
  node.registry = new ethers.Contract(REGISTRY_ADDR, registryABI, node.wallet);
  node.staking = new ethers.Contract(STAKING_ADDR, stakingABI, node.wallet);
  node.zfi = new ethers.Contract(ZFI_ADDR, zfiABI, node.wallet);
  node.exchangeCoordinator = new ethers.Contract(
    COORDINATOR_ADDR,
    exchangeCoordinatorABI,
    node.wallet,
  );
  let BRIDGE_ADDR = process.env.BRIDGE_ADDR || (defaults ? defaults.BRIDGE_ADDR : null);
  const CONFIG_ADDR = process.env.CONFIG_ADDR;
  if (CONFIG_ADDR && CONFIG_ADDR !== ethers.ZeroAddress) {
    try {
      node.config = new ethers.Contract(CONFIG_ADDR, configABI, node.provider);
      const bridgeFromConfig = await node.config.bridge();
      if (bridgeFromConfig && bridgeFromConfig !== ethers.ZeroAddress) {
        BRIDGE_ADDR = bridgeFromConfig;
        console.log(`  Config: ${CONFIG_ADDR}`);
      }
    } catch (e) {
      console.log(`  Config: failed to load (${e.message})`);
    }
  }
  if (BRIDGE_ADDR && BRIDGE_ADDR !== ethers.ZeroAddress) {
    node.bridge = new ethers.Contract(BRIDGE_ADDR, bridgeABI, node.wallet);
    console.log(`  Bridge: ${BRIDGE_ADDR}`);
  } else {
    console.log('  Bridge: not configured');
  }
  return {
    REGISTRY_ADDR,
    STAKING_ADDR,
    ZFI_ADDR,
    COORDINATOR_ADDR,
    BRIDGE_ADDR,
    CONFIG_ADDR,
  };
}
export default initContracts;
