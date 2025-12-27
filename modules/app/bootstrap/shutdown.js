import { DRY_RUN } from '../runtime.js';
import { ethers } from 'ethers';
async function attemptCoordinatorHandoff(node) {
  if (!node._activeClusterId || !node._clusterMembers || !node.p2p) {
    return false;
  }
  const selfAddr = node.wallet?.address?.toLowerCase();
  if (!selfAddr) return false;
  const isCoordinator = node._isCoordinator || node._expectedCoordinator === selfAddr;
  if (!isCoordinator) {
    return false;
  }
  console.log('[Shutdown] Initiating graceful coordinator handoff...');
  try {
    const members = node._clusterMembers
      .map(m => m.toLowerCase())
      .filter(m => m !== selfAddr)
      .sort();
    if (members.length === 0) {
      console.log('[Shutdown] No other members to hand off to');
      return false;
    }
    const seed = ethers.keccak256(
      ethers.solidityPacked(
        ['bytes32', 'uint256'],
        [node._activeClusterId, BigInt(Date.now())]
      )
    );
    const idx = Number(BigInt(seed) % BigInt(members.length));
    const nextCoordinator = members[idx];
    const handoffMsg = JSON.stringify({
      type: 'coord-handoff',
      version: 1,
      timestamp: Date.now(),
      from: selfAddr,
      to: nextCoordinator,
      clusterId: node._activeClusterId,
      reason: 'graceful_shutdown',
    });
    let signature = null;
    try {
      const msgHash = ethers.solidityPackedKeccak256(
        ['string', 'bytes32', 'address', 'address', 'uint256'],
        ['coord-handoff-v1', node._activeClusterId, selfAddr, nextCoordinator, BigInt(Date.now())]
      );
      signature = await node.wallet.signMessage(ethers.getBytes(msgHash));
    } catch {}
    const payload = signature
      ? JSON.stringify({ ...JSON.parse(handoffMsg), sig: signature })
      : handoffMsg;
    await node.p2p.broadcastRoundData(
      node._activeClusterId,
      null,
      9998, // Handoff round number
      payload
    );
    console.log(`[Shutdown] Coordinator handoff broadcasted to ${nextCoordinator.slice(0, 10)}...`);
    await new Promise(r => setTimeout(r, 2000));
    return true;
  } catch (e) {
    console.log('[Shutdown] Coordinator handoff failed:', e.message || String(e));
    return false;
  }
}
export async function stopZNode() {
  try {
    this._p2pRecoveryDisabled = true;
    try {
      await attemptCoordinatorHandoff(this);
    } catch (e) {
      console.log('[Shutdown] Coordinator handoff error:', e.message || String(e));
    }
    if (typeof this.clearAllTimers === 'function') {
      this.clearAllTimers();
      console.log('[Shutdown] Cleared all registered timers');
    }
    if (this._coordHeartbeatTimer) {
      clearInterval(this._coordHeartbeatTimer);
      this._coordHeartbeatTimer = null;
    }
    if (this._coordMonitorTimer) {
      clearInterval(this._coordMonitorTimer);
      this._coordMonitorTimer = null;
    }
    try {
      if (this.registry && this.wallet && this.registry.nodes) {
        const info = await this.registry.nodes(this.wallet.address);
        let registered = false;
        if (info && info.registered !== undefined) {
          registered = !!info.registered;
        } else if (Array.isArray(info) && info.length >= 2) {
          registered = !!info[1];
        }
        if (registered) {
          console.log('[INFO] Unregistering from network (shutdown)...');
          if (DRY_RUN) {
            console.log('[DRY_RUN] Would send unregisterNode transaction');
          } else {
            try {
              const tx = await this.txManager.sendTransaction(
                (overrides) => this.registry.unregisterNode(overrides),
                { operation: 'unregisterNode', retryWithSameNonce: true },
              );
              await tx.wait();
              console.log('[OK] Unregistered from network');
            } catch (e) {
              console.log('Unregister on shutdown failed:', e.message || String(e));
            }
          }
        }
      }
    } catch (e) {
      console.log('Deregistration status check failed:', e.message || String(e));
    }
    if (this.p2p && typeof this.p2p.stop === 'function') {
      try {
        await this.p2p.stop();
      } catch (e) {
        console.log('P2P stop error:', e.message || String(e));
      }
    }
    if (this.healthServer) {
      try {
        this.healthServer.close();
        console.log('[Health] Server stopped');
      } catch (e) {
        console.log('Health server stop error:', e.message || String(e));
      }
    }
    if (this.rpcManager) {
      this.rpcManager.stopHealthWatch();
    }
    if (this._heartbeatTimer) {
      clearInterval(this._heartbeatTimer);
      this._heartbeatTimer = null;
    }
    if (this._heartbeatProofTimer) {
      clearInterval(this._heartbeatProofTimer);
      this._heartbeatProofTimer = null;
    }
    if (this._downtimeSignerTimer) {
      clearInterval(this._downtimeSignerTimer);
      this._downtimeSignerTimer = null;
    }
    if (this._slashingTimer) {
      clearInterval(this._slashingTimer);
      this._slashingTimer = null;
    }
    if (this._monitorTimer) {
      clearInterval(this._monitorTimer);
      this._monitorTimer = null;
    }
    if (this._txManagerTimer) {
      clearInterval(this._txManagerTimer);
      this._txManagerTimer = null;
    }
    console.log('ZNode stopped.');
  } catch (e) {
    console.log('Error during shutdown:', e.message || String(e));
  }
}
export default {
  stopZNode,
  attemptCoordinatorHandoff,
};
