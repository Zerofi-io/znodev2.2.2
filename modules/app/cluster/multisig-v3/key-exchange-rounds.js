import crypto from 'crypto';
export async function runKeyExchangeRounds(clusterId, clusterNodes, startRound = 4) {
  const maxRounds = Number(process.env.MAX_KEY_EXCHANGE_ROUNDS || 7);
  const roundTimeoutExchange = Number(process.env.ROUND_TIMEOUT_EXCHANGE_MS || 180000);
  const debugKex = process.env.MONERO_KEX_DEBUG === '1';
  let round = startRound;
  let prevRound = 3;
  for (let i = 0; i < maxRounds; i++) {
    console.log(`  [INFO] Key exchange round ${round} (${i + 1}/${maxRounds})`);
    const expectedPeers = clusterNodes.length - 1;
    const prev = await this._waitForPeerPayloads(
      clusterId,
      this._sessionId,
      prevRound,
      clusterNodes,
      expectedPeers,
      roundTimeoutExchange,
    );
    const peersPrev = prev.payloads;
    const actualPrev = Array.isArray(peersPrev) ? peersPrev.length : 0;
    if (!prev.complete) {
      console.log(
        `  [ERROR] Round ${prevRound}: expected ${expectedPeers} peer payloads, got ${actualPrev}`,
      );
      return {
        success: false,
        lastRound: prevRound,
        lastPeerPayloads: Array.isArray(peersPrev) ? peersPrev : [],
      };
    }
    try {
      const fullHashes = peersPrev.map((s) =>
        crypto.createHash('sha256').update(String(s)).digest('hex'),
      );
      const unique = new Set(fullHashes);
      if (unique.size !== fullHashes.length) {
        const seen = new Map();
        const dups = [];
        fullHashes.forEach((h, idx) => {
          if (seen.has(h)) dups.push(`${seen.get(h)}<->${idx}:${h.slice(0, 10)}`);
          else seen.set(h, idx);
        });
        console.log(
          `  [ERROR] Duplicate peer payloads detected for round ${prevRound}: ${dups.join(', ')}`,
        );
        return {
          success: false,
          lastRound: prevRound,
          lastPeerPayloads: peersPrev,
        };
      }
      if (debugKex) {
        const short = fullHashes.map((h, idx) => `#${idx}:${h.slice(0, 10)}`);
        console.log(`  [INFO] Round ${prevRound}: peer payload hashes: ${short.join(', ')}`);
      }
    } catch {}
    let preKexHash = null;
    try {
      preKexHash = await this.p2p.computeConsensusHash(
        clusterId,
        this._sessionId,
        prevRound,
        clusterNodes,
      );
    } catch (e) {
      console.log(
        `  [ERROR] Failed to compute consensus hash for pre-kex Round ${prevRound}:`,
        e.message || String(e),
      );
      return {
        success: false,
        lastRound: prevRound,
        lastPeerPayloads: peersPrev,
      };
    }
    if (!preKexHash) {
      console.log(`  [ERROR] Empty consensus hash for pre-kex Round ${prevRound}`);
      return {
        success: false,
        lastRound: prevRound,
        lastPeerPayloads: peersPrev,
      };
    }
    console.log(
      `  [INFO] PBFT Consensus: confirming Round ${prevRound} state before KEX (hash=${preKexHash.substring(0, 18)}...)`,
    );
    let preKexConsensus;
    try {
      preKexConsensus = await this.p2p.runConsensus(
        clusterId,
        this._sessionId,
        `pre-exchange-${round}`,
        preKexHash,
        clusterNodes,
        roundTimeoutExchange,
        { requireAll: true },
      );
    } catch (e) {
      console.log(
        `  [ERROR] PBFT pre-exchange consensus error for Round ${round}:`,
        e.message || String(e),
      );
      return {
        success: false,
        lastRound: prevRound,
        lastPeerPayloads: peersPrev,
      };
    }
    if (!preKexConsensus.success) {
      console.log(
        `  [ERROR] PBFT pre-exchange consensus failed for Round ${round} - missing: ${(preKexConsensus.missing || []).join(', ')}`,
      );
      return {
        success: false,
        lastRound: prevRound,
        lastPeerPayloads: peersPrev,
      };
    }
    console.log(`  [OK] PBFT pre-exchange consensus reached for Round ${round}`);
    let myPayload = '';
    try {
      const res = await this.monero.exchangeMultisigKeys(peersPrev, this.moneroPassword);
      if (res && typeof res === 'object') {
        if (typeof res.multisig_info === 'string') {
          myPayload = res.multisig_info;
        } else if (typeof res.multisigInfo === 'string') {
          myPayload = res.multisigInfo;
        } else {
          myPayload = '';
        }
      } else if (typeof res === 'string') {
        myPayload = res;
      }
      if (!myPayload) {
        console.log(
          '  [WARN]  exchangeMultisigKeys returned no multisig_info; treating as failure',
        );
        return {
          success: false,
          lastRound: round - 1,
          lastPeerPayloads: peersPrev,
        };
      }
    } catch (e) {
      const msg = e && e.message ? e.message : String(e);
      if (/Messages don't have the expected kex round number/i.test(msg)) {
        console.log(
          '  [ERROR] Monero KEX round mismatch detected; aborting immediately and notifying peers',
        );
        this._noteMoneroKexRoundMismatch(round, msg);
        try {
          await this.p2p.broadcastRoundData(
            clusterId,
            this._sessionId,
            round,
            JSON.stringify({ type: 'kex-abort', reason: 'round_mismatch', round }),
          );
        } catch {}
        return {
          success: false,
          abort: true,
          abortReason: 'kex_round_mismatch',
          lastRound: prevRound,
          lastPeerPayloads: peersPrev,
        };
      }
      if (/kex is already complete/i.test(msg) || /already complete/i.test(msg)) {
        console.log('  [OK] Multisig key exchange already complete at wallet level');
        try {
          const info = await this.monero.call('is_multisig');
          if (info && info.ready) {
            return {
              success: true,
              lastRound: prevRound,
              lastPeerPayloads: peersPrev,
            };
          }
          console.log(
            '  [WARN]  Wallet reports exchange complete but multisig not ready; rotating wallet.',
          );
          await this._rotateBaseWalletAfterKexError('kex complete but not ready');
        } catch (e2) {
          console.log(
            '  [WARN]  is_multisig check failed during already-complete handling:',
            e2.message || String(e2),
          );
          await this._rotateBaseWalletAfterKexError('kex complete; is_multisig failed');
        }
        return {
          success: false,
          lastRound: prevRound,
          lastPeerPayloads: peersPrev,
        };
      }
      console.log(`  [ERROR] exchange_multisig_keys error in round ${round}:`, msg);
      this._noteMoneroRpcFailure('exchange_multisig_keys', msg);
      await this._rotateBaseWalletAfterKexError(msg || 'unknown Monero KEX error');
      return {
        success: false,
        lastRound: prevRound,
        lastPeerPayloads: peersPrev,
      };
    }
    console.log(`  [INFO] Broadcasting Round ${round} via P2P...`);
    await this.p2p.broadcastRoundData(clusterId, this._sessionId, round, myPayload);
    console.log(`  [OK] Round ${round} broadcast complete`);
    console.log(`  [INFO] Waiting for Round ${round} peer payloads...`);
    const current = await this._waitForPeerPayloads(
      clusterId,
      this._sessionId,
      round,
      clusterNodes,
      expectedPeers,
      roundTimeoutExchange,
    );
    const peersCurrent = current.payloads;
    const actualCurrent = Array.isArray(peersCurrent) ? peersCurrent.length : 0;
    if (Array.isArray(peersCurrent)) {
      for (const payload of peersCurrent) {
        try {
          if (typeof payload === 'string' && payload.startsWith('{')) {
            const parsed = JSON.parse(payload);
            if (parsed && parsed.type === 'kex-abort') {
              console.log(
                `  [ERROR] Peer sent abort signal: ${parsed.reason || 'unknown'}; aborting KEX`,
              );
              return {
                success: false,
                abort: true,
                abortReason: `peer_abort_${parsed.reason || 'unknown'}`,
                lastRound: round,
                lastPeerPayloads: peersCurrent,
              };
            }
          }
        } catch {}
      }
    }
    if (!current.complete) {
      console.log(
        `  [ERROR] Round ${round}: expected ${expectedPeers} peer payloads, got ${actualCurrent}`,
      );
      this._noteMoneroKexTimeout(round);
      return {
        success: false,
        lastRound: round,
        lastPeerPayloads: Array.isArray(peersCurrent) ? peersCurrent : [],
      };
    }
    const consensusHashN = await this.p2p.computeConsensusHash(
      clusterId,
      this._sessionId,
      round,
      clusterNodes,
    );
    console.log(`  [INFO] PBFT Consensus: confirming Round ${round} complete...`);
    let roundNConsensus;
    try {
      roundNConsensus = await this.p2p.runConsensus(
        clusterId,
        this._sessionId,
        `round-${round}`,
        consensusHashN,
        clusterNodes,
        roundTimeoutExchange,
        { requireAll: true },
      );
    } catch (e) {
      console.log(`  [ERROR] PBFT Round ${round} consensus error: ${e.message || String(e)}`);
      return {
        success: false,
        lastRound: round,
        lastPeerPayloads: peersCurrent,
      };
    }
    if (!roundNConsensus.success) {
      console.log(
        `  [ERROR] PBFT Round ${round} consensus failed - missing: ${(roundNConsensus.missing || []).join(', ')}`,
      );
      return {
        success: false,
        lastRound: round,
        lastPeerPayloads: peersCurrent,
      };
    }
    console.log(`  [OK] PBFT Round ${round} consensus reached`);
    try {
      const info = await this.monero.call('is_multisig');
      if (info && info.ready) {
        console.log(`  [OK] Multisig is ready after Round ${round}`);
        return {
          success: true,
          lastRound: round,
          lastPeerPayloads: peersCurrent,
        };
      }
    } catch (e) {
      console.log('  [WARN]  is_multisig check failed:', e.message || String(e));
    }
    prevRound = round;
    round += 1;
  }
  console.log('  [WARN]  Max key exchange rounds reached without ready multisig');
  const lastPayloads = await this.p2p.getPeerPayloads(
    clusterId,
    this._sessionId,
    prevRound,
    clusterNodes,
  );
  return {
    success: false,
    lastRound: prevRound,
    lastPeerPayloads: lastPayloads,
  };
}
