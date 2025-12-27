package p2p

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/math"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/signer/core/apitypes"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

const ProtocolHeartbeat = "/znode/heartbeat/1.0.0"

type HeartbeatConfig struct {
	ChainID        int64
	StakingAddress string
}

type HeartbeatMessage struct {
	Type      string `json:"type"`
	Address   string `json:"address"`
	PeerID    string `json:"peerId"`
	ApiBase   string `json:"apiBase,omitempty"`
	Timestamp int64  `json:"timestamp"`
	Nonce     string `json:"nonce"`
	Signature string `json:"signature"`
}

type PeerPresence struct {
	Address   string
	PeerID    string
	ApiBase   string
	LastSeen  time.Time
	Signature string
}

type HeartbeatManager struct {
	host        *Host
	config      *HeartbeatConfig
	apiBase     string
	recentPeers map[string]*PeerPresence
	mu          sync.RWMutex
	stopChan    chan struct{}
	running     bool
	intervalMs  int
	ttlMs       int
}

func NewHeartbeatManager(host *Host) *HeartbeatManager {
	return &HeartbeatManager{
		host:        host,
		recentPeers: make(map[string]*PeerPresence),
		intervalMs:  30000,
		ttlMs:       300000,
	}
}

func (hm *HeartbeatManager) SetDomain(chainID int64, stakingAddress string) {
	hm.mu.Lock()
	defer hm.mu.Unlock()
	hm.config = &HeartbeatConfig{
		ChainID:        chainID,
		StakingAddress: strings.ToLower(stakingAddress),
	}
	log.Printf("[Heartbeat] Domain set: chainId=%d, staking=%s", chainID, truncateStr(stakingAddress, 10))
}

func (hm *HeartbeatManager) SetApiBase(apiBase string) {
	hm.mu.Lock()
	defer hm.mu.Unlock()
	hm.apiBase = apiBase
	log.Printf("[Heartbeat] ApiBase set: %s", apiBase)
}

func (hm *HeartbeatManager) Start(intervalMs int) error {
	hm.mu.Lock()
	if hm.running {
		hm.mu.Unlock()
		return nil
	}
	hm.running = true
	hm.intervalMs = intervalMs
	hm.stopChan = make(chan struct{})
	hm.mu.Unlock()
	go hm.broadcastLoop()
	log.Printf("[Heartbeat] Started broadcasting every %dms", intervalMs)
	return nil
}

func (hm *HeartbeatManager) Stop() {
	hm.mu.Lock()
	defer hm.mu.Unlock()
	if hm.running {
		close(hm.stopChan)
		hm.running = false
	}
}

func (hm *HeartbeatManager) broadcastLoop() {
	ticker := time.NewTicker(time.Duration(hm.intervalMs) * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-hm.stopChan:
			return
		case <-ticker.C:
			if err := hm.Broadcast(); err != nil {
				log.Printf("[Heartbeat] Broadcast error: %v", err)
			}
		}
	}
}

func (hm *HeartbeatManager) Broadcast() error {
	hm.mu.RLock()
	config := hm.config
	apiBase := hm.apiBase
	hm.mu.RUnlock()
	if config == nil {
		return fmt.Errorf("heartbeat domain not configured")
	}
	timestamp := hm.host.NetNowMs()
	nonce := fmt.Sprintf("0x%x", timestamp)
	signature, err := hm.signHeartbeat(timestamp, nonce)
	if err != nil {
		return fmt.Errorf("failed to sign heartbeat: %w", err)
	}
	msg := &HeartbeatMessage{
		Type:      "p2p/heartbeat",
		Address:   hm.host.ethAddress,
		PeerID:    hm.host.PeerID(),
		ApiBase:   apiBase,
		Timestamp: timestamp,
		Nonce:     nonce,
		Signature: signature,
	}
	var sent int
	sentTo := make(map[peer.ID]struct{})
	sendOnce := func(pid peer.ID) {
		if pid == hm.host.host.ID() {
			return
		}
		if _, ok := sentTo[pid]; ok {
			return
		}
		if err := hm.sendHeartbeat(pid, msg); err != nil {
			return
		}
		sentTo[pid] = struct{}{}
		sent++
	}

	_, members, inCluster := hm.host.activeIsolationMembers()
	if inCluster {
		for addr := range members {
			if addr == hm.host.ethAddress {
				continue
			}
			pidStr := hm.host.getPeerIDForAddress(addr)
			if pidStr == "" {
				continue
			}
			pid, err := peer.Decode(pidStr)
			if err != nil {
				continue
			}
			sendOnce(pid)
		}
	}
	peers := hm.host.host.Network().Peers()
	for _, peerID := range peers {
		sendOnce(peerID)
	}
	if sent > 0 {
		log.Printf("[Heartbeat] Broadcast to %d peers", sent)
	}
	hm.recordPresence(hm.host.ethAddress, hm.host.PeerID(), apiBase, signature)
	hm.cleanupOld()
	return nil
}

func (hm *HeartbeatManager) signHeartbeat(timestamp int64, nonce string) (string, error) {
	hm.mu.RLock()
	config := hm.config
	hm.mu.RUnlock()
	if config == nil {
		return "", fmt.Errorf("domain not configured")
	}
	typedData := apitypes.TypedData{
		Types: apitypes.Types{
			"EIP712Domain": {
				{Name: "name", Type: "string"},
				{Name: "version", Type: "string"},
				{Name: "chainId", Type: "uint256"},
				{Name: "verifyingContract", Type: "address"},
			},
			"Heartbeat": {
				{Name: "node", Type: "address"},
				{Name: "timestamp", Type: "uint256"},
				{Name: "nonce", Type: "string"},
			},
		},
		PrimaryType: "Heartbeat",
		Domain: apitypes.TypedDataDomain{
			Name:              "ZFIStaking",
			Version:           "1",
			ChainId:           math.NewHexOrDecimal256(config.ChainID),
			VerifyingContract: config.StakingAddress,
		},
		Message: apitypes.TypedDataMessage{
			"node":      hm.host.ethAddress,
			"timestamp": math.NewHexOrDecimal256(timestamp),
			"nonce":     nonce,
		},
	}
	domainSeparator, err := typedData.HashStruct("EIP712Domain", typedData.Domain.Map())
	if err != nil {
		return "", fmt.Errorf("failed to hash domain: %w", err)
	}
	messageHash, err := typedData.HashStruct(typedData.PrimaryType, typedData.Message)
	if err != nil {
		return "", fmt.Errorf("failed to hash message: %w", err)
	}
	rawData := []byte(fmt.Sprintf("\x19\x01%s%s", string(domainSeparator), string(messageHash)))
	hash := crypto.Keccak256Hash(rawData)
	sig, err := crypto.Sign(hash.Bytes(), hm.host.ethPrivKey)
	if err != nil {
		return "", fmt.Errorf("failed to sign: %w", err)
	}
	if sig[64] < 27 {
		sig[64] += 27
	}
	return fmt.Sprintf("0x%x", sig), nil
}

func (hm *HeartbeatManager) sendHeartbeat(peerID peer.ID, msg *HeartbeatMessage) error {
	ctx, cancel := context.WithTimeout(hm.host.ctx, 5*time.Second)
	defer cancel()
	stream, err := hm.host.host.NewStream(ctx, peerID, protocol.ID(ProtocolHeartbeat))
	if err != nil {
		return err
	}
	defer stream.Close()
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	stream.SetWriteDeadline(time.Now().Add(3 * time.Second))
	if _, err := stream.Write(append(data, '\n')); err != nil {
		return err
	}
	return nil
}

func (hm *HeartbeatManager) HandleHeartbeatStream(stream network.Stream) {
	defer stream.Close()
	buf := make([]byte, 4096)
	stream.SetReadDeadline(time.Now().Add(5 * time.Second))
	n, err := stream.Read(buf)
	if err != nil {
		return
	}
	var msg HeartbeatMessage
	if err := json.Unmarshal(buf[:n], &msg); err != nil {
		return
	}
	if !hm.verifyHeartbeat(&msg) {
		log.Printf("[Heartbeat] Invalid signature from %s", truncateStr(msg.Address, 8))
		return
	}
	sender := strings.ToLower(msg.Address)
	hm.recordPresence(sender, msg.PeerID, msg.ApiBase, msg.Signature)
	hm.host.mu.Lock()
	for _, cs := range hm.host.clusters {
		cs.mu.Lock()
		isMember := false
		for _, m := range cs.Members {
			if strings.ToLower(m.Address) == sender {
				isMember = true
				break
			}
		}
		if isMember {
			if _, exists := cs.PeerBindings[sender]; !exists {
				cs.PeerBindings[sender] = msg.PeerID
			}
		}
		cs.mu.Unlock()
	}
	hm.host.mu.Unlock()
}

func (hm *HeartbeatManager) verifyHeartbeat(msg *HeartbeatMessage) bool {
	hm.mu.RLock()
	config := hm.config
	hm.mu.RUnlock()
	if config == nil {
		return true
	}
	now := hm.host.NetNowMs()
	if !ValidateTimestampWindow(msg.Timestamp, now, MessageMaxAgeMs, MessageMaxFutureMs) {
		return false
	}
	typedData := apitypes.TypedData{
		Types: apitypes.Types{
			"EIP712Domain": {
				{Name: "name", Type: "string"},
				{Name: "version", Type: "string"},
				{Name: "chainId", Type: "uint256"},
				{Name: "verifyingContract", Type: "address"},
			},
			"Heartbeat": {
				{Name: "node", Type: "address"},
				{Name: "timestamp", Type: "uint256"},
				{Name: "nonce", Type: "string"},
			},
		},
		PrimaryType: "Heartbeat",
		Domain: apitypes.TypedDataDomain{
			Name:              "ZFIStaking",
			Version:           "1",
			ChainId:           math.NewHexOrDecimal256(config.ChainID),
			VerifyingContract: config.StakingAddress,
		},
		Message: apitypes.TypedDataMessage{
			"node":      strings.ToLower(msg.Address),
			"timestamp": math.NewHexOrDecimal256(msg.Timestamp),
			"nonce":     msg.Nonce,
		},
	}
	domainSeparator, err := typedData.HashStruct("EIP712Domain", typedData.Domain.Map())
	if err != nil {
		return false
	}
	messageHash, err := typedData.HashStruct(typedData.PrimaryType, typedData.Message)
	if err != nil {
		return false
	}
	rawData := []byte(fmt.Sprintf("\x19\x01%s%s", string(domainSeparator), string(messageHash)))
	hash := crypto.Keccak256Hash(rawData)
	sigBytes := common.FromHex(msg.Signature)
	if len(sigBytes) != 65 {
		return false
	}
	if sigBytes[64] >= 27 {
		sigBytes[64] -= 27
	}
	pubKey, err := crypto.SigToPub(hash.Bytes(), sigBytes)
	if err != nil {
		return false
	}
	recoveredAddr := crypto.PubkeyToAddress(*pubKey)
	return strings.EqualFold(recoveredAddr.Hex(), msg.Address)
}

func (hm *HeartbeatManager) recordPresence(address, peerID, apiBase, signature string) {
	hm.mu.Lock()
	defer hm.mu.Unlock()
	addrLower := strings.ToLower(address)
	hm.recentPeers[addrLower] = &PeerPresence{
		Address:   addrLower,
		PeerID:    peerID,
		ApiBase:   apiBase,
		LastSeen:  time.Now(),
		Signature: signature,
	}
}

func (hm *HeartbeatManager) cleanupOld() {
	hm.mu.Lock()
	defer hm.mu.Unlock()
	cutoff := time.Now().Add(-time.Duration(hm.ttlMs) * time.Millisecond)
	for addr, presence := range hm.recentPeers {
		if presence.LastSeen.Before(cutoff) {
			delete(hm.recentPeers, addr)
		}
	}
}

func (hm *HeartbeatManager) GetHeartbeats(ttlMs int) map[string]int64 {
	hm.mu.RLock()
	defer hm.mu.RUnlock()
	if ttlMs <= 0 {
		ttlMs = hm.ttlMs
	}
	cutoff := time.Now().Add(-time.Duration(ttlMs) * time.Millisecond)
	result := make(map[string]int64)
	for addr, presence := range hm.recentPeers {
		if presence.LastSeen.After(cutoff) {
			result[addr] = presence.LastSeen.UnixMilli()
		}
	}
	return result
}

func (hm *HeartbeatManager) GetHeartbeatsWithApiBase(ttlMs int) []map[string]interface{} {
	hm.mu.RLock()
	defer hm.mu.RUnlock()
	if ttlMs <= 0 {
		ttlMs = hm.ttlMs
	}
	cutoff := time.Now().Add(-time.Duration(ttlMs) * time.Millisecond)
	var result []map[string]interface{}
	for _, presence := range hm.recentPeers {
		if presence.LastSeen.After(cutoff) {
			result = append(result, map[string]interface{}{
				"address":   presence.Address,
				"apiBase":   presence.ApiBase,
				"timestamp": presence.LastSeen.UnixMilli(),
			})
		}
	}
	return result
}

func (hm *HeartbeatManager) GetRecentPeers(ttlMs int) []string {
	hm.mu.RLock()
	defer hm.mu.RUnlock()
	if ttlMs <= 0 {
		ttlMs = hm.ttlMs
	}
	cutoff := time.Now().Add(-time.Duration(ttlMs) * time.Millisecond)
	var peers []string
	for addr, presence := range hm.recentPeers {
		if presence.LastSeen.After(cutoff) {
			peers = append(peers, addr)
		}
	}
	return peers
}

func (hm *HeartbeatManager) GetPeerPresence() map[string]*PeerPresence {
	hm.mu.RLock()
	defer hm.mu.RUnlock()
	result := make(map[string]*PeerPresence)
	for addr, presence := range hm.recentPeers {
		result[addr] = &PeerPresence{
			Address:   presence.Address,
			PeerID:    presence.PeerID,
			ApiBase:   presence.ApiBase,
			LastSeen:  presence.LastSeen,
			Signature: presence.Signature,
		}
	}
	return result
}

func (hm *HeartbeatManager) GetPeerIDForAddress(address string) string {
	address = strings.ToLower(address)
	hm.mu.RLock()
	defer hm.mu.RUnlock()
	if presence, ok := hm.recentPeers[address]; ok {
		return presence.PeerID
	}
	return ""
}
