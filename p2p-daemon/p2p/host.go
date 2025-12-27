package p2p

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	ma "github.com/multiformats/go-multiaddr"

	ethcrypto "github.com/ethereum/go-ethereum/crypto"
)

const (
	ProtocolRoundDataV1       = "/znode/round/1.0.0"
	ProtocolRoundDataV2       = "/znode/round/2.0.0"
	ProtocolRoundData         = ProtocolRoundDataV2
	ProtocolSignature         = "/znode/sig/1.0.0"
	ProtocolIdentity          = "/znode/identity/1.0.0"
	ProtocolNudge             = "/znode/nudge/1.0.0"
	ProtocolDiscoveryExchange = "/znode/discovery/1.0.0"
)

type HostConfig struct {
	PrivateKey     string
	EthAddress     string
	ListenAddr     string
	BootstrapAddrs []string
}

type PeerInfo struct {
	Address string
	PeerID  string
}

type ClusterState struct {
	ID               string
	Members          []PeerInfo
	IsCoordinator    bool
	RoundData        map[int]map[string]string
	RoundDataTimes   map[int]time.Time
	Signatures       map[int]map[string]SigData
	Identities       map[string]IdentityData
	PeerBindings     map[string]string
	PeerScores       map[string]float64
	PeerBindingTimes map[string]time.Time
	mu               sync.RWMutex
}

type SigData struct {
	DataHash  string
	Signature string
	Timestamp int64
}

type IdentityData struct {
	PeerID    string
	PublicKey string
	Timestamp int64
}

type Host struct {
	presel                *PreSelectionManager
	host                  host.Host
	ctx                   context.Context
	netTime               *NetTime
	ethAddress            string
	pbft                  *PBFTManager
	ethPrivKey            *ecdsa.PrivateKey
	clusters              map[string]*ClusterState
	pendingRoundData      map[string]map[int]map[string]string
	pendingRoundUpdated   map[string]time.Time
	pendingRoundMu        sync.Mutex
	pendingSignatures     map[string]map[int]map[string]SigData
	pendingSigUpdated     map[string]time.Time
	pendingSigMu          sync.Mutex
	heartbeat             *HeartbeatManager
	discovery             *DiscoveryManager
	peerStore             *PersistentPeerStore
	mu                    sync.RWMutex
	messageCache          *MessageCache
	rateLimiter           *RateLimiter
	cleanupTicker         *time.Ticker
	cleanupDone           chan struct{}
	discoveryExchangeMu   sync.Mutex
	lastDiscoveryExchange map[peer.ID]time.Time
}

func NewHost(ctx context.Context, config *HostConfig) (*Host, error) {

	keyBytes, err := hex.DecodeString(strings.TrimPrefix(config.PrivateKey, "0x"))
	if err != nil {
		return nil, fmt.Errorf("invalid private key: %w", err)
	}

	ethPrivKey, err := ethcrypto.ToECDSA(keyBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to parse private key: %w", err)
	}

	libp2pPrivKey, err := crypto.UnmarshalSecp256k1PrivateKey(keyBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to convert key: %w", err)
	}

	listenAddr, err := ma.NewMultiaddr(config.ListenAddr)
	if err != nil {
		return nil, fmt.Errorf("invalid listen address: %w", err)
	}

	opts := []libp2p.Option{
		libp2p.Identity(libp2pPrivKey),
		libp2p.ListenAddrs(listenAddr),
		libp2p.EnableRelay(),
	}
	cm, err := newConnManagerFromEnv()
	if err != nil {
		log.Printf("[WARN] Conn manager disabled: %v", err)
	} else if cm != nil {
		opts = append(opts, libp2p.ConnectionManager(cm))
	}
	h, err := libp2p.New(opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create host: %w", err)
	}

	host := &Host{
		host:                h,
		ctx:                 ctx,
		netTime:             NewNetTime(),
		ethAddress:          strings.ToLower(config.EthAddress),
		ethPrivKey:          ethPrivKey,
		clusters:            make(map[string]*ClusterState),
		pendingRoundData:    make(map[string]map[int]map[string]string),
		pendingRoundUpdated: make(map[string]time.Time),
		pendingSignatures:   make(map[string]map[int]map[string]SigData),
		pendingSigUpdated:   make(map[string]time.Time),
		messageCache:        NewMessageCache(MaxReplayCacheSize),
		rateLimiter:         NewRateLimiter(RateLimitPerSecond, RateLimitBurst),
		cleanupDone:         make(chan struct{}),
	}

	host.peerStore = NewPersistentPeerStoreFromEnv()
	if host.peerStore != nil {
		if stored := host.peerStore.LoadBootstrapMultiaddrs(); len(stored) > 0 {

			existing := make(map[string]struct{}, len(config.BootstrapAddrs)+len(stored))
			for _, a := range config.BootstrapAddrs {
				if a == "" {
					continue
				}
				existing[a] = struct{}{}
			}
			for _, a := range stored {
				if a == "" {
					continue
				}
				if _, ok := existing[a]; ok {
					continue
				}
				config.BootstrapAddrs = append(config.BootstrapAddrs, a)
				existing[a] = struct{}{}
			}
			log.Printf("[Peers] Loaded %d persisted peers for bootstrap", len(stored))
		}
	}

	host.heartbeat = NewHeartbeatManager(host)
	h.SetStreamHandler(protocol.ID(ProtocolHeartbeat), host.heartbeat.HandleHeartbeatStream)
	h.SetStreamHandler(protocol.ID(ProtocolRoundData), host.handleRoundDataStreamV2)
	if os.Getenv("P2P_ALLOW_ROUND_V1") == "1" {
		log.Printf("[WARN] P2P_ALLOW_ROUND_V1=1 enabled: accepting insecure round-data v1 protocol")
		h.SetStreamHandler(protocol.ID(ProtocolRoundDataV1), host.handleRoundDataStreamV1)
	}
	h.SetStreamHandler(protocol.ID(ProtocolSignature), host.handleSignatureStream)
	h.SetStreamHandler(protocol.ID(ProtocolIdentity), host.handleIdentityStream)
	h.SetStreamHandler(protocol.ID(ProtocolNudge), host.handleNudgeStream)
	h.SetStreamHandler(protocol.ID(ProtocolDiscoveryExchange), host.handleDiscoveryExchangeStream)

	host.pbft = NewPBFTManager(host)
	host.pbft.SetupHandler()

	host.presel = NewPreSelectionManager(host)
	host.presel.SetupHandler()
	host.presel.StartCleanup(ctx, 5*time.Minute, 10*time.Minute)

	var bootstrapPeers []peer.AddrInfo
	for _, addrStr := range config.BootstrapAddrs {
		if addrStr == "" {
			continue
		}
		addr, err := ma.NewMultiaddr(addrStr)
		if err != nil {
			log.Printf("Invalid bootstrap addr %s: %v", addrStr, err)
			continue
		}
		peerInfo, err := peer.AddrInfoFromP2pAddr(addr)
		if err != nil {
			log.Printf("Invalid bootstrap peer info %s: %v", addrStr, err)
			continue
		}

		if peerInfo.ID == h.ID() {
			log.Printf("Skipping bootstrap peer %s (self)", peerInfo.ID)
			continue
		}
		if cm := h.ConnManager(); cm != nil {
			cm.Protect(peerInfo.ID, "bootstrap")
		}
		go func(pi peer.AddrInfo) {
			if err := h.Connect(ctx, pi); err != nil {
				expected, actual, ok := parsePeerIDMismatchError(err)
				if ok && expected == pi.ID {
					corrected := peer.AddrInfo{ID: actual, Addrs: pi.Addrs}
					if err2 := h.Connect(ctx, corrected); err2 == nil {
						log.Printf("[Discovery] Bootstrap peer id corrected: %s -> %s", expected.String()[:16], actual.String()[:16])
						if cm := h.ConnManager(); cm != nil {
							cm.Protect(corrected.ID, "bootstrap")
						}
						if host.peerStore != nil {
							host.peerStore.RememberPeer(corrected)
						}
						go host.BootstrapDiscoveryFromPeer(corrected.ID)
						return
					}
				}
				log.Printf("Failed to connect to bootstrap peer %s: %v", pi.ID, err)
			} else {
				log.Printf("Connected to bootstrap peer %s", pi.ID)
				go host.BootstrapDiscoveryFromPeer(pi.ID)
			}
		}(*peerInfo)
		bootstrapPeers = append(bootstrapPeers, *peerInfo)
	}

	discovery, err := NewDiscoveryManager(ctx, h, bootstrapPeers)
	if err != nil {
		log.Printf("Warning: Failed to initialize DHT discovery: %v", err)
	} else {
		host.discovery = discovery

		if host.peerStore != nil {
			discovery.SetPeerFoundCallback(func(info peer.AddrInfo) {
				host.peerStore.RememberPeer(info)
			})
		}
		discovery.SetPeerConnectedCallback(func(peerID peer.ID) {
			if len(host.host.Network().Peers()) >= minDesiredPeers {
				return
			}
			go host.BootstrapDiscoveryFromPeer(peerID)
		})
		discovery.Start()
		log.Printf("DHT peer discovery started")
	}

	host.startCleanupRoutine()
	return host, nil
}

func (h *Host) PeerID() string {
	return h.host.ID().String()
}

func (h *Host) Addrs() []ma.Multiaddr {
	return h.host.Addrs()
}

func (h *Host) ConnectedPeerCount() int {
	return len(h.host.Network().Peers())
}

func (h *Host) ActiveClusters() []string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	clusters := make([]string, 0, len(h.clusters))
	for id := range h.clusters {
		clusters = append(clusters, id)
	}
	return clusters
}

func (h *Host) Close() error {
	return h.host.Close()
}

func (h *Host) JoinCluster(clusterID string, members []PeerInfo, isCoordinator bool) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	clusterID = strings.ToLower(clusterID)

	base := baseClusterID(clusterID)
	if isKeccakClusterID(base) {
		addrList := make([]string, 0, len(members))
		for _, m := range members {
			addrList = append(addrList, m.Address)
		}
		expected, err := computeClusterIDFromMembers(addrList)
		if err != nil {
			return err
		}
		if expected != base {
			return fmt.Errorf("cluster id mismatch")
		}
	}

	existingCS := h.clusters[clusterID]

	cs := &ClusterState{
		ID:             clusterID,
		Members:        members,
		IsCoordinator:  isCoordinator,
		RoundData:      make(map[int]map[string]string),
		RoundDataTimes: make(map[int]time.Time),
		Signatures:     make(map[int]map[string]SigData),
		Identities:     make(map[string]IdentityData),
		PeerBindings:   make(map[string]string),
		PeerScores:     make(map[string]float64),
	}

	for _, m := range members {
		cs.PeerScores[strings.ToLower(m.Address)] = 100.0
		if m.PeerID != "" {
			cs.PeerBindings[strings.ToLower(m.Address)] = m.PeerID
		}
	}

	if existingCS != nil {
		existingCS.mu.RLock()
		preIDs := 0
		preRounds := 0
		preSigs := 0
		for round, data := range existingCS.RoundData {
			m := make(map[string]string, len(data))
			for addr, payload := range data {
				m[addr] = payload
			}
			cs.RoundData[round] = m
			preRounds += len(m)
		}
		for round, sigs := range existingCS.Signatures {
			m := make(map[string]SigData, len(sigs))
			for addr, sig := range sigs {
				m[addr] = sig
			}
			cs.Signatures[round] = m
			preSigs += len(m)
		}
		for addr, identity := range existingCS.Identities {
			cs.Identities[addr] = identity
			if identity.PeerID != "" && cs.PeerBindings[addr] == "" {
				cs.PeerBindings[addr] = identity.PeerID
			}
			preIDs++
		}
		for addr, peerID := range existingCS.PeerBindings {
			if peerID != "" && cs.PeerBindings[addr] == "" {
				cs.PeerBindings[addr] = peerID
			}
		}
		existingCS.mu.RUnlock()
		if preIDs > 0 || preRounds > 0 || preSigs > 0 {
			log.Printf("[P2P] Merged pre-received data into cluster %s (identities=%d round=%d sig=%d)", truncateStr(clusterID, 10), preIDs, preRounds, preSigs)
		}
	}

	if merged := h.mergePendingRoundData(clusterID, cs); merged > 0 {
		log.Printf("[P2P] Merged %d pre-received round payloads into cluster %s", merged, truncateStr(clusterID, 10))
	}
	if merged := h.mergePendingSignatures(clusterID, cs); merged > 0 {
		log.Printf("[P2P] Merged %d pre-received signatures into cluster %s", merged, truncateStr(clusterID, 10))
	}

	h.clusters[clusterID] = cs
	log.Printf("[P2P] Joined cluster %s with %d members (coordinator: %v)", truncateStr(clusterID, 10), len(members), isCoordinator)

	return nil
}

func (h *Host) LeaveCluster(clusterID string) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	clusterID = strings.ToLower(clusterID)
	delete(h.clusters, clusterID)
	log.Printf("[P2P] Left cluster %s", truncateStr(clusterID, 10))

	return nil
}

func (h *Host) getCluster(clusterID string) *ClusterState {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.clusters[strings.ToLower(clusterID)]
}

const pendingRoundDataTTL = 20 * time.Minute
const pendingRoundDataMaxClusters = 64

func (h *Host) cleanupPendingRoundDataLocked(now time.Time) {
	for id, ts := range h.pendingRoundUpdated {
		if now.Sub(ts) > pendingRoundDataTTL {
			delete(h.pendingRoundUpdated, id)
			delete(h.pendingRoundData, id)
		}
	}
}

func (h *Host) evictOldestPendingRoundClusterLocked() {
	oldestID := ""
	var oldestTS time.Time
	for id, ts := range h.pendingRoundUpdated {
		if oldestID == "" || ts.Before(oldestTS) {
			oldestID = id
			oldestTS = ts
		}
	}
	if oldestID == "" {
		return
	}
	delete(h.pendingRoundUpdated, oldestID)
	delete(h.pendingRoundData, oldestID)
}

func (h *Host) storePendingRoundData(clusterID string, round int, address, payload string) (bool, bool) {
	clusterID = strings.ToLower(clusterID)
	address = strings.ToLower(address)
	now := time.Now()
	h.pendingRoundMu.Lock()
	defer h.pendingRoundMu.Unlock()
	h.cleanupPendingRoundDataLocked(now)
	rounds := h.pendingRoundData[clusterID]
	if rounds == nil {
		for len(h.pendingRoundData) >= pendingRoundDataMaxClusters {
			before := len(h.pendingRoundData)
			h.evictOldestPendingRoundClusterLocked()
			if len(h.pendingRoundData) == before {
				break
			}
		}
		if len(h.pendingRoundData) >= pendingRoundDataMaxClusters {
			return false, false
		}
		rounds = make(map[int]map[string]string)
		h.pendingRoundData[clusterID] = rounds
	}
	h.pendingRoundUpdated[clusterID] = now
	m := rounds[round]
	if m == nil {
		m = make(map[string]string)
		rounds[round] = m
	}
	overwriteRejected := false
	if existing, ok := m[address]; ok && existing != payload {
		overwriteRejected = true
	} else {
		m[address] = payload
	}
	return true, overwriteRejected
}

func (h *Host) takePendingRoundData(clusterID string) map[int]map[string]string {
	clusterID = strings.ToLower(clusterID)
	h.pendingRoundMu.Lock()
	defer h.pendingRoundMu.Unlock()
	rounds := h.pendingRoundData[clusterID]
	if rounds == nil {
		return nil
	}
	delete(h.pendingRoundData, clusterID)
	delete(h.pendingRoundUpdated, clusterID)
	return rounds
}

func (h *Host) mergePendingRoundData(clusterID string, cs *ClusterState) int {
	pending := h.takePendingRoundData(clusterID)
	if pending == nil {
		return 0
	}
	clusterID = strings.ToLower(clusterID)
	merged := 0
	cs.mu.Lock()
	for round, data := range pending {
		if cs.RoundData[round] == nil {
			cs.RoundData[round] = make(map[string]string)
		}
		for addr, payload := range data {
			if existing, ok := cs.RoundData[round][addr]; ok && existing != payload {
				continue
			}
			if _, ok := cs.RoundData[round][addr]; !ok {
				merged++
			}
			cs.RoundData[round][addr] = payload
			cs.RoundDataTimes[round] = time.Now()
		}
	}
	cs.mu.Unlock()
	return merged
}

func (h *Host) cleanupPendingSignaturesLocked(now time.Time) {
	for id, ts := range h.pendingSigUpdated {
		if now.Sub(ts) > pendingRoundDataTTL {
			delete(h.pendingSigUpdated, id)
			delete(h.pendingSignatures, id)
		}
	}
}

func (h *Host) evictOldestPendingSigClusterLocked() {
	oldestID := ""
	var oldestTS time.Time
	for id, ts := range h.pendingSigUpdated {
		if oldestID == "" || ts.Before(oldestTS) {
			oldestID = id
			oldestTS = ts
		}
	}
	if oldestID == "" {
		return
	}
	delete(h.pendingSigUpdated, oldestID)
	delete(h.pendingSignatures, oldestID)
}

func (h *Host) storePendingSignature(clusterID string, round int, address, dataHash, signature string) bool {
	clusterID = strings.ToLower(clusterID)
	address = strings.ToLower(address)
	now := time.Now()
	h.pendingSigMu.Lock()
	defer h.pendingSigMu.Unlock()
	h.cleanupPendingSignaturesLocked(now)
	rounds := h.pendingSignatures[clusterID]
	if rounds == nil {
		for len(h.pendingSignatures) >= pendingRoundDataMaxClusters {
			before := len(h.pendingSignatures)
			h.evictOldestPendingSigClusterLocked()
			if len(h.pendingSignatures) == before {
				break
			}
		}
		if len(h.pendingSignatures) >= pendingRoundDataMaxClusters {
			return false
		}
		rounds = make(map[int]map[string]SigData)
		h.pendingSignatures[clusterID] = rounds
	}
	h.pendingSigUpdated[clusterID] = now
	m := rounds[round]
	if m == nil {
		m = make(map[string]SigData)
		rounds[round] = m
	}
	m[address] = SigData{DataHash: dataHash, Signature: signature, Timestamp: now.UnixMilli()}
	return true
}

func (h *Host) takePendingSignatures(clusterID string) map[int]map[string]SigData {
	clusterID = strings.ToLower(clusterID)
	h.pendingSigMu.Lock()
	defer h.pendingSigMu.Unlock()
	rounds := h.pendingSignatures[clusterID]
	if rounds == nil {
		return nil
	}
	delete(h.pendingSignatures, clusterID)
	delete(h.pendingSigUpdated, clusterID)
	return rounds
}

func (h *Host) mergePendingSignatures(clusterID string, cs *ClusterState) int {
	pending := h.takePendingSignatures(clusterID)
	if pending == nil {
		return 0
	}
	merged := 0
	cs.mu.Lock()
	for round, data := range pending {
		if cs.Signatures[round] == nil {
			cs.Signatures[round] = make(map[string]SigData)
		}
		for addr, sig := range data {
			if _, ok := cs.Signatures[round][addr]; !ok {
				merged++
			}
			cs.Signatures[round][addr] = sig
		}
	}
	cs.mu.Unlock()
	return merged
}

func (h *Host) ensureClusterForPBFT(clusterID string, members []string) *ClusterState {
	clusterID = strings.ToLower(clusterID)
	h.mu.RLock()
	cs := h.clusters[clusterID]
	h.mu.RUnlock()
	if cs != nil {
		return cs
	}
	peerInfos := make([]PeerInfo, len(members))
	for i, m := range members {
		addr := strings.ToLower(m)
		peerInfos[i] = PeerInfo{Address: addr}
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if existing := h.clusters[clusterID]; existing != nil {
		return existing
	}
	cs = &ClusterState{
		ID:               clusterID,
		Members:          peerInfos,
		RoundData:        make(map[int]map[string]string),
		RoundDataTimes:   make(map[int]time.Time),
		Signatures:       make(map[int]map[string]SigData),
		Identities:       make(map[string]IdentityData),
		PeerBindings:     make(map[string]string),
		PeerScores:       make(map[string]float64),
		PeerBindingTimes: make(map[string]time.Time),
	}
	for _, p := range peerInfos {
		addr := strings.ToLower(p.Address)
		cs.PeerScores[addr] = 100.0
	}
	h.clusters[clusterID] = cs
	return cs
}

func (h *Host) BroadcastRoundData(clusterID string, round int, payload string) (int, error) {
	if strings.Contains(payload, "\n") {
		return 0, fmt.Errorf("payload contains newline (\\n); cannot send over round-data protocol")
	}
	if strings.Contains(payload, "|") {
		return 0, fmt.Errorf("payload contains '|' delimiter; cannot send over round-data protocol")
	}

	activeBase, _, inScope := h.activeIsolationMembers()
	if inScope {
		if baseClusterID(clusterID) != activeBase {
			return 0, fmt.Errorf("isolation scope mismatch")
		}
	}

	cs := h.getCluster(clusterID)
	if cs == nil {
		return 0, fmt.Errorf("cluster not found: %s", clusterID)
	}

	cs.mu.Lock()
	if cs.RoundData[round] == nil {
		cs.RoundData[round] = make(map[string]string)
	}
	if existing, ok := cs.RoundData[round][h.ethAddress]; ok && existing != payload {
		cs.mu.Unlock()
		return 0, fmt.Errorf("round payload overwrite rejected: cluster=%s round=%d", truncateStr(clusterID, 10), round)
	}
	cs.RoundData[round][h.ethAddress] = payload
	cs.RoundDataTimes[round] = time.Now()
	cs.mu.Unlock()

	cs.mu.RLock()
	members := make([]string, 0, len(cs.Members))
	bindings := make(map[string]string, len(cs.PeerBindings))
	for _, m := range cs.Members {
		addr := strings.ToLower(m.Address)
		if addr != "" {
			members = append(members, addr)
		}
	}
	for addr, pid := range cs.PeerBindings {
		if pid != "" {
			bindings[addr] = pid
		}
	}
	cs.mu.RUnlock()

	targets := make([]peer.ID, 0, len(members))
	for _, addr := range members {
		if addr == h.ethAddress {
			continue
		}
		pidStr := bindings[addr]
		if pidStr == "" {
			pidStr = h.getPeerIDForAddress(addr)
		}
		if pidStr == "" {
			continue
		}
		pid, err := peer.Decode(pidStr)
		if err != nil {
			continue
		}
		targets = append(targets, pid)
	}

	log.Printf("[P2P] Broadcasting round %d to %d peers", round, len(targets))

	var wg sync.WaitGroup
	var delivered int
	var deliveredMu sync.Mutex

	for _, pid := range targets {
		wg.Add(1)
		go func(pid peer.ID) {
			defer wg.Done()
			if err := h.sendRoundData(pid.String(), clusterID, round, payload); err != nil {
				log.Printf("[P2P] Failed to send round %d to %s: %v", round, truncateStr(pid.String(), 12), err)
			} else {
				deliveredMu.Lock()
				delivered++
				deliveredMu.Unlock()
			}
		}(pid)
	}

	wg.Wait()
	log.Printf("[P2P] Round %d broadcast complete: delivered to %d peers", round, delivered)
	return delivered, nil
}

func (h *Host) sendRoundData(peerIDStr string, clusterID string, round int, payload string) error {
	peerID, err := peer.Decode(peerIDStr)
	if err != nil {
		return fmt.Errorf("invalid peer ID: %w", err)
	}
	if strings.Contains(payload, "\n") {
		return fmt.Errorf("payload contains newline (\\n); cannot send over round-data protocol")
	}
	if strings.Contains(payload, "|") {
		return fmt.Errorf("payload contains '|' delimiter; cannot send over round-data protocol")
	}
	if strings.Contains(clusterID, "\n") {
		return fmt.Errorf("clusterID contains newline (\\n); cannot send over round-data protocol")
	}
	if strings.Contains(clusterID, "|") {
		return fmt.Errorf("clusterID contains '|' delimiter; cannot send over round-data protocol")
	}
	timestamp := h.NetNowMs()
	allowV1 := os.Getenv("P2P_ALLOW_ROUND_V1") == "1"
	ctx, cancel := context.WithTimeout(h.ctx, 10*time.Second)
	defer cancel()
	protoID := protocol.ID(ProtocolRoundData)
	stream, err := h.host.NewStream(ctx, peerID, protoID)
	if err != nil && allowV1 {
		protoID = protocol.ID(ProtocolRoundDataV1)
		stream, err = h.host.NewStream(ctx, peerID, protoID)
	}
	if err != nil {
		return fmt.Errorf("failed to open stream: %w", err)
	}
	defer stream.Close()
	var signature string
	if protoID == protocol.ID(ProtocolRoundDataV1) {
		signature, err = h.signPayload(payload)
		if err != nil {
			return fmt.Errorf("failed to sign payload: %w", err)
		}
	} else {
		signature, err = h.signRoundData(clusterID, round, h.ethAddress, payload, timestamp)
		if err != nil {
			return fmt.Errorf("failed to sign round data: %w", err)
		}
	}
	msg := fmt.Sprintf("%s|%d|%s|%s|%s|%d\n", clusterID, round, h.ethAddress, payload, signature, timestamp)
	stream.SetWriteDeadline(time.Now().Add(5 * time.Second))
	if _, err := stream.Write([]byte(msg)); err != nil {
		return fmt.Errorf("failed to write: %w", err)
	}
	ack := make([]byte, 3)
	stream.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := io.ReadFull(stream, ack); err != nil {
		return fmt.Errorf("no ACK received: %w", err)
	}
	if string(ack) != "ACK" {
		return fmt.Errorf("unexpected ack: %q", string(ack))
	}
	return nil
}

func (h *Host) handleRoundDataStreamV2(stream network.Stream) {
	defer stream.Close()
	remotePeer := stream.Conn().RemotePeer().String()
	if !h.rateLimiter.Allow(remotePeer) {
		log.Printf("[P2P] Rate limit exceeded for peer %s", truncateStr(remotePeer, 12))
		return
	}
	reader := bufio.NewReader(stream)
	stream.SetReadDeadline(time.Now().Add(10 * time.Second))
	msgBytes := make([]byte, 0, 1024)
	for {
		b, err := reader.ReadByte()
		if err != nil {
			if err == io.EOF && len(msgBytes) > 0 {
				break
			}
			log.Printf("[P2P] Failed to read round data: %v", err)
			return
		}
		if b == '\n' {
			break
		}
		msgBytes = append(msgBytes, b)
		if len(msgBytes) > MaxMessageSize {
			log.Printf("[P2P] Message exceeds max size (%d bytes) from %s", MaxMessageSize, truncateStr(remotePeer, 12))
			return
		}
	}
	msg := strings.TrimSpace(string(msgBytes))
	parts := strings.SplitN(msg, "|", 6)
	if len(parts) < 6 {
		log.Printf("[P2P] Invalid round data format (need 6 parts, got %d)", len(parts))
		return
	}
	clusterID := parts[0]
	round := 0
	fmt.Sscanf(parts[1], "%d", &round)
	address := strings.ToLower(parts[2])
	payload := parts[3]
	signature := parts[4]
	timestampStr := parts[5]
	var timestamp int64
	fmt.Sscanf(timestampStr, "%d", &timestamp)
	nowMs := h.NetNowMs()
	if !ValidateTimestampWindow(timestamp, nowMs, MessageMaxAgeMs, MessageMaxFutureMs) {
		log.Printf("[P2P] Rejected round %d data from %s: timestamp too old or invalid", round, truncateStr(address, 8))
		return
	}
	msgHash := ComputeMessageHash(clusterID, round, address, payload, timestamp)
	if !h.messageCache.Add(msgHash, nowMs) {
		log.Printf("[P2P] Rejected round %d data from %s: duplicate message", round, truncateStr(address, 8))
		return
	}
	if !verifyRoundDataSignatureV2(clusterID, round, address, payload, timestamp, signature) {
		log.Printf("[P2P] Rejected round %d data from %s: invalid signature", round, truncateStr(address, 8))
		return
	}
	activeBase, activeMembers, inScope := h.activeIsolationMembers()
	msgBase := baseClusterID(clusterID)
	if inScope {
		if msgBase != activeBase {
			return
		}
		if _, ok := activeMembers[address]; !ok {
			return
		}
	} else {
		if isKeccakClusterID(msgBase) {
			return
		}
	}
	cs := h.getCluster(clusterID)
	if cs != nil {
		overwriteRejected := false
		cs.mu.Lock()
		isMember := false
		for _, m := range cs.Members {
			if strings.ToLower(m.Address) == address {
				isMember = true
				break
			}
		}
		if !isMember {
			cs.mu.Unlock()
			return
		}
		if bound, ok := cs.PeerBindings[address]; ok && bound != "" && bound != remotePeer {
			cs.mu.Unlock()
			return
		}
		if cs.RoundData[round] == nil {
			cs.RoundData[round] = make(map[string]string)
		}
		if existing, ok := cs.RoundData[round][address]; ok && existing != payload {
			overwriteRejected = true
		} else {
			cs.RoundData[round][address] = payload
			cs.RoundDataTimes[round] = time.Now()
		}
		cs.mu.Unlock()
		if overwriteRejected {
			log.Printf("[P2P] Rejected round %d overwrite from %s (cluster=%s)", round, truncateStr(address, 8), truncateStr(clusterID, 10))
		} else {
			log.Printf("[P2P] Received verified round %d data from %s", round, truncateStr(address, 8))
		}
		h.updatePeerBinding(cs, address, remotePeer)
	} else {
		stored, overwriteRejected := h.storePendingRoundData(clusterID, round, address, payload)
		if !stored {
			log.Printf("[P2P] Dropped round %d data from %s (cluster=%s)", round, truncateStr(address, 8), truncateStr(clusterID, 10))
			return
		}
		if overwriteRejected {
			log.Printf("[P2P] Rejected round %d overwrite from %s (cluster=%s)", round, truncateStr(address, 8), truncateStr(clusterID, 10))
		} else {
			log.Printf("[P2P] Buffered verified round %d data from %s (cluster=%s)", round, truncateStr(address, 8), truncateStr(clusterID, 10))
		}
	}
	stream.SetWriteDeadline(time.Now().Add(2 * time.Second))
	if _, err := stream.Write([]byte("ACK")); err != nil {
		log.Printf("[P2P] Failed to send ACK for round %d data: %v", round, err)
	}
}
func (h *Host) handleRoundDataStreamV1(stream network.Stream) {
	defer stream.Close()
	remotePeer := stream.Conn().RemotePeer().String()
	if !h.rateLimiter.Allow(remotePeer) {
		log.Printf("[P2P] Rate limit exceeded for peer %s", truncateStr(remotePeer, 12))
		return
	}
	reader := bufio.NewReader(stream)
	stream.SetReadDeadline(time.Now().Add(10 * time.Second))
	msgBytes := make([]byte, 0, 1024)
	for {
		b, err := reader.ReadByte()
		if err != nil {
			if err == io.EOF && len(msgBytes) > 0 {
				break
			}
			log.Printf("[P2P] Failed to read round data: %v", err)
			return
		}
		if b == '\n' {
			break
		}
		msgBytes = append(msgBytes, b)
		if len(msgBytes) > MaxMessageSize {
			log.Printf("[P2P] Message exceeds max size (%d bytes) from %s", MaxMessageSize, truncateStr(remotePeer, 12))
			return
		}
	}
	msg := strings.TrimSpace(string(msgBytes))
	parts := strings.SplitN(msg, "|", 6)
	if len(parts) < 6 {
		log.Printf("[P2P] Invalid round data format (need 6 parts, got %d)", len(parts))
		return
	}
	clusterID := parts[0]
	round := 0
	fmt.Sscanf(parts[1], "%d", &round)
	address := strings.ToLower(parts[2])
	payload := parts[3]
	signature := parts[4]
	timestampStr := parts[5]
	var timestamp int64
	fmt.Sscanf(timestampStr, "%d", &timestamp)
	nowMs := h.NetNowMs()
	if !ValidateTimestampWindow(timestamp, nowMs, MessageMaxAgeMs, MessageMaxFutureMs) {
		log.Printf("[P2P] Rejected round %d data from %s: timestamp too old or invalid", round, truncateStr(address, 8))
		return
	}
	msgHash := ComputeMessageHash(clusterID, round, address, payload, timestamp)
	if !h.messageCache.Add(msgHash, nowMs) {
		log.Printf("[P2P] Rejected round %d data from %s: duplicate message", round, truncateStr(address, 8))
		return
	}
	if !verifyPayloadSignature(payload, signature, address) {
		log.Printf("[P2P] Rejected round %d data from %s: invalid signature", round, truncateStr(address, 8))
		return
	}
	activeBase, activeMembers, inScope := h.activeIsolationMembers()
	msgBase := baseClusterID(clusterID)
	if inScope {
		if msgBase != activeBase {
			return
		}
		if _, ok := activeMembers[address]; !ok {
			return
		}
	} else {
		if isKeccakClusterID(msgBase) {
			return
		}
	}
	cs := h.getCluster(clusterID)
	if cs != nil {
		overwriteRejected := false
		cs.mu.Lock()
		isMember := false
		for _, m := range cs.Members {
			if strings.ToLower(m.Address) == address {
				isMember = true
				break
			}
		}
		if !isMember {
			cs.mu.Unlock()
			return
		}
		if bound, ok := cs.PeerBindings[address]; ok && bound != "" && bound != remotePeer {
			cs.mu.Unlock()
			return
		}
		if cs.RoundData[round] == nil {
			cs.RoundData[round] = make(map[string]string)
		}
		if existing, ok := cs.RoundData[round][address]; ok && existing != payload {
			overwriteRejected = true
		} else {
			cs.RoundData[round][address] = payload
			cs.RoundDataTimes[round] = time.Now()
		}
		cs.mu.Unlock()
		if overwriteRejected {
			log.Printf("[P2P] Rejected round %d overwrite from %s (cluster=%s)", round, truncateStr(address, 8), truncateStr(clusterID, 10))
		} else {
			log.Printf("[P2P] Received verified round %d data from %s", round, truncateStr(address, 8))
		}
		h.updatePeerBinding(cs, address, remotePeer)
	} else {
		stored, overwriteRejected := h.storePendingRoundData(clusterID, round, address, payload)
		if !stored {
			log.Printf("[P2P] Dropped round %d data from %s (cluster=%s)", round, truncateStr(address, 8), truncateStr(clusterID, 10))
			return
		}
		if overwriteRejected {
			log.Printf("[P2P] Rejected round %d overwrite from %s (cluster=%s)", round, truncateStr(address, 8), truncateStr(clusterID, 10))
		} else {
			log.Printf("[P2P] Buffered verified round %d data from %s (cluster=%s)", round, truncateStr(address, 8), truncateStr(clusterID, 10))
		}
	}
	stream.SetWriteDeadline(time.Now().Add(2 * time.Second))
	if _, err := stream.Write([]byte("ACK")); err != nil {
		log.Printf("[P2P] Failed to send ACK for round %d data: %v", round, err)
	}
}

func (h *Host) WaitForRoundCompletion(clusterID string, round int, members []string, timeoutMs int, requireAll bool) (*RoundCompletionResult, error) {
	cs := h.getCluster(clusterID)
	if cs == nil {
		return nil, fmt.Errorf("cluster not found: %s", clusterID)
	}

	timeout := time.Duration(timeoutMs) * time.Millisecond
	deadline := time.Now().Add(timeout)
	rebroadcastInterval := 10 * time.Second
	lastRebroadcast := time.Now()

	membersLower := make([]string, len(members))
	for i, m := range members {
		membersLower[i] = strings.ToLower(m)
	}

	cs.mu.RLock()
	ourPayload := ""
	if cs.RoundData[round] != nil {
		ourPayload = cs.RoundData[round][h.ethAddress]
	}
	cs.mu.RUnlock()

	required := pbftQuorum(len(membersLower))
	if requireAll {
		required = len(membersLower)
	}

	log.Printf("[P2P] Waiting for round %d completion from %d members (required=%d)", round, len(members), required)

	for time.Now().Before(deadline) {
		cs.mu.RLock()
		roundData := cs.RoundData[round]
		var received, missing []string
		for _, addr := range membersLower {
			if roundData != nil && roundData[addr] != "" {
				received = append(received, addr)
			} else {
				missing = append(missing, addr)
			}
		}
		cs.mu.RUnlock()

		if len(received) >= required {
			log.Printf("[P2P] Round %d complete: %d/%d received (required=%d)", round, len(received), len(membersLower), required)
			return &RoundCompletionResult{
				Complete: true,
				Received: received,
				Missing:  missing,
			}, nil
		}

		if time.Since(lastRebroadcast) >= rebroadcastInterval && len(missing) > 0 {
			log.Printf("[P2P] Round %d progress: %d/%d (missing: %d). Re-broadcasting... Missing addrs: %v",
				round, len(received), len(membersLower), len(missing), missing)

			if ourPayload != "" {
				delivered, _ := h.BroadcastRoundData(clusterID, round, ourPayload)
				log.Printf("[P2P] Re-broadcast round %d data to %d peers", round, delivered)
			}

			h.nudgeMissingPeers(cs, clusterID, round, missing)
			lastRebroadcast = time.Now()
		}

		time.Sleep(500 * time.Millisecond)
	}

	cs.mu.RLock()
	roundData := cs.RoundData[round]
	var received, missing []string
	for _, addr := range membersLower {
		if roundData != nil && roundData[addr] != "" {
			received = append(received, addr)
		} else {
			missing = append(missing, addr)
		}
	}
	cs.mu.RUnlock()

	log.Printf("[P2P] Round %d timeout: %d/%d received. Missing: %v", round, len(received), len(membersLower), missing)

	return &RoundCompletionResult{
		Complete: false,
		Received: received,
		Missing:  missing,
	}, fmt.Errorf("timeout waiting for round %d completion", round)
}

type RoundCompletionResult struct {
	Complete bool     `json:"complete"`
	Received []string `json:"received"`
	Missing  []string `json:"missing"`
}

func (h *Host) nudgeMissingPeers(cs *ClusterState, clusterID string, round int, missing []string) {
	for _, addr := range missing {
		peerID := h.getPeerID(cs, addr)
		if peerID == "" {
			continue
		}

		go func(pid, address string) {
			if err := h.sendNudge(pid, clusterID, round); err != nil {
				log.Printf("[P2P] Nudge failed for %s: %v", truncateStr(address, 8), err)
			}
		}(peerID, addr)
	}
}

func (h *Host) sendNudge(peerIDStr string, clusterID string, round int) error {
	peerID, err := peer.Decode(peerIDStr)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(h.ctx, 5*time.Second)
	defer cancel()

	stream, err := h.host.NewStream(ctx, peerID, protocol.ID(ProtocolNudge))
	if err != nil {
		return err
	}
	defer stream.Close()

	msg := fmt.Sprintf("%s|%d|%s\n", clusterID, round, h.ethAddress)
	stream.Write([]byte(msg))

	return nil
}

func (h *Host) handleNudgeStream(stream network.Stream) {
	defer stream.Close()

	buf := make([]byte, 1024)
	n, err := stream.Read(buf)
	if err != nil {
		return
	}

	msg := string(buf[:n])
	parts := strings.SplitN(strings.TrimSpace(msg), "|", 3)
	if len(parts) < 3 {
		return
	}

	clusterID := parts[0]
	round := 0
	fmt.Sscanf(parts[1], "%d", &round)
	sender := strings.ToLower(parts[2])

	activeBase, activeMembers, inScope := h.activeIsolationMembers()
	msgBase := baseClusterID(clusterID)
	if inScope {
		if msgBase != activeBase {
			return
		}
		if _, ok := activeMembers[sender]; !ok {
			return
		}
	} else {
		if isKeccakClusterID(msgBase) {
			return
		}
	}

	cs := h.getCluster(clusterID)
	if cs == nil {
		return
	}

	remotePeer := stream.Conn().RemotePeer().String()

	cs.mu.RLock()
	isMember := false
	for _, m := range cs.Members {
		if strings.ToLower(m.Address) == sender {
			isMember = true
			break
		}
	}
	if !isMember {
		cs.mu.RUnlock()
		return
	}
	if bound, ok := cs.PeerBindings[sender]; ok && bound != "" && bound != remotePeer {
		cs.mu.RUnlock()
		return
	}

	payload := ""
	if cs.RoundData[round] != nil {
		payload = cs.RoundData[round][h.ethAddress]
	}
	cs.mu.RUnlock()

	if payload != "" {
		h.sendRoundData(remotePeer, clusterID, round, payload)
	}
}

func (h *Host) GetPeerPayloads(clusterID string, round int, members []string) map[string]string {
	cs := h.getCluster(clusterID)
	if cs == nil {
		return nil
	}

	cs.mu.RLock()
	defer cs.mu.RUnlock()

	roundData := cs.RoundData[round]
	if roundData == nil {
		return make(map[string]string)
	}

	result := make(map[string]string)
	for _, m := range members {
		addr := strings.ToLower(m)
		if addr == h.ethAddress {
			continue
		}
		if payload, ok := roundData[addr]; ok {
			result[addr] = payload
		}
	}

	return result
}

func (h *Host) BroadcastSignature(clusterID string, round int, dataHash, signature string) (int, error) {
	activeBase, _, inScope := h.activeIsolationMembers()
	if inScope {
		if baseClusterID(clusterID) != activeBase {
			return 0, fmt.Errorf("isolation scope mismatch")
		}
	}

	cs := h.getCluster(clusterID)
	if cs == nil {
		return 0, fmt.Errorf("cluster not found: %s", clusterID)
	}

	cs.mu.Lock()
	if cs.Signatures[round] == nil {
		cs.Signatures[round] = make(map[string]SigData)
	}
	cs.Signatures[round][h.ethAddress] = SigData{
		DataHash:  dataHash,
		Signature: signature,
		Timestamp: time.Now().UnixMilli(),
	}
	cs.mu.Unlock()

	cs.mu.RLock()
	members := make([]string, 0, len(cs.Members))
	bindings := make(map[string]string, len(cs.PeerBindings))
	for _, m := range cs.Members {
		addr := strings.ToLower(m.Address)
		if addr != "" {
			members = append(members, addr)
		}
	}
	for addr, pid := range cs.PeerBindings {
		if pid != "" {
			bindings[addr] = pid
		}
	}
	cs.mu.RUnlock()

	targets := make([]peer.ID, 0, len(members))
	for _, addr := range members {
		if addr == h.ethAddress {
			continue
		}
		pidStr := bindings[addr]
		if pidStr == "" {
			pidStr = h.getPeerIDForAddress(addr)
		}
		if pidStr == "" {
			continue
		}
		pid, err := peer.Decode(pidStr)
		if err != nil {
			continue
		}
		targets = append(targets, pid)
	}

	var wg sync.WaitGroup
	var delivered int
	var mu sync.Mutex

	for _, pid := range targets {
		wg.Add(1)
		go func(pid peer.ID) {
			defer wg.Done()
			if err := h.sendSignature(pid.String(), clusterID, round, dataHash, signature); err != nil {
				log.Printf("[P2P] Failed to send signature to %s: %v", truncateStr(pid.String(), 12), err)
			} else {
				mu.Lock()
				delivered++
				mu.Unlock()
			}
		}(pid)
	}

	wg.Wait()
	return delivered, nil
}

func (h *Host) sendSignature(peerIDStr string, clusterID string, round int, dataHash, signature string) error {
	peerID, err := peer.Decode(peerIDStr)
	if err != nil {
		return fmt.Errorf("invalid peer ID: %w", err)
	}

	ctx, cancel := context.WithTimeout(h.ctx, 10*time.Second)
	defer cancel()

	stream, err := h.host.NewStream(ctx, peerID, protocol.ID(ProtocolSignature))
	if err != nil {
		return fmt.Errorf("failed to open stream: %w", err)
	}
	defer stream.Close()

	msg := fmt.Sprintf("%s|%d|%s|%s|%s|%d\n", clusterID, round, h.ethAddress, dataHash, signature, time.Now().UnixMilli())
	stream.SetWriteDeadline(time.Now().Add(5 * time.Second))
	if _, err := stream.Write([]byte(msg)); err != nil {
		return fmt.Errorf("failed to write: %w", err)
	}

	ack := make([]byte, 3)
	stream.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := io.ReadFull(stream, ack); err != nil {
		return fmt.Errorf("no ACK received: %w", err)
	}
	if string(ack) != "ACK" {
		return fmt.Errorf("unexpected ack: %q", string(ack))
	}

	return nil
}

func (h *Host) handleSignatureStream(stream network.Stream) {
	defer stream.Close()

	reader := bufio.NewReader(stream)
	msg, err := reader.ReadString('\n')
	if err != nil {
		log.Printf("[P2P] Failed to read signature: %v", err)
		return
	}

	msg = strings.TrimSpace(msg)
	parts := strings.SplitN(msg, "|", 6)
	if len(parts) < 5 {
		log.Printf("[P2P] Invalid signature format (need 5 parts, got %d)", len(parts))
		return
	}

	clusterID := parts[0]
	round := 0
	if _, err := fmt.Sscanf(parts[1], "%d", &round); err != nil {
		log.Printf("[P2P] Invalid signature round: %v", err)
		return
	}
	address := strings.ToLower(parts[2])
	dataHash := parts[3]
	signature := parts[4]

	activeBase, activeMembers, inScope := h.activeIsolationMembers()
	msgBase := baseClusterID(clusterID)
	if inScope {
		if msgBase != activeBase {
			return
		}
		if _, ok := activeMembers[address]; !ok {
			return
		}
	} else {
		if isKeccakClusterID(msgBase) {
			return
		}
	}

	remotePeer := stream.Conn().RemotePeer().String()
	cs := h.getCluster(clusterID)
	if cs != nil {
		cs.mu.Lock()
		isMember := false
		for _, m := range cs.Members {
			if strings.ToLower(m.Address) == address {
				isMember = true
				break
			}
		}
		if !isMember {
			cs.mu.Unlock()
			return
		}
		if bound, ok := cs.PeerBindings[address]; ok && bound != "" && bound != remotePeer {
			cs.mu.Unlock()
			return
		}
		if cs.Signatures[round] == nil {
			cs.Signatures[round] = make(map[string]SigData)
		}
		cs.Signatures[round][address] = SigData{DataHash: dataHash, Signature: signature, Timestamp: time.Now().UnixMilli()}
		cs.mu.Unlock()
		log.Printf("[P2P] Received signature for round %d from %s", round, truncateStr(address, 8))
		h.updatePeerBinding(cs, address, remotePeer)
	} else {
		if !h.storePendingSignature(clusterID, round, address, dataHash, signature) {
			log.Printf("[P2P] Dropped signature for round %d from %s (cluster=%s)", round, truncateStr(address, 8), truncateStr(clusterID, 10))
			return
		}
		log.Printf("[P2P] Buffered signature for round %d from %s (cluster=%s)", round, truncateStr(address, 8), truncateStr(clusterID, 10))
	}

	stream.SetWriteDeadline(time.Now().Add(2 * time.Second))
	if _, err := stream.Write([]byte("ACK")); err != nil {
		log.Printf("[P2P] Failed to send ACK for signature: %v", err)
	}
}

func (h *Host) WaitForSignatureBarrier(clusterID string, round int, members []string, timeoutMs int) (*SignatureBarrierResult, error) {
	cs := h.getCluster(clusterID)
	if cs == nil {
		return nil, fmt.Errorf("cluster not found: %s", clusterID)
	}

	timeout := time.Duration(timeoutMs) * time.Millisecond
	deadline := time.Now().Add(timeout)
	rebroadcastInterval := 10 * time.Second
	lastRebroadcast := time.Now()

	membersLower := make([]string, len(members))
	for i, m := range members {
		membersLower[i] = strings.ToLower(m)
	}

	quorum := pbftQuorum(len(membersLower))

	expectedHash := h.ComputePayloadConsensusHash(clusterID, round, members)

	cs.mu.RLock()
	var ourSig SigData
	if cs.Signatures[round] != nil {
		ourSig = cs.Signatures[round][h.ethAddress]
	}
	cs.mu.RUnlock()

	if expectedHash == "" {
		if ourSig.DataHash != "" {
			expectedHash = ourSig.DataHash
			log.Printf("[P2P] Round %d signature barrier: using broadcast hash %s (no round data)", round, truncateStr(expectedHash, 16))
		} else {
			return nil, fmt.Errorf("cannot determine expected hash - no round data and no signature broadcast")
		}
	} else {
		log.Printf("[P2P] Round %d signature barrier: consensus hash %s", round, truncateStr(expectedHash, 16))
	}

	for time.Now().Before(deadline) {
		cs.mu.RLock()
		sigData := cs.Signatures[round]
		var sigs []SignatureInfo
		var missing []string
		var hashMismatches []string

		for _, addr := range membersLower {
			if sigData != nil {
				if sd, ok := sigData[addr]; ok {

					if sd.DataHash != expectedHash {
						hashMismatches = append(hashMismatches, addr)
						continue
					}
					sigs = append(sigs, SignatureInfo{
						Address:   addr,
						DataHash:  sd.DataHash,
						Signature: sd.Signature,
					})
					continue
				}
			}
			missing = append(missing, addr)
		}
		cs.mu.RUnlock()

		if len(sigs) >= quorum && len(hashMismatches) == 0 {
			log.Printf("[P2P] Round %d signature barrier complete (%d/%d, quorum=%d)", round, len(sigs), len(membersLower), quorum)
			return &SignatureBarrierResult{
				Complete:      true,
				Signatures:    sigs,
				Missing:       nil,
				ConsensusHash: expectedHash,
			}, nil
		}

		if len(hashMismatches) > 0 {
			log.Printf("[P2P] Round %d hash MISMATCH from %d peers: %v", round, len(hashMismatches), hashMismatches)
			return &SignatureBarrierResult{
				Complete:      false,
				Signatures:    sigs,
				Missing:       missing,
				ConsensusHash: expectedHash,
				Mismatched:    hashMismatches,
			}, fmt.Errorf("hash mismatch from %d peers", len(hashMismatches))
		}

		if time.Since(lastRebroadcast) >= rebroadcastInterval && len(missing) > 0 {
			log.Printf("[P2P] Round %d signature progress: %d/%d (missing: %d). Re-broadcasting...",
				round, len(sigs), len(membersLower), len(missing))

			if ourSig.DataHash != "" {
				delivered, _ := h.BroadcastSignature(clusterID, round, ourSig.DataHash, ourSig.Signature)
				log.Printf("[P2P] Re-broadcast round %d signature to %d peers", round, delivered)
			}
			lastRebroadcast = time.Now()
		}

		time.Sleep(500 * time.Millisecond)
	}

	cs.mu.RLock()
	sigData := cs.Signatures[round]
	var sigs []SignatureInfo
	var missing []string
	for _, addr := range membersLower {
		if sigData != nil {
			if sd, ok := sigData[addr]; ok {
				sigs = append(sigs, SignatureInfo{
					Address:   addr,
					DataHash:  sd.DataHash,
					Signature: sd.Signature,
				})
				continue
			}
		}
		missing = append(missing, addr)
	}
	cs.mu.RUnlock()

	log.Printf("[P2P] Round %d signature barrier timeout: %d/%d. Missing: %v", round, len(sigs), len(membersLower), missing)

	return &SignatureBarrierResult{
		Complete:      false,
		Signatures:    sigs,
		Missing:       missing,
		ConsensusHash: expectedHash,
	}, fmt.Errorf("timeout waiting for signature barrier")
}

type SignatureBarrierResult struct {
	Complete      bool            `json:"complete"`
	Signatures    []SignatureInfo `json:"signatures"`
	Missing       []string        `json:"missing"`
	ConsensusHash string          `json:"consensusHash"`
	Mismatched    []string        `json:"mismatched,omitempty"`
}

type SignatureInfo struct {
	Address   string `json:"address"`
	DataHash  string `json:"dataHash"`
	Signature string `json:"signature"`
}

func (h *Host) BroadcastIdentity(clusterID string, publicKey string) (int, error) {
	activeBase, _, inScope := h.activeIsolationMembers()
	if inScope {
		if baseClusterID(clusterID) != activeBase {
			return 0, fmt.Errorf("isolation scope mismatch")
		}
	}

	cs := h.getCluster(clusterID)
	if cs == nil {
		return 0, fmt.Errorf("cluster not found: %s", clusterID)
	}

	cs.mu.Lock()
	cs.Identities[h.ethAddress] = IdentityData{
		PeerID:    h.PeerID(),
		PublicKey: publicKey,
		Timestamp: time.Now().UnixMilli(),
	}
	cs.PeerBindings[h.ethAddress] = h.PeerID()
	cs.mu.Unlock()

	cs.mu.RLock()
	members := make([]string, 0, len(cs.Members))
	bindings := make(map[string]string, len(cs.PeerBindings))
	for _, m := range cs.Members {
		addr := strings.ToLower(m.Address)
		if addr != "" {
			members = append(members, addr)
		}
	}
	for addr, pid := range cs.PeerBindings {
		if pid != "" {
			bindings[addr] = pid
		}
	}
	cs.mu.RUnlock()

	targets := make([]peer.ID, 0, len(members))
	for _, addr := range members {
		if addr == h.ethAddress {
			continue
		}
		pidStr := bindings[addr]
		if pidStr == "" {
			pidStr = h.getPeerIDForAddress(addr)
		}
		if pidStr == "" {
			continue
		}
		pid, err := peer.Decode(pidStr)
		if err != nil {
			continue
		}
		targets = append(targets, pid)
	}

	log.Printf("[P2P] Broadcasting identity to %d peers for cluster %s", len(targets), truncateStr(clusterID, 10))

	var wg sync.WaitGroup
	var delivered int
	var mu sync.Mutex

	for _, pid := range targets {
		wg.Add(1)
		go func(pid peer.ID) {
			defer wg.Done()
			if err := h.sendIdentity(pid.String(), clusterID, publicKey); err != nil {
				log.Printf("[P2P] Failed to send identity to %s: %v", truncateStr(pid.String(), 12), err)
			} else {
				mu.Lock()
				delivered++
				mu.Unlock()
			}
		}(pid)
	}

	wg.Wait()
	log.Printf("[P2P] Identity broadcast complete: delivered to %d peers", delivered)
	return delivered, nil
}

func (h *Host) sendIdentity(peerIDStr string, clusterID string, publicKey string) error {
	peerID, err := peer.Decode(peerIDStr)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(h.ctx, 10*time.Second)
	defer cancel()

	stream, err := h.host.NewStream(ctx, peerID, protocol.ID(ProtocolIdentity))
	if err != nil {
		return err
	}
	defer stream.Close()

	cs := h.getCluster(clusterID)
	var membersList string
	if cs != nil {
		cs.mu.RLock()
		addrs := make([]string, 0, len(cs.Members))
		for _, m := range cs.Members {
			addrs = append(addrs, strings.ToLower(m.Address))
		}
		cs.mu.RUnlock()
		sort.Strings(addrs)
		membersList = strings.Join(addrs, ",")
	}

	identityData := fmt.Sprintf("%s|%s|%s|%s", clusterID, h.PeerID(), publicKey, membersList)
	signature, err := h.signPayload(identityData)
	if err != nil {
		return fmt.Errorf("failed to sign identity: %w", err)
	}

	msg := fmt.Sprintf("%s|%s|%s|%s|%s|%s\n", clusterID, h.ethAddress, h.PeerID(), publicKey, membersList, signature)
	stream.SetWriteDeadline(time.Now().Add(5 * time.Second))
	if _, err := stream.Write([]byte(msg)); err != nil {
		return err
	}

	ack := make([]byte, 3)
	stream.SetReadDeadline(time.Now().Add(5 * time.Second))
	stream.Read(ack)

	return nil
}

func (h *Host) handleIdentityStream(stream network.Stream) {
	defer stream.Close()

	buf := make([]byte, 8192)
	stream.SetReadDeadline(time.Now().Add(5 * time.Second))
	n, err := stream.Read(buf)
	if err != nil {
		return
	}

	msg := string(buf[:n])
	parts := strings.SplitN(strings.TrimSpace(msg), "|", 6)
	if len(parts) < 6 {

		if len(parts) == 5 {
			log.Printf("[P2P] Received old identity format without member list from %s - ignoring", truncateStr(parts[1], 8))
		} else {
			log.Printf("[P2P] Invalid identity format (need 6 parts, got %d)", len(parts))
		}
		return
	}

	clusterID := parts[0]
	clusterKey := strings.ToLower(clusterID)
	address := strings.ToLower(parts[1])
	peerIDStr := parts[2]
	publicKey := parts[3]
	membersList := parts[4]
	signature := parts[5]

	activeBase, activeMembers, inScope := h.activeIsolationMembers()
	if inScope {
		if baseClusterID(clusterID) != activeBase {
			return
		}
		if _, ok := activeMembers[address]; !ok {
			return
		}
	}

	var members []string
	if membersList != "" {
		members = strings.Split(membersList, ",")
	}

	base := baseClusterID(clusterID)
	if isKeccakClusterID(base) {
		expected, err := computeClusterIDFromMembers(members)
		if err != nil || expected != base {
			log.Printf("[P2P] Rejected identity for cluster %s: cluster id mismatch", truncateStr(clusterKey, 10))
			return
		}
	}

	isMember := false
	for _, m := range members {
		if strings.ToLower(m) == h.ethAddress {
			isMember = true
			break
		}
	}
	if !isMember {

		return
	}

	senderIsMember := false
	for _, m := range members {
		if strings.ToLower(m) == address {
			senderIsMember = true
			break
		}
	}
	if !senderIsMember {
		log.Printf("[P2P] Rejected identity from %s: sender not in member list", truncateStr(address, 8))
		return
	}

	identityData := fmt.Sprintf("%s|%s|%s|%s", clusterID, peerIDStr, publicKey, membersList)
	if !verifyPayloadSignature(identityData, signature, address) {
		log.Printf("[P2P] Rejected identity from %s: invalid signature", truncateStr(address, 8))
		return
	}

	cs := h.getCluster(clusterKey)
	if cs == nil {

		log.Printf("[P2P] Auto-creating cluster %s from identity (we are a member)", truncateStr(clusterKey, 10))
		h.mu.Lock()
		memberInfos := make([]PeerInfo, len(members))
		for i, m := range members {
			memberInfos[i] = PeerInfo{Address: strings.ToLower(m)}
		}
		cs = &ClusterState{
			ID:             clusterKey,
			Members:        memberInfos,
			Identities:     make(map[string]IdentityData),
			PeerBindings:   make(map[string]string),
			RoundData:      make(map[int]map[string]string),
			RoundDataTimes: make(map[int]time.Time),
			Signatures:     make(map[int]map[string]SigData),
			PeerScores:     make(map[string]float64),
		}
		for _, m := range members {
			cs.PeerScores[strings.ToLower(m)] = 100.0
		}
		h.clusters[clusterKey] = cs
		h.mu.Unlock()
	}

	cs.mu.Lock()
	cs.Identities[address] = IdentityData{
		PeerID:    peerIDStr,
		PublicKey: publicKey,
		Timestamp: time.Now().UnixMilli(),
	}
	cs.PeerBindings[address] = peerIDStr
	cs.mu.Unlock()
	log.Printf("[P2P] Received verified identity from %s -> %s", truncateStr(address, 8), truncateStr(peerIDStr, 12))
	remotePeer := stream.Conn().RemotePeer()
	if cm := h.host.ConnManager(); cm != nil {
		cm.Protect(remotePeer, "cluster")
	}

	stream.SetWriteDeadline(time.Now().Add(2 * time.Second))
	stream.Write([]byte("ACK"))
}

func (h *Host) WaitForIdentities(clusterID string, members []string, timeoutMs int) (*IdentityResult, error) {
	cs := h.getCluster(clusterID)
	if cs == nil {
		return nil, fmt.Errorf("cluster not found: %s", clusterID)
	}

	timeout := time.Duration(timeoutMs) * time.Millisecond
	deadline := time.Now().Add(timeout)
	rebroadcastInterval := 10 * time.Second
	lastRebroadcast := time.Now()

	membersLower := make([]string, len(members))
	for i, m := range members {
		membersLower[i] = strings.ToLower(m)
	}

	h.BroadcastIdentity(clusterID, "")
	log.Printf("[P2P] Identity exchange started for cluster %s with %d members", truncateStr(clusterID, 10), len(members))

	for time.Now().Before(deadline) {
		cs.mu.RLock()
		var identities []IdentityInfo
		var missing []string
		for _, addr := range membersLower {
			if id, ok := cs.Identities[addr]; ok {
				identities = append(identities, IdentityInfo{
					Address:   addr,
					PeerID:    id.PeerID,
					PublicKey: id.PublicKey,
				})
			} else {
				missing = append(missing, addr)
			}
		}
		cs.mu.RUnlock()

		if len(identities) >= len(membersLower) {
			log.Printf("[P2P] Identity exchange complete: %d/%d identities received", len(identities), len(membersLower))
			return &IdentityResult{
				Complete:   true,
				Identities: identities,
				Missing:    nil,
			}, nil
		}

		if time.Since(lastRebroadcast) >= rebroadcastInterval {
			log.Printf("[P2P] Identity progress: %d/%d (missing: %d). Re-broadcasting...",
				len(identities), len(membersLower), len(missing))

			delivered, _ := h.BroadcastIdentity(clusterID, "")
			log.Printf("[P2P] Re-broadcast identity to %d peers", delivered)
			lastRebroadcast = time.Now()
		}

		time.Sleep(500 * time.Millisecond)
	}

	cs.mu.RLock()
	var identities []IdentityInfo
	var missing []string
	for _, addr := range membersLower {
		if id, ok := cs.Identities[addr]; ok {
			identities = append(identities, IdentityInfo{
				Address:   addr,
				PeerID:    id.PeerID,
				PublicKey: id.PublicKey,
			})
		} else {
			missing = append(missing, addr)
		}
	}
	cs.mu.RUnlock()

	log.Printf("[P2P] Identity exchange timeout: %d/%d identities. Missing: %v",
		len(identities), len(membersLower), missing)

	return &IdentityResult{
		Complete:   false,
		Identities: identities,
		Missing:    missing,
	}, fmt.Errorf("timeout waiting for identities")
}

type IdentityResult struct {
	Complete   bool           `json:"complete"`
	Identities []IdentityInfo `json:"identities"`
	Missing    []string       `json:"missing"`
}

type IdentityInfo struct {
	Address   string `json:"address"`
	PeerID    string `json:"peerId"`
	PublicKey string `json:"publicKey"`
}

func (h *Host) GetPeerBindings(clusterID string) map[string]string {
	cs := h.getCluster(clusterID)
	if cs == nil {
		return make(map[string]string)
	}

	cs.mu.RLock()
	defer cs.mu.RUnlock()

	result := make(map[string]string)
	for addr, peerID := range cs.PeerBindings {
		result[addr] = peerID
	}
	return result
}

func (h *Host) ClearPeerBindings(clusterID string) {
	cs := h.getCluster(clusterID)
	if cs == nil {
		return
	}

	cs.mu.Lock()
	cs.PeerBindings = make(map[string]string)
	cs.Identities = make(map[string]IdentityData)
	cs.mu.Unlock()

	log.Printf("[P2P] Cleared peer bindings for cluster %s", truncateStr(clusterID, 10))
}

func (h *Host) GetPeerScores(clusterID string) map[string]float64 {
	cs := h.getCluster(clusterID)
	if cs == nil {
		return make(map[string]float64)
	}

	cs.mu.RLock()
	defer cs.mu.RUnlock()

	result := make(map[string]float64)
	for addr, score := range cs.PeerScores {
		result[addr] = score
	}
	return result
}

func (h *Host) ReportPeerFailure(clusterID, address, reason string) {
	cs := h.getCluster(clusterID)
	if cs == nil {
		return
	}

	h.penalizePeer(cs, strings.ToLower(address), 10.0)
	log.Printf("[P2P] Peer %s reported failed: %s", truncateStr(address, 8), reason)
}

func (h *Host) getPeerID(cs *ClusterState, address string) string {
	cs.mu.RLock()
	peerID := cs.PeerBindings[address]
	cs.mu.RUnlock()
	if peerID != "" {
		return peerID
	}
	return h.getPeerIDForAddress(address)
}

func (h *Host) updatePeerBinding(cs *ClusterState, address, peerID string) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	if cs.PeerBindingTimes == nil {
		cs.PeerBindingTimes = make(map[string]time.Time)
	}
	existingPeerID, exists := cs.PeerBindings[address]
	if exists && existingPeerID != peerID {
		lastChange := cs.PeerBindingTimes[address]
		cooldown := 5 * time.Minute
		if time.Since(lastChange) < cooldown {
			log.Printf("[P2P] PeerID binding change rejected for %s: cooldown not expired (last change %v ago)", truncateStr(address, 8), time.Since(lastChange))
			return
		}
		log.Printf("[P2P] PeerID binding changed for %s: %s -> %s", truncateStr(address, 8), truncateStr(existingPeerID, 12), truncateStr(peerID, 12))
	}
	cs.PeerBindings[address] = peerID
	cs.PeerBindingTimes[address] = time.Now()
}
func (h *Host) penalizePeer(cs *ClusterState, address string, penalty float64) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	if score, ok := cs.PeerScores[address]; ok {
		cs.PeerScores[address] = score - penalty
		if cs.PeerScores[address] < 0 {
			cs.PeerScores[address] = 0
		}
	}
}

func (h *Host) GetHeartbeatManager() *HeartbeatManager {
	return h.heartbeat
}

func (h *Host) signPayload(payload string) (string, error) {
	hash := ethcrypto.Keccak256Hash([]byte(payload))
	sig, err := ethcrypto.Sign(hash.Bytes(), h.ethPrivKey)
	if err != nil {
		return "", err
	}

	if sig[64] < 27 {
		sig[64] += 27
	}
	return hex.EncodeToString(sig), nil
}

func (h *Host) signRoundData(clusterID string, round int, address string, payload string, timestamp int64) (string, error) {
	preimage := fmt.Sprintf("znode-round-v2|%s|%d|%s|%s|%d", clusterID, round, strings.ToLower(address), payload, timestamp)
	hash := ethcrypto.Keccak256Hash([]byte(preimage))
	sig, err := ethcrypto.Sign(hash.Bytes(), h.ethPrivKey)
	if err != nil {
		return "", err
	}
	if sig[64] < 27 {
		sig[64] += 27
	}
	return hex.EncodeToString(sig), nil
}

func verifyRoundDataSignatureV2(clusterID string, round int, address string, payload string, timestamp int64, signature string) bool {
	sigBytes, err := hex.DecodeString(signature)
	if err != nil || len(sigBytes) != 65 {
		return false
	}
	if sigBytes[64] >= 27 {
		sigBytes[64] -= 27
	}
	preimage := fmt.Sprintf("znode-round-v2|%s|%d|%s|%s|%d", clusterID, round, strings.ToLower(address), payload, timestamp)
	hash := ethcrypto.Keccak256Hash([]byte(preimage))
	pubKey, err := ethcrypto.SigToPub(hash.Bytes(), sigBytes)
	if err != nil {
		return false
	}
	recoveredAddr := strings.ToLower(ethcrypto.PubkeyToAddress(*pubKey).Hex())
	return recoveredAddr == strings.ToLower(address)
}

func verifyPayloadSignature(payload, signature, claimedAddress string) bool {
	sigBytes, err := hex.DecodeString(signature)
	if err != nil || len(sigBytes) != 65 {
		return false
	}

	if sigBytes[64] >= 27 {
		sigBytes[64] -= 27
	}

	hash := ethcrypto.Keccak256Hash([]byte(payload))
	pubKey, err := ethcrypto.SigToPub(hash.Bytes(), sigBytes)
	if err != nil {
		return false
	}

	recoveredAddr := strings.ToLower(ethcrypto.PubkeyToAddress(*pubKey).Hex())
	return recoveredAddr == strings.ToLower(claimedAddress)
}

func (h *Host) ComputePayloadConsensusHash(clusterID string, round int, members []string) string {
	cs := h.getCluster(clusterID)
	if cs == nil {
		return ""
	}

	sortedMembers := make([]string, len(members))
	copy(sortedMembers, members)
	for i := range sortedMembers {
		sortedMembers[i] = strings.ToLower(sortedMembers[i])
	}

	for i := 0; i < len(sortedMembers)-1; i++ {
		for j := i + 1; j < len(sortedMembers); j++ {
			if sortedMembers[i] > sortedMembers[j] {
				sortedMembers[i], sortedMembers[j] = sortedMembers[j], sortedMembers[i]
			}
		}
	}

	cs.mu.RLock()
	defer cs.mu.RUnlock()

	roundData := cs.RoundData[round]
	if roundData == nil {
		return ""
	}

	var parts []string
	for _, addr := range sortedMembers {
		if payload, ok := roundData[addr]; ok {
			parts = append(parts, fmt.Sprintf("%s:%s", addr, payload))
		}
	}

	canonical := strings.Join(parts, "|")
	hash := ethcrypto.Keccak256Hash([]byte(canonical))
	return hash.Hex()
}

func (h *Host) RunConsensus(clusterID, phase, data string, members []string, requireAll bool) (*ConsensusResult, error) {
	if h.pbft == nil {
		return nil, fmt.Errorf("PBFT manager not initialized")
	}
	return h.pbft.RunConsensus(clusterID, phase, data, members, requireAll)
}

func (h *Host) GetPBFTManager() *PBFTManager {
	return h.pbft
}

func (h *Host) getPeerIDForAddress(address string) string {
	address = strings.ToLower(address)

	h.mu.RLock()
	for _, cs := range h.clusters {
		cs.mu.RLock()
		if peerID, ok := cs.PeerBindings[address]; ok {
			cs.mu.RUnlock()
			h.mu.RUnlock()
			return peerID
		}
		cs.mu.RUnlock()
	}
	h.mu.RUnlock()

	if h.heartbeat != nil {
		if peerID := h.heartbeat.GetPeerIDForAddress(address); peerID != "" {
			return peerID
		}
	}

	return ""
}

func (h *Host) GetPreSelectionManager() *PreSelectionManager {
	return h.presel
}

func (h *Host) GetRoundData(clusterID string, round int) map[string]string {
	cs := h.getCluster(clusterID)
	activeBase, _, inScope := h.activeIsolationMembers()
	if inScope {
		msgBase := baseClusterID(clusterID)
		if msgBase != activeBase {
			return nil
		}
	}
	if cs == nil {
		return nil
	}
	cs.mu.RLock()
	roundData := cs.RoundData[round]
	if roundData == nil {
		cs.mu.RUnlock()
		return nil
	}
	result := make(map[string]string, len(roundData))
	for k, v := range roundData {
		result[k] = v
	}
	cs.mu.RUnlock()
	return result
}
func (h *Host) startCleanupRoutine() {
	h.cleanupTicker = time.NewTicker(time.Duration(CleanupIntervalMs) * time.Millisecond)
	go func() {
		for {
			select {
			case <-h.cleanupTicker.C:
				h.cleanupOldData()
			case <-h.cleanupDone:
				return
			}
		}
	}()
}
func (h *Host) cleanupOldData() {
	h.messageCache.Cleanup(h.NetNowMs(), MessageMaxAgeMs)
	h.rateLimiter.Cleanup(3600)
	now := time.Now()
	cutoff := now.Add(-time.Duration(RoundDataTTLMs) * time.Millisecond)
	h.mu.Lock()
	for _, cs := range h.clusters {
		cs.mu.Lock()
		var toDelete []int
		for round, ts := range cs.RoundDataTimes {
			if ts.Before(cutoff) {
				toDelete = append(toDelete, round)
			}
		}
		for _, round := range toDelete {
			delete(cs.RoundData, round)
			delete(cs.RoundDataTimes, round)
			delete(cs.Signatures, round)
		}
		cs.mu.Unlock()
	}
	h.mu.Unlock()
	h.pendingRoundMu.Lock()
	for clusterID, timestamp := range h.pendingRoundUpdated {
		if timestamp.Before(cutoff) {
			delete(h.pendingRoundData, clusterID)
			delete(h.pendingRoundUpdated, clusterID)
		}
	}
	h.pendingRoundMu.Unlock()
}
func (h *Host) stopCleanupRoutine() {
	if h.cleanupTicker != nil {
		h.cleanupTicker.Stop()
	}
	close(h.cleanupDone)
}

func (h *Host) GetDiscoveryManager() *DiscoveryManager {
	return h.discovery
}

func (h *Host) ResetDiscovery() {
	if h.discovery != nil {
		h.discovery.ResetDialState()
	}
}

func (h *Host) ForceBootstrap() {
	if h.discovery != nil {
		h.discovery.ForceRediscovery()
	}
}

func (h *Host) GetDialStateDebug() map[string]interface{} {
	if h.discovery == nil {
		return map[string]interface{}{
			"error": "discovery manager not initialized",
		}
	}
	return h.discovery.GetDialStateDebug()
}
