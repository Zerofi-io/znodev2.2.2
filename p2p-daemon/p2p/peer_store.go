package p2p

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

const peersFileEnvKey = "P2P_PEERS_FILE"
const defaultMaxPeers = 1000
const defaultPeerMaxAgeDays = 30

type storedPeer struct {
	ID       string   `json:"id"`
	Addrs    []string `json:"addrs"`
	LastSeen int64    `json:"lastSeen"`
}

type storedPeerFile struct {
	Version int          `json:"version"`
	Peers   []storedPeer `json:"peers"`
}

type PersistentPeerStore struct {
	path       string
	maxPeers   int
	maxAgeDays int
	mu         sync.Mutex
}

func NewPersistentPeerStoreFromEnv() *PersistentPeerStore {
	path := os.Getenv(peersFileEnvKey)
	if path == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			log.Printf("[Peers] Could not determine home directory for peer store: %v (persistence disabled)", err)
			return nil
		}
		path = filepath.Join(home, ".znode", "p2p-peers.json")
	}
	maxPeers := defaultMaxPeers
	if v := os.Getenv("P2P_MAX_STORED_PEERS"); v != "" {
		var n int
		if _, err := fmt.Sscanf(v, "%d", &n); err == nil && n > 0 {
			maxPeers = n
		}
	}
	maxAgeDays := defaultPeerMaxAgeDays
	if v := os.Getenv("P2P_PEER_MAX_AGE_DAYS"); v != "" {
		var n int
		if _, err := fmt.Sscanf(v, "%d", &n); err == nil && n > 0 {
			maxAgeDays = n
		}
	}
	return &PersistentPeerStore{path: path, maxPeers: maxPeers, maxAgeDays: maxAgeDays}
}

func (s *PersistentPeerStore) LoadBootstrapMultiaddrs() []string {
	if s == nil || s.path == "" {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	file, err := s.loadLocked()
	if err != nil {
		log.Printf("[Peers] Failed to load persisted peers from %s: %v", s.path, err)
		return nil
	}
	addrSet := make(map[string]struct{})
	for _, p := range file.Peers {
		for _, a := range p.Addrs {
			if a == "" {
				continue
			}
			addrSet[a] = struct{}{}
		}
	}
	if len(addrSet) == 0 {
		return nil
	}
	result := make([]string, 0, len(addrSet))
	for a := range addrSet {
		result = append(result, a)
	}
	return result
}

func (s *PersistentPeerStore) RememberPeer(info peer.AddrInfo) {
	if s == nil || s.path == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	file, err := s.loadLocked()
	if err != nil {
		log.Printf("[Peers] Failed to load persisted peers from %s: %v", s.path, err)
		file = &storedPeerFile{Version: 1}
	}
	now := time.Now().Unix()
	addrs, err := peer.AddrInfoToP2pAddrs(&info)
	if err != nil {
		log.Printf("[Peers] Failed to convert peer %s addrs for persistence: %v", info.ID, err)
		return
	}
	newAddrSet := make(map[string]struct{}, len(addrs))
	for _, a := range addrs {
		if a == nil {
			continue
		}
		str := a.String()
		if str == "" {
			continue
		}
		newAddrSet[str] = struct{}{}
	}
	if len(newAddrSet) == 0 {
		return
	}
	updated := false
	for i := range file.Peers {
		if file.Peers[i].ID == info.ID.String() {
			existing := make(map[string]struct{}, len(file.Peers[i].Addrs))
			for _, a := range file.Peers[i].Addrs {
				if a == "" {
					continue
				}
				existing[a] = struct{}{}
			}
			for a := range newAddrSet {
				if _, ok := existing[a]; !ok {
					file.Peers[i].Addrs = append(file.Peers[i].Addrs, a)
					existing[a] = struct{}{}
				}
			}
			file.Peers[i].LastSeen = now
			updated = true
			break
		}
	}
	if !updated {
		addrsSlice := make([]string, 0, len(newAddrSet))
		for a := range newAddrSet {
			addrsSlice = append(addrsSlice, a)
		}
		file.Peers = append(file.Peers, storedPeer{
			ID:       info.ID.String(),
			Addrs:    addrsSlice,
			LastSeen: now,
		})
	}
	file.Peers = s.prunePeers(file.Peers, now)
	if err := s.saveLocked(file); err != nil {
		log.Printf("[Peers] Failed to persist peers to %s: %v", s.path, err)
	}
}

func (s *PersistentPeerStore) prunePeers(peers []storedPeer, now int64) []storedPeer {
	cutoff := now - int64(s.maxAgeDays*24*60*60)
	filtered := make([]storedPeer, 0, len(peers))
	for _, p := range peers {
		if p.LastSeen >= cutoff {
			filtered = append(filtered, p)
		}
	}
	if len(filtered) <= s.maxPeers {
		return filtered
	}
	sort.Slice(filtered, func(i, j int) bool {
		return filtered[i].LastSeen > filtered[j].LastSeen
	})
	return filtered[:s.maxPeers]
}

func (s *PersistentPeerStore) loadLocked() (*storedPeerFile, error) {
	data, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return &storedPeerFile{Version: 1}, nil
		}
		return &storedPeerFile{Version: 1}, err
	}
	var file storedPeerFile
	if err := json.Unmarshal(data, &file); err != nil {
		log.Printf("[Peers] Invalid peers file %s (starting fresh): %v", s.path, err)
		return &storedPeerFile{Version: 1}, nil
	}
	if file.Version == 0 {
		file.Version = 1
	}
	return &file, nil
}

func (s *PersistentPeerStore) saveLocked(file *storedPeerFile) error {
	if file == nil {
		file = &storedPeerFile{Version: 1}
	}
	dir := filepath.Dir(s.path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, "peers-*.json")
	if err != nil {
		return err
	}
	enc := json.NewEncoder(tmp)
	enc.SetIndent("", "  ")
	if err := enc.Encode(file); err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmp.Name())
		return err
	}
	return os.Rename(tmp.Name(), s.path)
}
