package p2p

import (
	"context"
	"log"
	"net"
	"sync"
	"time"

	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	drouting "github.com/libp2p/go-libp2p/p2p/discovery/routing"
	dutil "github.com/libp2p/go-libp2p/p2p/discovery/util"

	ma "github.com/multiformats/go-multiaddr"
)

const (
	DiscoveryNamespace      = "znode/v1"
	DiscoveryInterval       = 30 * time.Second
	maxDialAttempts         = 5
	initialBackoff          = 1 * time.Minute
	maxBackoff              = 30 * time.Minute
	minDesiredPeers         = 5
	criticalPeerCount       = 2
	maintenanceInterval     = 30 * time.Second
	bootstrapRetryInterval  = 60 * time.Second
	lowPeerThresholdMinutes = 10
	dialStatePruneAfter     = 6 * time.Hour
)

type dialState struct {
	attempts      int
	lastAttempt   time.Time
	cooldownUntil time.Time
}

type DiscoveryManager struct {
	host                host.Host
	ctx                 context.Context
	cancel              context.CancelFunc
	dht                 *dht.IpfsDHT
	discovery           *drouting.RoutingDiscovery
	mu                  sync.RWMutex
	running             bool
	noPeersLogged       bool
	onPeerFound         func(peer.AddrInfo)
	onPeerConnectedCb   func(peer.ID)
	dialStates          map[peer.ID]*dialState
	bootstrapPeerIDs    map[peer.ID]bool
	bootstrapPeers      []peer.AddrInfo
	lowPeersSince       time.Time
	disconnectNotifier  *network.NotifyBundle
	findPeersInProgress bool
}

func NewDiscoveryManager(ctx context.Context, h host.Host, bootstrapPeers []peer.AddrInfo) (*DiscoveryManager, error) {
	dctx, cancel := context.WithCancel(ctx)
	kdht, err := dht.New(dctx, h,
		dht.Mode(dht.ModeAutoServer),
		dht.BootstrapPeers(bootstrapPeers...),
	)
	if err != nil {
		cancel()
		return nil, err
	}
	if err := kdht.Bootstrap(dctx); err != nil {
		cancel()
		return nil, err
	}
	routingDiscovery := drouting.NewRoutingDiscovery(kdht)
	bootstrapIDs := make(map[peer.ID]bool, len(bootstrapPeers))
	for _, p := range bootstrapPeers {
		bootstrapIDs[p.ID] = true
	}
	dm := &DiscoveryManager{
		host:             h,
		ctx:              dctx,
		cancel:           cancel,
		dht:              kdht,
		discovery:        routingDiscovery,
		dialStates:       make(map[peer.ID]*dialState),
		bootstrapPeerIDs: bootstrapIDs,
		bootstrapPeers:   bootstrapPeers,
	}
	return dm, nil
}

func (dm *DiscoveryManager) SetPeerFoundCallback(cb func(peer.AddrInfo)) {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	dm.onPeerFound = cb
}

func (dm *DiscoveryManager) SetPeerConnectedCallback(cb func(peer.ID)) {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	dm.onPeerConnectedCb = cb
}
func (dm *DiscoveryManager) replaceBootstrapPeer(oldID peer.ID, newInfo peer.AddrInfo) {
	if oldID == "" || newInfo.ID == "" || oldID == newInfo.ID {
		return
	}
	dm.mu.Lock()
	delete(dm.bootstrapPeerIDs, oldID)
	dm.bootstrapPeerIDs[newInfo.ID] = true
	replaced := false
	for i := range dm.bootstrapPeers {
		if dm.bootstrapPeers[i].ID != oldID {
			continue
		}
		dm.bootstrapPeers[i] = newInfo
		replaced = true
	}
	if !replaced {
		dm.bootstrapPeers = append(dm.bootstrapPeers, newInfo)
	}
	dm.mu.Unlock()
}

func (dm *DiscoveryManager) Start() {
	dm.mu.Lock()
	if dm.running {
		dm.mu.Unlock()
		return
	}
	dm.running = true
	dm.mu.Unlock()
	log.Printf("[Discovery] Starting DHT-based peer discovery (namespace: %s)", DiscoveryNamespace)
	dm.setupDisconnectHandler()
	go dm.advertiseLoop()
	go dm.discoverLoop()
	go dm.maintenanceLoop()
	go dm.bootstrapMaintainerLoop()
}

func (dm *DiscoveryManager) Stop() {
	dm.mu.Lock()
	if !dm.running {
		dm.mu.Unlock()
		return
	}
	dm.running = false
	cancel := dm.cancel
	dht := dm.dht
	notifier := dm.disconnectNotifier
	dm.disconnectNotifier = nil
	dm.mu.Unlock()
	if notifier != nil {
		dm.host.Network().StopNotify(notifier)
	}
	cancel()
	dht.Close()
}

func (dm *DiscoveryManager) setupDisconnectHandler() {
	nb := &network.NotifyBundle{
		DisconnectedF: func(n network.Network, c network.Conn) {
			peerID := c.RemotePeer()
			peerCount := len(n.Peers())
			if peerCount < minDesiredPeers {
				log.Printf("[Discovery] Peer %s disconnected, count=%d (below threshold %d), scheduling redial",
					peerID.String()[:16], peerCount, minDesiredPeers)
				go dm.scheduleRedial(peerID, 5*time.Second)
			}
			dm.mu.Lock()
			if dm.bootstrapPeerIDs[peerID] {
				log.Printf("[Discovery] Bootstrap peer %s disconnected, will reconnect", peerID.String()[:16])
			}
			dm.mu.Unlock()
		},
	}
	dm.mu.Lock()
	dm.disconnectNotifier = nb
	dm.mu.Unlock()
	dm.host.Network().Notify(nb)
}

func (dm *DiscoveryManager) scheduleRedial(peerID peer.ID, delay time.Duration) {
	time.Sleep(delay)
	dm.mu.Lock()
	if !dm.running {
		dm.mu.Unlock()
		return
	}
	dm.mu.Unlock()
	if dm.host.Network().Connectedness(peerID) == network.Connected {
		return
	}
	peerInfo := dm.host.Peerstore().PeerInfo(peerID)
	if len(peerInfo.Addrs) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(dm.ctx, 30*time.Second)
	defer cancel()
	filtered := peerInfo
	filtered.Addrs = filterDialableMultiaddrs(peerInfo.Addrs)
	if len(filtered.Addrs) == 0 {
		return
	}
	if err := dm.host.Connect(ctx, filtered); err != nil {
		log.Printf("[Discovery] Scheduled redial to %s failed: %v", peerID.String()[:16], err)
	} else {
		log.Printf("[Discovery] Scheduled redial to %s succeeded", peerID.String()[:16])
		dm.onPeerConnected(peerID)
	}
}

func (dm *DiscoveryManager) maintenanceLoop() {
	ticker := time.NewTicker(maintenanceInterval)
	defer ticker.Stop()
	for {
		select {
		case <-dm.ctx.Done():
			return
		case <-ticker.C:
			dm.pruneDialStates()
			dm.checkPeerHealth()
		}
	}
}

func (dm *DiscoveryManager) pruneDialStates() {
	now := time.Now()
	dm.mu.Lock()
	defer dm.mu.Unlock()
	pruned := 0
	for peerID, state := range dm.dialStates {
		if dm.bootstrapPeerIDs[peerID] {
			continue
		}
		if state == nil || state.lastAttempt.IsZero() {
			continue
		}
		if now.Sub(state.lastAttempt) > dialStatePruneAfter && now.After(state.cooldownUntil) {
			delete(dm.dialStates, peerID)
			pruned++
		}
	}
	if pruned > 0 {
		log.Printf("[Discovery] Pruned %d stale dial state entries", pruned)
	}
}

func (dm *DiscoveryManager) checkPeerHealth() {
	peerCount := len(dm.host.Network().Peers())
	dm.mu.Lock()
	if peerCount < minDesiredPeers {
		if dm.lowPeersSince.IsZero() {
			dm.lowPeersSince = time.Now()
			log.Printf("[Discovery] WARNING: Peer count %d below minimum %d", peerCount, minDesiredPeers)
		}
		lowDuration := time.Since(dm.lowPeersSince)
		if peerCount < criticalPeerCount {
			log.Printf("[Discovery] CRITICAL: Peer count %d below critical threshold %d, clearing all dial cooldowns",
				peerCount, criticalPeerCount)
			dm.dialStates = make(map[peer.ID]*dialState)
			dm.mu.Unlock()
			dm.forceBootstrapReconnect()
			go dm.findPeers()
			return
		}
		if lowDuration > lowPeerThresholdMinutes*time.Minute {
			log.Printf("[Discovery] Peer count %d below threshold for %v, clearing expired cooldowns",
				peerCount, lowDuration)
			dm.clearExpiredCooldowns()
			dm.mu.Unlock()
			go dm.findPeers()
			return
		}
	} else {
		if !dm.lowPeersSince.IsZero() {
			log.Printf("[Discovery] Peer count recovered to %d", peerCount)
		}
		dm.lowPeersSince = time.Time{}
	}
	dm.mu.Unlock()
}

func (dm *DiscoveryManager) clearExpiredCooldowns() {
	now := time.Now()
	cleared := 0
	for peerID, state := range dm.dialStates {
		if now.After(state.cooldownUntil) {
			delete(dm.dialStates, peerID)
			cleared++
		}
	}
	if cleared > 0 {
		log.Printf("[Discovery] Cleared %d expired dial cooldowns", cleared)
	}
}

func (dm *DiscoveryManager) bootstrapMaintainerLoop() {
	ticker := time.NewTicker(bootstrapRetryInterval)
	defer ticker.Stop()
	for {
		select {
		case <-dm.ctx.Done():
			return
		case <-ticker.C:
			dm.maintainBootstrapConnections()
		}
	}
}

func (dm *DiscoveryManager) maintainBootstrapConnections() {
	dm.mu.RLock()
	bootstrapPeers := dm.bootstrapPeers
	dm.mu.RUnlock()
	for _, peerInfo := range bootstrapPeers {
		if dm.host.Network().Connectedness(peerInfo.ID) == network.Connected {
			continue
		}
		go dm.connectToBootstrapPeer(peerInfo)
	}
}

func (dm *DiscoveryManager) connectToBootstrapPeer(peerInfo peer.AddrInfo) {
	ctx, cancel := context.WithTimeout(dm.ctx, 30*time.Second)
	defer cancel()
	filtered := peerInfo
	filtered.Addrs = filterDialableMultiaddrs(peerInfo.Addrs)
	if len(filtered.Addrs) == 0 {
		return
	}
	if err := dm.host.Connect(ctx, filtered); err != nil {
		expected, actual, ok := parsePeerIDMismatchError(err)
		if ok && expected == peerInfo.ID {
			corrected := peer.AddrInfo{ID: actual, Addrs: filtered.Addrs}
			if err2 := dm.host.Connect(ctx, corrected); err2 == nil {
				log.Printf("[Discovery] Bootstrap peer id corrected: %s -> %s", expected.String()[:16], actual.String()[:16])
				dm.replaceBootstrapPeer(peerInfo.ID, corrected)
				if cm := dm.host.ConnManager(); cm != nil {
					cm.Protect(corrected.ID, "bootstrap")
				}
				dm.onPeerConnected(corrected.ID)
				dm.mu.RLock()
				cb := dm.onPeerFound
				dm.mu.RUnlock()
				if cb != nil {
					cb(corrected)
				}
				return
			}
		}
		log.Printf("[Discovery] Bootstrap peer %s reconnect failed: %v", peerInfo.ID.String()[:16], err)
	} else {
		log.Printf("[Discovery] Bootstrap peer %s reconnected", peerInfo.ID.String()[:16])
		if cm := dm.host.ConnManager(); cm != nil {
			cm.Protect(peerInfo.ID, "bootstrap")
		}
		dm.onPeerConnected(peerInfo.ID)
	}
}

func (dm *DiscoveryManager) forceBootstrapReconnect() {
	log.Printf("[Discovery] Forcing bootstrap peer reconnection")
	dm.mu.RLock()
	bootstrapPeers := dm.bootstrapPeers
	dm.mu.RUnlock()
	var wg sync.WaitGroup
	for _, peerInfo := range bootstrapPeers {
		wg.Add(1)
		go func(pi peer.AddrInfo) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(dm.ctx, 30*time.Second)
			defer cancel()
			filtered := pi
			filtered.Addrs = filterDialableMultiaddrs(pi.Addrs)
			if len(filtered.Addrs) == 0 {
				return
			}
			if err := dm.host.Connect(ctx, filtered); err != nil {
				expected, actual, ok := parsePeerIDMismatchError(err)
				if ok && expected == pi.ID {
					corrected := peer.AddrInfo{ID: actual, Addrs: filtered.Addrs}
					if err2 := dm.host.Connect(ctx, corrected); err2 == nil {
						log.Printf("[Discovery] Bootstrap peer id corrected: %s -> %s", expected.String()[:16], actual.String()[:16])
						dm.replaceBootstrapPeer(pi.ID, corrected)
						if cm := dm.host.ConnManager(); cm != nil {
							cm.Protect(corrected.ID, "bootstrap")
						}
						dm.onPeerConnected(corrected.ID)
						dm.mu.RLock()
						cb := dm.onPeerFound
						dm.mu.RUnlock()
						if cb != nil {
							cb(corrected)
						}
						return
					}
				}
				log.Printf("[Discovery] Force bootstrap %s failed: %v", pi.ID.String()[:16], err)
			} else {
				log.Printf("[Discovery] Force bootstrap %s succeeded", pi.ID.String()[:16])
				if cm := dm.host.ConnManager(); cm != nil {
					cm.Protect(pi.ID, "bootstrap")
				}
				dm.onPeerConnected(pi.ID)
			}
		}(peerInfo)
	}
	wg.Wait()
}

func (dm *DiscoveryManager) advertiseLoop() {
	dm.advertise()
	ticker := time.NewTicker(DiscoveryInterval)
	defer ticker.Stop()
	for {
		select {
		case <-dm.ctx.Done():
			return
		case <-ticker.C:
			dm.advertise()
		}
	}
}

func (dm *DiscoveryManager) advertise() {
	if len(dm.host.Network().Peers()) == 0 {
		dm.mu.Lock()
		if !dm.noPeersLogged {
			log.Printf("[Discovery] No connected peers yet; skipping DHT advertise until bootstrap completes")
			dm.noPeersLogged = true
		}
		dm.mu.Unlock()
		return
	}
	dm.mu.Lock()
	dm.noPeersLogged = false
	dm.mu.Unlock()
	ttl, err := dm.discovery.Advertise(dm.ctx, DiscoveryNamespace)
	if err != nil {
		log.Printf("[Discovery] Advertise error: %v", err)
		return
	}
	log.Printf("[Discovery] Advertised to DHT (TTL: %v)", ttl)
}

func (dm *DiscoveryManager) discoverLoop() {
	time.Sleep(5 * time.Second)
	dm.findPeers()
	ticker := time.NewTicker(DiscoveryInterval)
	defer ticker.Stop()
	for {
		select {
		case <-dm.ctx.Done():
			return
		case <-ticker.C:
			dm.findPeers()
		}
	}
}

func (dm *DiscoveryManager) findPeers() {
	dm.mu.Lock()
	if !dm.running || dm.findPeersInProgress {
		dm.mu.Unlock()
		return
	}
	dm.findPeersInProgress = true
	dm.mu.Unlock()
	defer func() {
		dm.mu.Lock()
		dm.findPeersInProgress = false
		dm.mu.Unlock()
	}()
	peerChan, err := dm.discovery.FindPeers(dm.ctx, DiscoveryNamespace)
	if err != nil {
		log.Printf("[Discovery] FindPeers error: %v", err)
		return
	}
	dutil.Advertise(dm.ctx, dm.discovery, DiscoveryNamespace)
	foundCount := 0
	for peerInfo := range peerChan {
		if peerInfo.ID == dm.host.ID() {
			continue
		}
		if dm.host.Network().Connectedness(peerInfo.ID) == network.Connected {
			continue
		}
		if len(peerInfo.Addrs) == 0 {
			continue
		}
		if !dm.shouldDialPeer(peerInfo.ID) {
			continue
		}
		foundCount++
		go dm.connectToPeer(peerInfo)
	}
	if foundCount > 0 {
		log.Printf("[Discovery] Found %d new peers to dial", foundCount)
	}
}

func (dm *DiscoveryManager) shouldDialPeer(peerID peer.ID) bool {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	if dm.bootstrapPeerIDs[peerID] {
		return true
	}
	state, exists := dm.dialStates[peerID]
	now := time.Now()
	if !exists {
		dm.dialStates[peerID] = &dialState{
			attempts:      1,
			lastAttempt:   now,
			cooldownUntil: time.Time{},
		}
		return true
	}
	if now.Before(state.cooldownUntil) {
		return false
	}
	if state.attempts >= maxDialAttempts && now.After(state.cooldownUntil) {
		state.attempts = 0
		log.Printf("[Discovery] Resetting dial attempts for peer %s (cooldown expired)", peerID.String()[:16])
	}
	state.attempts++
	state.lastAttempt = now
	if state.attempts >= maxDialAttempts {
		backoff := initialBackoff * time.Duration(1<<uint(state.attempts-maxDialAttempts))
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
		state.cooldownUntil = now.Add(backoff)
		log.Printf("[Discovery] Peer %s hit max attempts, cooldown until %v",
			peerID.String()[:16], state.cooldownUntil.Format(time.RFC3339))
	}
	return true
}

func (dm *DiscoveryManager) onPeerConnected(peerID peer.ID) {
	dm.mu.Lock()
	delete(dm.dialStates, peerID)
	cb := dm.onPeerConnectedCb
	dm.mu.Unlock()
	if cb != nil {
		cb(peerID)
	}
}

func filterDialableMultiaddrs(addrs []ma.Multiaddr) []ma.Multiaddr {
	if len(addrs) == 0 {
		return nil
	}
	out := make([]ma.Multiaddr, 0, len(addrs))
	for _, a := range addrs {
		if a == nil {
			continue
		}
		if isLoopbackOrUnspecifiedMultiaddr(a) {
			continue
		}
		out = append(out, a)
	}
	return out
}

func isLoopbackOrUnspecifiedMultiaddr(a ma.Multiaddr) bool {
	if ipStr, err := a.ValueForProtocol(ma.P_IP4); err == nil {
		ip := net.ParseIP(ipStr)
		if ip != nil && (ip.IsLoopback() || ip.IsUnspecified()) {
			return true
		}
	}
	if ipStr, err := a.ValueForProtocol(ma.P_IP6); err == nil {
		ip := net.ParseIP(ipStr)
		if ip != nil && (ip.IsLoopback() || ip.IsUnspecified()) {
			return true
		}
	}
	return false
}

func (dm *DiscoveryManager) connectToPeer(peerInfo peer.AddrInfo) {
	ctx, cancel := context.WithTimeout(dm.ctx, 30*time.Second)
	defer cancel()
	filtered := peerInfo
	filtered.Addrs = filterDialableMultiaddrs(peerInfo.Addrs)
	if len(filtered.Addrs) == 0 {
		log.Printf("[Discovery] Skipping peer %s: no dialable addrs after filtering", peerInfo.ID.String()[:16])
		return
	}
	if err := dm.host.Connect(ctx, filtered); err != nil {
		log.Printf("[Discovery] Failed to connect to %s: %v", peerInfo.ID.String()[:16], err)
		return
	}
	log.Printf("[Discovery] Connected to peer: %s", peerInfo.ID.String()[:16])
	dm.onPeerConnected(peerInfo.ID)
	dm.mu.RLock()
	cb := dm.onPeerFound
	dm.mu.RUnlock()
	if cb != nil {
		cb(filtered)
	}
}

func (dm *DiscoveryManager) GetConnectedPeers() int {
	return len(dm.host.Network().Peers())
}

func (dm *DiscoveryManager) GetDHT() *dht.IpfsDHT {
	return dm.dht
}

func (dm *DiscoveryManager) ResetDialState() {
	dm.mu.Lock()
	count := len(dm.dialStates)
	dm.dialStates = make(map[peer.ID]*dialState)
	dm.lowPeersSince = time.Time{}
	dm.mu.Unlock()
	log.Printf("[Discovery] Reset dial state (cleared %d entries)", count)
}

func (dm *DiscoveryManager) ForceRediscovery() {
	dm.mu.Lock()
	dm.dialStates = make(map[peer.ID]*dialState)
	dm.mu.Unlock()
	log.Printf("[Discovery] Force rediscovery triggered")
	dm.forceBootstrapReconnect()
	go dm.findPeers()
}

func (dm *DiscoveryManager) GetDialStateDebug() map[string]interface{} {
	dm.mu.RLock()
	defer dm.mu.RUnlock()
	states := make([]map[string]interface{}, 0, len(dm.dialStates))
	for peerID, state := range dm.dialStates {
		states = append(states, map[string]interface{}{
			"peerId":        peerID.String()[:16],
			"attempts":      state.attempts,
			"lastAttempt":   state.lastAttempt.Unix(),
			"cooldownUntil": state.cooldownUntil.Unix(),
			"inCooldown":    time.Now().Before(state.cooldownUntil),
		})
	}
	lowSince := int64(0)
	lowDuration := float64(0)
	if !dm.lowPeersSince.IsZero() {
		lowSince = dm.lowPeersSince.Unix()
		lowDuration = time.Since(dm.lowPeersSince).Seconds()
	}
	return map[string]interface{}{
		"dialStates":         states,
		"totalDialStates":    len(dm.dialStates),
		"connectedPeers":     len(dm.host.Network().Peers()),
		"lowPeersSince":      lowSince,
		"lowPeersDuration":   lowDuration,
		"bootstrapPeerCount": len(dm.bootstrapPeerIDs),
	}
}
