package p2p

import (
	"fmt"
	"os"
	"time"

	"github.com/libp2p/go-libp2p/p2p/net/connmgr"
)

func envPositiveInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	var n int
	if _, err := fmt.Sscanf(v, "%d", &n); err != nil || n <= 0 {
		return def
	}
	return n
}

func newConnManagerFromEnv() (*connmgr.BasicConnMgr, error) {
	if os.Getenv("P2P_CONNMGR_DISABLE") == "1" {
		return nil, nil
	}
	low := envPositiveInt("P2P_CONNMGR_LOW", 128)
	high := envPositiveInt("P2P_CONNMGR_HIGH", 256)
	if high < low {
		high = low
	}
	graceSec := envPositiveInt("P2P_CONNMGR_GRACE_SEC", 90)
	grace := time.Duration(graceSec) * time.Second
	return connmgr.NewConnManager(low, high, connmgr.WithGracePeriod(grace))
}
