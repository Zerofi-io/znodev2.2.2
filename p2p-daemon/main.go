// p2p-daemon: Go-based P2P coordination daemon for znode cluster formation
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/zerofi-io/znodev2/p2p-daemon/cmd"
)

var (
	version = "0.1.0"
)

func main() {
	// CLI flags
	socketPath := flag.String("socket", "/tmp/znode-p2p.sock", "Unix socket path for JSON-RPC")
	privateKey := flag.String("key", "", "Ethereum private key (hex)")
	keyFile := flag.String("key-file", "", "Path to file containing Ethereum private key (hex)")
	ethAddress := flag.String("eth-address", "", "Ethereum address")
	listenAddr := flag.String("listen", "/ip4/0.0.0.0/tcp/9000", "libp2p listen address")
	bootstrapPeers := flag.String("bootstrap", "", "Comma-separated bootstrap peer multiaddrs")
	showVersion := flag.Bool("version", false, "Show version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Printf("p2p-daemon version %s\n", version)
		os.Exit(0)
	}

	// Validate required flags
	key := strings.TrimSpace(*privateKey)
	if key != "" {
		log.Println("WARNING: --key flag is deprecated and will be removed in a future version. Use --key-file instead for security.")
	}
	if key == "" && strings.TrimSpace(*keyFile) != "" {
		kf := strings.TrimSpace(*keyFile)
		info, err := os.Stat(kf)
		if err != nil {
			log.Fatalf("Failed to stat --key-file: %v", err)
		}
		if info.Mode().Perm()&0o077 != 0 {
			log.Fatalf("--key-file must be accessible only by the owner (chmod 600): %s", kf)
		}
		b, err := os.ReadFile(kf)
		if err != nil {
			log.Fatalf("Failed to read --key-file: %v", err)
		}
		key = strings.TrimSpace(string(b))
	}
	if key == "" {
		log.Fatal("--key-file is required (--key is deprecated)")
	}
	if *ethAddress == "" {
		log.Fatal("--eth-address is required")
	}

	// Create daemon config
	config := &cmd.Config{
		SocketPath:     *socketPath,
		PrivateKey:     key,
		EthAddress:     *ethAddress,
		ListenAddr:     *listenAddr,
		BootstrapPeers: *bootstrapPeers,
	}

	// Create and start daemon
	daemon, err := cmd.NewDaemon(config)
	if err != nil {
		log.Fatalf("Failed to create daemon: %v", err)
	}

	// Handle shutdown signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("Shutting down...")
		daemon.Stop()
		os.Exit(0)
	}()

	// Start daemon (blocks)
	log.Printf("Starting p2p-daemon %s", version)
	if err := daemon.Start(); err != nil {
		log.Fatalf("Daemon error: %v", err)
	}
}
