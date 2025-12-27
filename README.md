# ZNode v2.2.2 TESTNET (PHASE 1)

ZNode is the node implementation for the ZeroFi XMR bridge network.

A typical deployment runs:
- The ZNode daemon (`node node.js`)
- A local Monero wallet RPC (`monero-wallet-rpc`)
- The Go-based `p2p-daemon` helper
- The Cluster router `cluster-aggregator.js` service (recommended for dashboards and multi-cluster routing)

This repository is designed for **one node per host** (one VPS per node). Some paths/process controls are global (e.g. `/tmp/znode-p2p.sock`, `pkill -f p2p-daemon...`), so running multiple nodes on a single machine is not supported.

## Minimum VPS requirements

Per node (one VPS):
- CPU: **2 vCPU (x86_64)**
- RAM: **4 GB** (8 GB recommended)
- Storage: **40 GB SSD** (80 GB recommended)
- OS: **Ubuntu 22.04** (or Debian with `apt-get` and `systemd`)
- Network: stable public IPv4/IPv6, 100 Mbps+ recommended, plus reliable outbound access to your Ethereum RPC and Monero daemon

## Ports and firewall

### Inbound (typical)
- **TCP 9000**: P2P transport (libp2p). Default listen address is `/ip4/0.0.0.0/tcp/9000`.
- **TCP 3002**: Bridge API (`BRIDGE_API_PORT`). The setup script enables HTTPS with mTLS by default.
- **TCP 3003**: Health server (`HEALTH_PORT`, default 3003). Used for monitoring; the cluster-aggregator also queries it.
- **TCP 4000**: Cluster Aggregator (`CLUSTER_AGG_PORT`, default 4000). Used by the bridge API for multi-cluster routing.

### Local-only
- **TCP 18083**: `monero-wallet-rpc` (`MONERO_RPC_PORT`, default bind `127.0.0.1`).
- **`/tmp/znode-p2p.sock`**: Unix socket used by `p2p-daemon`.

### Outbound
- Ethereum JSON-RPC (`RPC_URL`)
- Monero daemon RPC (`MONERO_DAEMON_ADDRESS`, typically port 18081)

Security guidance:
- Restrict inbound access to **3002/3003** to only the hosts that need it (e.g. your aggregator(s) and/or monitoring), using a firewall or security group.
- If you set `HEALTH_API_KEY`, clients must send `Authorization: Bearer <key>` or `X-API-Key: <key>`.

## Install

The repo includes a setup script that installs dependencies and generates a working `.env`.

1. Clone or upload the repository to the VPS:
   ```bash
   git clone https://github.com/Zerofi-io/znodev2.2.2
   cd znodev2.2.2
   ```

2. Run the setup script (root):
   ```bash
   sudo ./scripts/setup.sh
   ```

   Optional: have the script open the P2P port in `ufw`:
   ```bash
   sudo ZNODE_CONFIGURE_FIREWALL=1 ./scripts/setup.sh
   ```

   The setup script will:
   - Install Node.js **20.x** if needed (Node **>= 18** required)
   - Install Go **1.24.x** (used to build `p2p-daemon`)
   - Install Node production dependencies
   - Install `monero-wallet-cli` and `monero-wallet-rpc`
   - Generate `.env` (and lock it to mode `0600`)
   - Generate Bridge API TLS materials (mTLS)
   - Create and enable systemd units (`znode.service` and `cluster-aggregator.service`) when run as root

3. Review `.env` and adjust settings for your environment (RPC URLs, contract addresses, bootstrap peers, etc.).

## Run

### Start
```bash
./start
```

`./start` will:
- Verify time sync (via `timedatectl`)
- Ensure `.env` exists (runs `./scripts/setup.sh` if missing)
- Ensure Node deps are installed
- Build `p2p-daemon` if needed
- Start via systemd when available, otherwise run a legacy background startup

### Stop
```bash
./stop
```

### Logs
- File logs:
  - `tail -f znode.log`
  - `tail -f cluster-aggregator.log`
  - `tail -f monero-rpc.log`
- systemd logs (if running with systemd):
  - `journalctl -u znode -f`
  - `journalctl -u cluster-aggregator -f`

## Configuration (.env)

Configuration is loaded from `.env` in the repository root.

Common variables you must set correctly:
- `PRIVATE_KEY`: EVM signing key used by the node (treat as a secret)
- `RPC_URL`: Ethereum JSON-RPC endpoint
- `CHAIN_ID`, `CHAIN_NAME`: chain selection (the setup script defaults to Sepolia)
- `PUBLIC_IP`: Your VPS public address used for P2P announcements (recommended)
- `P2P_BOOTSTRAP_PEERS`: comma-separated multiaddrs used for peer discovery
- `MONERO_DAEMON_ADDRESS`, `MONERO_DAEMON_LOGIN`: Monero daemon RPC endpoint and credentials

Bridge API variables:
- `BRIDGE_API_PORT`, `BRIDGE_API_BIND_IP`
- `BRIDGE_API_TLS_ENABLED=1` with `BRIDGE_API_TLS_KEY_PATH`, `BRIDGE_API_TLS_CERT_PATH`, `BRIDGE_API_TLS_CA_PATH`
- `BRIDGE_API_AUTH_TOKEN` (used for protected routes)

Cluster Aggregator variables:
- `CLUSTER_AGG_PORT`
- `CLUSTER_AGG_NODE_AUTH_TOKEN` (used by the aggregator when calling node Bridge APIs)

P2P listen customization:
- Default P2P listen is on TCP 9000.
- To change it, set `P2P_LISTEN_ADDR` (for example: `/ip4/0.0.0.0/tcp/9001`) and update your firewall rules accordingly.

Cluster formation:
- `CLUSTER_SIZE` is **fixed at 11** in v2.2.2.
- `CLUSTER_THRESHOLD` defaults to **7**.
- `ENABLE_MULTI_CLUSTER_FORMATION=1` enables deterministic multi-cluster partitioning when you have a multiple-of-11 nodes online.

## Troubleshooting

- Clock not synchronized:
  - Check `timedatectl status`
  - Enable NTP: `sudo timedatectl set-ntp true`

- P2P connectivity issues:
  - Ensure inbound TCP **9000** is open
  - Set `PUBLIC_IP`
  - Verify `P2P_BOOTSTRAP_PEERS` is correct

- `p2p-daemon` build failures:
  - Ensure Go **1.24.x** is installed
  - Re-run `./start`, or build manually: `cd p2p-daemon && go build -o p2p-daemon .`

- Monero wallet RPC issues:
  - Check `monero-rpc.log`
  - Verify `MONERO_DAEMON_ADDRESS` is reachable and credentials are correct
