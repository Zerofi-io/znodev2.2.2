#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT_DIR"

kill_monero_rpc_on_port() {
  local port="$1"
  local pids=""
  if command -v ss >/dev/null 2>&1; then
    pids=$(ss -ltnp 2>/dev/null | awk -v p=":"$port"" '/monero-wallet-r/ && $4 ~ p { match($0, /pid=([0-9]+)/, m); if (m[1] != "") print m[1]; }' | sort -u)
  elif command -v netstat >/dev/null 2>&1; then
    pids=$(netstat -ltnp 2>/dev/null | awk -v p=":"$port"" '$4 ~ p { n = split($7, a, "/"); if (n > 1 && a[2] ~ /^monero-wallet-r/) print a[1]; }' | sort -u)
  fi
  if [ -n "$pids" ]; then
    echo "[start-monero-rpc] Detected existing monero-wallet-rpc on port $port (PIDs: $pids), stopping..."
    for pid in $pids; do
      kill "$pid" 2>/dev/null || true
    done
    sleep 1
    for pid in $pids; do
      if kill -0 "$pid" 2>/dev/null; then
        kill -9 "$pid" 2>/dev/null || true
      fi
    done
  fi
}

if [ -f "$ROOT_DIR/.env" ]; then
  set -a
  . "$ROOT_DIR/.env"
  set +a
fi

RPC_BIN="${MONERO_WALLET_RPC_BIN:-}"
if [ -n "$RPC_BIN" ] && [ -x "$RPC_BIN" ]; then
  :
elif [ -n "$RPC_BIN" ] && command -v "$RPC_BIN" >/dev/null 2>&1; then
  RPC_BIN="$(command -v "$RPC_BIN")"
elif command -v monero-wallet-rpc >/dev/null 2>&1; then
  RPC_BIN="$(command -v monero-wallet-rpc)"
else
  echo "[start-monero-rpc] ERROR: monero-wallet-rpc binary not found on PATH"
  echo "[start-monero-rpc] Install Monero CLI (monero-wallet-rpc) and re-run ./scripts/setup.sh"
  exit 1
fi

echo "[start-monero-rpc] Using RPC binary: $RPC_BIN"

PORT="${MONERO_RPC_PORT:-18083}"
BIND="${MONERO_RPC_BIND_IP:-127.0.0.1}"
HOME="${HOME:-/root}"
WALLET_DIR="${MONERO_WALLET_DIR:-$HOME/.monero-wallets}"
DAEMON="${MONERO_DAEMON_ADDRESS:-127.0.0.1:18081}"
DAEMON_LOGIN="${MONERO_DAEMON_LOGIN:-}"
DAEMON_USER="${MONERO_DAEMON_USER:-}"
DAEMON_PASS="${MONERO_DAEMON_PASSWORD:-}"

if [ -z "$DAEMON_LOGIN" ] && [ -n "$DAEMON_USER" ]; then
  if [ -n "$DAEMON_PASS" ]; then
    DAEMON_LOGIN="$DAEMON_USER:$DAEMON_PASS"
  else
    DAEMON_LOGIN="$DAEMON_USER"
  fi
fi
LOG_FILE="${MONERO_RPC_LOG:-$PWD/monero-rpc.log}"
PID_FILE="${MONERO_RPC_PID_FILE:-$PWD/monero-rpc.pid}"
RPC_USER="${MONERO_WALLET_RPC_USER:-}"
RPC_PASS="${MONERO_WALLET_RPC_PASSWORD:-}"

mkdir -p "$WALLET_DIR"

find "$WALLET_DIR" -name "*.lock" -type f -delete 2>/dev/null || true

if [ -f "$PID_FILE" ]; then
  PID=$(cat "$PID_FILE" 2>/dev/null || true)
  if [ -n "$PID" ] && kill -0 "$PID" 2>/dev/null; then
    echo "[start-monero-rpc] Stopping existing RPC (PID: $PID)..."
    kill "$PID" 2>/dev/null || true
    sleep 1
  fi
  rm -f "$PID_FILE"
fi

kill_monero_rpc_on_port "$PORT"

echo "[start-monero-rpc] Starting wallet RPC (daemon: $DAEMON, wallet dir: $WALLET_DIR, port: $PORT)"

RPC_CMD=(
  "$RPC_BIN"
  --daemon-address "$DAEMON"
  --rpc-bind-port "$PORT"
  --rpc-bind-ip "$BIND"
  --wallet-dir "$WALLET_DIR"
  --rpc-ssl disabled
  --log-level 1
)

if [ -n "$DAEMON_LOGIN" ]; then
  RPC_CMD+=(--daemon-login "$DAEMON_LOGIN")
  echo "[start-monero-rpc] Daemon authentication enabled"
fi


BRIDGE_DATA_DIR="${BRIDGE_DATA_DIR:-$ROOT_DIR/.znode-bridge}"
AUTH_FILE="$BRIDGE_DATA_DIR/monero_wallet_rpc_auth.json"
mkdir -p "$BRIDGE_DATA_DIR"
chmod 700 "$BRIDGE_DATA_DIR" 2>/dev/null || true

if [ -z "$RPC_USER" ] || [ -z "$RPC_PASS" ]; then
  if [ -f "$AUTH_FILE" ]; then
    CREDS="$(python3 - <<'PY' "$AUTH_FILE"
import json, sys
p = sys.argv[1]
try:
  with open(p, 'r') as f:
    d = json.load(f)
  u = d.get('user')
  pw = d.get('password')
  if isinstance(u, str) and isinstance(pw, str) and u and pw:
    print(u)
    print(pw)
except Exception:
  pass
PY
)"
    RPC_USER="$(printf '%s\n' "$CREDS" | sed -n '1p')"
    RPC_PASS="$(printf '%s\n' "$CREDS" | sed -n '2p')"
  fi
fi

if [ -z "$RPC_USER" ] || [ -z "$RPC_PASS" ]; then
  RPC_USER="znode_rpc"
  RPC_PASS="$(python3 - <<'PY'
import secrets
print(secrets.token_hex(32))
PY
)"
  umask 077
  python3 - <<'PY' "$AUTH_FILE" "$RPC_USER" "$RPC_PASS"
import json, os, sys, tempfile, time
path = sys.argv[1]
user = sys.argv[2]
pw = sys.argv[3]
os.makedirs(os.path.dirname(path), exist_ok=True)
data = {"user": user, "password": pw, "createdAt": int(time.time() * 1000)}
fd, tmp = tempfile.mkstemp(prefix='.monero_wallet_rpc_auth.', dir=os.path.dirname(path))
try:
  with os.fdopen(fd, 'w') as f:
    json.dump(data, f)
  os.chmod(tmp, 0o600)
  os.replace(tmp, path)
  os.chmod(path, 0o600)
finally:
  try:
    if os.path.exists(tmp):
      os.unlink(tmp)
  except Exception:
    pass
PY
fi

RPC_CMD+=(--rpc-login "$RPC_USER:$RPC_PASS")
echo "[start-monero-rpc] RPC authentication enabled"

if [ "$BIND" != "127.0.0.1" ] && [ "$BIND" != "::1" ]; then
  if [ "${TEST_MODE:-0}" != "1" ] && [ "${MONERO_RPC_ALLOW_NONLOCAL:-0}" != "1" ]; then
    echo "[start-monero-rpc] ERROR: Refusing to bind wallet RPC to $BIND in production mode"
    echo "[start-monero-rpc] Set MONERO_RPC_BIND_IP=127.0.0.1 or MONERO_RPC_ALLOW_NONLOCAL=1 (unsafe)"
    exit 1
  fi
  echo "[start-monero-rpc] WARNING: Binding to $BIND (ensure firewall and consider TLS/mTLS)"
fi

if [ "${MONERO_TRUST_DAEMON:-0}" = "1" ]; then
  RPC_CMD+=(--trusted-daemon)
  echo "[start-monero-rpc] WARNING: Daemon marked as trusted"
fi

nohup "${RPC_CMD[@]}" >"$LOG_FILE" 2>&1 &
echo $! > "$PID_FILE"

for i in $(seq 1 15); do
  if command -v ss >/dev/null 2>&1 && ss -ltnp 2>/dev/null | grep -q ":$PORT"; then
    echo "[start-monero-rpc] READY on $BIND:$PORT"
    exit 0
  elif command -v netstat >/dev/null 2>&1 && netstat -ltn 2>/dev/null | grep -q ":$PORT"; then
    echo "[start-monero-rpc] READY on $BIND:$PORT"
    exit 0
  elif command -v curl >/dev/null 2>&1; then
    if curl -s --max-time 2 -u "$RPC_USER:$RPC_PASS" -X POST "http://${BIND}:${PORT}/json_rpc" \
         -d '{"jsonrpc":"2.0","id":"0","method":"get_version"}' \
         -H "Content-Type: application/json" >/dev/null 2>&1; then
      echo "[start-monero-rpc] READY on $BIND:$PORT"
      exit 0
    fi
  fi
  sleep 1
done

echo "[start-monero-rpc] WARNING: RPC not ready on $BIND:$PORT"
exit 1
