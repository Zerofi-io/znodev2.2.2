#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."
SUDO=""
SUDO_E=""
if [ "$(id -u)" -ne 0 ]; then
  if command -v sudo >/dev/null 2>&1; then
    SUDO="sudo"
    SUDO_E="sudo -E"
  else
    echo "ERROR: This script must be run as root or have sudo installed"
    exit 1
  fi
fi
if command -v apt-get >/dev/null 2>&1; then
  MISSING_PKGS=""
  command -v curl >/dev/null 2>&1 || MISSING_PKGS="$MISSING_PKGS curl"
  command -v wget >/dev/null 2>&1 || MISSING_PKGS="$MISSING_PKGS wget"
  command -v tar >/dev/null 2>&1 || MISSING_PKGS="$MISSING_PKGS tar"
  command -v bzip2 >/dev/null 2>&1 || MISSING_PKGS="$MISSING_PKGS bzip2"
  [ -f /etc/ssl/certs/ca-certificates.crt ] || MISSING_PKGS="$MISSING_PKGS ca-certificates"
  command -v gpg >/dev/null 2>&1 || MISSING_PKGS="$MISSING_PKGS gnupg"
  command -v openssl >/dev/null 2>&1 || MISSING_PKGS="$MISSING_PKGS openssl"
  command -v xxd >/dev/null 2>&1 || MISSING_PKGS="$MISSING_PKGS xxd"
  command -v ss >/dev/null 2>&1 || MISSING_PKGS="$MISSING_PKGS iproute2"
  command -v netstat >/dev/null 2>&1 || MISSING_PKGS="$MISSING_PKGS net-tools"
  command -v git >/dev/null 2>&1 || MISSING_PKGS="$MISSING_PKGS git"
  if [ -n "$MISSING_PKGS" ]; then
    $SUDO apt-get update -y >/dev/null 2>&1 || true
    $SUDO apt-get install -y $MISSING_PKGS >/dev/null 2>&1 || echo "Failed to install packages:$MISSING_PKGS"
  fi
fi

echo "================================================================"
echo "                    ZNode Setup Script                          "
echo "================================================================"
echo ""

echo "[1/6] Checking Node.js version..."

install_nodejs() {
  echo "  Installing Node.js 20.x LTS..."
  curl -fsSL https://deb.nodesource.com/setup_20.x | $SUDO_E bash - >/dev/null 2>&1
  $SUDO apt-get install -y nodejs >/dev/null 2>&1
  if ! command -v node >/dev/null 2>&1; then
    echo " ERROR: Failed to install Node.js automatically."
    echo "   Please install Node.js >= 18 manually from https://nodejs.org/"
    exit 1
  fi
  echo "   Node.js installed successfully"
}

if ! command -v node >/dev/null 2>&1; then
  echo "  Node.js not found. Installing..."
  install_nodejs
fi

NODE_VER_RAW="$(node -v | sed 's/^v//')"
NODE_MAJOR="${NODE_VER_RAW%%.*}"
if [ "${NODE_MAJOR:-0}" -lt 18 ]; then
  echo "  Node.js version too old (v$NODE_VER_RAW). Upgrading..."
  install_nodejs
  NODE_VER_RAW="$(node -v | sed 's/^v//')"
fi

echo " Node.js v$NODE_VER_RAW detected"
echo ""

echo "[2/6] Checking Go installation..."
export PATH="/usr/local/go/bin:$PATH"
GO_VERSION="1.24.0"
GO_MIN_MAJOR=1
GO_MIN_MINOR=24

install_go() {
  echo "  Installing Go $GO_VERSION..."
  cd /tmp
  wget -q "https://go.dev/dl/go${GO_VERSION}.linux-amd64.tar.gz" -O go.tar.gz
  $SUDO rm -rf /usr/local/go
  $SUDO tar -C /usr/local -xzf go.tar.gz
  rm go.tar.gz
  export PATH="/usr/local/go/bin:$PATH"
  if [ ! -f /etc/profile.d/go.sh ]; then
    echo 'export PATH="/usr/local/go/bin:$PATH"' | $SUDO tee /etc/profile.d/go.sh > /dev/null
    $SUDO chmod +x /etc/profile.d/go.sh
  fi
  cd "$SCRIPT_DIR/.."
}

if command -v go &> /dev/null; then
  GO_VER_RAW="$(go version | grep -oP 'go\K[0-9]+\.[0-9]+' | head -1)"
  GO_MAJOR="${GO_VER_RAW%%.*}"
  GO_MINOR="${GO_VER_RAW##*.}"
  if [ "${GO_MAJOR:-0}" -lt "$GO_MIN_MAJOR" ] || { [ "${GO_MAJOR:-0}" -eq "$GO_MIN_MAJOR" ] && [ "${GO_MINOR:-0}" -lt "$GO_MIN_MINOR" ]; }; then
    echo "  Go $GO_VER_RAW found but >= $GO_MIN_MAJOR.$GO_MIN_MINOR required"
    install_go
  else
    echo " Go $GO_VER_RAW detected"
  fi
elif [ -x "/usr/local/go/bin/go" ]; then
  export PATH="/usr/local/go/bin:$PATH"
  echo " Go found at /usr/local/go/bin/go"
else
  install_go
fi

if ! /usr/local/go/bin/go version &> /dev/null && ! go version &> /dev/null; then
  echo " ERROR: Go installation failed"
  exit 1
fi
echo ""

echo "[3/6] Installing npm dependencies..."
if [ -f package-lock.json ]; then
  npm ci --omit=dev --quiet
else
  npm install --omit=dev --quiet
fi
echo " Dependencies installed"
echo ""

echo "[4/6] Configuring environment..."
WRITE_ENV=1
ENV_FILE="$SCRIPT_DIR/../.env"
if [ -f "$ENV_FILE" ]; then
  echo "  Existing .env file detected"
  read -r -p "Overwrite existing .env? [y/N]: " ans
  case "${ans,,}" in
    y|yes) echo "Overwriting existing configuration..." ;;
    *) WRITE_ENV=0; echo " Keeping existing .env"; echo "";;
  esac
fi

strip_surrounding_quotes() {
  local s="${1:-}"
  if [[ "$s" == \"*\" ]] && [[ "$s" == *\" ]]; then
    s="${s#\"}"
    s="${s%\"}"
  fi
  if [[ "$s" == \'*\' ]] && [[ "$s" == *\' ]]; then
    s="${s#\'}"
    s="${s%\'}"
  fi
  printf '%s' "$s"
}

env_get() {
  local key="$1"
  local file="$2"
  if [ ! -f "$file" ]; then
    return 0
  fi
  awk -F= -v k="$key" '
    $0 ~ "^[[:space:]]*(export[[:space:]]+)?" k "[[:space:]]*=" {
      sub(/^[[:space:]]*(export[[:space:]]+)?/ , "")
      sub(/^[^=]*=/, "")
      print
      exit
    }
  ' "$file" 2>/dev/null || true
}

expand_home_path() {
  local p="${1:-}"
  p="${p//\$HOME/$HOME}"
  p="${p//\$\{HOME\}/$HOME}"
  printf '%s' "$p"
}

ensure_bridge_api_tls() {
  local tls_key_path_raw="${1:-}"
  local tls_cert_path_raw="${2:-}"
  local tls_ca_cert_path_raw="${3:-}"
  local private_key_raw="${4:-}"
  local auth_token_raw="${5:-}"
  local public_ip_raw="${6:-}"

  local private_key auth_token public_ip
  private_key="$(strip_surrounding_quotes "$private_key_raw")"
  auth_token="$(strip_surrounding_quotes "$auth_token_raw")"
  public_ip="$(strip_surrounding_quotes "$public_ip_raw")"

  if [ -z "$private_key" ] || [ -z "$auth_token" ]; then
    echo "  ERROR: Missing PRIVATE_KEY or BRIDGE_API_AUTH_TOKEN; cannot generate Bridge API TLS certificates"
    return 1
  fi

  local tls_key_path tls_cert_path tls_ca_cert_path
  tls_key_path="$(strip_surrounding_quotes "$tls_key_path_raw")"
  tls_cert_path="$(strip_surrounding_quotes "$tls_cert_path_raw")"
  tls_ca_cert_path="$(strip_surrounding_quotes "$tls_ca_cert_path_raw")"

  if [ -z "$tls_key_path" ]; then
    tls_key_path="$HOME/.znode-bridge/tls/bridge_api.key"
  fi
  if [ -z "$tls_cert_path" ]; then
    tls_cert_path="$HOME/.znode-bridge/tls/bridge_api.crt"
  fi
  if [ -z "$tls_ca_cert_path" ]; then
    tls_ca_cert_path="$HOME/.znode-bridge/tls/bridge_api_ca.crt"
  fi

  tls_key_path="$(expand_home_path "$tls_key_path")"
  tls_cert_path="$(expand_home_path "$tls_cert_path")"
  tls_ca_cert_path="$(expand_home_path "$tls_ca_cert_path")"

  local node_dir ca_dir
  node_dir="$(dirname "$tls_key_path")"
  ca_dir="$(dirname "$tls_ca_cert_path")"

  mkdir -p "$node_dir" >/dev/null 2>&1 || true
  mkdir -p "$ca_dir" >/dev/null 2>&1 || true
  chmod 700 "$node_dir" >/dev/null 2>&1 || true
  chmod 700 "$ca_dir" >/dev/null 2>&1 || true

  local ca_key ca_cert node_key node_cert
  ca_key="$ca_dir/bridge_api_ca.key"
  ca_cert="$tls_ca_cert_path"
  node_key="$tls_key_path"
  node_cert="$tls_cert_path"

  local ca_seed_hex node_seed_hex
  ca_seed_hex="$(printf "%s" "$auth_token" | openssl dgst -sha256 -binary 2>/dev/null | xxd -p -c 256)"
  node_seed_hex="$(printf "%s" "${private_key}${auth_token}" | openssl dgst -sha256 -binary 2>/dev/null | xxd -p -c 256)"

  local old_ca_fp new_ca_fp ca_changed
  old_ca_fp=""
  if [ -f "$ca_cert" ]; then
    old_ca_fp="$(openssl x509 -in "$ca_cert" -noout -fingerprint -sha256 2>/dev/null || true)"
  fi

  node "$SCRIPT_DIR/gen-ed25519-pkcs8.js" "$ca_seed_hex" > "$ca_key"
  chmod 600 "$ca_key" >/dev/null 2>&1 || true
  node "$SCRIPT_DIR/gen-ca-cert.js" < "$ca_key" > "$ca_cert"
  chmod 644 "$ca_cert" >/dev/null 2>&1 || true

  new_ca_fp="$(openssl x509 -in "$ca_cert" -noout -fingerprint -sha256 2>/dev/null || true)"
  ca_changed=0
  if [ "$old_ca_fp" != "$new_ca_fp" ]; then
    ca_changed=1
  fi

  if [ ! -s "$node_key" ]; then
    node "$SCRIPT_DIR/gen-ed25519-pkcs8.js" "$node_seed_hex" > "$node_key"
    chmod 600 "$node_key" >/dev/null 2>&1 || true
  fi

  local need_node_cert
  need_node_cert=0
  if [ ! -s "$node_cert" ]; then
    need_node_cert=1
  fi

  if [ "$need_node_cert" -eq 0 ] && [ -n "${public_ip:-}" ]; then
    if ! openssl x509 -in "$node_cert" -noout -ext subjectAltName 2>/dev/null | grep -q "$public_ip"; then
      need_node_cert=1
    fi
  fi

  if [ "$ca_changed" -eq 1 ]; then
    need_node_cert=1
  fi

  if [ "$need_node_cert" -eq 1 ]; then
    local cn ext csr serial
    cn="${public_ip:-localhost}"
    ext="$node_dir/bridge_api_ext.cnf"
    csr="$node_dir/bridge_api.csr"
    serial="$ca_dir/bridge_api_ca.srl"

    cat > "$ext" <<EOFT
[v3_req]
basicConstraints = CA:FALSE
keyUsage = critical, digitalSignature
extendedKeyUsage = serverAuth, clientAuth
subjectAltName = @alt_names
[alt_names]
IP.1 = 127.0.0.1
DNS.1 = localhost
EOFT

    if [ -n "${public_ip:-}" ]; then
      if [[ "$public_ip" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
        echo "IP.2 = $public_ip" >> "$ext"
      else
        echo "DNS.2 = $public_ip" >> "$ext"
      fi
    fi

    if [ ! -f "$serial" ]; then
      echo 01 > "$serial"
    fi

    openssl req -new -key "$node_key" -out "$csr" -subj "/CN=$cn" >/dev/null 2>&1 || { echo "  ERROR: openssl req failed"; return 1; }
    openssl x509 -req -in "$csr" -CA "$ca_cert" -CAkey "$ca_key" -CAserial "$serial" -out "$node_cert" -days 825 -extfile "$ext" -extensions v3_req >/dev/null 2>&1 || { echo "  ERROR: openssl x509 signing failed"; return 1; }
    chmod 644 "$node_cert" >/dev/null 2>&1 || true
    rm -f "$csr" "$ext" >/dev/null 2>&1 || true
  fi

  if [ ! -s "$ca_key" ] || [ ! -s "$ca_cert" ] || [ ! -s "$node_key" ] || [ ! -s "$node_cert" ]; then
    echo "  ERROR: Bridge API TLS generation failed (missing expected files)"
    echo "         ca_key=$ca_key"
    echo "         ca_cert=$ca_cert"
    echo "         node_key=$node_key"
    echo "         node_cert=$node_cert"
    return 1
  fi

  echo "  Bridge API TLS configured."
}

if [ "$WRITE_ENV" -ne 1 ]; then
  tls_enabled="$(strip_surrounding_quotes "$(env_get BRIDGE_API_TLS_ENABLED "$ENV_FILE")")"
  if [ -z "$tls_enabled" ] || [ "$tls_enabled" = "1" ]; then
    existing_private_key="$(env_get PRIVATE_KEY "$ENV_FILE")"
    existing_auth_token="$(env_get BRIDGE_API_AUTH_TOKEN "$ENV_FILE")"
    existing_public_ip="$(env_get P2P_ANNOUNCE_IP "$ENV_FILE")"
    if [ -z "$(strip_surrounding_quotes "$existing_public_ip")" ]; then
      existing_public_ip="$(env_get PUBLIC_IP "$ENV_FILE")"
    fi

    existing_tls_key_path="$(env_get BRIDGE_API_TLS_KEY_PATH "$ENV_FILE")"
    existing_tls_cert_path="$(env_get BRIDGE_API_TLS_CERT_PATH "$ENV_FILE")"
    existing_tls_ca_path="$(env_get BRIDGE_API_TLS_CA_PATH "$ENV_FILE")"

    ensure_bridge_api_tls "$existing_tls_key_path" "$existing_tls_cert_path" "$existing_tls_ca_path" "$existing_private_key" "$existing_auth_token" "$existing_public_ip"
  fi
fi

if [ "$WRITE_ENV" -eq 1 ]; then
DEFAULT_RPC_URL="http://185.191.116.142:8547"
DEFAULT_P2P_BOOTSTRAP_PEERS=/ip4/185.191.116.142/tcp/9000/p2p/16Uiu2HAmDPgaLxg1KfAfPt3uoLKPz4g4NSwXjhb7U2jaExuSyjiF

echo ""
echo "Required Configuration:"
echo "----------------------"

read -r -p "Ethereum Sepolia RPC URL [press Enter for default]: " RPC_URL
RPC_URL="${RPC_URL:-$DEFAULT_RPC_URL}"

RPC_API_KEY="0c352d92ed1aa5d82b487c0908876f7ae8b0ed707aa0640aef767e77c0494f62"

read -r -s -p "Ethereum private key (0x...): " PRIVATE_KEY
echo ""
while [ -z "${PRIVATE_KEY}" ]; do
  echo "  Private key is required."
  read -r -s -p "Ethereum private key (0x...): " PRIVATE_KEY
  echo ""
done

if [[ ! "$PRIVATE_KEY" =~ ^0x ]]; then
  PRIVATE_KEY="0x${PRIVATE_KEY}"
fi

MONERO_WALLET_PASSWORD="$(openssl rand -hex 32 2>/dev/null || head -c 32 /dev/urandom | xxd -p -c 256)"
echo "  Generated Monero wallet password (stored in .env only)"

BRIDGE_API_AUTH_TOKEN="eb6a1d78440cf7aab806017a6b980ce6a624b4301853269e7636ba966ee3f7e5"
CLUSTER_AGG_NODE_AUTH_TOKEN="$BRIDGE_API_AUTH_TOKEN"
echo "  Bridge API auth token set for all nodes."
MONERO_RPC_URL="http://127.0.0.1:18083"
MONERO_DAEMON_ADDRESS="185.191.116.142:18081"
MONERO_DAEMON_LOGIN="zerofi:zerofi"
MONERO_TRUST_DAEMON="0"

echo ""
read -r -p "Public IP address for P2P (optional, press Enter to skip): " PUBLIC_IP

if [ -z "$PUBLIC_IP" ]; then
  if command -v curl >/dev/null 2>&1; then
    PUBLIC_IP="$(curl -fsS --max-time 5 https://api.ipify.org || true)"
  elif command -v wget >/dev/null 2>&1; then
    PUBLIC_IP="$(wget -qO- https://api.ipify.org || true)"
  fi
fi

if [ -n "$PUBLIC_IP" ]; then
  case "$PUBLIC_IP" in
    *@*) PUBLIC_IP="${PUBLIC_IP##*@}" ;;
  esac
fi
BRIDGE_API_TLS_KEY_PATH="$HOME/.znode-bridge/tls/bridge_api.key"
BRIDGE_API_TLS_CERT_PATH="$HOME/.znode-bridge/tls/bridge_api.crt"
BRIDGE_API_TLS_CA_PATH="$HOME/.znode-bridge/tls/bridge_api_ca.crt"
ensure_bridge_api_tls "$BRIDGE_API_TLS_KEY_PATH" "$BRIDGE_API_TLS_CERT_PATH" "$BRIDGE_API_TLS_CA_PATH" "$PRIVATE_KEY" "$BRIDGE_API_AUTH_TOKEN" "${PUBLIC_IP:-}"
P2P_BOOTSTRAP_PEERS=$DEFAULT_P2P_BOOTSTRAP_PEERS
fi

MONERO_WALLET_CLI=""
MONERO_WALLET_RPC_BIN=""
if command -v monero-wallet-cli >/dev/null 2>&1; then
  MONERO_WALLET_CLI="$(command -v monero-wallet-cli)"
  echo " Found monero-wallet-cli at $MONERO_WALLET_CLI"
fi
if command -v monero-wallet-rpc >/dev/null 2>&1; then
  MONERO_WALLET_RPC_BIN="$(command -v monero-wallet-rpc)"
  echo " Found monero-wallet-rpc at $MONERO_WALLET_RPC_BIN"
  if [ -z "$MONERO_WALLET_CLI" ]; then
    RPC_DIR="$(dirname "$MONERO_WALLET_RPC_BIN")"
    if [ -x "$RPC_DIR/monero-wallet-cli" ]; then
      MONERO_WALLET_CLI="$RPC_DIR/monero-wallet-cli"
      echo " Found monero-wallet-cli next to monero-wallet-rpc at $MONERO_WALLET_CLI"
    fi
  fi
fi

install_monero_cli_from_download() {
  command -v curl >/dev/null 2>&1 || return 1
  command -v tar >/dev/null 2>&1 || return 1
  command -v gpg >/dev/null 2>&1 || return 1
  command -v sha256sum >/dev/null 2>&1 || return 1

  local CLI_URL HASHES_URL KEY_URL EXPECTED_FPR TMP_DIR ARCHIVE HASHES_FILE KEY_FILE GNUPG_HOME
  CLI_URL="https://downloads.getmonero.org/cli/linux64"
  HASHES_URL="https://www.getmonero.org/downloads/hashes.txt"
  KEY_URL="https://raw.githubusercontent.com/monero-project/monero/master/utils/gpg_keys/binaryfate.asc"
  EXPECTED_FPR="81AC591FE9C4B65C5806AFC3F0AF4D462A0BDF92"

  TMP_DIR="$(mktemp -d)" || return 1
  ARCHIVE="$TMP_DIR/monero-cli.tar.bz2"
  HASHES_FILE="$TMP_DIR/hashes.txt"
  KEY_FILE="$TMP_DIR/binaryfate.asc"
  GNUPG_HOME="$TMP_DIR/gnupg"

  local rc
  rc=0

  local EFFECTIVE_URL
  if ! EFFECTIVE_URL="$(curl -fsSL -o "$ARCHIVE" -w '%{url_effective}' "$CLI_URL")"; then
    echo "Failed to download Monero CLI from $CLI_URL"
    rc=1
  elif [ ! -s "$ARCHIVE" ]; then
    echo "Downloaded Monero archive is empty"
    rc=1
  elif ! curl -fsSL -o "$HASHES_FILE" "$HASHES_URL" >/dev/null 2>&1; then
    echo "Failed to download hashes.txt from $HASHES_URL"
    rc=1
  elif ! curl -fsSL -o "$KEY_FILE" "$KEY_URL" >/dev/null 2>&1; then
    echo "Failed to download binaryFate signing key from $KEY_URL"
    rc=1
  else
    local FILE_NAME
    FILE_NAME="$(basename "$EFFECTIVE_URL")"
    echo " Downloaded Monero archive: $FILE_NAME"

    local KEY_FPR
    KEY_FPR="$(gpg --batch --quiet --with-colons --import-options show-only --import "$KEY_FILE" | awk -F: '$1=="fpr"{print $10; exit}')"
    if [ "$KEY_FPR" != "$EXPECTED_FPR" ]; then
      echo "[Monero] ERROR: binaryFate key fingerprint mismatch (got $KEY_FPR, expected $EXPECTED_FPR)."
      rc=1
    else
      mkdir -p "$GNUPG_HOME" 2>/dev/null || true
      chmod 700 "$GNUPG_HOME" 2>/dev/null || true

      if ! gpg --batch --homedir "$GNUPG_HOME" --import "$KEY_FILE" >/dev/null 2>&1; then
        echo "[Monero] ERROR: Failed to import signing key for verification."
        rc=1
      else
        local VERIFY_OUT
        if ! VERIFY_OUT="$(gpg --batch --homedir "$GNUPG_HOME" --status-fd 1 --verify "$HASHES_FILE" 2>/dev/null)"; then
          echo "[Monero] ERROR: hashes.txt signature verification failed."
          rc=1
        else
          local SIGNER_FPR
          SIGNER_FPR="$(printf '%s\n' "$VERIFY_OUT" | awk '/^\[GNUPG:\] VALIDSIG /{print $3; exit}')"
          if [ "$SIGNER_FPR" != "$EXPECTED_FPR" ]; then
            echo "[Monero] ERROR: hashes.txt signature signer mismatch (signer: ${SIGNER_FPR:-unknown})."
            rc=1
          else
            local EXPECTED_HASH
            EXPECTED_HASH="$(grep -E "^[0-9a-f]{64}  +$FILE_NAME$" "$HASHES_FILE" | awk '{print $1}' | head -n 1 || true)"
            if [ -z "$EXPECTED_HASH" ]; then
              echo "[Monero] ERROR: Could not find SHA256 for $FILE_NAME in hashes.txt."
              rc=1
            elif ! echo "$EXPECTED_HASH  $ARCHIVE" | sha256sum -c - >/dev/null 2>&1; then
              echo "[Monero] ERROR: SHA256 mismatch for $FILE_NAME."
              rc=1
            elif ! tar -xjf "$ARCHIVE" -C "$TMP_DIR" >/dev/null 2>&1; then
              echo "Failed to extract downloaded Monero CLI archive."
              rc=1
            else
              local CLI_BIN RPC_BIN
              CLI_BIN="$(find "$TMP_DIR" -maxdepth 3 -type f -name 'monero-wallet-cli' | head -n 1 || true)"
              RPC_BIN="$(find "$TMP_DIR" -maxdepth 3 -type f -name 'monero-wallet-rpc' | head -n 1 || true)"

              if [ -n "$CLI_BIN" ] && [ -f "$CLI_BIN" ]; then
                if $SUDO install -m 0755 "$CLI_BIN" /usr/local/bin/monero-wallet-cli 2>/dev/null; then
                  MONERO_WALLET_CLI="/usr/local/bin/monero-wallet-cli"
                  echo " Installed monero-wallet-cli to $MONERO_WALLET_CLI"
                else
                  echo "Failed to install monero-wallet-cli to /usr/local/bin (insufficient permissions?)."
                fi
              else
                echo "Could not locate monero-wallet-cli in downloaded archive."
                rc=1
              fi

              if [ -n "$RPC_BIN" ] && [ -f "$RPC_BIN" ]; then
                if $SUDO install -m 0755 "$RPC_BIN" /usr/local/bin/monero-wallet-rpc 2>/dev/null; then
                  MONERO_WALLET_RPC_BIN="/usr/local/bin/monero-wallet-rpc"
                  echo " Installed monero-wallet-rpc to $MONERO_WALLET_RPC_BIN"
                else
                  echo "Failed to install monero-wallet-rpc to /usr/local/bin (insufficient permissions?)."
                fi
              else
                echo "Could not locate monero-wallet-rpc in downloaded archive."
                rc=1
              fi
            fi
          fi
        fi
      fi
    fi
  fi

  rm -rf "$TMP_DIR" 2>/dev/null || true

  [ $rc -eq 0 ] && [ -n "${MONERO_WALLET_CLI:-}" ] && [ -n "${MONERO_WALLET_RPC_BIN:-}" ]
}

if [ -z "$MONERO_WALLET_CLI" ]; then
  echo "monero-wallet-cli not found on PATH. Attempting automatic install of Monero CLI (wallet-cli + wallet-rpc)..."
  if ! install_monero_cli_from_download; then
    echo "[Monero] Falling back to apt-get installation (if available)."
  fi
fi

if [ -z "$MONERO_WALLET_CLI" ] || [ -z "$MONERO_WALLET_RPC_BIN" ]; then
  if command -v apt-get >/dev/null 2>&1; then
    echo "Attempting to install Monero CLI from apt-get..."
    $SUDO apt-get update -y >/dev/null 2>&1 || true
    $SUDO apt-get install -y monero monero-cli monero-wallet-cli monero-wallet-rpc >/dev/null 2>&1 || true
    if [ -z "$MONERO_WALLET_CLI" ] && command -v monero-wallet-cli >/dev/null 2>&1; then
      MONERO_WALLET_CLI="$(command -v monero-wallet-cli)"
      echo " Installed monero-wallet-cli via apt-get at $MONERO_WALLET_CLI"
    fi
    if [ -z "$MONERO_WALLET_RPC_BIN" ] && command -v monero-wallet-rpc >/dev/null 2>&1; then
      MONERO_WALLET_RPC_BIN="$(command -v monero-wallet-rpc)"
      echo " Installed monero-wallet-rpc via apt-get at $MONERO_WALLET_RPC_BIN"
    fi
  fi
fi

if [ -z "$MONERO_WALLET_CLI" ]; then
  echo "monero-wallet-cli is still missing. Multisig auto-enable will log a warning until you install it."
fi
RPC_BIN_CHECK="${MONERO_WALLET_RPC_BIN:-}"
if [ -n "$RPC_BIN_CHECK" ] && [ -x "$RPC_BIN_CHECK" ]; then
  :
elif [ -n "$RPC_BIN_CHECK" ] && command -v "$RPC_BIN_CHECK" >/dev/null 2>&1; then
  MONERO_WALLET_RPC_BIN="$(command -v "$RPC_BIN_CHECK")"
elif command -v monero-wallet-rpc >/dev/null 2>&1; then
  MONERO_WALLET_RPC_BIN="$(command -v monero-wallet-rpc)"
else
  echo "ERROR: monero-wallet-rpc not found on PATH after automatic install attempt."
  echo "       Please install Monero CLI (monero-wallet-rpc) manually or re-run scripts/setup.sh as root on a machine with internet access."
  exit 1
fi

if [ "$WRITE_ENV" -eq 1 ]; then
echo ""
echo "[5/6] Writing configuration..."

cat > "$ENV_FILE" << EOF_ENV

PRIVATE_KEY=${PRIVATE_KEY}
RPC_URL=${RPC_URL}
RPC_API_KEY=${RPC_API_KEY}
CHAIN_ID=11155111
CHAIN_NAME=sepolia
MONERO_WALLET_PASSWORD=${MONERO_WALLET_PASSWORD}
MONERO_WALLET_CLI="$MONERO_WALLET_CLI"
MONERO_WALLET_RPC_BIN="$MONERO_WALLET_RPC_BIN"
MONERO_WALLET_DIR=$HOME/.monero-wallets

TEST_MODE=0
DRY_RUN=0

P2P_IMPL=libp2p
ENABLE_HEARTBEAT_ORACLE=1
ENABLE_MULTI_CLUSTER_FORMATION=1
ENABLE_HEARTBEAT_FILTERED_SELECTION=1
ENFORCE_SELECTED_MEMBERS_P2P_VISIBILITY=1
COOLDOWN_TO_NEXT_EPOCH_WINDOW=1
HEARTBEAT_ONLINE_TTL_MS=300000
PUBLIC_IP=${PUBLIC_IP}
P2P_BOOTSTRAP_PEERS=${P2P_BOOTSTRAP_PEERS}

MONERO_RPC_URL=${MONERO_RPC_URL}
MONERO_DAEMON_ADDRESS=${MONERO_DAEMON_ADDRESS}
MONERO_DAEMON_LOGIN=${MONERO_DAEMON_LOGIN}
MONERO_TRUST_DAEMON=${MONERO_TRUST_DAEMON}

DEPOSIT_REQUEST_ROUND=9700
MINT_SIGNATURE_ROUND=9800
MULTISIG_SYNC_ROUND=9810

REGISTRY_ADDR=0xE11E3fE20Ba0cC6f6479cB57796152337095Ca5E
STAKING_ADDR=0x77acf14Ac6ea520e6fCEc600249af2d3a7A6DEFC
ZFI_ADDR=0x17eA0F5A0Ab09CAB962456be0D6DB1F382aF4B21
COORDINATOR_ADDR=0xb27d910913b463d0ea02A4C5C53Bc55b79DF4960
CONFIG_ADDR=0x6Fb39048f38FfF3dd969489Bc3d5703B923e19Fa

BRIDGE_ADDR=0x36992D214161F2A8FeA3f779D43732178352F4Da

BRIDGE_ENABLED=1
SWEEP_ENABLED=0
BRIDGE_API_ENABLED=1
BRIDGE_API_PORT=3002
BRIDGE_API_BIND_IP=0.0.0.0
BRIDGE_API_AUTH_TOKEN=${BRIDGE_API_AUTH_TOKEN}
CLUSTER_AGG_NODE_AUTH_TOKEN=${CLUSTER_AGG_NODE_AUTH_TOKEN}
BRIDGE_API_MAX_BODY_BYTES=32768
BRIDGE_API_KEEP_ALIVE_TIMEOUT_MS=5000
BRIDGE_API_HEADERS_TIMEOUT_MS=15000
BRIDGE_API_REQUEST_TIMEOUT_MS=60000
BRIDGE_API_TLS_ENABLED=1
BRIDGE_API_TLS_KEY_PATH=$HOME/.znode-bridge/tls/bridge_api.key
BRIDGE_API_TLS_CERT_PATH=$HOME/.znode-bridge/tls/bridge_api.crt
BRIDGE_API_TLS_CA_PATH=$HOME/.znode-bridge/tls/bridge_api_ca.crt

HEARTBEAT_INTERVAL=30
STALE_ROUND_MIN_AGE_MS=600000
STICKY_QUEUE=0
FORCE_SELECT=0
EOF_ENV

chmod 600 "$ENV_FILE" || true
echo "Configuration written to $ENV_FILE"
echo ""

fi

echo "[6/6] Validating configuration..."
if node --check node.js 2>/dev/null; then
  echo "Syntax validation passed"
else
  echo "  Warning: Could not validate syntax"
fi
echo ""

if [ "${ZNODE_CONFIGURE_FIREWALL:-0}" = "1" ]; then
  if ! command -v ufw >/dev/null 2>&1; then
    if command -v apt-get >/dev/null 2>&1; then
      echo "[Firewall] ufw not found; installing via apt-get..."
      $SUDO apt-get update -y >/dev/null 2>&1 && $SUDO apt-get install -y ufw >/dev/null 2>&1         || echo "[Firewall] Warning: failed to install ufw. Please install it manually and open port 9000."
    else
      echo "[Firewall] ufw not found and apt-get not available. Please install ufw manually and open port 9000."
    fi
  fi

  if command -v ufw >/dev/null 2>&1; then
    echo "[Firewall] Detected ufw; ensuring P2P port 9000 is open..."
    ufw allow 9000/tcp >/dev/null 2>&1 || echo "[Firewall] Warning: failed to run 'ufw allow 9000/tcp'"
  else
    echo "[Firewall] ufw not found. Please ensure TCP port 9000 is open inbound for P2P."
  fi
else
  echo "[Firewall] Skipping automatic firewall configuration (set ZNODE_CONFIGURE_FIREWALL=1 to enable)."
  echo "[Firewall] Please ensure TCP port 9000 is open inbound for P2P."
fi

echo "================================================================"
echo "Setup Complete!"
echo "================================================================"
echo ""
echo "Next steps:"
echo "  1. Start the node: ./start"
echo "  2. View logs: tail -f znode.log"
echo ""
echo "IMPORTANT:"
echo "  - Node configured for production (Sepolia testnet)"
echo "  - See README.md for full documentation"
echo ""

create_systemd_service() {
  if [ "$(id -u)" -ne 0 ]; then
    echo "Not running as root, skipping systemd service creation."
    echo "   Run as root or manually create the service file if desired."
    return
  fi

  local SERVICE_FILE="/etc/systemd/system/znode.service"
  local AGG_SERVICE_FILE="/etc/systemd/system/cluster-aggregator.service"
  local NODE_DIR="$SCRIPT_DIR/.."
  NODE_DIR="$(cd "$NODE_DIR" && pwd)"

  echo "Creating systemd service files..."

  cat > "$SERVICE_FILE" << SERVICEOF
[Unit]
Description=ZeroFi Node v2.1.0
After=network.target
Wants=cluster-aggregator.service

[Service]
Type=simple
WorkingDirectory=$NODE_DIR
EnvironmentFile=$NODE_DIR/.env
ExecStartPre=/bin/bash -c 'cd $NODE_DIR && ./scripts/start-monero-rpc.sh'
ExecStart=/usr/bin/node $NODE_DIR/node.js
ExecStopPost=/bin/bash -c 'pkill -f "p2p-daemon.*--socket" || true; rm -f /tmp/znode-p2p.sock'
Restart=on-failure
RestartSec=10
StandardOutput=append:$NODE_DIR/znode.log
StandardError=append:$NODE_DIR/znode.log

[Install]
WantedBy=multi-user.target
SERVICEOF

  cat > "$AGG_SERVICE_FILE" << SERVICEOF2
[Unit]
Description=ZeroFi Cluster Aggregator
After=network.target znode.service
PartOf=znode.service

[Service]
Type=simple
WorkingDirectory=$NODE_DIR
EnvironmentFile=$NODE_DIR/.env
ExecStart=/usr/bin/node $NODE_DIR/cluster-aggregator.js
Restart=on-failure
RestartSec=10
StandardOutput=append:$NODE_DIR/cluster-aggregator.log
StandardError=append:$NODE_DIR/cluster-aggregator.log

[Install]
WantedBy=multi-user.target
SERVICEOF2

  systemctl daemon-reload
  systemctl enable znode.service 2>/dev/null || true
  systemctl enable cluster-aggregator.service 2>/dev/null || true

  echo " Systemd services created and enabled: znode.service, cluster-aggregator.service"
  echo "   Start with: ./start"
  echo "   Stop with:  ./stop"
  echo "   Logs:       journalctl -u znode -f  OR  tail -f znode.log"
}

create_systemd_service
