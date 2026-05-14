#!/bin/sh
# =============================================================================
# OpenBao unified entrypoint — initialization + server startup
# =============================================================================

BAO_DATA_DIR="${BAO_DATA_DIR:-/openbao/data}"
TOKEN_FILE="${BAO_DATA_DIR}/root-token.txt"
UNSEAL_FILE="${BAO_DATA_DIR}/unseal-keys.txt"
INIT_MARKER="${BAO_DATA_DIR}/.init-complete"
CONFIG_FILE="/etc/openbao/config.hcl"

mkdir -p "$BAO_DATA_DIR"

echo "Starting OpenBao unified entrypoint..."
echo "Data directory: $BAO_DATA_DIR"
echo "Init marker: $INIT_MARKER"
echo ""

# Start OpenBao server in background
echo "Starting OpenBao server..."
bao server -config="$CONFIG_FILE" > /dev/null 2>&1 &
BAO_PID=$!

# Wait for server to be ready
echo "Waiting for OpenBao to respond..."
MAX_RETRIES=120
RETRY=0
while [ $RETRY -lt $MAX_RETRIES ]; do
  if bao status -address="http://127.0.0.1:8200" 2>&1 | grep -q "Seal Type" ; then
    echo "OpenBao is responding"
    break
  fi
  RETRY=$((RETRY + 1))
  if [ $((RETRY % 20)) -eq 0 ]; then
    echo "  Attempt $RETRY/$MAX_RETRIES..."
  fi
  sleep 1
done

if [ $RETRY -ge $MAX_RETRIES ]; then
  echo "ERROR: OpenBao did not respond after $MAX_RETRIES attempts"
  kill $BAO_PID
  exit 1
fi

# Check if already initialized
echo ""
echo "Checking OpenBao status..."
STATUS=$(bao status -address="http://127.0.0.1:8200" -format=json 2>&1)

# Check using pattern - normalize JSON first
STATUS_NORM=$(echo "$STATUS" | tr -d '\n')
if echo "$STATUS_NORM" | grep -q '"initialized"[[:space:]]*:[[:space:]]*true'; then
  echo "OpenBao already initialized"
  
  # Check if sealed - normalize first
  if echo "$STATUS_NORM" | grep -q '"sealed"[[:space:]]*:[[:space:]]*true'; then
    echo "OpenBao is sealed - unsealing with 2 of 3 keys..."
    if [ -f "$UNSEAL_FILE" ]; then
      KEY1=$(sed -n '1p' "$UNSEAL_FILE")
      KEY2=$(sed -n '2p' "$UNSEAL_FILE")
      
      if [ -z "$KEY1" ] || [ -z "$KEY2" ]; then
        echo "ERROR: Not enough unseal keys in $UNSEAL_FILE"
        kill $BAO_PID
        exit 1
      fi
      
      echo "  Providing unseal key 1/2..."
      bao operator unseal -address="http://127.0.0.1:8200" "$KEY1" >/dev/null 2>&1
      echo "  Providing unseal key 2/2..."
      bao operator unseal -address="http://127.0.0.1:8200" "$KEY2" >/dev/null 2>&1
      echo "  Unsealed"
    else
      echo "ERROR: No unseal keys found at $UNSEAL_FILE"
      kill $BAO_PID
      exit 1
    fi
  else
    echo "OpenBao is already unsealed"
  fi
  
  # Print root token if exists
  if [ -f "$TOKEN_FILE" ]; then
    ROOT_TOKEN=$(cat "$TOKEN_FILE")
    echo "Root token: ${ROOT_TOKEN:0:10}... (file: $TOKEN_FILE)"
  fi
else
  echo "Initializing OpenBao..."
  
  INIT_OUTPUT_FILE="/tmp/bao-init.json"
  bao operator init \
    -address="http://127.0.0.1:8200" \
    -key-shares=3 \
    -key-threshold=2 \
    -format=json > "$INIT_OUTPUT_FILE" 2>&1
  
  INIT_JSON=$(cat "$INIT_OUTPUT_FILE")
  
  # Normalize JSON
  INIT_JSON_NORMALIZED=$(echo "$INIT_JSON" | tr -d '\n' | tr -s ' ')
  
  # Extract root token
  ROOT_TOKEN=$(echo "$INIT_JSON_NORMALIZED" | sed 's/.*"root_token": *"\([^"]*\)".*/\1/')
  
  if [ -z "$ROOT_TOKEN" ] || [ "$ROOT_TOKEN" = "$INIT_JSON_NORMALIZED" ]; then
    echo "ERROR: Failed to extract root token"
    kill $BAO_PID
    exit 1
  fi
  
  # Save root token
  echo "$ROOT_TOKEN" | tee "$TOKEN_FILE" >/dev/null
  chmod 644 "$TOKEN_FILE"
  echo "Root token saved: ${ROOT_TOKEN:0:10}... (file: $TOKEN_FILE)"
  
  # Extract all 3 unseal keys
  echo "Extracting unseal keys..."
  UNSEAL_KEYS=$(echo "$INIT_JSON" | awk '/"unseal_keys_b64"/,/\]/' | grep -o '"[A-Za-z0-9+/=]*"' | sed 's/"//g' | head -3)
  
  echo "$UNSEAL_KEYS" | tee "$UNSEAL_FILE" >/dev/null
  chmod 644 "$UNSEAL_FILE"
  echo "Unseal keys saved: $UNSEAL_FILE"
  
  # Get first 2 keys for unsealing
  KEY1=$(echo "$UNSEAL_KEYS" | sed -n '1p')
  KEY2=$(echo "$UNSEAL_KEYS" | sed -n '2p')
  
  if [ -z "$KEY1" ] || [ -z "$KEY2" ]; then
    echo "ERROR: Failed to extract unseal keys"
    kill $BAO_PID
    exit 1
  fi
  
  echo "Unsealing OpenBao with 2 of 3 keys..."
  bao operator unseal -address="http://127.0.0.1:8200" "$KEY1" >/dev/null 2>&1
  bao operator unseal -address="http://127.0.0.1:8200" "$KEY2" >/dev/null 2>&1
  echo "  Unsealed"
fi

echo ""
echo "============================================================"
echo "  OpenBao ready - server running"
echo "============================================================"
echo ""

# Wait for unsealing to complete
sleep 2

# Initialize secrets only on first run
if [ ! -f "$INIT_MARKER" ]; then
  echo ""
  echo "Running initial secrets setup (marker not found at $INIT_MARKER)..."
  
  # Get root token
  if [ -z "$ROOT_TOKEN" ] && [ -f "$TOKEN_FILE" ]; then
    ROOT_TOKEN=$(cat "$TOKEN_FILE")
  fi
  
  if [ -z "$ROOT_TOKEN" ]; then
    echo "ERROR: Cannot get root token for secrets setup"
  else
    export BAO_TOKEN="$ROOT_TOKEN"
    export BAO_ADDR="http://127.0.0.1:8200"
    
    # ── Enable KV v2 engine ────────────────────────────────────────────────────
    echo "Enabling KV v2 secrets engine..."
    SECRETS_LIST=$(bao secrets list -address="http://127.0.0.1:8200" -format=json 2>&1)
    if echo "$SECRETS_LIST" | grep -q 'secret/'; then
      echo "  KV v2 already enabled at /secret"
    else
      bao secrets enable -address="http://127.0.0.1:8200" -path=secret kv-v2 >/dev/null 2>&1
      if [ $? -eq 0 ]; then
        echo "  ✓ KV v2 enabled"
      else
        echo "  ✗ Failed to enable KV v2"
      fi
    fi
    
    # ── Write demo secrets ─────────────────────────────────────────────────────
    echo "Writing demo secrets..."
    
    bao kv put -address="http://127.0.0.1:8200" secret/data-processor/postgres \
      username=orders_user \
      password=orders_pass \
      host=postgres \
      port=5432 \
      jars=/home/airflow/jars/postgresql-42.7.3.jar >/dev/null 2>&1 && echo "  ✓ postgres" || echo "  ✗ postgres"
    
    bao kv put -address="http://127.0.0.1:8200" secret/data-platform/hadoop \
      hdfs_user=hadoop \
      endpoint=hdfs://namenode:9000 >/dev/null 2>&1 && echo "  ✓ hadoop" || echo "  ✗ hadoop"
    
    bao kv put -address="http://127.0.0.1:8200" secret/data-platform/kafka \
      bootstrap_servers=kafka:9092 >/dev/null 2>&1 && echo "  ✓ kafka" || echo "  ✗ kafka"
    
    # Mark initialization complete
    touch "$INIT_MARKER"
    echo "Initial setup complete"
  fi
fi

echo ""

# Bring server to foreground
fg 2>/dev/null || wait $BAO_PID
