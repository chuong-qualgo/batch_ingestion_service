#!/bin/sh
# =============================================================================
# OpenBao initialization — runs after OpenBao container starts
# Initializes OpenBao, saves root token, unseals, and seeds secrets
# =============================================================================

BAO_ADDR="${BAO_ADDR:-http://openbao:8200}"
TOKEN_FILE="${TOKEN_FILE:-/openbao/data/root-token.txt}"
UNSEAL_FILE="${UNSEAL_FILE:-/openbao/data/unseal-key.txt}"

echo "=== OpenBao Init Script Started ==="
echo "BAO_ADDR: $BAO_ADDR"
echo "TOKEN_FILE: $TOKEN_FILE"
echo ""

# Wait for OpenBao to be responding
echo "Waiting for OpenBao to respond..."
sleep 3

MAX_RETRIES=120
RETRY=0
while [ $RETRY -lt $MAX_RETRIES ]; do
  if bao status -address="$BAO_ADDR" 2>&1 | grep -q "Seal Type" ; then
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
  exit 1
fi

# Check status
echo ""
echo "Checking OpenBao status..."
STATUS=$(bao status -address="$BAO_ADDR" -format=json 2>&1)

IS_INIT=$(echo "$STATUS" | grep -o '"initialized": *true' | head -1)

if [ -n "$IS_INIT" ]; then
  echo "OpenBao already initialized"
  IS_SEALED=$(echo "$STATUS" | grep -o '"sealed": *true' | head -1)
  if [ -n "$IS_SEALED" ]; then
    echo "OpenBao is sealed - unsealing..."
    if [ -f "$UNSEAL_FILE" ]; then
      UNSEAL_KEY=$(cat "$UNSEAL_FILE")
      bao operator unseal -address="$BAO_ADDR" "$UNSEAL_KEY" >/dev/null 2>&1 || echo "Unseal completed"
    else
      echo "ERROR: OpenBao is sealed but no unseal key available at $UNSEAL_FILE"
      exit 1
    fi
  fi
else
  echo "Initializing OpenBao..."
  
  INIT_JSON=$(bao operator init \
    -address="$BAO_ADDR" \
    -key-shares=1 \
    -key-threshold=1 \
    -format=json 2>&1)
  
  # Extract tokens using sed/grep
  ROOT_TOKEN=$(echo "$INIT_JSON" | grep -o '"root_token": *"[^"]*"' | cut -d'"' -f4)
  UNSEAL_KEY=$(echo "$INIT_JSON" | grep -o '"unseal_keys_b64": *\[ *"[^"]*"' | cut -d'"' -f4)
  
  if [ -z "$ROOT_TOKEN" ]; then
    echo "ERROR: Failed to extract root token"
    echo "Init output: $INIT_JSON"
    exit 1
  fi
  
  echo "Saving root token to $TOKEN_FILE..."
  echo "$ROOT_TOKEN" > "$TOKEN_FILE"
  chmod 644 "$TOKEN_FILE"
  
  if [ -n "$UNSEAL_KEY" ]; then
    echo "Saving unseal key to $UNSEAL_FILE..."
    echo "$UNSEAL_KEY" > "$UNSEAL_FILE"
    chmod 644 "$UNSEAL_FILE"
    
    echo "Unsealing OpenBao..."
    bao operator unseal -address="$BAO_ADDR" "$UNSEAL_KEY" >/dev/null 2>&1 || echo "Unseal completed"
  fi
fi

# Extract token for remaining operations
if [ -z "$ROOT_TOKEN" ] && [ -f "$TOKEN_FILE" ]; then
  ROOT_TOKEN=$(cat "$TOKEN_FILE")
  echo "Using root token from file"
fi

if [ -z "$ROOT_TOKEN" ]; then
  echo "ERROR: No root token available"
  exit 1
fi

export BAO_TOKEN="$ROOT_TOKEN"
export BAO_ADDR="$BAO_ADDR"

# Wait a moment to ensure OpenBao is fully unsealed
sleep 2

# ── Enable KV v2 engine ────────────────────────────────────────────────────
echo ""
echo "Enabling KV v2 secrets engine..."
SECRETS_LIST=$(bao secrets list -address="$BAO_ADDR" -format=json 2>&1)
if echo "$SECRETS_LIST" | grep -q 'secret/'; then
  echo "  KV v2 already enabled at /secret"
else
  bao secrets enable -address="$BAO_ADDR" -path=secret kv-v2 >/dev/null 2>&1
  if [ $? -eq 0 ]; then
    echo "  ✓ KV v2 enabled"
  else
    echo "  ✗ Failed to enable KV v2"
  fi
fi

# ── Write demo secrets ─────────────────────────────────────────────────────
echo "Writing demo secrets..."

bao kv put -address="$BAO_ADDR" secret/data-processor/postgres \
  username=orders_user \
  password=orders_pass >/dev/null 2>&1 && echo "  ✓ postgres" || echo "  ✗ postgres"

bao kv put -address="$BAO_ADDR" secret/data-platform/hadoop \
  hdfs_user=hadoop >/dev/null 2>&1 && echo "  ✓ hadoop" || echo "  ✗ hadoop"

bao kv put -address="$BAO_ADDR" secret/data-platform/redis \
  password=redis_pass >/dev/null 2>&1 && echo "  ✓ redis" || echo "  ✗ redis"

echo ""
echo "============================================================"
echo "  OpenBao initialisation complete"
if [ -n "$ROOT_TOKEN" ]; then
  echo "  Root token: ${ROOT_TOKEN:0:10}... (truncated)"
fi
echo "============================================================"
