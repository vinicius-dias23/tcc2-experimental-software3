#!/bin/bash
# Registra (ou atualiza) o conector Debezium do Software B e espera a tarefa ficar RUNNING.
# Roda depois do seed: com snapshot.mode=no_data, o slot de replicação nasce aqui e só
# as mudanças posteriores (a carga pela API) são propagadas.
set -euo pipefail

CONNECT_URL="${CONNECT_URL:-http://kafka-connect:8083}"
NAME="${CONNECTOR_NAME:-retailrocket-origem}"
CONFIG="${CONNECTOR_CONFIG:-/infra/retailrocket-connector.json}"

until curl -fsS "$CONNECT_URL/connectors" >/dev/null; do
  echo "aguardando o Kafka Connect em $CONNECT_URL..."
  sleep 3
done

curl -fsS -X PUT -H 'Content-Type: application/json' --data @"$CONFIG" \
  "$CONNECT_URL/connectors/$NAME/config" >/dev/null
echo "conector $NAME registrado"

for _ in $(seq 1 60); do
  status=$(curl -fsS "$CONNECT_URL/connectors/$NAME/status" || true)
  if echo "$status" | grep -q '"tasks":\[{[^]]*"state":"RUNNING"'; then
    echo "conector $NAME em execução: $status"
    exit 0
  fi
  sleep 2
done
echo "o conector não chegou a RUNNING: $status" >&2
exit 1
