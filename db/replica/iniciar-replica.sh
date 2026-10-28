#!/bin/bash
# Réplica física (streaming assíncrono) do banco de origem, usada no Cenário 2.
#
# Na primeira subida copia a origem com pg_basebackup e grava standby.signal (-R); depois
# entrega ao entrypoint oficial do postgres, que encontra o diretório pronto e só inicia o
# servidor em modo standby. Não há slot de replicação física: como no RDS com PostgreSQL 16,
# o único slot da origem é o do Debezium (Software B), e ele não existe na réplica.
set -euo pipefail

PGDATA="${PGDATA:-/var/lib/postgresql/data}"
PRIMARIO="${PRIMARIO_HOST:-postgres-origem}"

if [ ! -s "$PGDATA/PG_VERSION" ]; then
  until pg_isready -h "$PRIMARIO" -U retailrocket -d retailrocket >/dev/null 2>&1; do
    echo "aguardando $PRIMARIO..."
    sleep 1
  done
  mkdir -p "$PGDATA"
  chown postgres:postgres "$PGDATA"
  chmod 700 "$PGDATA"
  echo "copiando $PRIMARIO com pg_basebackup"
  gosu postgres env PGPASSWORD="$POSTGRES_PASSWORD" \
    pg_basebackup -h "$PRIMARIO" -U retailrocket -D "$PGDATA" -X stream -R --checkpoint=fast
fi

exec docker-entrypoint.sh "$@"
