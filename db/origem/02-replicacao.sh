#!/bin/bash
# Libera conexões de replicação física, usadas só pela réplica do Cenário 2
# (postgres-origem-replica). Não muda nada nos outros cenários: sem réplica no ar,
# nenhuma conexão desse tipo é aberta.
set -e
echo "host replication all all scram-sha-256" >> "$PGDATA/pg_hba.conf"
