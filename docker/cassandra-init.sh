#!/usr/bin/env bash
set -euo pipefail

CASSANDRA_HOST="${CASSANDRA_HOST:-cassandra-test}"
CASSANDRA_PORT="${CASSANDRA_PORT:?CASSANDRA_PORT is required}"
CASSANDRA_KEYSPACE="${CASSANDRA_KEYSPACE:?CASSANDRA_KEYSPACE is required}"
TABLE_NAME="event_reactions"

until cqlsh "$CASSANDRA_HOST" "$CASSANDRA_PORT" -e "DESCRIBE KEYSPACES" >/dev/null 2>&1; do
  sleep 2
done

cqlsh "$CASSANDRA_HOST" "$CASSANDRA_PORT" <<EOF
CREATE KEYSPACE IF NOT EXISTS $CASSANDRA_KEYSPACE
  WITH replication = {'class':'SimpleStrategy','replication_factor':1};

CREATE TABLE IF NOT EXISTS ${CASSANDRA_KEYSPACE}.${TABLE_NAME} (
  event_id text,
  created_by text,
  like_value tinyint,
  created_at timestamp,
  PRIMARY KEY ((event_id), created_by)
);

CREATE INDEX IF NOT EXISTS event_reactions_like_value_idx
  ON ${CASSANDRA_KEYSPACE}.${TABLE_NAME} (like_value);

CREATE INDEX IF NOT EXISTS event_reactions_created_by_idx
  ON ${CASSANDRA_KEYSPACE}.${TABLE_NAME} (created_by);
EOF

echo "Cassandra schema initialization completed successfully"
