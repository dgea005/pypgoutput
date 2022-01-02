#!/bin/bash
set -e

export PGHOST=localhost
export PGPORT=5432
export PGDATABASE=test_db
export PGUSER=postgres
export PGPASSWORD=test_pw

docker-compose -f tests/docker-compose.yaml down -v --remove-orphans
docker-compose -f tests/docker-compose.yaml up -d

echo "Postgres docker running. Waiting until ready"
RETRIES=5
until psql -h $PGHOST -p $PGPORT -U $PGUSER -d $PGDATABASE -c "select 1" > /dev/null 2>&1 || [ $RETRIES -eq 0 ]; do
  echo "Waiting for postgres server, $((RETRIES--)) remaining attempts..."
  sleep 1
done

# psql -h $PGHOST -p $PGPORT -U $PGUSER -d $PGDATABASE -a << EOF
# CREATE PUBLICATION pub FOR ALL TABLES;
# SELECT * FROM pg_create_logical_replication_slot('my_slot', 'pgoutput');
# EOF

# PYTHON variable comes from this script being run from the Makefile
${PYTHON} -m pytest -svx tests/test_integration.py
