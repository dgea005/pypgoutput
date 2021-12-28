#!/bin/bash
set -e
source ../dev-venv/bin/activate


export PGHOST=localhost
export PGPORT=5432
export PGDATABASE=test
export PGUSER=test
export PGPASSWORD=test

docker-compose -f docker-compose.yaml down -v --remove-orphans
docker-compose -f docker-compose.yaml up -d

echo "Postgres docker running. Waiting until ready"
RETRIES=5
until psql -h $PGHOST -p $PGPORT -U $PGUSER -d $PGDATABASE -c "select 1" > /dev/null 2>&1 || [ $RETRIES -eq 0 ]; do
  echo "Waiting for postgres server, $((RETRIES--)) remaining attempts..."
  sleep 1
done

# Start script to create individual binary files of pgoutput records

psql -h $PGHOST -p $PGPORT -U $PGUSER -d $PGDATABASE -a --file=test_queries.sql
sleep 1
python example_script.py
