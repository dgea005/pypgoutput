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

# PYTHON variable comes from this script being run from the Makefile
${PYTHON} -m coverage run -a -m pytest -svx tests/test_decoders.py
${PYTHON} -m coverage run -a -m pytest -svx tests/test_utils.py

# first reader test with PGPORT=5432 is for Postgres 12
${PYTHON} -m coverage run -a -m pytest -svx --log-cli-level=INFO tests/test_reader.py

# second reader test with PGPORT=5433 is for Postgres 13
export PGPORT=5433
${PYTHON} -m coverage run -a -m pytest -vx --log-cli-level=INFO tests/test_reader.py

${PYTHON} -m coverage report -m
