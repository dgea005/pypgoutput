#!/bin/bash

set -e

#make setup
. test-env/bin/activate

TEST_FOLDER="files"
rm -rf ${TEST_FOLDER} && mkdir -p ${TEST_FOLDER}

export PGHOST=localhost
export PGPORT=5432
export PGDATABASE=test
export PGUSER=test
export PGPASSWORD=test

docker-compose -f docker-compose.yml down -v --remove-orphans
docker-compose -f docker-compose.yml up -d

echo "Postgres docker running. Waiting until ready"
RETRIES=5
until psql -h $PGHOST -p $PGPORT -U $PGUSER -d $PGDATABASE -c "select 1" > /dev/null 2>&1 || [ $RETRIES -eq 0 ]; do
  echo "Waiting for postgres server, $((RETRIES--)) remaining attempts..."
  sleep 1
done

PYTHON_VER=$(which python)
echo "${PYTHON_VER}"

# Start script to create individual binary files of pgoutput records
nohup python get_replication_records.py &

psql -h $PGHOST -p $PGPORT -U $PGUSER -d $PGDATABASE -a --file=test_queries.sql

pytest -svx test_decoders.py
