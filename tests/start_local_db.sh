#!/bin/bash
set -e

export PGHOST=localhost
export PGPORT=5432
export PGDATABASE=test
export PGUSER=test
export PGPASSWORD=test

docker-compose -f tests/docker-compose.yaml down -v --remove-orphans
docker-compose -f tests/docker-compose.yaml up -d
# docker system prune -a 

# docker rm --force -v test-db

# docker ps -a --filter "name=test-db" | grep -q . && docker stop test-db && docker rm -fv test-db

# docker run -d  \
#     --name test-db \
#     -e POSTGRES_DB=test \
#     -e POSTGRES_USER=test \
#     -e POSTGRES_PASSWORD=test \
#     -e POSTGRES_PORT=5432 \
#     -e PGDATA=/var/lib/postgresql/data/pgdata \
#     -v "$PWD/tests/init.sql:/docker-entrypoint-initdb.d/init.sql" \
#     -v "database-data:/var/lib/postgresql/data/" \
#     -p 5432:5432 \
#     postgres:12.3 -c "wal_level=logical"


echo "Postgres docker running. Waiting until ready"
RETRIES=5
until psql -h $PGHOST -p $PGPORT -U $PGUSER -d $PGDATABASE -c "select 1" > /dev/null 2>&1 || [ $RETRIES -eq 0 ]; do
  echo "Waiting for postgres server, $((RETRIES--)) remaining attempts..."
  sleep 1
done

