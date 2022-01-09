# pypgoutput

Python package to read, parse and convert PostgreSQL logical decoding messages and convert to CDC messages. Built using psycopg2's logical replication support objects and the built in Postgres pgoutput plugin.

## Example

First setup publication and slot in the DB:

```{sql}
CREATE PUBLICATION test_pub FOR ALL TABLES;
SELECT * FROM pg_create_logical_replication_slot('test_slot', 'pgoutput');
```

Second, run the script to collect the changes:

```{python}
import dataclasses
import json
import os
import pypgoutput

HOST = os.environ.get("PGHOST")
PORT = os.environ.get("PGPORT")
DATABASE_NAME = os.environ.get("PGDATABASE")
USER = os.environ.get("PGUSER")
PASSWORD = os.environ.get("PGPASSWORD")


cdc_reader = pypgoutput.LogicalReplicationReader(
                publication_name="test_pub",
                slot_name="test_slot",
                host=HOST,
                database=DATABASE_NAME,
                port=PORT,
                user=USER,
                password=PASSWORD,
            )
for message in cdc_reader:
    print(json.dumps(dataclasses.asdict(message), indent=2, default=str))

cdc_reader.stop()
```

Generate some change messages
```{sql}
CREATE TABLE public.readme (id integer primary key, created_at timestamptz default now());

INSERT INTO public.readme (id) SELECT data FROM generate_series(1, 3) AS data;
```

Output:
```{json}
{
  "op": "I",
  "message_id": "9602eeed-d350-4fb7-8edb-8a0952d51f55",
  "lsn": 23721984,
  "transaction": {
    "tx_id": 501,
    "begin_lsn": 23723440,
    "commit_ts": "2022-01-09 00:15:02.627549+00:00"
  },
  "table_schema": {
    "column_definitions": [
      {
        "name": "id",
        "part_of_pkey": 1,
        "type_id": 1,
        "type_name": "integer",
        "optional": false
      },
      {
        "name": "created_at",
        "part_of_pkey": 0,
        "type_id": 0,
        "type_name": "timestamp with time zone",
        "optional": true
      }
    ],
    "db": "test_db",
    "schema": "public",
    "table": "readme",
    "relation_id": 16408
  },
  "before": null,
  "after": {
    "id": "1",
    "created_at": "2022-01-09 00:15:02.625822+00"
  }
}
{
  "op": "I",
  "message_id": "e18b71b7-1942-433e-8761-c6325c48dea6",
  "lsn": 23722216,
  "transaction": {
    "tx_id": 501,
    "begin_lsn": 23723440,
    "commit_ts": "2022-01-09 00:15:02.627549+00:00"
  },
  "table_schema": {
    "column_definitions": [
      {
        "name": "id",
        "part_of_pkey": 1,
        "type_id": 1,
        "type_name": "integer",
        "optional": false
      },
      {
        "name": "created_at",
        "part_of_pkey": 0,
        "type_id": 0,
        "type_name": "timestamp with time zone",
        "optional": true
      }
    ],
    "db": "test_db",
    "schema": "public",
    "table": "readme",
    "relation_id": 16408
  },
  "before": null,
  "after": {
    "id": "2",
    "created_at": "2022-01-09 00:15:02.625822+00"
  }
}
{
  "op": "I",
  "message_id": "30e38600-dcd2-4394-b144-8234f9280241",
  "lsn": 23722352,
  "transaction": {
    "tx_id": 501,
    "begin_lsn": 23723440,
    "commit_ts": "2022-01-09 00:15:02.627549+00:00"
  },
  "table_schema": {
    "column_definitions": [
      {
        "name": "id",
        "part_of_pkey": 1,
        "type_id": 1,
        "type_name": "integer",
        "optional": false
      },
      {
        "name": "created_at",
        "part_of_pkey": 0,
        "type_id": 0,
        "type_name": "timestamp with time zone",
        "optional": true
      }
    ],
    "db": "test_db",
    "schema": "public",
    "table": "readme",
    "relation_id": 16408
  },
  "before": null,
  "after": {
    "id": "3",
    "created_at": "2022-01-09 00:15:02.625822+00"
  }
}

```

## Why use this package?

* Preference to use the built in pgoutput plugin. Some plugins are not available in managed database services such as RDS or cannot be updated to a new version.
* The pgoutput plugin includes useful metadata such as relid.

## Useful links

* logical decoding: <https://www.postgresql.org/docs/12/logicaldecoding.html>
* message specification: <https://www.postgresql.org/docs/12/protocol-logicalrep-message-formats.html>
* postgres publication: <https://www.postgresql.org/docs/12/sql-createpublication.html>
* psycopg2 replication support: <https://www.psycopg.org/docs/extras.html#replication-support-objects>
