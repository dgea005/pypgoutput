# pypgoutput

Python package to read, parse and convert PostgreSQL logical decoding messages to change data capture messages. Built using psycopg2's logical replication support objects, PostgreSQL's pgoutput plugin and Pydantic.

Uses python >= 3.8

## Installation

```console
$ pip install pypgoutput
```

## How it works

* Replication messages are consumed via psycopg2's replication connection. <https://www.psycopg.org/docs/extras.html#replication-support-objects>
* The binary messages from pgoutput logical decoding are parsed in the `decoders.py` module.
* Parsed messages are converted to change events and yieled from the `LogicalReplicationReader`
* Change events are nested Pydantic models where the tuple data (before/after) schema is dynamically generated depending on the table being processed.

## Example

First, setup a publication and a logical replication slot in the source database.

```sql
CREATE PUBLICATION test_pub FOR ALL TABLES;
SELECT * FROM pg_create_logical_replication_slot('test_slot', 'pgoutput');
```

Second, run the script to collect the changes:

```py
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
    print(message.json(indent=2))

cdc_reader.stop()
```

Generate some change messages

```sql
CREATE TABLE public.readme (id integer primary key, created_at timestamptz default now());

INSERT INTO public.readme (id) SELECT data FROM generate_series(1, 3) AS data;
```

Output:

```json
{
  "op": "I",
  "message_id": "4606b12b-ab41-41e6-9717-7ce92f8a9857",
  "lsn": 23530912,
  "transaction": {
    "tx_id": 499,
    "begin_lsn": 23531416,
    "commit_ts": "2022-01-14T17:22:10.298334+00:00"
  },
  "table_schema": {
    "column_definitions": [
      {
        "name": "id",
        "part_of_pkey": true,
        "type_id": 23,
        "type_name": "integer",
        "optional": false
      },
      {
        "name": "created_at",
        "part_of_pkey": false,
        "type_id": 1184,
        "type_name": "timestamp with time zone",
        "optional": true
      }
    ],
    "db": "test_db",
    "schema_name": "public",
    "table": "readme",
    "relation_id": 16403
  },
  "before": null,
  "after": {
    "id": 1,
    "created_at": "2022-01-14T17:22:10.296740+00:00"
  }
}
{
  "op": "I",
  "message_id": "1ede0643-42b6-4bb1-8a98-8e4ca10c7915",
  "lsn": 23531144,
  "transaction": {
    "tx_id": 499,
    "begin_lsn": 23531416,
    "commit_ts": "2022-01-14T17:22:10.298334+00:00"
  },
  "table_schema": {
    "column_definitions": [
      {
        "name": "id",
        "part_of_pkey": true,
        "type_id": 23,
        "type_name": "integer",
        "optional": false
      },
      {
        "name": "created_at",
        "part_of_pkey": false,
        "type_id": 1184,
        "type_name": "timestamp with time zone",
        "optional": true
      }
    ],
    "db": "test_db",
    "schema_name": "public",
    "table": "readme",
    "relation_id": 16403
  },
  "before": null,
  "after": {
    "id": 2,
    "created_at": "2022-01-14T17:22:10.296740+00:00"
  }
}
{
  "op": "I",
  "message_id": "fb477de5-8281-4102-96ee-649a838d38f2",
  "lsn": 23531280,
  "transaction": {
    "tx_id": 499,
    "begin_lsn": 23531416,
    "commit_ts": "2022-01-14T17:22:10.298334+00:00"
  },
  "table_schema": {
    "column_definitions": [
      {
        "name": "id",
        "part_of_pkey": true,
        "type_id": 23,
        "type_name": "integer",
        "optional": false
      },
      {
        "name": "created_at",
        "part_of_pkey": false,
        "type_id": 1184,
        "type_name": "timestamp with time zone",
        "optional": true
      }
    ],
    "db": "test_db",
    "schema_name": "public",
    "table": "readme",
    "relation_id": 16403
  },
  "before": null,
  "after": {
    "id": 3,
    "created_at": "2022-01-14T17:22:10.296740+00:00"
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
