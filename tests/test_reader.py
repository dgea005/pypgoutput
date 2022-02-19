import logging
import multiprocessing
import os
import typing
from datetime import datetime, timezone

import psycopg2
import psycopg2.errors as psycopg_errors
import psycopg2.extensions
import psycopg2.extras
import pytest

import pypgoutput

HOST = os.environ.get("PGHOST")
PORT = os.environ.get("PGPORT")
DATABASE_NAME = os.environ.get("PGDATABASE")
USER = os.environ.get("PGUSER")
PASSWORD = os.environ.get("PGPASSWORD")

PUBLICATION_NAME = "test_pub"
SLOT_NAME = "test_slot"

logging.basicConfig(level=logging.DEBUG, format="%(relativeCreated)6d %(processName)s %(message)s")
logger = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def cursor() -> typing.Generator[psycopg2.extras.DictCursor, None, None]:
    connection = psycopg2.connect(
        host=HOST,
        database=DATABASE_NAME,
        port=PORT,
        user=USER,
        password=PASSWORD,
    )
    connection.autocommit = True
    curs = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
    yield curs
    curs.close()
    connection.close()


@pytest.fixture(scope="module")
def configure_test_db(
    cursor: psycopg2.extras.DictCursor,
) -> typing.Generator[pypgoutput.LogicalReplicationReader, None, None]:
    cursor.execute(f"DROP PUBLICATION IF EXISTS {PUBLICATION_NAME};")
    try:
        cursor.execute(f"SELECT pg_drop_replication_slot('{SLOT_NAME}');")
    except psycopg_errors.UndefinedObject as err:
        logger.warning(f"slot {SLOT_NAME} could not be dropped because it does not exist. {err}")
    cursor.execute(f"CREATE PUBLICATION {PUBLICATION_NAME} FOR ALL TABLES;")
    cursor.execute(f"SELECT * FROM pg_create_logical_replication_slot('{SLOT_NAME}', 'pgoutput');")
    cdc_reader = pypgoutput.LogicalReplicationReader(
        publication_name=PUBLICATION_NAME,
        slot_name=SLOT_NAME,
        host=HOST,
        database=DATABASE_NAME,
        port=PORT,
        user=USER,
        password=PASSWORD,
    )
    # assumes all tables are in publication
    query = """DROP TABLE IF EXISTS public.integration CASCADE;
    CREATE TABLE public.integration (
        id integer primary key,
        json_data jsonb,
        amount numeric(10, 2),
        updated_at timestamptz not null
    );"""
    cursor.execute(query)
    yield cdc_reader
    logger.info("Closing test CDC reader")
    try:
        cdc_reader.stop()
    except Exception as err:
        logger.warning("Test failed but reader is already closed", err)


def test_000_dummy_test(cursor: psycopg2.extras.DictCursor) -> None:
    """make sure connection/cursor and DB is operational for tests"""
    cursor.execute("SELECT 1 as n;")
    result = cursor.fetchone()
    assert result["n"] == 1


def test_001_insert(
    cursor: psycopg2.extras.DictCursor, configure_test_db: typing.Generator[pypgoutput.ChangeEvent, None, None]
) -> None:
    """with default replica identity"""
    cdc_reader = configure_test_db
    cursor.execute(
        """INSERT INTO public.integration (id, json_data, amount, updated_at) VALUES (10, '{"data": 10}', 10.20, '2020-01-01 00:00:00+00');"""
    )
    cursor.execute("SELECT COUNT(*) AS n FROM public.integration;")
    assert cursor.fetchone()["n"] == 1

    message = next(cdc_reader)
    assert message.op == "I"
    assert message.table_schema.db == "test_db"
    assert message.table_schema.schema_name == "public"
    assert message.table_schema.table == "integration"

    assert message.table_schema.column_definitions[0].name == "id"
    assert message.table_schema.column_definitions[0].part_of_pkey == 1
    assert message.table_schema.column_definitions[0].type_name == "integer"
    assert message.table_schema.column_definitions[0].optional is False

    assert message.table_schema.column_definitions[1].name == "json_data"
    assert message.table_schema.column_definitions[1].part_of_pkey == 0
    assert message.table_schema.column_definitions[1].type_name == "jsonb"
    assert message.table_schema.column_definitions[1].optional is True

    assert message.table_schema.column_definitions[2].name == "amount"
    assert message.table_schema.column_definitions[2].part_of_pkey == 0
    assert message.table_schema.column_definitions[2].type_name == "numeric(10,2)"
    assert message.table_schema.column_definitions[2].optional is True

    assert message.table_schema.column_definitions[3].name == "updated_at"
    assert message.table_schema.column_definitions[3].part_of_pkey == 0
    assert message.table_schema.column_definitions[3].type_name == "timestamp with time zone"
    assert message.table_schema.column_definitions[3].optional is False

    query = "SELECT oid, typname FROM pg_type WHERE oid = %s::oid;"
    cursor.execute(query, vars=(message.table_schema.column_definitions[0].type_id,))
    result = cursor.fetchone()
    assert result["oid"] == message.table_schema.column_definitions[0].type_id
    assert result["typname"] == "int4"  # integer

    cursor.execute(query, vars=(message.table_schema.column_definitions[1].type_id,))
    result = cursor.fetchone()
    assert result["oid"] == message.table_schema.column_definitions[1].type_id
    assert result["typname"] == "jsonb"

    cursor.execute(query, vars=(message.table_schema.column_definitions[2].type_id,))
    result = cursor.fetchone()
    assert result["oid"] == message.table_schema.column_definitions[2].type_id
    assert result["typname"] == "numeric"

    cursor.execute(query, vars=(message.table_schema.column_definitions[3].type_id,))
    result = cursor.fetchone()
    assert result["oid"] == message.table_schema.column_definitions[3].type_id
    assert result["typname"] == "timestamptz"

    assert message.before is None
    assert message.after is not None
    assert list(message.after.keys()) == ["id", "json_data", "amount", "updated_at"]
    assert message.after["id"] == 10
    assert message.after["json_data"] == {"data": 10}
    assert message.after["amount"] == 10.2
    assert message.after["updated_at"] == datetime.strptime(
        "2020-01-01 00:00:00+00".split("+")[0], "%Y-%m-%d %H:%M:%S"
    ).replace(tzinfo=timezone.utc)


# TODO: ordering of these tests is dependent on the names and should not be
def test_002_update(
    cursor: psycopg2.extras.DictCursor, configure_test_db: typing.Generator[pypgoutput.ChangeEvent, None, None]
) -> None:
    """with default replica identity"""
    cdc_reader = configure_test_db
    cursor.execute("UPDATE public.integration SET updated_at = '2020-02-01 00:00:00+00' WHERE id = 10;")
    message = next(cdc_reader)
    assert message.op == "U"
    assert message.table_schema.db == "test_db"
    assert message.table_schema.schema_name == "public"
    assert message.table_schema.table == "integration"

    assert message.table_schema.column_definitions[0].name == "id"
    assert message.table_schema.column_definitions[0].part_of_pkey == 1
    assert message.table_schema.column_definitions[0].type_name == "integer"
    assert message.table_schema.column_definitions[0].optional is False

    assert message.table_schema.column_definitions[1].name == "json_data"
    assert message.table_schema.column_definitions[1].part_of_pkey == 0
    assert message.table_schema.column_definitions[1].type_name == "jsonb"
    assert message.table_schema.column_definitions[1].optional is True

    assert message.table_schema.column_definitions[2].name == "amount"
    assert message.table_schema.column_definitions[2].part_of_pkey == 0
    assert message.table_schema.column_definitions[2].type_name == "numeric(10,2)"
    assert message.table_schema.column_definitions[2].optional is True

    assert message.table_schema.column_definitions[3].name == "updated_at"
    assert message.table_schema.column_definitions[3].part_of_pkey == 0
    assert message.table_schema.column_definitions[3].type_name == "timestamp with time zone"
    assert message.table_schema.column_definitions[3].optional is False
    # TODO check what happens with replica identity full?
    assert message.before is None
    assert message.after is not None
    assert list(message.after.keys()) == ["id", "json_data", "amount", "updated_at"]
    assert message.after["id"] == 10
    assert message.after["json_data"] == {"data": 10}
    assert message.after["amount"] == 10.2
    assert message.after["updated_at"] == datetime.strptime(
        "2020-02-01 00:00:00+00".split("+")[0], "%Y-%m-%d %H:%M:%S"
    ).replace(tzinfo=timezone.utc)


def test_003_delete(
    cursor: psycopg2.extras.DictCursor, configure_test_db: typing.Generator[pypgoutput.ChangeEvent, None, None]
) -> None:
    """with default replica identity"""
    cdc_reader = configure_test_db
    cursor.execute("DELETE FROM public.integration WHERE id = 10;")
    message = next(cdc_reader)
    assert message.op == "D"
    assert message.table_schema.db == "test_db"
    assert message.table_schema.schema_name == "public"
    assert message.table_schema.table == "integration"

    assert message.table_schema.column_definitions[0].name == "id"
    assert message.table_schema.column_definitions[0].part_of_pkey == 1
    assert message.table_schema.column_definitions[0].type_name == "integer"
    assert message.table_schema.column_definitions[0].optional is False

    assert message.table_schema.column_definitions[1].name == "json_data"
    assert message.table_schema.column_definitions[1].part_of_pkey == 0
    assert message.table_schema.column_definitions[1].type_name == "jsonb"
    assert message.table_schema.column_definitions[1].optional is True

    assert message.table_schema.column_definitions[2].name == "amount"
    assert message.table_schema.column_definitions[2].part_of_pkey == 0
    assert message.table_schema.column_definitions[2].type_name == "numeric(10,2)"
    assert message.table_schema.column_definitions[2].optional is True

    assert message.table_schema.column_definitions[3].name == "updated_at"
    assert message.table_schema.column_definitions[3].part_of_pkey == 0
    assert message.table_schema.column_definitions[3].type_name == "timestamp with time zone"
    assert message.table_schema.column_definitions[3].optional is False
    assert message.before is not None
    assert message.before["id"] == 10
    assert message.after is None


def test_004_truncate(
    cursor: psycopg2.extras.DictCursor, configure_test_db: typing.Generator[pypgoutput.ChangeEvent, None, None]
) -> None:
    cdc_reader = configure_test_db
    cursor.execute("INSERT INTO public.integration (id, updated_at) VALUES (11, '2020-01-01 00:00:00+00');")
    cursor.execute("SELECT COUNT(*) AS n FROM public.integration;")
    assert cursor.fetchone()["n"] == 1
    insert_msg = next(cdc_reader)
    assert insert_msg.op == "I"
    cursor.execute("TRUNCATE public.integration;")
    message = next(cdc_reader)
    assert message.op == "T"
    assert message.table_schema.db == "test_db"
    assert message.table_schema.schema_name == "public"
    assert message.table_schema.table == "integration"

    assert message.table_schema.column_definitions[0].name == "id"
    assert message.table_schema.column_definitions[0].part_of_pkey == 1
    assert message.table_schema.column_definitions[0].type_name == "integer"
    assert message.table_schema.column_definitions[0].optional is False

    assert message.table_schema.column_definitions[1].name == "json_data"
    assert message.table_schema.column_definitions[1].part_of_pkey == 0
    assert message.table_schema.column_definitions[1].type_name == "jsonb"
    assert message.table_schema.column_definitions[1].optional is True

    assert message.table_schema.column_definitions[2].name == "amount"
    assert message.table_schema.column_definitions[2].part_of_pkey == 0
    assert message.table_schema.column_definitions[2].type_name == "numeric(10,2)"
    assert message.table_schema.column_definitions[2].optional is True

    assert message.table_schema.column_definitions[3].name == "updated_at"
    assert message.table_schema.column_definitions[3].part_of_pkey == 0
    assert message.table_schema.column_definitions[3].type_name == "timestamp with time zone"
    assert message.table_schema.column_definitions[3].optional is False
    assert message.before is None
    assert message.after is None


def test_005_extractor_error(cursor: psycopg2.extras.DictCursor) -> None:
    pipe_out_conn, pipe_in_conn = multiprocessing.Pipe(duplex=True)
    dsn = psycopg2.extensions.make_dsn(host=HOST, database=DATABASE_NAME, port=PORT, user=USER, password=PASSWORD)
    extractor = pypgoutput.ExtractRaw(
        dsn=dsn, publication_name=PUBLICATION_NAME, slot_name=SLOT_NAME, pipe_conn=pipe_in_conn
    )
    extractor.connect()
    with pytest.raises(psycopg_errors.ObjectInUse):
        extractor.run()
