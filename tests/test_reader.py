import logging
import os
from typing import Generator

import psycopg2
import psycopg2.errors
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
def connection():
    connection = psycopg2.connect(
        host=HOST,
        database=DATABASE_NAME,
        port=PORT,
        user=USER,
        password=PASSWORD,
    )
    connection.autocommit = True
    yield connection
    connection.close()


@pytest.fixture(scope="module")
def cursor(connection):
    curs = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
    yield curs
    curs.close()


@pytest.fixture(scope="module")
def configure_test_db(cursor):
    cursor.execute(f"DROP PUBLICATION IF EXISTS {PUBLICATION_NAME};")
    try:
        cursor.execute(f"SELECT pg_drop_replication_slot('{SLOT_NAME}');")
    except psycopg2.errors.UndefinedObject as err:
        logger.warning(f"slot {SLOT_NAME} could not be dropped because it does not exist", err)
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
        updated_at timestamptz
    );"""
    cursor.execute(query)
    yield cdc_reader
    logger.info("Closing test CDC reader")
    cdc_reader.stop()


def test_000_dummy_test(cursor):
    """make sure connection/cursor and DB is operational for tests"""
    cursor.execute("SELECT 1 as n;")
    result = cursor.fetchone()
    assert result["n"] == 1


def test_001_insert(cursor, configure_test_db: Generator[pypgoutput.ChangeEvent, None, None]):
    """with default replica identity"""
    cdc_reader = configure_test_db
    cursor.execute("INSERT INTO public.integration (id, updated_at) VALUES (10, '2020-01-01 00:00:00+00');")
    cursor.execute("SELECT COUNT(*) AS n FROM public.integration;")
    assert cursor.fetchone()["n"] == 1

    message = next(cdc_reader)
    # print(json.dumps(message, indent=2, default=str))

    assert message.op == "I"
    assert message.table_schema.db == "test_db"
    assert message.table_schema.schema == "public"
    assert message.table_schema.table == "integration"

    assert message.table_schema.column_definitions[0].name == "id"
    assert message.table_schema.column_definitions[0].part_of_pkey == 1
    assert message.table_schema.column_definitions[0].type_name == "integer"
    assert message.table_schema.column_definitions[0].optional is False

    assert message.table_schema.column_definitions[1].name == "updated_at"
    assert message.table_schema.column_definitions[1].part_of_pkey == 0
    assert message.table_schema.column_definitions[1].type_name == "timestamp with time zone"
    assert message.table_schema.column_definitions[1].optional is True

    assert message.before is None
    assert list(message.after.keys()) == ["id", "updated_at"]
    # TODO these types should be cast correctly at some point
    assert message.after["id"] == "10"
    assert message.after["updated_at"] == "2020-01-01 00:00:00+00"


# TODO: ordering of these tests is dependent on the names and should not be
def test_002_update(cursor, configure_test_db: Generator[pypgoutput.ChangeEvent, None, None]):
    """with default replica identity"""
    cdc_reader = configure_test_db
    cursor.execute("UPDATE public.integration SET updated_at = '2020-02-01 00:00:00+00' WHERE id = 10;")
    message = next(cdc_reader)
    assert message.op == "U"
    assert message.table_schema.db == "test_db"
    assert message.table_schema.schema == "public"
    assert message.table_schema.table == "integration"

    assert message.table_schema.column_definitions[0].name == "id"
    assert message.table_schema.column_definitions[0].part_of_pkey == 1
    assert message.table_schema.column_definitions[0].type_name == "integer"
    assert message.table_schema.column_definitions[0].optional is False

    assert message.table_schema.column_definitions[1].name == "updated_at"
    assert message.table_schema.column_definitions[1].part_of_pkey == 0
    assert message.table_schema.column_definitions[1].type_name == "timestamp with time zone"
    assert message.table_schema.column_definitions[1].optional is True
    # TODO check what happens with replica identity full?
    assert message.before is None
    assert list(message.after.keys()) == ["id", "updated_at"]
    # TODO these types should be cast correctly at some point
    assert message.after["id"] == "10"
    assert message.after["updated_at"] == "2020-02-01 00:00:00+00"


def test_003_delete(cursor, configure_test_db: Generator[pypgoutput.ChangeEvent, None, None]):
    """with default replica identity"""
    cdc_reader = configure_test_db
    cursor.execute("DELETE FROM public.integration WHERE id = 10;")
    message = next(cdc_reader)
    assert message.op == "D"
    assert message.table_schema.db == "test_db"
    assert message.table_schema.schema == "public"
    assert message.table_schema.table == "integration"

    assert message.table_schema.column_definitions[0].name == "id"
    assert message.table_schema.column_definitions[0].part_of_pkey == 1
    assert message.table_schema.column_definitions[0].type_name == "integer"
    assert message.table_schema.column_definitions[0].optional is False

    assert message.table_schema.column_definitions[1].name == "updated_at"
    assert message.table_schema.column_definitions[1].part_of_pkey == 0
    assert message.table_schema.column_definitions[1].type_name == "timestamp with time zone"
    assert message.table_schema.column_definitions[1].optional is True
    assert message.before["id"] == "10"
    # TODO: check and test what happens with replica identity
    assert message.before["updated_at"] is None
    assert message.after is None


def test_004_truncate(cursor, configure_test_db: Generator[pypgoutput.ChangeEvent, None, None]):
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
    assert message.table_schema.schema == "public"
    assert message.table_schema.table == "integration"

    assert message.table_schema.column_definitions[0].name == "id"
    assert message.table_schema.column_definitions[0].part_of_pkey == 1
    assert message.table_schema.column_definitions[0].type_name == "integer"
    assert message.table_schema.column_definitions[0].optional is False

    assert message.table_schema.column_definitions[1].name == "updated_at"
    assert message.table_schema.column_definitions[1].part_of_pkey == 0
    assert message.table_schema.column_definitions[1].type_name == "timestamp with time zone"
    assert message.table_schema.column_definitions[1].optional is True
    assert message.before is None
    assert message.after is None
