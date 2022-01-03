import logging
import os

import psycopg2
import psycopg2.extras
import pytest

import pypgoutput

HOST = os.environ.get("PGHOST")
PORT = os.environ.get("PGPORT")
DATABASE_NAME = os.environ.get("PGDATABASE")
USER = os.environ.get("PGUSER")
PASSWORD = os.environ.get("PGPASSWORD")

SLOT_NAME = "my_slot"
LOCAL_DSN = f"host=localhost user={USER} port={PORT} dbname={DATABASE_NAME} password={PASSWORD}"

logging.basicConfig(level=logging.DEBUG, format="%(relativeCreated)6d %(processName)s %(message)s")
logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
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


@pytest.fixture(scope="session")
def cursor(connection):
    curs = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
    yield curs
    curs.close()


@pytest.fixture(scope="session")
def configure_test_db(cursor):
    cursor.execute("CREATE PUBLICATION pub FOR ALL TABLES;")
    cursor.execute("SELECT * FROM pg_create_logical_replication_slot('my_slot', 'pgoutput');")
    cdc_reader = pypgoutput.LogicalReplicationReader(db_name=DATABASE_NAME, db_dsn=LOCAL_DSN, slot_name=SLOT_NAME)
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


def test_001_insert(cursor, configure_test_db):
    """with default replica identity"""
    cdc_reader = configure_test_db
    # cursor.execute("SELECT COUNT(*) AS n FROM public.integration;")
    # assert cursor.fetchone()["n"] == 0
    cursor.execute("INSERT INTO public.integration (id, updated_at) VALUES (10, '2020-01-01 00:00:00+00');")
    cursor.execute("SELECT COUNT(*) AS n FROM public.integration;")
    assert cursor.fetchone()["n"] == 1

    message = next(cdc_reader)
    # print(json.dumps(message, indent=2, default=str))
    assert message["op"] == "I"
    assert message["source"]["db"] == "test_db"
    assert message["source"]["schema"] == "public"
    assert message["source"]["table"] == "integration"

    assert message["table_schema"][0]["name"] == "id"
    assert message["table_schema"][0]["part_of_pkey"] == 1
    assert message["table_schema"][0]["type"] == "integer"
    assert message["table_schema"][0]["optional"] is False

    assert message["table_schema"][1]["name"] == "updated_at"
    assert message["table_schema"][1]["part_of_pkey"] == 0
    assert message["table_schema"][1]["type"] == "timestamp with time zone"
    assert message["table_schema"][1]["optional"] is True

    assert message["before"] is None
    assert list(message["after"].keys()) == ["id", "updated_at"]
    # TODO these types should be cast correctly at some point
    assert message["after"]["id"] == "10"
    assert message["after"]["updated_at"] == "2020-01-01 00:00:00+00"


# TODO: ordering of these tests is dependent on the names and should not be
def test_002_update(cursor, configure_test_db):
    """with default replica identity"""
    cdc_reader = configure_test_db
    cursor.execute("UPDATE public.integration SET updated_at = '2020-02-01 00:00:00+00' WHERE id = 10;")
    message = next(cdc_reader)
    assert message["op"] == "U"
    assert message["source"]["db"] == "test_db"
    assert message["source"]["schema"] == "public"
    assert message["source"]["table"] == "integration"

    assert message["table_schema"][0]["name"] == "id"
    assert message["table_schema"][0]["part_of_pkey"] == 1
    assert message["table_schema"][0]["type"] == "integer"
    assert message["table_schema"][0]["optional"] is False

    assert message["table_schema"][1]["name"] == "updated_at"
    assert message["table_schema"][1]["part_of_pkey"] == 0
    assert message["table_schema"][1]["type"] == "timestamp with time zone"
    assert message["table_schema"][1]["optional"] is True
    # TODO check what happens with replica identity full?
    assert message["before"] is None
    assert list(message["after"].keys()) == ["id", "updated_at"]
    # TODO these types should be cast correctly at some point
    assert message["after"]["id"] == "10"
    assert message["after"]["updated_at"] == "2020-02-01 00:00:00+00"


def test_003_delete(cursor, configure_test_db):
    """with default replica identity"""
    cdc_reader = configure_test_db
    cursor.execute("DELETE FROM public.integration WHERE id = 10;")
    message = next(cdc_reader)
    assert message["op"] == "D"
    assert message["source"]["db"] == "test_db"
    assert message["source"]["schema"] == "public"
    assert message["source"]["table"] == "integration"

    assert message["table_schema"][0]["name"] == "id"
    assert message["table_schema"][0]["part_of_pkey"] == 1
    assert message["table_schema"][0]["type"] == "integer"
    assert message["table_schema"][0]["optional"] is False

    assert message["table_schema"][1]["name"] == "updated_at"
    assert message["table_schema"][1]["part_of_pkey"] == 0
    assert message["table_schema"][1]["type"] == "timestamp with time zone"
    assert message["table_schema"][1]["optional"] is True
    assert message["before"]["id"] == "10"
    assert message["before"]["updated_at"] is None  # check what happens with replica identity
    assert message["after"] is None


def test_004_truncate(cursor, configure_test_db):
    cdc_reader = configure_test_db
    cursor.execute("INSERT INTO public.integration (id, updated_at) VALUES (11, '2020-01-01 00:00:00+00');")
    cursor.execute("SELECT COUNT(*) AS n FROM public.integration;")
    assert cursor.fetchone()["n"] == 1
    insert_msg = next(cdc_reader)
    assert insert_msg["op"] == "I"
    cursor.execute("TRUNCATE public.integration;")
    message = next(cdc_reader)
    assert message["op"] == "T"
    assert message["source"]["db"] == "test_db"
    assert message["source"]["schema"] == "public"
    assert message["source"]["table"] == "integration"

    assert message["table_schema"][0]["name"] == "id"
    assert message["table_schema"][0]["part_of_pkey"] == 1
    assert message["table_schema"][0]["type"] == "integer"
    assert message["table_schema"][0]["optional"] is False

    assert message["table_schema"][1]["name"] == "updated_at"
    assert message["table_schema"][1]["part_of_pkey"] == 0
    assert message["table_schema"][1]["type"] == "timestamp with time zone"
    assert message["table_schema"][1]["optional"] is True
    assert message["before"] is None
    assert message["after"] is None