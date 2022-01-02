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


@pytest.fixture
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


@pytest.fixture
def cursor(connection):
    curs = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
    yield curs
    curs.close()


@pytest.fixture
def configure_logical_decoding(cursor):
    # TODO: should run only once per test session
    cursor.execute("CREATE PUBLICATION pub FOR ALL TABLES;")
    cursor.execute("SELECT * FROM pg_create_logical_replication_slot('my_slot', 'pgoutput');")


def test_000_dummy_test(cursor):
    """make sure connection/cursor and DB is operational for tests"""
    cursor.execute("SELECT 1 as n;")
    result = cursor.fetchone()
    assert result["n"] == 1


def test_reader(cursor, configure_logical_decoding):
    cdc_reader = pypgoutput.LogicalReplicationReader(db_name=DATABASE_NAME, db_dsn=LOCAL_DSN, slot_name=SLOT_NAME)
    # assumes all tables are in publication
    query = """
    DROP TABLE IF EXISTS public.integration;
    CREATE TABLE public.integration (id integer primary key, updated_at timestamptz);
    CREATE TRIGGER updated_at_trigger
    BEFORE INSERT OR UPDATE
    ON public.integration
    FOR EACH ROW
    EXECUTE PROCEDURE public.updated_at_trigger();
    """
    cursor.execute(query)
    cursor.execute("SELECT COUNT(*) AS n FROM public.integration;")
    assert cursor.fetchone()["n"] == 0
    cursor.execute("INSERT INTO public.integration (id) VALUES (10);")
    logger.info("writing data to table")
    cursor.execute("SELECT COUNT(*) AS n FROM public.integration;")
    assert cursor.fetchone()["n"] == 1

    message = next(cdc_reader)
    assert message["op"] == "I"
    assert message["source"]["db"] == "test_db"
    assert message["source"]["schema"] == "public"
    assert message["source"]["table"] == "integration"
    logger.info("closing extractor")
    cdc_reader.stop()
