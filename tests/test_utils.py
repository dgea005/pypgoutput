import logging
import os
import typing

import psycopg2
import psycopg2.extras
import pytest

import pypgoutput

HOST = os.environ.get("PGHOST")
PORT = os.environ.get("PGPORT")
DATABASE_NAME = os.environ.get("PGDATABASE")
USER = os.environ.get("PGUSER")
PASSWORD = os.environ.get("PGPASSWORD")

DSN = f"host={HOST} port={PORT} dbname={DATABASE_NAME} user={USER} password={PASSWORD}"

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
def table(cursor: psycopg2.extras.DictCursor) -> None:
    query = """
    DROP TABLE IF EXISTS public.utils_test CASCADE;
    CREATE TABLE public.utils (
        c0 integer primary key,
        c1 timestamptz,
        c2 text not null
    );"""
    cursor.execute(query)


def test_source_db_handler_fetchone() -> None:
    handler = pypgoutput.SourceDBHandler(dsn=DSN)
    handler.connect()
    result = handler.fetchone("SELECT 1 AS n;")
    assert result["n"] == 1
    # test invalid query
    with pytest.raises(pypgoutput.QueryError):
        handler.fetchone("SELECT COUNT(*) FROM public.missing_table")
    handler.close()


def test_source_db_handler_fetch() -> None:
    handler = pypgoutput.SourceDBHandler(dsn=DSN)
    handler.connect()
    result = handler.fetch("SELECT n FROM generate_series(0, 5) AS n;")
    assert result == [[n] for n in range(6)]
    # test invalid query
    with pytest.raises(pypgoutput.QueryError):
        handler.fetch("SELECT abcd")
    handler.close()


def test_source_db_handler_column_optional(table: typing.Callable[[None], None]) -> None:
    handler = pypgoutput.SourceDBHandler(dsn=DSN)
    handler.connect()
    result = handler.fetch_if_column_is_optional(table_schema="public", table_name="utils", column_name="c0")
    assert result is False
    result = handler.fetch_if_column_is_optional(table_schema="public", table_name="utils", column_name="c1")
    assert result is True
    result = handler.fetch_if_column_is_optional(table_schema="public", table_name="utils", column_name="c2")
    assert result is False
    handler.close()


def test_source_db_handler_column_type(
    cursor: psycopg2.extras.DictCursor, table: typing.Callable[[None], None]
) -> None:
    cursor.execute("SELECT oid FROM pg_type WHERE typname='timestamptz'")
    oid = cursor.fetchone()
    handler = pypgoutput.SourceDBHandler(dsn=DSN)
    handler.connect()
    result = handler.fetch_column_type(type_id=oid["oid"], atttypmod=-1)
    assert result == "timestamp with time zone"
    handler.close()
