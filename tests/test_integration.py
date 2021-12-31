import psycopg2
import psycopg2.extras
import pytest


@pytest.fixture
def connection():
    connection = psycopg2.connect(
        host="localhost",
        database="test",
        port=5432,
        user="test",
        password="test",
    )
    yield connection
    connection.close()


@pytest.fixture
def cursor(connection):
    curs = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
    yield curs
    curs.close()


def test_000_dummy_test(cursor):
    """make sure connection/cursor and DB is operational for tests"""
    cursor.execute("SELECT 1 as n;")
    result = cursor.fetchone()
    assert result["n"] == 1
