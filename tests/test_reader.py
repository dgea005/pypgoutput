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

TEST_TABLE_DDL = """
CREATE TABLE public.integration (
        id integer primary key,
        json_data jsonb,
        amount numeric(10, 2),
        updated_at timestamptz not null,
        text_data text
);
"""
TEST_TABLE_COLUMNS = ["id", "json_data", "amount", "updated_at", "text_data"]


logger = logging.getLogger("tests")
log_handler = logging.StreamHandler()
log_handler.setLevel(logging.INFO)
logger.addHandler(log_handler)


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
    version_query = "SELECT version();"
    curs.execute(version_query)
    result = curs.fetchone()
    logger.info(f"PG test version: {result}")
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
    query = f"""DROP TABLE IF EXISTS public.integration CASCADE;
    {TEST_TABLE_DDL}
    """
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


def validate_message_table_schema(message: pypgoutput.ChangeEvent, replica_identity_full: bool = False) -> None:
    """Each message from the test table will have the same schema to be tested.
    Schema of table is in configure_test_db
    """
    assert message.table_schema.db == "test_db"
    assert message.table_schema.schema_name == "public"
    assert message.table_schema.table == "integration"

    assert message.table_schema.column_definitions[0].name == "id"
    assert message.table_schema.column_definitions[0].part_of_pkey == 1
    assert message.table_schema.column_definitions[0].type_name == "integer"
    assert message.table_schema.column_definitions[0].optional is False

    assert message.table_schema.column_definitions[1].name == "json_data"
    assert message.table_schema.column_definitions[1].part_of_pkey == int(replica_identity_full)
    assert message.table_schema.column_definitions[1].type_name == "jsonb"
    assert message.table_schema.column_definitions[1].optional is True

    assert message.table_schema.column_definitions[2].name == "amount"
    assert message.table_schema.column_definitions[2].part_of_pkey == int(replica_identity_full)
    assert message.table_schema.column_definitions[2].type_name == "numeric(10,2)"
    assert message.table_schema.column_definitions[2].optional is True

    assert message.table_schema.column_definitions[3].name == "updated_at"
    assert message.table_schema.column_definitions[3].part_of_pkey == int(replica_identity_full)
    assert message.table_schema.column_definitions[3].type_name == "timestamp with time zone"
    assert message.table_schema.column_definitions[3].optional is False

    assert message.table_schema.column_definitions[4].name == "text_data"
    assert message.table_schema.column_definitions[4].part_of_pkey == int(replica_identity_full)
    assert message.table_schema.column_definitions[4].type_name == "text"
    assert message.table_schema.column_definitions[4].optional is True


def test_001_insert(
    cursor: psycopg2.extras.DictCursor, configure_test_db: typing.Generator[pypgoutput.ChangeEvent, None, None]
) -> None:
    """with default replica identity"""
    cdc_reader = configure_test_db
    cursor.execute(
        """INSERT INTO public.integration (id, json_data, amount, updated_at, text_data)
        VALUES (10, '{"data": 10}', 10.20, '2020-01-01 00:00:00+00', 'dummy_value');
        """
    )
    cursor.execute("SELECT COUNT(*) AS n FROM public.integration;")
    assert cursor.fetchone()["n"] == 1

    message = next(cdc_reader)
    assert message.op == "I"

    validate_message_table_schema(message=message)

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

    cursor.execute(query, vars=(message.table_schema.column_definitions[4].type_id,))
    result = cursor.fetchone()
    assert result["oid"] == message.table_schema.column_definitions[4].type_id
    assert result["typname"] == "text"

    assert message.before is None
    assert message.after is not None
    assert list(message.after.keys()) == TEST_TABLE_COLUMNS
    assert message.after["id"] == 10
    assert message.after["json_data"] == {"data": 10}
    assert message.after["amount"] == 10.2
    assert message.after["updated_at"] == datetime.strptime(
        "2020-01-01 00:00:00+00".split("+")[0], "%Y-%m-%d %H:%M:%S"
    ).replace(tzinfo=timezone.utc)
    assert message.after["text_data"] == "dummy_value"


def test_002_update(
    cursor: psycopg2.extras.DictCursor, configure_test_db: typing.Generator[pypgoutput.ChangeEvent, None, None]
) -> None:
    """with default replica identity"""
    cdc_reader = configure_test_db
    cursor.execute("UPDATE public.integration SET updated_at = '2020-02-01 00:00:00+00' WHERE id = 10;")
    message = next(cdc_reader)
    assert message.op == "U"
    validate_message_table_schema(message=message)
    # TODO check what happens with replica identity full?
    assert message.before is None
    assert message.after is not None
    assert list(message.after.keys()) == TEST_TABLE_COLUMNS
    assert message.after["id"] == 10
    assert message.after["json_data"] == {"data": 10}
    assert message.after["amount"] == 10.2
    assert message.after["updated_at"] == datetime.strptime(
        "2020-02-01 00:00:00+00".split("+")[0], "%Y-%m-%d %H:%M:%S"
    ).replace(tzinfo=timezone.utc)
    assert message.after["text_data"] == "dummy_value"


# TODO: ordering of these tests is dependent on the names and should not be
def test_003_update_key(
    cursor: psycopg2.extras.DictCursor, configure_test_db: typing.Generator[pypgoutput.ChangeEvent, None, None]
) -> None:
    """with default replica identity"""
    cdc_reader = configure_test_db
    cursor.execute("UPDATE public.integration SET id = 11 WHERE id = 10;")
    message = next(cdc_reader)
    assert message.op == "U"
    validate_message_table_schema(message=message)
    assert message.before is not None
    assert list(message.before.keys()) == ["id"]
    assert message.before["id"] == 10

    assert message.after is not None
    assert list(message.after.keys()) == TEST_TABLE_COLUMNS
    assert message.after["id"] == 11
    assert message.after["json_data"] == {"data": 10}
    assert message.after["amount"] == 10.2
    assert message.after["updated_at"] == datetime.strptime(
        "2020-02-01 00:00:00+00".split("+")[0], "%Y-%m-%d %H:%M:%S"
    ).replace(tzinfo=timezone.utc)
    assert message.after["text_data"] == "dummy_value"


def test_004_delete(
    cursor: psycopg2.extras.DictCursor, configure_test_db: typing.Generator[pypgoutput.ChangeEvent, None, None]
) -> None:
    """with default replica identity"""
    cdc_reader = configure_test_db
    cursor.execute("DELETE FROM public.integration WHERE id = 11;")
    message = next(cdc_reader)
    assert message.op == "D"
    validate_message_table_schema(message=message)
    assert message.before is not None
    assert message.before["id"] == 11
    assert message.after is None


def test_005_update_replica_identity_full(
    cursor: psycopg2.extras.DictCursor, configure_test_db: typing.Generator[pypgoutput.ChangeEvent, None, None]
) -> None:
    cdc_reader = configure_test_db
    cursor.execute("ALTER TABLE public.integration REPLICA IDENTITY FULL;")
    cursor.execute(
        """INSERT INTO public.integration (id, json_data, amount, updated_at, text_data)
        VALUES (12, '{"data": 10}', 10.20, '2020-01-01 00:00:00+00', 'dummy_value');
    """
    )
    cursor.execute("UPDATE public.integration SET text_data = 'new_text_value' WHERE id = 12;")
    message = next(cdc_reader)
    message = next(cdc_reader)
    assert message.op == "U"
    validate_message_table_schema(message=message, replica_identity_full=True)
    assert message.before is not None
    assert list(message.before.keys()) == TEST_TABLE_COLUMNS
    assert message.before["id"] == 12
    assert message.before["json_data"] == {"data": 10}
    assert message.before["amount"] == 10.2
    assert message.before["updated_at"] == datetime.strptime(
        "2020-01-01 00:00:00+00".split("+")[0], "%Y-%m-%d %H:%M:%S"
    ).replace(tzinfo=timezone.utc)
    assert message.before["text_data"] == "dummy_value"

    assert message.after is not None
    assert list(message.after.keys()) == TEST_TABLE_COLUMNS
    assert message.after["id"] == 12
    assert message.after["json_data"] == {"data": 10}
    assert message.after["amount"] == 10.2
    assert message.after["updated_at"] == datetime.strptime(
        "2020-01-01 00:00:00+00".split("+")[0], "%Y-%m-%d %H:%M:%S"
    ).replace(tzinfo=timezone.utc)
    assert message.after["text_data"] == "new_text_value"


def test_006_delete_replica_identity_full(
    cursor: psycopg2.extras.DictCursor, configure_test_db: typing.Generator[pypgoutput.ChangeEvent, None, None]
) -> None:
    cdc_reader = configure_test_db
    cursor.execute("DELETE FROM public.integration WHERE id = 12;")
    message = next(cdc_reader)
    assert message.op == "D"
    validate_message_table_schema(message=message, replica_identity_full=True)
    assert message.before is not None
    assert list(message.before.keys()) == TEST_TABLE_COLUMNS
    assert message.before["id"] == 12
    assert message.before["json_data"] == {"data": 10}
    assert message.before["amount"] == 10.2
    assert message.before["updated_at"] == datetime.strptime(
        "2020-01-01 00:00:00+00".split("+")[0], "%Y-%m-%d %H:%M:%S"
    ).replace(tzinfo=timezone.utc)
    assert message.before["text_data"] == "new_text_value"
    assert message.after is None


def test_007_truncate(
    cursor: psycopg2.extras.DictCursor, configure_test_db: typing.Generator[pypgoutput.ChangeEvent, None, None]
) -> None:
    cdc_reader = configure_test_db
    cursor.execute("INSERT INTO public.integration (id, updated_at) VALUES (14, '2020-01-01 00:00:00+00');")
    insert_msg = next(cdc_reader)
    assert insert_msg.op == "I"
    cursor.execute("TRUNCATE public.integration;")
    message = next(cdc_reader)
    assert message.op == "T"
    validate_message_table_schema(message=message, replica_identity_full=True)
    assert message.before is None
    assert message.after is None


def test_008_extractor_error(cursor: psycopg2.extras.DictCursor) -> None:
    pipe_out_conn, pipe_in_conn = multiprocessing.Pipe(duplex=True)
    dsn = psycopg2.extensions.make_dsn(host=HOST, database=DATABASE_NAME, port=PORT, user=USER, password=PASSWORD)
    extractor = pypgoutput.ExtractRaw(
        dsn=dsn, publication_name=PUBLICATION_NAME, slot_name=SLOT_NAME, pipe_conn=pipe_in_conn
    )
    extractor.connect()
    with pytest.raises(psycopg_errors.ObjectInUse):
        extractor.run()
