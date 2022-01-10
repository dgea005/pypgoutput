import logging
import multiprocessing
import time
import typing
import uuid
from collections import OrderedDict
from datetime import datetime
from multiprocessing.connection import Connection
from multiprocessing.context import Process

import psycopg2
import psycopg2.extensions
import psycopg2.extras
import pydantic

from pypgoutput.decoders import TupleData, decode_message
from pypgoutput.utils import SourceDBHandler

logger = logging.getLogger(__name__)


class ReplicationMessage(pydantic.BaseModel):
    message_id: pydantic.UUID4
    data_start: int
    payload: bytes
    send_time: datetime
    data_size: int
    wal_end: int


class LogicalReplicationReader:
    """
    1. One process continuously extracts (ExtractRaw) raw messages
        a. Uses pyscopg2's LogicalReplicationConnection and replication expert
        b. Send raw pg-output replication messages and metadata to a pipe for another process to read from
    2. Main process extracts the raw messages from the pipe (sent in 1.b.)
        a. Decode binary pgoutput message to B, C, I, U, D or R message type
        b. Pass decoded message into transform function that produces change events with additional metadata cached in
           from previous messages and by looking up values in the source DBs catalog
    """

    def __init__(self, publication_name: str, slot_name: str, dsn: typing.Optional[str] = None, **kwargs) -> None:
        self.dsn = psycopg2.extensions.make_dsn(dsn=dsn, **kwargs)
        self.publication_name = publication_name
        self.slot_name = slot_name
        self.setup()

    def setup(self):
        self.pipe_out_conn, self.pipe_in_conn = multiprocessing.Pipe(duplex=True)
        self.extractor = ExtractRaw(
            pipe_conn=self.pipe_in_conn, dsn=self.dsn, publication_name=self.publication_name, slot_name=self.slot_name
        )
        self.extractor.connect()
        self.extractor.start()
        # TODO: make some aspect of this output configurable, raw msg return
        self.raw_msgs = self.read_raw_extracted()
        self.transformed_msgs = transform_raw_to_change_event(message_stream=self.raw_msgs, dsn=self.dsn)

    def stop(self) -> None:
        """Stop reader process and close the pipe"""
        self.extractor.terminate()
        time.sleep(0.1)
        self.extractor.close()
        self.pipe_out_conn.close()
        self.pipe_in_conn.close()

    def read_raw_extracted(self) -> typing.Generator[ReplicationMessage, None, None]:
        """yields ReplicationMessages from the pipe as written by extractor process"""
        empty_count = 0
        iter_count = 0
        msg_count = 0
        while True:
            if not self.pipe_out_conn.poll(timeout=0.5):
                empty_count += 1
            else:
                item = self.pipe_out_conn.recv()
                msg_count += 1
                yield item
                self.pipe_out_conn.send({"id": item.message_id})
            if iter_count % 50 == 0:
                logger.info(f"pipe poll count: {iter_count}, messages processed: {msg_count}")
            iter_count += 1

    def __iter__(self):
        return self

    def __next__(self):
        try:
            return next(self.transformed_msgs)
        except Exception as err:
            self.stop()  # try to close everything
            logger.error(f"Error extracting LR logs: {err}")
            raise StopIteration from err


class ExtractRaw(Process):
    """
    Consume logical replication messages using psycopg2's LogicalReplicationConnection. Run as a separate process
    due to using consume_stream's endless loop. Consume msg sends data into a pipe for another process to extract

    Docs:
    https://www.psycopg.org/docs/extras.html#replication-support-objects
    https://www.psycopg.org/docs/extras.html#psycopg2.extras.ReplicationCursor.consume_stream
    """

    def __init__(self, dsn: str, publication_name: str, slot_name: str, pipe_conn: Connection):
        Process.__init__(self)
        self.dsn = dsn
        self.publication_name = publication_name
        self.slot_name = slot_name
        self.pipe_conn = pipe_conn
        self.conn = None
        self.cur = None

    def connect(self) -> None:
        self.conn = psycopg2.connect(self.dsn, connection_factory=psycopg2.extras.LogicalReplicationConnection)
        self.cur = self.conn.cursor()

    def run(self) -> None:
        replication_options = {"publication_names": self.publication_name, "proto_version": "1"}
        try:
            self.cur.start_replication(slot_name=self.slot_name, decode=False, options=replication_options)
        except psycopg2.ProgrammingError:
            self.cur.create_replication_slot(self.slot_name, output_plugin="pgoutput")
            self.cur.start_replication(slot_name=self.slot_name, decode=False, options=replication_options)
        try:
            logger.info(f"Starting replication from slot: {self.slot_name}")
            self.cur.consume_stream(self.msg_consumer)
        except Exception as err:
            logger.error(f"Error consuming stream from slot: '{self.slot_name}'. {err}")
            self.cur.close()
            self.conn.close()

    def msg_consumer(self, msg):
        message_id = uuid.uuid4()
        message = ReplicationMessage(
            message_id=message_id,
            data_start=msg.data_start,
            payload=msg.payload,
            send_time=msg.send_time,
            data_size=msg.data_size,
            wal_end=msg.wal_end,
        )
        self.pipe_conn.send(message)
        result = self.pipe_conn.recv()  # how would this wait until processing is done?
        if result["id"] == message_id:
            msg.cursor.send_feedback(flush_lsn=msg.data_start)
            logger.debug(f"Flushed message: '{str(message_id)}'")
        else:
            logger.warning(f"Could not confirm message: {str(message_id)}. Did not flush at {str(msg.data_start)}")


class ColumnDefinition(pydantic.BaseModel):
    name: str
    part_of_pkey: bool
    type_id: int
    type_name: str
    optional: bool


class TableSchema(pydantic.BaseModel):
    column_definitions: typing.List[ColumnDefinition]
    db: str
    schema_name: str
    table: str
    relation_id: int
    model: typing.Any  # pydantic model used for before/after tuple


class Tables(typing.TypedDict):
    # TypedDict is from PEP 589 https://www.python.org/dev/peps/pep-0589/
    relation_id: TableSchema


class PgTypes(typing.TypedDict):
    """Mapping of DB internal type_id to human readable name"""

    type_id: str


class Transaction(pydantic.BaseModel):
    tx_id: int
    begin_lsn: int
    commit_ts: datetime


class ChangeEvent(pydantic.BaseModel):
    op: str  # (ENUM of I, U, D, T)
    message_id: pydantic.UUID4
    lsn: int
    transaction: Transaction  # replication/source metadata
    table_schema: TableSchema
    before: typing.Optional[typing.Dict]  # depends on the source table
    after: typing.Optional[typing.Dict]


def map_tuple_to_dict(tuple_data: TupleData, relation: TableSchema) -> OrderedDict:
    """Convert tuple data to an OrderedDict with keys from relation mapped in order to tuple data"""
    output = OrderedDict()
    for idx, col in enumerate(tuple_data.column_data):
        column_name = relation.column_definitions[idx].name
        output[column_name] = col.col_data
    return output


def convert_pg_type_to_py_type(pg_type_name: str) -> type:
    """try out PEP-636 https://docs.python.org/3/whatsnew/3.10.html#pep-634-structural-pattern-matching"""
    match pg_type_name:
        case "bigint" | "integer" | "smallint":
            return int
        case "timestamp with time zone" | "timestamp without time zone":
            return datetime
        # json not tested yet
        case "json" | "jsonb":
            return dict
        case _:
            return str


def transform_raw_to_change_event(
    message_stream: typing.Generator[ReplicationMessage, None, None], dsn: str
) -> typing.Generator[ChangeEvent, None, None]:
    """Convert raw messages to change events

    Replication protocol https://www.postgresql.org/docs/12/protocol-logical-replication.html

    Every sent transaction contains zero or more DML messages (Insert, Update, Delete).
    In case of a cascaded setup it can also contain Origin messages.
    The origin message indicates that the transaction originated on different replication node.
    Since a replication node in the scope of logical replication protocol can be pretty much anything,
    the only identifier is the origin name.
    It's downstream's responsibility to handle this as needed (if needed).
    The Origin message is always sent before any DML messages in the transaction.
    """
    source_handler = SourceDBHandler(dsn=dsn)
    database = source_handler.conn.get_dsn_parameters()["dbname"]
    table_schemas = Tables()
    pg_types = PgTypes()
    # begin and commit messages
    transaction_metadata = None

    for msg in message_stream:
        decoded_msg = decode_message(msg.payload)
        if decoded_msg.byte1 == "R":
            relation_id = decoded_msg.relation_id
            column_definitions = []
            for column in decoded_msg.columns:
                pg_types[column.type_id] = source_handler.fetch_column_type(
                    type_id=column.type_id, atttypmod=column.atttypmod
                )
                # pre-compute schema of the table for attaching to messages
                is_optional = source_handler.fetch_if_column_is_optional(
                    table_schema=decoded_msg.namespace, table_name=decoded_msg.relation_name, column_name=column.name
                )
                column_definitions.append(
                    ColumnDefinition(
                        name=column.name,
                        part_of_pkey=column.part_of_pkey,
                        type_id=column.type_id,
                        type_name=pg_types[column.type_id],
                        optional=is_optional,
                    )
                )
            schema_mapping_args = {
                c.name: (convert_pg_type_to_py_type(c.type_name), None if c.optional else ...)
                for c in column_definitions
            }
            table_schemas[relation_id] = TableSchema(
                db=database,
                schema_name=decoded_msg.namespace,
                table=decoded_msg.relation_name,
                column_definitions=column_definitions,
                relation_id=relation_id,
                model=pydantic.create_model(f"DynamicSchemaModel_{relation_id}", **schema_mapping_args),
            )

        elif decoded_msg.byte1 == "B":
            # overwrite transaction_metadata, once we reach next BEGIN the previous TX is processed
            transaction_metadata = Transaction(
                tx_id=decoded_msg.tx_xid, begin_lsn=decoded_msg.lsn, commit_ts=decoded_msg.commit_ts
            )
        elif decoded_msg.byte1 in ("I", "U", "D"):
            before = None
            after = None
            if decoded_msg.byte1 in ("U", "D"):
                if decoded_msg.old_tuple:
                    before = map_tuple_to_dict(
                        tuple_data=decoded_msg.old_tuple,
                        relation=table_schemas[decoded_msg.relation_id],
                    )
            if decoded_msg.byte1 != "D":
                after = map_tuple_to_dict(
                    tuple_data=decoded_msg.new_tuple,
                    relation=table_schemas[decoded_msg.relation_id],
                )
            payload = ChangeEvent(
                op=decoded_msg.byte1,
                message_id=msg.message_id,
                lsn=msg.data_start,
                transaction=transaction_metadata,
                table_schema=table_schemas[relation_id],
                before=table_schemas[relation_id].model(**before) if before else None,
                after=table_schemas[relation_id].model(**after) if after else None,
            )
            yield payload

        elif decoded_msg.byte1 == "T":
            for relation_id in decoded_msg.relation_ids:
                payload = ChangeEvent(
                    op=decoded_msg.byte1,
                    message_id=msg.message_id,
                    lsn=msg.data_start,
                    transaction=transaction_metadata,
                    table_schema=table_schemas[relation_id],
                    before=None,
                    after=None,
                )
                yield payload

        elif decoded_msg.byte1 == "C":
            # transaction is now processed
            transaction_metadata = None
