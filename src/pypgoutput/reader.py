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

import pypgoutput.decoders as decoders
from pypgoutput.utils import SourceDBHandler

logger = logging.getLogger(__name__)


class PgoutputError(Exception):
    pass


class ReplicationMessage(pydantic.BaseModel):
    message_id: pydantic.UUID4
    data_start: int
    payload: bytes
    send_time: datetime
    data_size: int
    wal_end: int


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
    before: typing.Optional[typing.Dict[str, typing.Any]]  # depends on the source table
    after: typing.Optional[typing.Dict[str, typing.Any]]


def map_tuple_to_dict(tuple_data: decoders.TupleData, relation: TableSchema) -> typing.OrderedDict[str, typing.Any]:
    """Convert tuple data to an OrderedDict with keys from relation mapped in order to tuple data"""
    output: typing.OrderedDict[str, typing.Any] = OrderedDict()
    for idx, col in enumerate(tuple_data.column_data):
        column_name = relation.column_definitions[idx].name
        output[column_name] = col.col_data
    return output


# eventually could do type conversion using the new pattern
# def convert_pg_type_to_py_type(pg_type_name: str) -> type:
#     """try out PEP-636 https://docs.python.org/3/whatsnew/3.10.html#pep-634-structural-pattern-matching"""
#     match pg_type_name:
#         case "bigint" | "integer" | "smallint":
#             return int
#         case "timestamp with time zone" | "timestamp without time zone":
#             return datetime
#         # json not tested yet
#         case "json" | "jsonb":
#             return dict
#         case _:
#             return str


def convert_pg_type_to_py_type(pg_type_name: str) -> type:
    if pg_type_name == "bigint" or pg_type_name == "integer" or pg_type_name == "smallint":
        return int
    elif pg_type_name == "timestamp with time zone" or pg_type_name == "timestamp without time zone":
        return datetime
        # json not tested yet
    elif pg_type_name == "json" or pg_type_name == "jsonb":
        return pydantic.Json
    elif pg_type_name[:7] == "numeric":
        return float
    else:
        return str


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

    def __init__(
        self,
        publication_name: str,
        slot_name: str,
        dsn: typing.Optional[str] = None,
        **kwargs: typing.Optional[str],
    ) -> None:
        self.dsn = psycopg2.extensions.make_dsn(dsn=dsn, **kwargs)
        self.publication_name = publication_name
        self.slot_name = slot_name

        # transform data containers
        self.table_schemas: typing.Dict[int, TableSchema] = dict()  # map relid to table schema
        # value pydantic model applied to before/after tuple
        self.table_models: typing.Dict[int, typing.Type[pydantic.BaseModel]] = dict()
        self.pg_types: typing.Dict[int, str] = dict()  # map type oid to readable name

        self.setup()

    def setup(self) -> None:
        self.pipe_out_conn, self.pipe_in_conn = multiprocessing.Pipe(duplex=True)
        self.extractor = ExtractRaw(
            pipe_conn=self.pipe_in_conn, dsn=self.dsn, publication_name=self.publication_name, slot_name=self.slot_name
        )
        self.extractor.connect()
        self.extractor.start()
        self.source_db_handler = SourceDBHandler(dsn=self.dsn)
        self.database = self.source_db_handler.conn.get_dsn_parameters()["dbname"]
        # TODO: make some aspect of this output configurable, raw msg return
        self.raw_msgs = self.read_raw_extracted()
        self.transformed_msgs = self.transform_raw(message_stream=self.raw_msgs)

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

    def transform_raw(
        self, message_stream: typing.Generator[ReplicationMessage, None, None]
    ) -> typing.Generator[ChangeEvent, None, None]:
        for msg in message_stream:
            message_type = (msg.payload[:1]).decode("utf-8")
            if message_type == "R":
                self.process_relation(message=msg)
            elif message_type == "B":
                transaction_metadata = self.process_begin(message=msg)
            # message processors below will throw an error if transaction_metadata doesn't exist
            elif message_type == "I":
                yield self.process_insert(message=msg, transaction=transaction_metadata)
            elif message_type == "U":
                yield self.process_update(message=msg, transaction=transaction_metadata)
            elif message_type == "D":
                yield self.process_delete(message=msg, transaction=transaction_metadata)
            elif message_type == "T":
                yield from self.process_truncate(message=msg, transaction=transaction_metadata)
            elif message_type == "C":
                del transaction_metadata  # null out this value after commit

    def process_relation(self, message: ReplicationMessage) -> None:
        relation_msg: decoders.Relation = decoders.Relation(message.payload)
        relation_id = relation_msg.relation_id
        column_definitions: typing.List[ColumnDefinition] = []
        for column in relation_msg.columns:
            self.pg_types[column.type_id] = self.source_db_handler.fetch_column_type(
                type_id=column.type_id, atttypmod=column.atttypmod
            )
            # pre-compute schema of the table for attaching to messages
            is_optional = self.source_db_handler.fetch_if_column_is_optional(
                table_schema=relation_msg.namespace, table_name=relation_msg.relation_name, column_name=column.name
            )
            column_definitions.append(
                ColumnDefinition(
                    name=column.name,
                    part_of_pkey=column.part_of_pkey,
                    type_id=column.type_id,
                    type_name=self.pg_types[column.type_id],
                    optional=is_optional,
                )
            )
        # in pydantic Ellipsis (...) indicates a field is required
        # this should be the type below but it doesn't work as the kwargs for create_model with mppy
        # schema_mapping_args: typing.Dict[str, typing.Tuple[type, typing.Optional[EllipsisType]]] = {
        schema_mapping_args: typing.Dict[str, typing.Any] = {
            c.name: (convert_pg_type_to_py_type(c.type_name), None if c.optional else ...) for c in column_definitions
        }
        self.table_models[relation_id] = pydantic.create_model(
            f"DynamicSchemaModel_{relation_id}", **schema_mapping_args
        )
        self.table_schemas[relation_id] = TableSchema(
            db=self.database,
            schema_name=relation_msg.namespace,
            table=relation_msg.relation_name,
            column_definitions=column_definitions,
            relation_id=relation_id,
        )

    def process_begin(self, message: ReplicationMessage) -> Transaction:
        begin_msg: decoders.Begin = decoders.Begin(message.payload)
        return Transaction(tx_id=begin_msg.tx_xid, begin_lsn=begin_msg.lsn, commit_ts=begin_msg.commit_ts)

    def process_insert(self, message: ReplicationMessage, transaction: Transaction) -> ChangeEvent:
        decoded_msg: decoders.Insert = decoders.Insert(message.payload)
        relation_id: int = decoded_msg.relation_id
        after = map_tuple_to_dict(tuple_data=decoded_msg.new_tuple, relation=self.table_schemas[relation_id])
        try:
            return ChangeEvent(
                op=decoded_msg.byte1,
                message_id=message.message_id,
                lsn=message.data_start,
                transaction=transaction,
                table_schema=self.table_schemas[relation_id],
                before=None,
                after=self.table_models[relation_id](**after),
            )
        except Exception as exc:
            raise PgoutputError(f"Error creating ChangeEvent: {exc}")

    def process_update(self, message: ReplicationMessage, transaction: Transaction) -> ChangeEvent:
        decoded_msg: decoders.Update = decoders.Update(message.payload)
        relation_id: int = decoded_msg.relation_id
        if decoded_msg.old_tuple:
            before = map_tuple_to_dict(tuple_data=decoded_msg.old_tuple, relation=self.table_schemas[relation_id])
        after = map_tuple_to_dict(tuple_data=decoded_msg.new_tuple, relation=self.table_schemas[relation_id])
        try:
            return ChangeEvent(
                op=decoded_msg.byte1,
                message_id=message.message_id,
                lsn=message.data_start,
                transaction=transaction,
                table_schema=self.table_schemas[relation_id],
                before=self.table_models[relation_id](**before) if decoded_msg.old_tuple else None,
                after=self.table_models[relation_id](**after),
            )
        except Exception as exc:
            raise PgoutputError(f"Error creating ChangeEvent: {exc}")

    def process_delete(self, message: ReplicationMessage, transaction: Transaction) -> ChangeEvent:
        decoded_msg: decoders.Delete = decoders.Delete(message.payload)
        relation_id: int = decoded_msg.relation_id
        before = map_tuple_to_dict(tuple_data=decoded_msg.old_tuple, relation=self.table_schemas[relation_id])
        try:
            return ChangeEvent(
                op=decoded_msg.byte1,
                message_id=message.message_id,
                lsn=message.data_start,
                transaction=transaction,
                table_schema=self.table_schemas[relation_id],
                before=self.table_models[relation_id](**before),
                after=None,
            )
        except Exception as exc:
            raise PgoutputError(f"Error creating ChangeEvent: {exc}")

    def process_truncate(
        self, message: ReplicationMessage, transaction: Transaction
    ) -> typing.Generator[ChangeEvent, None, None]:
        decoded_msg: decoders.Truncate = decoders.Truncate(message.payload)
        for relation_id in decoded_msg.relation_ids:
            try:
                yield ChangeEvent(
                    op=decoded_msg.byte1,
                    message_id=message.message_id,
                    lsn=message.data_start,
                    transaction=transaction,
                    table_schema=self.table_schemas[relation_id],
                    before=None,
                    after=None,
                )
            except Exception as exc:
                raise PgoutputError(f"Error creating ChangeEvent: {exc}")

    # how to put a better type hint?
    def __iter__(self) -> typing.Any:
        return self

    def __next__(self) -> ChangeEvent:
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

    def __init__(self, dsn: str, publication_name: str, slot_name: str, pipe_conn: Connection) -> None:
        Process.__init__(self)
        self.dsn = dsn
        self.publication_name = publication_name
        self.slot_name = slot_name
        self.pipe_conn = pipe_conn

    def connect(self) -> None:
        self.conn = psycopg2.extras.LogicalReplicationConnection(self.dsn)
        self.cur = psycopg2.extras.ReplicationCursor(self.conn)

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

    def msg_consumer(self, msg: psycopg2.extras.ReplicationMessage) -> None:
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
