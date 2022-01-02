import logging
import multiprocessing
import time
import uuid
from collections import OrderedDict
from multiprocessing.connection import Connection
from multiprocessing.context import Process
from typing import Dict, Generator

import psycopg2
import psycopg2.extras

from pypgoutput.decoders import TupleData, decode_message
from pypgoutput.utils import SourceDBHandler

logger = logging.getLogger(__name__)


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

    def __init__(self, db_name: str, db_dsn: str, slot_name: str) -> None:
        self.db_name = db_name
        self.db_dsn = db_dsn
        self.slot_name = slot_name
        self.extractor = None
        self.setup()

    def setup(self):
        self.pipe_out_conn, self.pipe_in_conn = multiprocessing.Pipe(duplex=True)
        self.extractor = ExtractRaw(pipe_conn=self.pipe_in_conn, db_dsn=self.db_dsn, slot_name=self.slot_name)
        self.extractor.start()
        # TODO: make some aspect of this output configurable
        self.raw_msgs = self.read_raw_extracted()
        self.transformed_msgs = transform_raw_to_change_event(
            message_stream=self.raw_msgs,
            source_dsn=self.db_dsn,
            source_db_name=self.db_name,
        )

    def stop(self):
        """Stop reader process and close the pipe"""
        self.extractor.terminate()
        time.sleep(0.1)
        self.extractor.close()
        self.pipe_out_conn.close()
        self.pipe_in_conn.close()

    def read_raw_extracted(self):
        """yield messages from the pipe populated by extractor"""
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
                self.pipe_out_conn.send({"id": item["message_id"]})
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

    def __init__(self, pipe_conn: Connection, db_dsn: str, slot_name: str):
        Process.__init__(self)
        self.pipe_conn = pipe_conn
        self.db_dsn = db_dsn
        self.slot_name = slot_name

    def run(self) -> None:
        conn = psycopg2.connect(self.db_dsn, connection_factory=psycopg2.extras.LogicalReplicationConnection)
        cur = conn.cursor()
        # TODO fix hardcoded "pub" publication name
        replication_options = {"publication_names": "pub", "proto_version": "1"}
        try:
            cur.start_replication(slot_name=self.slot_name, decode=False, options=replication_options)
        except psycopg2.ProgrammingError:
            cur.create_replication_slot(self.slot_name, output_plugin="pgoutput")
            cur.start_replication(slot_name=self.slot_name, decode=False, options=replication_options)
        try:
            logger.info(f"Starting replication from slot: {self.slot_name}")
            cur.consume_stream(self.msg_consumer)
        except Exception as err:
            logger.error(f"Error consuming stream from slot: '{self.slot_name}'. {err}")
            cur.close()
            conn.close()

    def msg_consumer(self, msg):
        message_id = uuid.uuid4()
        # TODO create a type for this dictionary with defined fields
        message = {
            "message_id": message_id,
            "data_start": msg.data_start,
            "payload": msg.payload,
            "send_time": msg.send_time,
            "data_size": msg.data_size,
            "wal_end": msg.wal_end,
        }
        self.pipe_conn.send(message)

        result = self.pipe_conn.recv()  # how would this wait until processing is done?
        if result["id"] == message_id:
            msg.cursor.send_feedback(flush_lsn=msg.data_start)
            logger.debug(f"Flushed message: '{str(message_id)}'")
        else:
            logger.warning(f"Could not confirm message: {str(message_id)}. Did not flush at {msg.data_start}")


def prepare_base_change_event(
    op: str,
    relation_id: int,
    table_metadata: dict,
    transaction_metadata: dict,
    lsn: int,
    message_id: uuid.uuid4,
) -> dict:
    """Construct payload dict of change event for I, U, D, T events
    TODO: define type for this that can be serialised to desired format. E.g. JSON
    """
    payload = {}
    payload["op"] = op
    payload["id"] = message_id
    payload["source"] = {
        "relation_id": relation_id,
        "db": table_metadata["db"],
        "schema": table_metadata["schema"],
        "table": table_metadata["table"],
        "tx_id": transaction_metadata["tx_xid"],
        "begin_lsn": transaction_metadata["begin_lsn"],
        "commit_ts": transaction_metadata["commit_ts"],
        "lsn": lsn,
    }
    payload["table_schema"] = table_metadata["column_definitions"]
    return payload


def map_tuple_to_dict(tuple_data: TupleData, relation: dict) -> OrderedDict:
    """Convert tuple data to an OrderedDict with keys from relation mapped in order to tuple data"""
    output = OrderedDict()
    for idx, col in enumerate(tuple_data.column_data):
        column_name = relation["column_definitions"][idx]["name"]
        output[column_name] = col.col_data
    return output


def transform_raw_to_change_event(
    message_stream: Generator[Dict, None, None], source_dsn: str, source_db_name: str
) -> Generator[Dict, None, None]:
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
    source_handler = SourceDBHandler(dsn=source_dsn)
    source_handler.connect()
    table_schemas = {}
    pg_types = {}
    # begin and commit messages
    transaction_metadata = {}

    for msg in message_stream:
        decoded_msg = decode_message(msg["payload"])
        if decoded_msg.byte1 == "R":
            relation_id = decoded_msg.relation_id
            table_schemas[relation_id] = {
                "column_definitions": [],
                "db": source_db_name,
                "schema": decoded_msg.namespace,
                "table": decoded_msg.relation_name,
            }
            for column in decoded_msg.columns:
                pg_types[column.type_id] = source_handler.fetch_column_type(
                    type_id=column.type_id, atttypmod=column.atttypmod
                )
                # pre-compute schema for attaching to messages
                table_schemas[relation_id]["column_definitions"].append(
                    {
                        "name": column.name,
                        "part_of_pkey": column.part_of_pkey,
                        "type_id": column.type_id,
                        "type": pg_types[column.type_id],
                        "optional": source_handler.fetch_if_column_is_optional(
                            table_schema=decoded_msg.namespace,
                            table_name=decoded_msg.relation_name,
                            column_name=column.name,
                        ),
                    }
                )
        elif decoded_msg.byte1 == "B":
            # overwrite transaction_metadata, once we reach next BEGIN the previous TX is processed
            transaction_metadata = {
                "tx_xid": decoded_msg.tx_xid,
                "begin_lsn": decoded_msg.lsn,
                "commit_ts": decoded_msg.commit_ts,
                "processed": False,
            }
        elif decoded_msg.byte1 in ("I", "U", "D"):
            payload = prepare_base_change_event(
                op=decoded_msg.byte1,
                relation_id=decoded_msg.relation_id,
                table_metadata=table_schemas[relation_id],
                transaction_metadata=transaction_metadata,
                lsn=msg["data_start"],
                message_id=msg["message_id"],
            )
            before = None
            if decoded_msg.byte1 in ("U", "D"):
                if decoded_msg.old_tuple:
                    before = map_tuple_to_dict(
                        tuple_data=decoded_msg.old_tuple,
                        relation=table_schemas[decoded_msg.relation_id],
                    )
            after = None
            if decoded_msg.byte1 != "D":
                after = map_tuple_to_dict(
                    tuple_data=decoded_msg.new_tuple,
                    relation=table_schemas[decoded_msg.relation_id],
                )

            payload["before"] = before
            payload["after"] = after
            yield payload

        elif decoded_msg.byte1 == "T":
            for rel_id in decoded_msg.relation_ids:
                payload = prepare_base_change_event(
                    op=decoded_msg.byte1,
                    relation_id=rel_id,
                    table_metadata=table_schemas[rel_id],
                    transaction_metadata=transaction_metadata,
                    lsn=msg["data_start"],
                    message_id=msg["message_id"],
                )
                payload["before"] = None
                payload["after"] = None
                yield payload

        elif decoded_msg.byte1 == "C":
            transaction_metadata["processed"] = True
