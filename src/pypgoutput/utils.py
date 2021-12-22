from collections import OrderedDict
from datetime import datetime
import logging
from typing import Dict, List, Generator
from uuid import uuid4
import psycopg2
import psycopg2.extras

from pypgoutput import decode_message, TupleData

logger = logging.getLogger(__name__)

class SourceDBHandler():
    def __init__(self, dsn):
        self.dsn = dsn

    def connect(self):
        self.conn = psycopg2.connect(self.dsn)
        self.conn.autocommit = True
        self.cur = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    def fetchone(self, query):
        try:
            self.cur.execute(query)
            result = self.cur.fetchone()
            return result
        except Exception as err:
            self.conn.rollback()
            raise Exception("Error running query") from err

    def fetch(self, query) -> List:
        try:
            self.cur.execute(query)
            result = self.cur.fetchall()
            return result
        except Exception as err:
            self.cur.rollback()
            raise Exception("Error running query") from err

    def fetch_column_type(self, type_id: int, atttypmod: int) -> str:
        """ Get formatted data type name
        """
        query = f"SELECT format_type({type_id}, {atttypmod}) AS data_type"
        result = self.fetchone(query=query)
        return result["data_type"]

    def fetch_if_column_is_optional(self, table_schema: str, table_name: str, column_name: str) -> bool:
        """ Check if a column is optional
        """
        query = f"""SELECT attnotnull
            FROM pg_attribute
            WHERE attrelid = '{table_schema}.{table_name}'::regclass
            AND attname = '{column_name}';
        """
        result = self.fetchone(query=query)
        # attnotnull returns if column has not null constraint, we want to flip it
        return False  if result["attnotnull"] else True

    def close(self):
        self.cur.close()
        self.conn.close()


def prepare_base_change_event(
        op: str,
        relation_id: int,
        table_metadata: dict,
        transaction_metadata: dict,
        lsn: int,
        message_id: uuid4
    ) -> dict:
    """ Construct payload dict of change event for I, U, D, T events
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
        "lsn": lsn
    }
    payload["table_schema"] = table_metadata["column_definitions"]
    return payload


def make_tuple_dict(tuple_data: TupleData, relation: dict) -> OrderedDict:
    """Convert tuple data to an OrderedDict with keys from relation mapped in order to tuple data"""
    output = OrderedDict()
    for idx, col in enumerate(tuple_data.column_data):
        column_name = relation["column_definitions"][idx]["name"]
        output[column_name] = col.col_data
    return output


def convert_raw_to_change_events(
        message_stream: Generator[Dict, None, None],
        source_dsn: str,
        source_db_name: str
    ) -> Generator[Dict, None, None]:
    """ Convert raw messages to change events

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
                "table": decoded_msg.relation_name
            }
            for column in decoded_msg.columns:
                pg_types[column.type_id] = source_handler.fetch_column_type(type_id=column.type_id, atttypmod=column.atttypmod)
                # pre-compute schema for attaching to messages
                table_schemas[relation_id]["column_definitions"].append({
                    "name": column.name,
                    "part_of_pkey": column.part_of_pkey,
                    "type_id": column.type_id,
                    "type": pg_types[column.type_id],
                    "optional": source_handler.fetch_if_column_is_optional(
                        table_schema=decoded_msg.namespace,
                        table_name=decoded_msg.relation_name,
                        column_name=column.name
                    )
                })
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
                message_id=msg["message_id"]
            )
            before = None
            if decoded_msg.byte1 in ("U", "D"):
                if decoded_msg.old_tuple:
                    before = make_tuple_dict(tuple_data=decoded_msg.old_tuple, relation=table_schemas[decoded_msg.relation_id])
            after = None
            if decoded_msg.byte1 != "D":
                after = make_tuple_dict(tuple_data=decoded_msg.new_tuple, relation=table_schemas[decoded_msg.relation_id])

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
                    message_id=msg["message_id"]
                )
                payload["before"] = None
                payload["after"] = None
                yield payload

        elif decoded_msg.byte1 == "C":
            transaction_metadata["processed"] = True


def convert_string_to_type(data_type, value):
    """ eventually cast to text values to python types
    """
    if data_type == "integer":
        output = int(value)
    elif data_type == "bigint":
        output = int(value)
    elif data_type == "timestamp with time zone":
        if value[-3:] == "+00":
            value += ":00"
        output = datetime.strptime(value, "%Y-%m-%d %H:%M:%S%z")
    else:

        raise Exception(f"type not recognised: {data_type}")
    return output

