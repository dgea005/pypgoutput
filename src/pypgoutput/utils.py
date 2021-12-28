import logging
from datetime import datetime
from typing import List

import psycopg2
import psycopg2.extras

logger = logging.getLogger(__name__)


class SourceDBHandler:
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
        """Get formatted data type name"""
        query = f"SELECT format_type({type_id}, {atttypmod}) AS data_type"
        result = self.fetchone(query=query)
        return result["data_type"]

    def fetch_if_column_is_optional(self, table_schema: str, table_name: str, column_name: str) -> bool:
        """Check if a column is optional"""
        query = f"""SELECT attnotnull
            FROM pg_attribute
            WHERE attrelid = '{table_schema}.{table_name}'::regclass
            AND attname = '{column_name}';
        """
        result = self.fetchone(query=query)
        # attnotnull returns if column has not null constraint, we want to flip it
        return False if result["attnotnull"] else True

    def close(self):
        self.cur.close()
        self.conn.close()


def convert_string_to_type(data_type, value):
    """eventually cast to text values to python types (for further serialisation) but not used yet"""
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
