from typing import List

import psycopg2
import psycopg2.extras


class QueryError(Exception):
    pass


class ResourceError(Exception):
    pass


class SourceDBHandler:
    def __init__(self, dsn: str) -> None:
        self.dsn = dsn
        self.connect()

    def connect(self) -> None:
        self.conn: psycopg2.connection = psycopg2.connect(self.dsn)
        self.conn.autocommit = True

    def fetchone(self, query: str) -> psycopg2.extras.DictRow:
        try:
            cursor = psycopg2.extras.DictCursor(self.conn)
        except Exception as err:
            raise ResourceError("Could not get cursor") from err
        try:
            cursor.execute(query)
            result: psycopg2.extras.DictRow = cursor.fetchone()
            return result
        except Exception as err:
            self.conn.rollback()
            raise QueryError("Error running query") from err
        finally:
            cursor.close()

    def fetch(self, query: str) -> List[psycopg2.extras.DictRow]:
        try:
            cursor = psycopg2.extras.DictCursor(self.conn)
        except Exception as err:
            raise ResourceError("Could not get cursor") from err
        try:
            cursor.execute(query)
            result: List[psycopg2.extras.DictRow] = cursor.fetchall()
            return result
        except Exception as err:
            self.conn.rollback()
            raise QueryError("Error running query") from err
        finally:
            cursor.close()

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

    def close(self) -> None:
        self.conn.close()
