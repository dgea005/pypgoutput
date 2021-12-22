"""
Pass data from LogicalStreamConsumer to processor with a pipe

Interface of multiprocessing connection
https://docs.python.org/3/library/multiprocessing.html#multiprocessing.connection.Connection
https://www.educative.io/edpresso/pipes-queues-and-lock-in-multiprocessing-in-python

"""
import json
import logging
from os import pipe
from uuid import uuid4
from time import sleep
from multiprocessing import Process, Pipe
from multiprocessing.connection import Connection
import psycopg2
import psycopg2.extras

import pypgoutput

logger = logging.getLogger(__name__)

class LogicalStreamConsumer(object):
    """ Class required for consuming logical replication messages from LogicalReplicationConnection
    """
    def __init__(self, pipe_conn: Connection):
        self.pipe_conn = pipe_conn

    def __call__(self, msg):
        message_id = uuid4()
        self.pipe_conn.send({
            "message_id":message_id,
            "data_start": msg.data_start,
            "payload": msg.payload,
            "send_time": msg.send_time,
            "data_size": msg.data_size,
            "wal_end": msg.wal_end
        })
        result = self.pipe_conn.recv()
        if result["id"] == message_id:
            msg.cursor.send_feedback(flush_lsn=msg.data_start)
            logger.debug(f"Flushed message: '{str(message_id)}'")
        else:
            logger.warning(f"Could not confirm message: {str(message_id)}. Did not flush at {msg.data_start}")


def run_logical_replication_consumer(pipe_conn: Connection, db_dsn: str, slot_name: str):
    """ Get replication messages via LogicalReplicationConnection
    """
    conn = psycopg2.connect(db_dsn, connection_factory=psycopg2.extras.LogicalReplicationConnection)
    cur = conn.cursor()
    replication_options = {'publication_names': 'pub', 'proto_version': '1'}
    try:
        cur.start_replication(slot_name=slot_name, decode=False, options=replication_options)
    except psycopg2.ProgrammingError:
        cur.create_replication_slot(slot_name, output_plugin='pgoutput')
        cur.start_replication(slot_name=slot_name, decode=False, options=replication_options)
    try:
        consumer = LogicalStreamConsumer(pipe_conn=pipe_conn)
    except Exception as err:
        logger.error(f"Error creating stream consumer from slot: '{slot_name}'. {err}")
    try:
        cur.consume_stream(consumer)
    except Exception as err:
        logger.error(f"Error consuming stream from slot: '{slot_name}'. {err}")
        cur.close()
        conn.close()


def process_messages(pipe: Connection):
    """ poll multiprocessing pipe
    """
    empty_count = 0
    iter_count = 0
    msg_count = 0

    # this exit condition is just for testing
    # while empty_count <= 3:
    while True:
        if not pipe.poll(timeout=0.5):
            empty_count += 1
        else:
            item = pipe.recv()
            msg_count += 1
            yield item
            pipe.send({"id": item["message_id"]})
        if iter_count % 50 == 0:
            logger.info(f"Loop of reader process at iteration: {iter_count}. Empty count: {empty_count}")
        iter_count += 1
    logger.info(f"Consumer finished processing data. Processed {msg_count} records")


def main(db_name: str, db_dsn: str, slot_name: str):
    try:
        out_p, in_p = Pipe(duplex=True)
        stream_consumer_proc = Process(target=run_logical_replication_consumer, name="logical_repl_consumer", args=(in_p, db_dsn, slot_name))
        stream_consumer_proc.start()
        message_stream = process_messages(pipe=out_p)
        for record in pypgoutput.convert_raw_to_change_events(
                            message_stream=message_stream,
                            source_db_name=db_name,
                            source_dsn=db_dsn):
            logger.info(json.dumps(record, indent=2, default=str))
            outfile = open("output.txt",'a')
            outfile.write(json.dumps(record, default=str))
            outfile.write("\n")
            outfile.close()
        stream_consumer_proc.terminate()
        sleep(0.1)
        if stream_consumer_proc.is_alive():
            logger.info("producer proc is still alive")
        stream_consumer_proc.close()
        out_p.close()
        in_p.close()
        logger.info("main process completed")
    except Exception as err:
        logger.error(f"Error running proc: {err}", exc_info=True)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, format='%(relativeCreated)6d %(processName)s %(message)s')
    SLOT_NAME = "my_slot"
    DB_NAME = "test"
    LOCAL_DSN = f"host=localhost user=test port=5432 dbname={DB_NAME} password=test"
    main(db_name=DB_NAME, db_dsn=LOCAL_DSN, slot_name=SLOT_NAME)
