import os
import sys
import psycopg2
from psycopg2.extras import LogicalReplicationConnection

SLOT_NAME = "my_slot"
TEST_DIR = "test_output_files"
LOCAL_DSN = "host=localhost user=test port=5432 dbname=test password=test"


def write_raw_test_file(lsn, message_type, payload, parent_dir=TEST_DIR):
    file_name = f"{lsn}_{message_type.lower()}"
    with open(f"{parent_dir}/{file_name}", 'wb') as f:
        f.write(payload)
    with open(f"{parent_dir}/manifest", "a") as manifest:
        manifest.write(file_name + "\n")


class LogicalStreamConsumer(object):
    def __call__(self, msg):
        first_byte = (msg.payload[:1]).decode('utf-8')
        write_raw_test_file(msg.data_start, first_byte, msg.payload)
        # TODO check why you flush data start (probably a checkpoint?)
        msg.cursor.send_feedback(flush_lsn=msg.data_start) 


def produce_logical_replication_messages():
    conn = psycopg2.connect(LOCAL_DSN, connection_factory=psycopg2.extras.LogicalReplicationConnection)
    cur = conn.cursor()
    replication_options = {'publication_names': 'pub', 'proto_version': '1'}

    try:
        cur.start_replication(slot_name=SLOT_NAME, decode=False, options=replication_options)
    except psycopg2.ProgrammingError:
        cur.create_replication_slot(SLOT_NAME, output_plugin='pgoutput')
        cur.start_replication(slot_name=SLOT_NAME, decode=False, options=replication_options)

    consumer = LogicalStreamConsumer()
    print("Starting streaming, press Control-C to end...", file=sys.stderr)
    try:
        cur.consume_stream(consumer)
    except KeyboardInterrupt:
        cur.close()
        conn.close()
    print(f"The slot '{SLOT_NAME}' still exists. Drop it with "
          f"SELECT pg_drop_replication_slot('{SLOT_NAME}'); if no longer needed.",
          file=sys.stderr)
    print("WARNING: Transaction logs will accumulate in pg_xlog until the slot is dropped.", file=sys.stderr)

if __name__ == "__main__":
    os.makedirs(TEST_DIR, exist_ok=True)
    produce_logical_replication_messages()
