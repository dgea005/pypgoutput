import sys
import psycopg2
from psycopg2.extras import LogicalReplicationConnection
import decoders

conn = psycopg2.connect(
    'host=localhost user=admin port=5432 dbname=test password=test',
    connection_factory=psycopg2.extras.LogicalReplicationConnection)
cur = conn.cursor()
replication_options = {
    'publication_names': 'pub',
    'proto_version': '1'}


try:
    cur.start_replication(
        slot_name='my_slot', decode=False,
        options=replication_options)
except psycopg2.ProgrammingError:
    cur.create_replication_slot('my_slot', output_plugin='pgoutput')
    cur.start_replication(
        slot_name='my_slot', decode=False,
        options=replication_options)


class LogicalStreamConsumer(object):
    def __call__(self, msg):
        #print(msg.payload)
    
        # TODO: add some kind of processing here
        first_byte = (msg.payload[:1]).decode('utf-8')

        if first_byte == 'B':
            output = decoders.Begin(msg.payload)
            print(output)
        elif first_byte == "C":
            output = decoders.Commit(msg.payload)
            print(output)
        elif first_byte == "R":
            #print(msg.payload)
            output = decoders.Relation(msg.payload)
            print(output)
        elif first_byte == "I":
            output = decoders.Insert(msg.payload)
            print(output)
        elif first_byte == "U":
            #print(msg.payload)
            output = decoders.Update(msg.payload)
            print(output)
        elif first_byte == 'D':
            output = decoders.Delete(msg.payload)
            print(output)
        else:
            print(msg.payload)

        msg.cursor.send_feedback(flush_lsn=msg.data_start)  # to check why you flush data start

        print(f"data start LSN : {msg.data_start}")


        

consumer = LogicalStreamConsumer()

print("Starting streaming, press Control-C to end...", file=sys.stderr)
try:
   cur.consume_stream(consumer)
except KeyboardInterrupt:
   cur.close()
   conn.close()
   print("The slot 'my_slot' still exists. Drop it with "
         "SELECT pg_drop_replication_slot('my_slot'); if no longer needed.",
         file=sys.stderr)
   print("WARNING: Transaction logs will accumulate in pg_xlog "
         "until the slot is dropped.", file=sys.stderr)
