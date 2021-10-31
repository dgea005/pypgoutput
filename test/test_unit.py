from datetime import datetime, timezone
from pypgoutput import decoders, TupleData


def test_relation_message():
    message = b'R\x00\x00@\x01public\x00test_table\x00d\x00\x02\x01id\x00\x00\x00\x00\x17\xff\xff\xff\xff\x00created\x00\x00\x00\x04\xa0\xff\xff\xff\xff'
    loaded_msg = decoders.decode_message(message)
    assert loaded_msg.byte1 == "R"
    assert loaded_msg.relation_id == 16385 
    assert loaded_msg.namespace == "public"
    assert loaded_msg.relation_name == "test_table"
    assert loaded_msg.replica_identity_setting == "d"
    assert loaded_msg.n_columns == 2
    assert type(loaded_msg.columns) == list
    assert len(loaded_msg.columns) == 2
    # (pk flag, col name, pg_type oid, atttypmod)
    assert loaded_msg.columns[0] == (1, "id", 23, -1) # is pk
    assert loaded_msg.columns[1] == (0, "created", 1184, -1) 


def test_begin_message():
    message = b'B\x00\x00\x00\x00\x01f4\x98\x00\x02ck\xd8i\x8a1\x00\x00\x01\xeb'
    loaded_msg = decoders.decode_message(message)
    assert loaded_msg.byte1 == "B"
    assert loaded_msg.final_tx_lsn == 23475352
    assert loaded_msg.tx_xid == 491
    assert loaded_msg.commit_tx_ts == datetime.strptime("2021-04-20 22:13:16.867121+00:00".split('+')[0], "%Y-%m-%d %H:%M:%S.%f").astimezone(timezone.utc)

    
def test_insert_message():
    message = b'I\x00\x00@\x01N\x00\x02t\x00\x00\x00\x015t\x00\x00\x00\x162012-01-01 12:00:00+00'
    loaded_msg = decoders.decode_message(message)
    assert loaded_msg.byte1 == "I"
    assert loaded_msg.relation_id == 16385
    assert loaded_msg.new_tuple_byte == "N"
    assert type(loaded_msg.tuple_data) == TupleData
    
    assert loaded_msg.tuple_data.n_columns == 2
    assert type(loaded_msg.tuple_data.column_data) == list
    
    # (col_type, col_data_length, col_data)
    assert loaded_msg.tuple_data.column_data[0] == ("t", 1, '5')
    assert loaded_msg.tuple_data.column_data[1] == ('t', 22, '2012-01-01 12:00:00+00')

def test_update_message():
    message = b'U\x00\x00@\x01N\x00\x02t\x00\x00\x00\x015t\x00\x00\x00\x162013-01-01 12:00:00+00'
    loaded_msg = decoders.decode_message(message)
    assert loaded_msg.byte1 == "U"
    assert loaded_msg.relation_id == 16385

def test_delete_message():
    message =b'D\x00\x00@\x01K\x00\x02t\x00\x00\x00\x014n'
    loaded_msg = decoders.decode_message(message)
    assert loaded_msg.byte1 == "D"
    assert loaded_msg.relation_id == 16385

def test_commit_message():
    message = b'C\x00\x00\x00\x00\x00\x01f4\x98\x00\x00\x00\x00\x01f4\xc8\x00\x02cl\x83\x8f\xd2\xa1'
    loaded_msg = decoders.decode_message(message)
    assert loaded_msg.byte1 == "C"
    assert loaded_msg.lsn_commit == 23475352
    assert loaded_msg.final_tx_lsn == 23475400
    assert loaded_msg.commit_tx_ts == datetime.strptime("2021-04-20 23:01:08.279969+00:00".split('+')[0], "%Y-%m-%d %H:%M:%S.%f").astimezone(timezone.utc)

