from pypgoutput import decoders
from pypgoutput import ColumnData

TEST_DIR = "test_output_files"

# files have been produced by get_replication_records.py
# file name schem: {lsn}_{message_type.lower()}
with open(f"{TEST_DIR}/manifest", "r") as f:
    test_files = (f.read()).split("\n")[:-1]


def test_decode_all_messages():
    # test that all the files above can be decoded
    for test_file in sorted(test_files):
        with open(f"{TEST_DIR}/{test_file}", 'rb') as f:
            test_message = f.read()
        message = decoders.decode_message(test_message)


def test_decoded_message_contents():
    """
    BEGIN;
    INSERT INTO test_table (id, created) VALUES (4, '2011-01-01 12:00:00');
    INSERT INTO test_table (id, created) VALUES (5, '2012-01-01 12:00:00');
    INSERT INTO test_table (created, id) VALUES ('2014-01-01 12:00:00', 6);
    UPDATE test_table set created = '2013-01-01 12:00:00' WHERE id = 5 ;
    DELETE FROM test_table where id = 4;
    COMMIT;
    """

    # get relation message for the id
    with open(f"{TEST_DIR}/0_r", "rb") as f:
        relation_message = decoders.decode_message(f.read())
    
    assert relation_message.namespace == 'public'
    assert relation_message.relation_name == 'test_table'

    begin_file_name = min([x for x in test_files if x[-1] == "b" ])
    with open(f"{TEST_DIR}/{begin_file_name}", "rb") as f:
        begin_message = decoders.decode_message(f.read())
        assert begin_message.byte1 == "B"

    expected = [
        {"type": "I", "id": "4", "created": "2011-01-01 12:00:00+00"},
        {"type": "I", "id": "5", "created": "2012-01-01 12:00:00+00"},
        {"type": "I", "id": "6", "created": "2014-01-01 12:00:00+00"},
        {"type": "U", "id": "5", "created": "2013-01-01 12:00:00+00"},
        {"type": "D", "id": "4", "created": "2011-01-01 12:00:00+00"},
    ]
    for idx, raw_msg in enumerate(sorted(t for t in test_files if t[-1] in ["i", "u", "d"])):
        print(f"reading message: {raw_msg}")
        with open(f"{TEST_DIR}/{raw_msg}", "rb") as f:
            decoded_msg = decoders.decode_message(f.read())
            decoded_msg.relation_id == relation_message.relation_id
            if decoded_msg.byte1 == "I":
                assert decoded_msg.new_tuple_byte == "N"
                assert decoded_msg.new_tuple.n_columns == 2
                assert decoded_msg.new_tuple.column_data[0] == ColumnData("t", len(expected[idx]["id"]), expected[idx]["id"])
                assert decoded_msg.new_tuple.column_data[1] == ColumnData("t", len(expected[idx]["created"]), expected[idx]["created"])
            elif decoded_msg.byte1 == "U":
                assert decoded_msg.new_tuple_byte == "N"
                assert decoded_msg.new_tuple.n_columns == 2
                assert decoded_msg.new_tuple.column_data[0] == ColumnData("t", len(expected[idx]["id"]), expected[idx]["id"])
                assert decoded_msg.new_tuple.column_data[1] == ColumnData("t", len(expected[idx]["created"]), expected[idx]["created"])
            elif decoded_msg.byte1 == "D":
                assert decoded_msg.message_type in ("K", "O")
                if decoded_msg.message_type == "K":
                    # Identifies the following TupleData submessage as a key. This field is present if the table in which the delete has happened uses an index as REPLICA IDENTITY.
                    assert decoded_msg.old_tuple.n_columns == 2
                    assert decoded_msg.old_tuple.column_data[0] == ColumnData("t", len(expected[idx]["id"]), expected[idx]["id"])
                    assert decoded_msg.old_tuple.column_data[1] == ColumnData("n", None, None)
                else: # == "O"
                    # Identifies the following TupleData message as a old tuple. This field is present if the table in which the delete has happened has REPLICA IDENTITY set to FULL.
                    pass
            
    commit_file_name = min([x for x in test_files if x[-1] == "c" ])
    with open(f"{TEST_DIR}/{commit_file_name}", "rb") as f:
        decoded_msg = decoders.decode_message(f.read())
        assert decoded_msg.byte1 == "C"
        assert decoded_msg.lsn_commit == begin_message.final_tx_lsn
        assert decoded_msg.commit_tx_ts == begin_message.commit_tx_ts
