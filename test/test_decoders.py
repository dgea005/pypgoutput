from pypgoutput import decoders

TEST_DIR = "files"

# files have been produced by get_replication_records.py
# file name schem: {lsn}_{message_type.lower()}
with open(f"{TEST_DIR}/manifest", "r") as f:
    test_files = (f.read()).split("\n")[:-1]


def test_can_decode_all_messages():
    # test that all the files above can be decoded
    for test_file in sorted(test_files):
        with open(f"{TEST_DIR}/{test_file}", 'rb') as f:
            test_message = f.read()
        message = decoders.decode_message(test_message)


def test_decoded_message_contents():
    """
    INSERT INTO test_table (id, created) VALUES (4, '2011-01-01 12:00:00');
    INSERT INTO test_table (id, created) VALUES (5, '2012-01-01 12:00:00');
    INSERT INTO test_table (created, id) VALUES ('2014-01-01 12:00:00', 6);
    UPDATE test_table set created = '2013-01-01 12:00:00' WHERE id = 5 ;
    DELETE FROM test_table where id = 4;
    """

    # GET RELATION_ID
    with open(f"{TEST_DIR}/0_r", "rb") as f:
        relation_message = decoders.decode_message(f.read())
    
    assert relation_message.namespace == 'public'
    assert relation_message.relation_name == 'test_table'

    test_file_name = min([x for x in test_files if x[-1] == "i" ])

    with open(f"{TEST_DIR}/{test_file_name}", "rb") as f:
        insert_message = decoders.decode_message(f.read())

    assert insert_message.byte1 == "I"
    assert insert_message.relation_id == relation_message.relation_id
    assert insert_message.new_tuple_byte == "N"
    
    assert insert_message.tuple_data.n_columns == 2
    assert insert_message.tuple_data.column_data[0] == ('t', 1, '4')
    assert insert_message.tuple_data.column_data[1] == ('t', 22, '2011-01-01 12:00:00+00')
