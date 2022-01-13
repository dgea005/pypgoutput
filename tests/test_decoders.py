from datetime import datetime, timezone

import pytest

from pypgoutput import ColumnData, ColumnType, TupleData, decoders


def test_relation_message():
    message = b"R\x00\x00@\x01public\x00test_table\x00d\x00\x02\x01id\x00\x00\x00\x00\x17\xff\xff\xff\xff\x00created\x00\x00\x00\x04\xa0\xff\xff\xff\xff"
    decoded_msg = decoders.Relation(message)
    assert decoded_msg.byte1 == "R"
    assert decoded_msg.relation_id == 16385
    assert decoded_msg.namespace == "public"
    assert decoded_msg.relation_name == "test_table"
    assert decoded_msg.replica_identity_setting == "d"
    assert decoded_msg.n_columns == 2
    assert type(decoded_msg.columns) == list
    assert len(decoded_msg.columns) == 2
    # (pk flag, col name, pg_type oid, atttypmod)
    assert decoded_msg.columns[0] == ColumnType(part_of_pkey=1, name="id", type_id=23, atttypmod=-1)  # is pk
    assert decoded_msg.columns[1] == ColumnType(part_of_pkey=0, name="created", type_id=1184, atttypmod=-1)

    # test exceptions
    # wrong first byte
    message = b"B\x00\x00@\x01public\x00test_table\x00d\x00\x02\x01id\x00\x00\x00\x00\x17\xff\xff\xff\xff\x00created\x00\x00\x00\x04\xa0\xff\xff\xff\xff"
    with pytest.raises(ValueError):
        decoded_msg = decoders.Relation(message)


def test_begin_message():
    message = b"B\x00\x00\x00\x00\x01f4\x98\x00\x02ck\xd8i\x8a1\x00\x00\x01\xeb"
    decoded_msg = decoders.Begin(message)
    assert decoded_msg.byte1 == "B"
    assert decoded_msg.lsn == 23475352
    assert decoded_msg.tx_xid == 491
    assert decoded_msg.commit_ts == datetime.strptime(
        "2021-04-20 20:13:16.867121+00:00".split("+")[0], "%Y-%m-%d %H:%M:%S.%f"
    ).replace(tzinfo=timezone.utc)

    # test exceptions
    # wrong first byte
    message = b"R\x00\x00\x00\x00\x01f4\x98\x00\x02ck\xd8i\x8a1\x00\x00\x01\xeb"
    with pytest.raises(ValueError):
        decoded_msg = decoders.Begin(message)


def test_insert_message():
    message = b"I\x00\x00@\x01N\x00\x02t\x00\x00\x00\x015t\x00\x00\x00\x162012-01-01 12:00:00+00"
    decoded_msg = decoders.Insert(message)
    assert decoded_msg.byte1 == "I"
    assert decoded_msg.relation_id == 16385
    assert decoded_msg.new_tuple_byte == "N"
    assert type(decoded_msg.new_tuple) == decoders.TupleData

    assert decoded_msg.new_tuple.n_columns == 2
    assert type(decoded_msg.new_tuple.column_data) == list

    # (col_type, col_data_length, col_data)
    assert decoded_msg.new_tuple.column_data[0] == ColumnData(col_data_category="t", col_data_length=1, col_data="5")
    assert decoded_msg.new_tuple.column_data[1] == ColumnData(
        col_data_category="t", col_data_length=22, col_data="2012-01-01 12:00:00+00"
    )

    # test exceptions
    # wrong first byte
    message = b"U\x00\x00@\x01N\x00\x02t\x00\x00\x00\x015t\x00\x00\x00\x162012-01-01 12:00:00+00"
    with pytest.raises(ValueError):
        decoded_msg = decoders.Insert(message)


def test_update_message():
    message = b"U\x00\x00@\x01N\x00\x02t\x00\x00\x00\x015t\x00\x00\x00\x162013-01-01 12:00:00+00"
    decoded_msg = decoders.Update(message)
    assert decoded_msg.byte1 == "U"
    assert decoded_msg.relation_id == 16385

    # test exceptions
    # wrong first byte
    message = b"I\x00\x00@\x01N\x00\x02t\x00\x00\x00\x015t\x00\x00\x00\x162013-01-01 12:00:00+00"
    with pytest.raises(ValueError):
        decoded_msg = decoders.Update(message)


def test_delete_message():
    message = b"D\x00\x00@\x01K\x00\x02t\x00\x00\x00\x014n"
    decoded_msg = decoders.Delete(message)
    assert decoded_msg.byte1 == "D"
    assert decoded_msg.relation_id == 16385

    # test exceptions
    # wrong first byte
    message = b"I\x00\x00@\x01K\x00\x02t\x00\x00\x00\x014n"
    with pytest.raises(ValueError):
        decoded_msg = decoders.Delete(message)


def test_commit_message():
    message = b"C\x00\x00\x00\x00\x00\x01f4\x98\x00\x00\x00\x00\x01f4\xc8\x00\x02cl\x83\x8f\xd2\xa1"
    decoded_msg = decoders.Commit(message)
    assert decoded_msg.byte1 == "C"
    assert decoded_msg.lsn_commit == 23475352
    assert decoded_msg.lsn == 23475400
    assert decoded_msg.commit_ts == datetime.strptime(
        "2021-04-20 21:01:08.279969+00:00".split("+")[0], "%Y-%m-%d %H:%M:%S.%f"
    ).replace(tzinfo=timezone.utc)

    # test exceptions
    # wrong first byte
    message = b"R\x00\x00\x00\x00\x00\x01f4\x98\x00\x00\x00\x00\x01f4\xc8\x00\x02cl\x83\x8f\xd2\xa1"
    with pytest.raises(ValueError):
        decoded_msg = decoders.Commit(message)


def test_truncate_message():
    message = b"T\x00\x00\x00\x01\x00\x00\x00@\x01"
    decoded_msg = decoders.Truncate(message)
    assert decoded_msg.byte1 == "T"
    assert decoded_msg.number_of_relations == 1
    assert decoded_msg.relation_ids == [16385]
    assert decoded_msg.option_bits == 0

    # test exceptions
    # wrong first byte
    message = b"D\x00\x00\x00\x01\x00\x00\x00@\x01"
    with pytest.raises(ValueError):
        decoded_msg = decoders.Truncate(message)


def test_tuple_data():
    test_tuple = TupleData(
        n_columns=1, column_data=[ColumnData(col_data_length=1, col_data="1", col_data_category="t")]
    )
    assert test_tuple.n_columns == 1
    assert test_tuple.column_data[0].col_data_category == "t"
    assert test_tuple.column_data[0].col_data_length == 1
    assert test_tuple.column_data[0].col_data == "1"


# def test_decode_func():
#     message = b"Z\x00\x00\x00\x01\x00\x00\x00@\x01"
#     decoded = pypgoutput.decode_message(message)
#     assert decoded is None
