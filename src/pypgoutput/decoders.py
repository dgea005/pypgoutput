from abc import ABC, abstractmethod
from datetime import datetime, timezone, timedelta



def convert_pg_ts(_ts_in_microseconds):
    ts = datetime(2000, 1, 1, 0, 0, 0, 0, tzinfo=timezone.utc) 
    return ts + timedelta(microseconds=_ts_in_microseconds)

def convert_bytes_to_int(_in_bytes):
    return int.from_bytes(_in_bytes, byteorder='big', signed=True)    

def convert_bytes_to_utf8(_in_bytes):
    return (_in_bytes).decode('utf-8')

def decode_unknown_length_string(_buffer, _position):
    """

    Returns end position and string
    """
    the_string = ""
    for i in range(128): # TODO could change to while true but don't expect long strings
        if _buffer[_position:_position+1] == b'\x00':
            _position += 1
            break
        try:
            character = convert_bytes_to_utf8(_buffer[_position:_position+1])
            the_string += character
        except Exception as e:
            print("could not decode string, assume end of string")
        finally:
            _position += 1
    return _position, the_string


def decode_message(_input_bytes):
    """Peak first byte and initialise the appropriate message object"""
    first_byte = (_input_bytes[:1]).decode('utf-8')
    if first_byte == 'B':
        output = Begin(_input_bytes)
    elif first_byte == "C":
        output = Commit(_input_bytes)
    elif first_byte == "R":
        output = Relation(_input_bytes)
    elif first_byte == "I":
        output = Insert(_input_bytes)
    elif first_byte == "U":
        output = Update(_input_bytes)
    elif first_byte == 'D':
        output = Delete(_input_bytes)
    elif first_byte == 'T':
        output = Truncate(_input_bytes)
    else:
        print(f"warning unrecognised message {_input_bytes}")
        output = None
    return output

class PgoutputMessage(ABC):

    def __init__(self, buffer):
        self.buffer = buffer
        self.byte1 = convert_bytes_to_utf8(self.buffer[0:1])
        self.decode_buffer()

    @abstractmethod
    def decode_buffer(self):
        pass

    @abstractmethod
    def __repr__(self):
        pass


class Begin(PgoutputMessage):
    """
    byte1 Byte1('B') Identifies the message as a begin message.
    final_tx_lsn Int64 The final LSN of the transaction.
    commit_tx_ts Int64 Commit timestamp of the transaction. The value is in number of microseconds since PostgreSQL epoch (2000-01-01).
    tx_xid Int32 Xid of the transaction.
    """

    def decode_buffer(self):
        if self.byte1 != 'B':
            raise Exception('first byte in buffer does not match Begin message')
        self.final_tx_lsn = convert_bytes_to_int(self.buffer[1:9])
        self.commit_tx_ts = convert_pg_ts( convert_bytes_to_int(self.buffer[9:17]) )
        self.tx_xid = convert_bytes_to_int(self.buffer[17:21])
        return self

    def __repr__(self):
        return f"BEGIN \n\tbyte1: '{self.byte1}', \n\tfinal_tx_lsn : {self.final_tx_lsn}, " \
               f"\n\tcommit_tx_ts {self.commit_tx_ts}, \n\ttx_xid : {self.tx_xid}"


class Commit(PgoutputMessage):
    """
    byte1: Byte1('C') Identifies the message as a commit message.
    flags: Int8 Flags; currently unused (must be 0).
    lsn_commit: Int64 The LSN of the commit.
    final_tx_lsn: Int64 The end LSN of the transaction.
    Int64 Commit timestamp of the transaction. The value is in number of microseconds since PostgreSQL epoch (2000-01-01).
    """

    def decode_buffer(self):
        if self.byte1 != 'C':
            raise Exception('first byte in buffer does not match Commit message')
        self.flags = convert_bytes_to_int(self.buffer[1:2])
        self.lsn_commit = convert_bytes_to_int(self.buffer[2:10])
        self.final_tx_lsn = convert_bytes_to_int(self.buffer[10:18])
        self.commit_tx_ts = convert_pg_ts(convert_bytes_to_int(self.buffer[18:26]))
        return self

    def __repr__(self):
        return f"COMMIT \n\tbyte1: {self.byte1}, \n\tflags {self.flags}, \n\tlsn_commit : {self.lsn_commit} " \
               f"\n\tfinal_tx_lsn : {self.final_tx_lsn}, \n\tcommit_tx_ts {self.commit_tx_ts}"     


class Origin:
    """
    Byte1('O') Identifies the message as an origin message.
    Int64  The LSN of the commit on the origin server.
    String Name of the origin.
    Note that there can be multiple Origin messages inside a single transaction.
    This seems to be what origin means: https://www.postgresql.org/docs/12/replication-origins.html
    """
    pass



class Relation(PgoutputMessage):
    """
    Byte1('R')  Identifies the message as a relation message.
    Int32 ID of the relation.
    String Namespace (empty string for pg_catalog).
    String Relation name.
    Int8 Replica identity setting for the relation (same as relreplident in pg_class).
        # select relreplident from pg_class where relname = 'test_table';
        # from reading the documentation and looking at the tables this is not int8 but a single character
    Int16 Number of columns.
    Next, the following message part appears for each column (except generated columns):
        Int8 Flags for the column. Currently can be either 0 for no flags or 1 which marks the column as part of the key.
        String Name of the column.
        Int32 ID of the column's data type.
        Int32 Type modifier of the column (atttypmod).
    """

    def decode_buffer(self):
        if self.byte1 != 'R':
            raise Exception('first byte in buffer does not match Relation message')
        self.relation_id = convert_bytes_to_int(self.buffer[1:5])
        position = 5
        position, self.namespace = decode_unknown_length_string(self.buffer, position)
        position, self.relation_name = decode_unknown_length_string(self.buffer, position)
        self.replica_identity_setting = convert_bytes_to_utf8(self.buffer[position:position+1])
        position += 1
        self.n_columns = convert_bytes_to_int(self.buffer[position:position+2])
        position += 2
        self.columns = list()

        for c in range(self.n_columns):
            part_of_pkey = convert_bytes_to_int(self.buffer[position:position+1])
            position += 1
            position, col_name = decode_unknown_length_string(self.buffer, position)
            data_type_id = convert_bytes_to_int(self.buffer[position:position+4])
            position += 4
            # TODO: check on use of signed / unsigned
            # check with select oid from pg_type where typname = <type>; timestamp == 1184, int4 = 23
            col_modifier = convert_bytes_to_int(self.buffer[position:position+4])
            position += 4            
            self.columns.append( (part_of_pkey, col_name, data_type_id, col_modifier, ))


    def __repr__(self):
        return f"RELATION \n\tbyte1 : '{self.byte1}', \n\trelation_id : {self.relation_id}" \
               f",\n\tnamespace/schema : '{self.namespace}',\n\trelation_name : '{self.relation_name}'" \
               f",\n\treplica_identity_setting : '{self.replica_identity_setting}',\n\tn_columns : {self.n_columns} " \
               f",\n\tcolumns : {self.columns}"

# renamed to PgType to not use "type" name
class PgType:
    """
    Byte1('Y') Identifies the message as a type message.
    Int32 ID of the data type.
    String Namespace (empty string for pg_catalog).
    String Name of the data type.
    """
    pass


class TupleData:
    """
    TupleData Int16  Number of columns.
    Next, one of the following submessages appears for each column (except generated columns):
            Byte1('n') Identifies the data as NULL value.
        Or
            Byte1('u') Identifies unchanged TOASTed value (the actual value is not sent).
        Or
            Byte1('t') Identifies the data as text formatted value.
            Int32 Length of the column value.
            Byten The value of the column, in text format. (A future release might support additional formats.) n is the above length.
    """
    def __init__(self, buffer):
        self.buffer = buffer
        self.pos = 0
        self.n_columns = None
        self.column_data = list()   # revisit but for now  column_data : [ ('n', ), ('n', ), ('t', 5, '12345', ), ('u', ), ]
        self.decode_buffer()

    def decode_buffer(self):
        self.n_columns = convert_bytes_to_int(self.buffer[0:2])
        self.pos = 2
        for c in range(self.n_columns):
            col_type = convert_bytes_to_utf8(self.buffer[self.pos:self.pos+1])
            self.pos += 1
            if col_type == 'n':
                # NULLs TODO: consider putting an actual None / NULL
                self.column_data.append( (col_type, ) )
            elif col_type == 'u':
                # TOASTed value (data not sent)
                self.column_data.append( (col_type, ))
            elif col_type == 't':
                # get column length
                col_data_length = convert_bytes_to_int(self.buffer[self.pos:self.pos+4])
                self.pos += 4
                col_data = convert_bytes_to_utf8(self.buffer[self.pos:self.pos+col_data_length])
                self.pos += col_data_length
                self.column_data.append( (col_type, col_data_length, col_data, ))
        return self

    def __repr__(self):
        return f"( n_columns : {self.n_columns}, data : {self.column_data})"

class Insert(PgoutputMessage):
    """
    Byte1('I')  Identifies the message as an insert message.
    Int32 ID of the relation corresponding to the ID in the relation message.
    Byte1('N') Identifies the following TupleData message as a new tuple.
    TupleData TupleData message part representing the contents of new tuple.
    """
    def decode_buffer(self):
        if self.byte1 != 'I':
            raise Exception(f"first byte in buffer does not match Insert message (expected 'I', got '{self.byte1}'")
        self.relation_id = convert_bytes_to_int(self.buffer[1:5])
        self.new_tuple_byte = convert_bytes_to_utf8(self.buffer[5:6])
        self.tuple_data = TupleData(self.buffer[6:])
        return self

    def __repr__(self):
        return f"INSERT \n\tbyte1: '{self.byte1}', \n\trelation_id : {self.relation_id} " \
               f"\n\tnew tuple byte: '{self.new_tuple_byte}', \n\ttuple_data : {self.tuple_data}"

class Update(PgoutputMessage):
    """
    Byte1('U')      Identifies the message as an update message.
    Int32           ID of the relation corresponding to the ID in the relation message.
    Byte1('K')      Identifies the following TupleData submessage as a key. This field is optional and is only present if the update changed data in any of the column(s) that are part of the REPLICA IDENTITY index.
    Byte1('O')      Identifies the following TupleData submessage as an old tuple. This field is optional and is only present if table in which the update happened has REPLICA IDENTITY set to FULL.
    TupleData       TupleData message part representing the contents of the old tuple or primary key. Only present if the previous 'O' or 'K' part is present.
    Byte1('N')      Identifies the following TupleData message as a new tuple.
    TupleData       TupleData message part representing the contents of a new tuple.

    The Update message may contain either a 'K' message part or an 'O' message part or neither of them, but never both of them.
    """
    def decode_buffer(self):
        self.optional_tuple_identifier = None
        self.new_tuple_identifier = None
        self.old_tuple = None
        if self.byte1 != 'U':
            raise Exception(f"first byte in buffer does not match Update message (expected 'U', got '{self.byte1}'")
        self.relation_id = convert_bytes_to_int(self.buffer[1:5])

        # i.e., K, O or N
        # TODO test update to PK, test update with REPLICA IDENTITY = FULL
        self.next_byte_identifier = convert_bytes_to_utf8(self.buffer[5:6])

        if self.next_byte_identifier == 'K' or self.next_byte_identifier == 'O':
            self.optional_tuple_identifier = self.next_byte_identifier
            self.old_tuple = TupleData(self.buffer[6:])
            buffer_pos = self.old_tuple.pos + 6
        else:
            buffer_pos = 5 # 5 because N would need to be read again
        
        self.new_tuple_identifier = convert_bytes_to_utf8(self.buffer[buffer_pos:buffer_pos+1])
        if self.new_tuple_identifier != 'N':
            raise Exception(f"did not find new_tuple_identifier ('N') at pos : {buffer_pos}, found : '{self.new_tuple_identifier}'")
        buffer_pos += 1
        self.new_tuple = TupleData(self.buffer[buffer_pos:])
        return self

    def __repr__(self):
        return f"UPDATE \n\tbyte1: '{self.byte1}', \n\trelation_id : {self.relation_id} " \
               f" \n\toptional_tuple_identifier : '{self.optional_tuple_identifier}', \n\toptional_old_tuple_data : {self.old_tuple} " \
               f" \n\tnew_tuple_identifier : '{self.new_tuple_identifier}', \n\tnew_tuple : {self.new_tuple}"

class Delete(PgoutputMessage):
    """
    Byte1('D')      Identifies the message as a delete message.
    Int32           ID of the relation corresponding to the ID in the relation message.
    Byte1('K')      Identifies the following TupleData submessage as a key. This field is present if the table in which the delete has happened uses an index as REPLICA IDENTITY.
    Byte1('O')      Identifies the following TupleData message as a old tuple. This field is present if the table in which the delete has happened has REPLICA IDENTITY set to FULL.
    TupleData       TupleData message part representing the contents of the old tuple or primary key, depending on the previous field.

    The Delete message may contain either a 'K' message part or an 'O' message part, but never both of them.
    """
    def decode_buffer(self):
        if self.byte1 != 'D':
            raise Exception(f"first byte in buffer does not match Delete message (expected 'D', got '{self.byte1}'")
        self.relation_id = convert_bytes_to_int(self.buffer[1:5])
        self.message_type = convert_bytes_to_utf8(self.buffer[5:6])
        if ((self.message_type != 'K') and (self.message_type != 'O')):
            raise Exception(f"message type byte is not 'K' or 'O', got : '{self.message_type}'")
        self.tuple_data = TupleData(self.buffer[6:])
        return self

    def __repr__(self):
        return f"DELETE \n\tbyte1: {self.byte1} \n\trelation_id : {self.relation_id} " \
               f"\n\tmessage_type : {self.message_type} \n\ttuple_data : {self.tuple_data}"



class Truncate(PgoutputMessage):
    """
    Byte1('T')      Identifies the message as a truncate message.
    Int32           Number of relations
    Int8            Option bits for TRUNCATE: 1 for CASCADE, 2 for RESTART IDENTITY
    Int32           ID of the relation corresponding to the ID in the relation message. This field is repeated for each relation.

    """
    def decode_buffer(self):
        if self.byte1 != 'T':
            raise Exception(f"first byte in buffer does not match Truncate message (expected 'T', got '{self.byte1}'")
        self.number_of_relations = convert_bytes_to_int(self.buffer[1:5])
        self.option_bits = convert_bytes_to_int(self.buffer[5:6])
        self.relation_ids = []
        buffer_pos = 6
        for rel in range(self.number_of_relations):
            self.relation_ids.append(convert_bytes_to_int(self.buffer[buffer_pos: buffer_pos+4]))
            buffer_pos += 4
            

    def __repr__(self):
        return f"TRUNCATE \n\tbyte1: {self.byte1} \n\tn_relations : {self.number_of_relations} "\
               f"option_bits : {self.option_bits}, relation_ids : {self.relation_ids}   "
