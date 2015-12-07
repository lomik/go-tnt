"""
lua box.select(0,0,tonumber64('379253126446'))
---
 - 379253126446: {1349108530, 1349108530, 53934378844168, 1349108530, 47188572766240, 1349108530, 46421775089752, 1349108530, 50926388183160, 1349108530, 50778301006032}
"""

import struct
import socket
import ctypes
import sys

# pylint: disable=C0103
struct_B = struct.Struct('<B')
struct_BB = struct.Struct('<BB')
struct_BBB = struct.Struct('<BBB')
struct_BBBB = struct.Struct('<BBBB')
struct_BBBBB = struct.Struct('<BBBBB')
struct_BL = struct.Struct("<BL")
struct_LB = struct.Struct("<LB")
struct_L = struct.Struct("<L")
struct_LL = struct.Struct("<LL")
struct_LLL = struct.Struct("<LLL")
struct_LLLL = struct.Struct("<LLLL")
struct_LLLLL = struct.Struct("<LLLLL")
struct_Q = struct.Struct("<Q")


REQUEST_TYPE_CALL = 22
REQUEST_TYPE_DELETE = 21
REQUEST_TYPE_INSERT = 13
REQUEST_TYPE_SELECT = 17
REQUEST_TYPE_UPDATE = 19


UPDATE_OPERATION_CODE = {'=': 0, '+': 1, '&': 2, '^': 3, '|': 4, 'splice': 5}

# Default value for socket timeout (seconds)
SOCKET_TIMEOUT = 1
# Default maximum number of attempts to reconnect
RECONNECT_MAX_ATTEMPTS = 10
# Default delay between attempts to reconnect (seconds)
RECONNECT_DELAY = 0.1
# Number of reattempts in case of server return completion_status == 1 (try again)
RETRY_MAX_ATTEMPTS = 10

def pack_header(request_type, body_length):
    return struct_LLL.pack(request_type, body_length, 0)

def pack_int(value):
    if not isinstance(value, int):
        raise TypeError("Invalid argument type '%s'. Only 'int' expected"%type(value).__name__)
    return struct_BL.pack(4, value)

def pack_int_base128(value):
    if value < 1 << 7:
        return struct_B.pack(value)

    if value < 1 << 14:
        return struct_BB.pack(value >> 7 & 0xff | 0x80, value & 0x7F)

    if value < 1 << 21:
        return struct_BBB.pack(
                    value >> 14 & 0xff | 0x80,
                    value >> 7 & 0xff | 0x80,
                    value & 0x7F
        )

    if value < 1 << 28:
        return struct_BBBB.pack(
                    value >> 21 & 0xff | 0x80,
                    value >> 14 & 0xff | 0x80,
                    value >> 7 & 0xff | 0x80,
                    value & 0x7F
        )

    if value < 1 << 35:
        return struct_BBBBB.pack(
                    value >> 28 & 0xff | 0x80,
                    value >> 21 & 0xff | 0x80,
                    value >> 14 & 0xff | 0x80,
                    value >> 7 & 0xff | 0x80,
                    value & 0x7F
        )

    raise OverflowError("Number too large to be packed")


def pack_str(value):
    if not isinstance(value, basestring):
        raise TypeError("Invalid argument type '%s', 'str' expected"%type(value).__name__)
    value_len_packed = pack_int_base128(len(value))
    return struct.pack("<%ds%ds"%(len(value_len_packed), len(value)), value_len_packed,  value)

def pack_field(value):
    if isinstance(value, basestring):
        return pack_str(value)
    elif isinstance(value, (int, long)):
        return pack_int(value)
    else:
        raise TypeError("Invalid argument type '%s'. Only 'str' or 'int' expected"%type(value).__name__)

def pack_tuple(values):
    assert isinstance(values, (tuple, list))
    cardinality = struct_L.pack(len(values))
    packed_items = [pack_field(v) for v in values]
    packed_items.insert(0, cardinality)
    return b"".join(packed_items)

def _pack_select(space_no, index_no, values, offset=0, limit=0xffffffff):
    # 'values' argument must be a list of tuples
    assert isinstance(values, (list, tuple))
    assert len(values) != 0
    assert isinstance(values[0], (list, tuple))
    
    request_body = \
        struct_LLLLL.pack(space_no, index_no, offset, limit, len(values)) + \
        b"".join([pack_tuple(t) for t in values])

    return pack_header(REQUEST_TYPE_SELECT, len(request_body)) + request_body

def pack_select(space_no, values, **kwargs):

    # Initialize arguments and its defaults from **kwargs
    offset = kwargs.get("offset", 0)
    limit = kwargs.get("limit", 0xffffffff)
    index = kwargs.get("index", 0)

    # Perform smart type cheching (scalar / list of scalars / list of tuples)
    if isinstance(values, (int, basestring)): # scalar
        # This request is looking for one single record
        values = [(values, )]
    elif isinstance(values, (list, tuple, set, frozenset)):
        assert len(values) > 0
        if isinstance(values[0], (int, basestring)): # list of scalars
            # This request is looking for several records using single-valued index
            # Ex: select(space_no, index_no, [1, 2, 3])
            # Transform a list of scalar values to a list of tuples
            values = [(v, ) for v in values]
        elif isinstance(values[0], (list, tuple)): # list of tuples
            # This request is looking for serveral records using composite index
            pass
        else:
            raise ValueError("Invalid value type, expected one of scalar (int or str) / list of scalars / list of tuples ")

    return _pack_select(space_no, index, values, offset, limit)

def pack_insert(space_no, values, **kwargs):
    assert isinstance(values, (tuple, list))
    return_tuple = kwargs.get("return_tuple", 0)
    
    flags = 1 if return_tuple else 0
    request_body = struct_LL.pack(space_no, flags) + pack_tuple(values)
    
    return pack_header(REQUEST_TYPE_INSERT, len(request_body)) + request_body


class field(bytes):
    '''\
    Represents a single element of the Tarantool's tuple
    '''
    def __new__(cls, value):
        '''\
        Create new instance of Tarantool field (single tuple element)
        '''
        # Since parent class is immutable, we should override __new__, not __init__

        if isinstance(value, unicode):
            return super(field, cls).__new__(cls, value.encode("utf-8", "replace"))

        if isinstance(value, str):
            return super(field, cls).__new__(cls, value)

        if isinstance(value, (bytearray, bytes)):
            return super(field, cls).__new__(cls, value)

        if isinstance(value, (int, long)):
            if 0 <= value <= 0xFFFFFFFF:
                # 32 bit integer
                return super(field, cls).__new__(cls, struct_L.pack(value))
            elif 0xFFFFFFFF < value <= 0xFFFFFFFFFFFFFFFF:
                # 64 bit integer
                return super(field, cls).__new__(cls, struct_Q.pack(value))
            else:
                raise ValueError("Integer argument out of range")

        # NOTE: It is posible to implement float
        raise TypeError("Unsupported argument type '%s'"%(type(value).__name__))


    def __int__(self):
        '''\
        Cast filed to int
        '''
        if len(self) == 4:
            return struct_L.unpack(self)[0]
        elif len(self) == 8:
            return struct_Q.unpack(self)[0]
        else:
            raise ValueError("Unable to cast field to int: length must be 4 or 8 bytes, field length is %d"%len(self))


    def __str__(self):
        '''\
        Cast filed to str
        '''
        return self.decode("utf-8", "replace")
    def __unicode__(self):
        '''\
        Cast filed to unicode
        '''
        return self.decode("utf-8", "replace")


def unpack_int_base128(varint):
    """Implement Perl unpack's 'w' option, aka base 128 decoding."""
    offset = 0
    res = ord(varint[offset])
    if ord(varint[offset]) >= 0x80:
        offset += 1
        res = ((res - 0x80) << 7) + ord(varint[offset])
        if ord(varint[offset]) >= 0x80:
            offset += 1
            res = ((res - 0x80) << 7) + ord(varint[offset])
            if ord(varint[offset]) >= 0x80:
                offset += 1
                res = ((res - 0x80) << 7) + ord(varint[offset])
                if ord(varint[offset]) >= 0x80:
                    offset += 1
                    res = ((res - 0x80) << 7) + ord(varint[offset])
    return res, offset + 1

def unpack_tuple(tuple_data):
    cardinality = struct_L.unpack(tuple_data[:4])[0]
    _tuple = ['']*cardinality
    offset = 4    # The first 4 bytes in the response body is the <count> we have already read
    for i in xrange(cardinality):
        field_size, data_len = unpack_int_base128(tuple_data[offset:])
        offset += data_len
        #print "field_size", field_size
        field_data = struct.unpack("<%ds"%field_size, tuple_data[offset:offset+field_size])[0]
        _tuple[i] = field(field_data)
        offset += field_size
    return tuple(_tuple)

def unpack_body(body):
    return_code, rowcount = struct_LL.unpack(body[:8])
    completion_status = return_code & 0x00ff
    return_code = return_code >> 8
    #print "return_code", return_code
    #print "rowcount", rowcount
    #print "completion_status", completion_status

    if return_code != 0:
        return_message = unicode(body[4:], "utf8", "replace")
        if completion_status == 2:
            raise NotImplementedError(u"Not handled error: %s" % [return_message,])

    if len(body) == 8: # no tuples
        return 

    body_len  = len(body)
    result = []
    if rowcount > 0:
        offset = 8
        while offset < body_len:
            tuple_size = struct_L.unpack(body[offset:offset+4])[0]+4
            #print "tuple_size", tuple_size
            tuple_data = struct.unpack("<%ds" % tuple_size, body[offset+4:offset+4+tuple_size])[0]
            tuple_value = unpack_tuple(tuple_data)
            #print tuple_value
            offset = offset + tuple_size + 4
            #print "offset", offset
            result.append(tuple_value)

    return result


def make_test_select(sock):
    EXISTS_USER_ID = 379253126446L
    p = pack_select(0, struct_Q.pack(EXISTS_USER_ID))

    sock.sendall(p)

    header = sock.recv(12)
    #print "header", [header,]

    body_length = struct_L.unpack(header[4:8])[0]

    #print "body_length", body_length

    if body_length != 0:
        body = sock.recv(body_length)
        # Immediately raises an exception if the data cannot be read
        #print "len(body)", len(body)
        if len(body) != body_length:
            raise socket.error(socket.errno.ECONNABORTED, "Software caused connection abort")
    else:
        body = b""

    #print "body", [body,]
    
    if body:
        result = unpack_body(body)
        print "result", str([tuple(map(int,s)) for s in result])

def main():
    _socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    _socket.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)
    _socket.connect(("127.0.0.1", 2001))
    for i in xrange(1):
        make_test_request(_socket)
    _socket.close()

if __name__ == '__main__':
  main()

