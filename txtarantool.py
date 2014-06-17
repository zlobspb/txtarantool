# -*- coding: utf-8 -*-
#
# Copyright (c) 2013, Alexander V. Panfilov
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or
# without modification, are permitted provided that the following
# conditions are met:
#
# 1. Redistributions of source code must retain the above
#    copyright notice, this list of conditions and the
#    following disclaimer.
#
# 2. Redistributions in binary form must reproduce the above
#    copyright notice, this list of conditions and the following
#    disclaimer in the documentation and/or other materials
#    provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY <COPYRIGHT HOLDER> ``AS IS'' AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
# TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
# A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
# <COPYRIGHT HOLDER> OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
# INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
# BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
# LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF
# THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
# SUCH DAMAGE.

import struct
import itertools
from collections import deque

from twisted.internet import defer
from twisted.internet import protocol
from twisted.internet import reactor
from twisted.internet import task
from twisted.protocols import basic
from twisted.protocols import policies
from twisted.python import log


class TarantoolError(Exception):
    pass


class ConnectionError(TarantoolError):
    pass


class InvalidData(TarantoolError):
    pass


struct_B = struct.Struct('<B')
struct_BB = struct.Struct('<BB')
struct_BBB = struct.Struct('<BBB')
struct_BBBB = struct.Struct('<BBBB')
struct_BBBBB = struct.Struct('<BBBBB')
struct_BL = struct.Struct("<BL")
struct_LB = struct.Struct("<LB")
struct_BQ = struct.Struct('<BQ')
struct_L = struct.Struct("<L")
struct_LL = struct.Struct("<LL")
struct_LLL = struct.Struct("<LLL")
struct_LLLL = struct.Struct("<LLLL")
struct_LLLLL = struct.Struct("<LLLLL")
struct_Q = struct.Struct("<Q")

UPDATE_OPERATION_CODE = {
    '=': 0,       # assign operation argument to field <field_no>; will extend the tuple if <field_no> == <max_field_no> + 1
    '+': 1,       # add argument to field <field_no>, both arguments are treated as signed 32 or 64 -bit ints
    '&': 2,       # bitwise AND of argument and field <field_no>
    '^': 3,       # bitwise XOR of argument and field <field_no>
    '|': 4,       # bitwise OR of argument and field <field_no>
    'splice': 5,  # implementation of Perl 'splice' command
    '#': 6,       # delete <field_no>
    '!': 7        # insert before <field_no>
}


class Request(object):
    """
    Represents a single request to the server in compliance with the Tarantool protocol.
    Responsible for data encapsulation and builds binary packet to be sent to the server.

    This is the abstract base class. Specific request types are implemented by the inherited classes.
    """

    TNT_OP_INSERT = 13
    TNT_OP_SELECT = 17
    TNT_OP_UPDATE = 19
    TNT_OP_DELETE = 21
    TNT_OP_CALL = 22
    TNT_OP_PING = 65280

    TNT_FLAG_RETURN = 0x01
    TNT_FLAG_ADD = 0x02
    TNT_FLAG_REPLACE = 0x04

    # Pre-generated results of pack_int_base128() for small arguments (0..16383)
    _int_base128 = tuple(
        (
            struct_B.pack(val) if val < 128 else struct_BB.pack(val >> 7 & 0xff | 0x80, val & 0x7F)
            for val in xrange(0x4000)
        )
    )

    def __init__(self, charset="utf-8", errors="strict"):
        self.charset = charset
        self.errors = errors
        self._bytes = None

    def __bytes__(self):
        return self._bytes
    __str__ = __bytes__

    @staticmethod
    def header(request_type, body_length, request_id):
        return struct_LLL.pack(request_type, body_length, request_id)

    @staticmethod
    def pack_int(value):
        """
        Pack integer field
        <field> ::= <int32_varint><data>

        :param value: integer value to be packed
        :type value: int

        :return: packed value
        :rtype: bytes
        """
        assert isinstance(value, (int, long))
        return struct_BL.pack(4, value)

    @staticmethod
    def pack_long(value):
        """
        Pack integer field
        <field> ::= <int32_varint><data>

        :param value: integer value to be packed
        :type value: long

        :return: packed value
        :rtype: bytes
        """
        assert isinstance(value, (int, long))
        return struct_BQ.pack(8, value)

    @classmethod
    def pack_int_base128(cls, value):
        """
        Pack integer value using LEB128 encoding
        :param value: integer value to encode
        :type value: int

        :return: encoded value
        :rtype: bytes
        """
        assert isinstance(value, int)
        if value < 1 << 14:
            return cls._int_base128[value]

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

        raise OverflowError("Number is too large to be packed")

    @classmethod
    def pack_str(cls, value):
        """
        Pack string field
        <field> ::= <int32_varint><data>

        :param value: string to be packed
        :type value: bytes or str

        :return: packed value
        :rtype: bytes
        """
        assert isinstance(value, str)
        value_len_packed = cls.pack_int_base128(len(value))
        return struct.pack("<%ds%ds" % (len(value_len_packed), len(value)), value_len_packed,  value)

    @classmethod
    def pack_unicode(cls, value, charset="utf-8", errors="strict"):
        """
        Pack string field
        <field> ::= <int32_varint><data>

        :param value: string to be packed
        :type value: unicode

        :return: packed value
        :rtype: bytes
        """
        assert isinstance(value, unicode)

        try:
            value = value.encode(charset, errors)
        except UnicodeEncodeError as e:
            raise InvalidData("Error encoding unicode value '%s': %s" % (repr(value), e))

        value_len_packed = cls.pack_int_base128(len(value))
        return struct.pack("<%ds%ds" % (len(value_len_packed), len(value)), value_len_packed,  value)

    def pack_field(self, value):
        """
        Pack single field (string or integer value)
        <field> ::= <int32_varint><data>

        :param value: value to be packed
        :type value: bytes, str, int or long

        :return: packed value
        :rtype: bytes
        """
        if isinstance(value, str):
            return self.pack_str(value)
        elif isinstance(value, unicode):
            return self.pack_unicode(value, self.charset, self.errors)
        elif isinstance(value, int):
            return self.pack_int(value)
        elif isinstance(value, long):
            return self.pack_long(value)
        else:
            raise TypeError("Invalid argument type '%s'. Only 'str', 'int' or long expected" % (type(value).__name__))

    def pack_tuple(self, values):
        """
        Pack tuple of values
        <tuple> ::= <cardinality><field>+

        :param value: tuple to be packed
        :type value: tuple of scalar values (bytes, str or int)

        :return: packed tuple
        :rtype: bytes
        """
        assert isinstance(values, (tuple, list))
        cardinality = [struct_L.pack(len(values))]
        packed_items = [self.pack_field(v) for v in values]
        return b''.join(itertools.chain(cardinality, packed_items))


class RequestPing(Request):
    """
    Represents PING request
    """
    def __init__(self, charset, errors):
        super(RequestPing, self).__init__(charset, errors)
        self._bytes = struct_LLL.pack(self.TNT_OP_PING, 0, 0)


class RequestInsert(Request):
    """
    Represents INSERT request

    <insert_request_body> ::= <space_no><flags><tuple>
    |--------------- header ----------------|--------- body ---------|
     <request_type><body_length><request_id> <space_no><flags><tuple>
                                                               |
                          items to add (multiple values)  -----+
    """
    def __init__(self, charset, errors, request_id, space_no, flags, *args):
        super(RequestInsert, self).__init__(charset, errors)
        request_body = struct_LL.pack(space_no, flags) + self.pack_tuple(args)
        self._bytes = self.header(self.TNT_OP_INSERT, len(request_body), request_id) + request_body


class RequestDelete(Request):
    """
    Represents DELETE request

    <delete_request_body> ::= <space_no><flags><tuple>
    |--------------- header ----------------|--------- body ---------|
     <request_type><body_length><request_id> <space_no><flags><tuple>
                                                               |
                          key to search in primary index  -----+
                          (tuple with single value)
    """
    def __init__(self, charset, errors, request_id, space_no, flags, *args):
        super(RequestDelete, self).__init__(charset, errors)
        request_body = struct_LL.pack(space_no, flags) + self.pack_tuple(args)
        self._bytes = self.header(self.TNT_OP_DELETE, len(request_body), request_id) + request_body


class RequestSelect(Request):
    """
    Represents SELECT request

    <select_request_body> ::= <space_no><index_no><offset><limit><count><tuple>+

    |--------------- header ----------------|---------------request_body ---------------------...|
     <request_type><body_length><request_id> <space_no><index_no><offset><limit><count><tuple>+
                                                        ^^^^^^^^                 ^^^^^^^^^^^^
                                                            |                          |
                                           Index to use ----+                          |
                                                                                       |
                            List of tuples to search in the index ---------------------+
                            (tuple cardinality can be > 1 when using composite indexes)
    """
    def __init__(self, charset, errors, request_id, space_no, index_no, offset, limit, *args):
        super(RequestSelect, self).__init__(charset, errors)
        request_body = struct_LLLLL.pack(space_no, index_no, offset, limit, 1) + self.pack_tuple(args)
        self._bytes = self.header(self.TNT_OP_SELECT, len(request_body), request_id) + request_body


class RequestUpdate(Request):
    """
    <update_request_body> ::= <space_no><flags><tuple><count><operation>+
    <operation> ::= <field_no><op_code><op_arg>

    |--------------- header ----------------|---------------request_body --------------...|
     <request_type><body_length><request_id> <space_no><flags><tuple><count><operation>+
                                                               |      |      |
                           Key to search in primary index -----+      |      +-- list of operations
                           (tuple with cardinality=1)                 +-- number of operations
    """
    def __init__(self, charset, errors, request_id, space_no, flags, key_list, op_list):
        super(RequestUpdate, self).__init__(charset, errors)
        request_body = struct_LL.pack(space_no, flags) + self.pack_tuple(key_list) \
            + struct_L.pack(len(op_list)) + self.pack_operations(op_list)
        self._bytes = self.header(self.TNT_OP_UPDATE, len(request_body), request_id) + request_body

    def pack_operations(cls, op_list):
        result = []
        for op in op_list:
            try:
                field_no, op_symbol, op_arg = op
            except ValueError:
                raise ValueError("Operation must be a tuple of 3 elements (field_id, op, value)")
            try:
                op_code = UPDATE_OPERATION_CODE[op_symbol]
            except KeyError:
                raise ValueError("Invalid operaction symbol '%s', expected one of %s"
                                 % (op_symbol, ', '.join(["'%s'" % (c) for c in sorted(UPDATE_OPERATION_CODE.keys())])))
            result.append(struct_LB.pack(field_no, op_code))
            result.append(cls.pack_field(op_arg))
        return b''.join(result)


class RequestCall(Request):
    """
    <call_request_body> ::= <flags><proc_name><tuple>
    <proc_name> ::= <field>

    |--------------- header ----------------|-----request_body -------|
     <request_type><body_length><request_id> <flags><proc_name><tuple>
                                                                |
                                    Lua function arguments -----+
    """
    def __init__(self, charset, errors, request_id, proc_name, flags, *args):
        super(RequestCall, self).__init__(charset, errors)
        request_body = struct_L.pack(flags) + self.pack_field(proc_name) + self.pack_tuple(args)
        self._bytes = self.header(self.TNT_OP_CALL, len(request_body), request_id) + request_body


class IprotoPacketReceiver(protocol.Protocol, basic._PauseableMixin):

    _header_size = 12
    _busyReceiving = False
    _header = None  # (type, body_length, request_id)
    _buffer = None
    _length = 0

    MAX_BODY = 16 * 1024

    def clearPacketBuffer(self):
        b = b''.join(self._buffer)

        if self._header:
            b = struct_LLL.pack(*self._header) + b

        self._header = None
        self._buffer = None
        self._length = 0

        return b

    def dataReceived(self, data):
        if self._buffer is None:
            self._buffer = []

        self._buffer.append(data)
        self._length += len(data)

        if self._header is None:
            if self._length < self._header_size:
                return

            data = b''.join(self._buffer)
            self._header = struct_LLL.unpack_from(data, 0)
            self._buffer = [data[self._header_size:]]
            self._length -= self._header_size

        if self._header[1] > self.MAX_BODY:
            return self.packetLengthExceeded(self._header, self._buffer)

        if self._length < self._header[1]:
            return

        header = self._header
        body_length = self._header[1]
        data = b''.join(self._buffer)

        self._length -= body_length
        self._buffer = [data[body_length:]]
        self._header = None

        self.packetReceived(header, data[:body_length])

    def packetLengthExceeded(self, header, body):
        return self.transport.loseConnection()

    def packetReceived(self, header, body):
        """
        Override this for when each packet is received.

        @param header: The Iproto header which was received.
        @type header: T{type, body_length, request_id}

        @param body: The Iproto packet body of size body_length.
        @type body: C{bytes}
        """
        raise NotImplementedError("Abstract method must be overridden")


class field(bytes):
    """
    Represents a single element of the Tarantool's tuple
    """
    def __new__(cls, value):
        """
        Create new instance of Tarantool field (single tuple element)
        """
        # Since parent class is immutable, we should override __new__, not __init__

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
        raise TypeError("Unsupported argument type '%s'" % (type(value).__name__))

    def __int__(self):
        """
        Cast filed to int
        """
        if len(self) == 4:
            return struct_L.unpack(self)[0]
        else:
            raise ValueError("Unable to cast field to int: length must be 4 bytes, field length is %d" % len(self))

    def __long__(self):
        """
        Cast filed to long
        """
        if len(self) == 8:
            return struct_Q.unpack(self)[0]
        else:
            raise ValueError("Unable to cast field to int: length must be 8 bytes, field length is %d" % len(self))


class Response(list):
    """
    Represents a single response from the server in compliance with the Tarantool protocol.
    Responsible for data encapsulation (i.e. received list of tuples) and parses binary
    packet received from the server.
    """

    def __init__(self, header, body, charset="utf-8", errors="strict", field_types=None):
        """
        Create an instance of `Response` using data received from the server.

        __init__() itself reads data from the socket, parses response body and
        sets appropriate instance attributes.

        :param header: header of the response
        :type header: array of bytes
        :param body: body of the response
        :type body: array of bytes
        """

        # This is not necessary, because underlying list data structures are created in the __new__(). But let it be.
        super(Response, self).__init__()

        self.charset = charset
        self.errors = errors

        self._body_length = None
        self._request_id = None
        self._request_type = None
        self._completion_status = None
        self._return_code = None
        self._return_message = None
        self._rowcount = None
        self.field_types = field_types

        # Unpack header
        if isinstance(header, (tuple, list)):
            self._request_type, self._body_length, self._request_id = header
        else:
            self._request_type, self._body_length, self._request_id = struct_LLL.unpack(header)

        if body:
            self._unpack_body(body)

    @staticmethod
    def _unpack_int_base128(varint, offset):
        """Implement Perl unpack's 'w' option, aka base 128 decoding."""
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

    def _unpack_tuple(self, buff):
        """
        Unpacks the tuple from byte buffer
        <tuple> ::= <cardinality><field>+

        :param buff: byte array of the form <cardinality><field>+
        :type buff: ctypes buffer or bytes

        :return: tuple of unpacked values
        :rtype: tuple
        """
        cardinality = struct_L.unpack_from(buff)[0]
        _tuple = ['']*cardinality
        offset = 4    # The first 4 bytes in the response body is the <count> we have already read
        for i in xrange(cardinality):
            field_size, offset = self._unpack_int_base128(buff, offset)
            field_data = struct.unpack_from("<%ds" % field_size, buff, offset)[0]
            _tuple[i] = field(field_data)
            offset += field_size

        return tuple(_tuple)

    def _unpack_body(self, buff):
        """
        Parse the response body.
        After body unpacking its data available as python list of tuples

        For each request type the response body has the same format:
        <insert_response_body> ::= <count> | <count><fq_tuple>
        <update_response_body> ::= <count> | <count><fq_tuple>
        <delete_response_body> ::= <count> | <count><fq_tuple>
        <select_response_body> ::= <count><fq_tuple>*
        <call_response_body>   ::= <count><fq_tuple>

        :param buff: buffer containing request body
        :type byff: ctypes buffer
        """

        # Unpack <return_code> and <count> (how many records affected or selected)
        self._return_code = struct_L.unpack_from(buff, offset=0)[0]

        # Separate return_code and completion_code
        self._completion_status = self._return_code & 0x00ff
        self._return_code >>= 8

        # In case of an error unpack the body as an error message
        if self._return_code != 0:
            self._return_message = unicode(buff[4:-1], self.charset, self.errors)
            if self._completion_status == 2:
                raise TarantoolError(self._return_code, self._return_message)

        # Unpack <count> (how many records affected or selected)
        self._rowcount = struct_L.unpack_from(buff, offset=4)[0]

        # If the response doesn't contain any tuple - there is nothing to unpack
        if self._body_length == 8:
            return

        # Parse response tuples (<fq_tuple>)
        if self._rowcount > 0:
            offset = 8    # The first 4 bytes in the response body is the <count> we have already read
            while offset < self._body_length:
                # In resonse tuples have the form <size><tuple> (<fq_tuple> ::= <size><tuple>).
                # Attribute <size> takes into account only size of tuple's <field> payload,
                # but does not include 4-byte of <cardinality> field.
                #Therefore the actual size of the <tuple> is greater to 4 bytes.
                tuple_size = struct.unpack_from("<L", buff, offset)[0] + 4
                tuple_data = struct.unpack_from("<%ds" % (tuple_size), buff, offset+4)[0]
                tuple_value = self._unpack_tuple(tuple_data)
                if self.field_types:
                    self.append(self._cast_tuple(tuple_value))
                else:
                    self.append(tuple_value)

                offset = offset + tuple_size + 4    # This '4' is a size of <size> attribute

    @property
    def completion_status(self):
        """
        :type: int

        Request completion status.

        There are only three completion status codes in use:

            * ``0`` -- "success"; the only possible :attr:`return_code` with this status is ``0``
            * ``1`` -- "try again"; an indicator of an intermittent error.
                    This status is handled automatically by this module.
            * ``2`` -- "error"; in this case :attr:`return_code` holds the actual error.
        """
        return self._completion_status

    @property
    def rowcount(self):
        """
        :type: int

        Number of rows affected or returned by a query.
        """
        return self._rowcount

    @property
    def return_code(self):
        """
        :type: int

        Required field in the server response.
        Value of :attr:`return_code` can be ``0`` if request was sucessfull or contains an error code.
        If :attr:`return_code` is non-zero than :attr:`return_message` contains an error message.
        """
        return self._return_code

    @property
    def return_message(self):
        """
        :type: str

        The error message returned by the server in case of :attr:`return_code` is non-zero.
        """
        return self._return_message

    def _cast_field(self, cast_to, value):
        """
        Convert field type from raw bytes to native python type

        :param cast_to: native python type to cast to
        :type cast_to: a type object (one of bytes, int, unicode (str for py3k))
        :param value: raw value from the database
        :type value: bytes

        :return: converted value
        :rtype: value of native python type (one of bytes, int, unicode (str for py3k))
        """
        if cast_to in (int, long, str):
            return cast_to(value)
        elif cast_to == unicode:
            try:
                value = value.decode(self.charset, self.errors)
            except UnicodeEncodeError, e:
                raise InvalidData("Error encoding unicode value '%s': %s" % (repr(value), e))

            return value
        elif cast_to in (any, bytes):
            return value
        else:
            raise TypeError("Invalid field type %s" % (cast_to))

    def _cast_tuple(self, values):
        """
        Convert values of the tuple from raw bytes to native python types

        :param values: tuple of the raw database values
        :type value: tuple of bytes

        :return: converted tuple value
        :rtype: value of native python types (bytes, int, unicode (or str for py3k))
        """
        result = []
        for i, value in enumerate(values):
            if i < len(self.field_types):
                result.append(self._cast_field(self.field_types[i], value))
            else:
                result.append(self._cast_field(self.field_types[-1], value))

        return tuple(result)

    def __repr__(self):
        """
        Return user friendy string representation of the object.
        Useful for the interactive sessions and debugging.

        :rtype: str or None
        """
        # If response is not empty then return default list representation
        # If there was an SELECT request - return list representation even it is empty
        if self._request_type == Request.TNT_OP_SELECT or len(self):
            return super(Response, self).__repr__()
        # Ping
        if self._request_type == Request.TNT_OP_PING:
            return "ping ok"
        # Return string of form "N records affected"
        affected = str(self.rowcount) + (" record" if self.rowcount == 1 else " records")
        if self._request_type == Request.TNT_OP_DELETE:
            return affected + " deleted"
        if self._request_type == Request.TNT_OP_INSERT:
            return affected + " inserted"
        if self._request_type == Request.TNT_OP_UPDATE:
            return affected + " updated"
        return affected + " affected"


class QueueUnderflow(Exception):
    pass


class IproDeferredQueue(object):

    def __init__(self, backlog=None):
        self.waiting = {0: deque()}
        self.backlog = backlog
        self.id = 1

    def _cancelGet(self, d):
        if d._ipro_request_id != 0:
            self.waiting.pop(d._ipro_request_id)
        else:
            self.waiting.get(0).remove(d)

    def broadcast(self, obj):
        for request_id in self.waiting.iterkeys():
            if request_id != 0:
                self.waiting.pop(request_id).callback(obj)
            else:
                for p in self.waiting.get(0):
                    p.callback(obj)

    def check_id(self, request_id):
        if request_id != 0:
            return request_id in self.waiting
        else:
            return len(self.waiting.get(0)) != 0

    def put(self, request_id, obj):
        if request_id != 0:
            self.waiting.pop(request_id).callback(obj)
        else:
            self.waiting.get(0).popleft().callback(obj)

    def get_ping(self):
        d = defer.Deferred(canceller=self._cancelGet)

        d._ipro_request_id = 0
        l = self.waiting.get(0)
        l.append(d)

        return d

    def get(self):
        if self.backlog is None or len(self.waiting) - 1 < self.backlog:
            d = defer.Deferred(canceller=self._cancelGet)

            d._ipro_request_id = self.id
            self.waiting[self.id] = d

            while True:
                self.id += 1
                if self.id > 0xffffffff:
                    self.id = 1
                if not self.id in self.waiting:
                    break

            return d
        else:
            raise QueueUnderflow()


class TarantoolProtocol(IprotoPacketReceiver, policies.TimeoutMixin, object):
    """
    Tarantool client protocol.
    """
    space_no = 0

    def __init__(self, charset="utf-8", errors="strict"):
        self.charset = charset
        self.errors = errors

        self.replyQueue = IproDeferredQueue()

    def connectionMade(self):
        self.connected = 1
        self.factory.addConnection(self)

    def connectionLost(self, why):
        self.connected = 0
        self.factory.delConnection(self)
        IprotoPacketReceiver.connectionLost(self, why)
        self.replyQueue.broadcast(ConnectionError("Lost connection"))

    def packetReceived(self, header, body):
        self.resetTimeout()

        if not self.replyQueue.check_id(header[2]):
            return self.transport.loseConnection()

        self.replyQueue.put(header[2], (header, body))

    @staticmethod
    def handle_reply(r, charset, errors, field_types):
        if isinstance(r, Exception):
            raise r

        return Response(r[0], r[1], charset, errors, field_types)

    def send_packet(self, packet, field_types=None):
        self.transport.write(bytes(packet))
        d = self.replyQueue.get()
        return d.addCallback(self.handle_reply, self.charset, self.errors, field_types)

    # Tarantool COMMANDS

    def ping(self):
        """
        send ping packet to tarantool server and receive response with empty body
        """
        d = self.replyQueue.get_ping()
        packet = RequestPing(self.charset, self.errors)
        self.transport.write(bytes(packet))
        return d.addCallback(self.handle_reply, self.charset, self.errors, None)

    def insert(self, space_no, *args):
        """
        insert tuple, if primary key exists server will return error
        """
        d = self.replyQueue.get()
        packet = RequestInsert(self.charset, self.errors, d._ipro_request_id, space_no, Request.TNT_FLAG_ADD, *args)
        self.transport.write(bytes(packet))
        return d.addCallback(self.handle_reply, self.charset, self.errors, None)

    def insert_ret(self, space_no, field_types, *args):
        """
        insert tuple, inserted tuple is sent back, if primary key exists server will return error
        """
        d = self.replyQueue.get()
        packet = RequestInsert(self.charset, self.errors, d._ipro_request_id,
                               space_no, Request.TNT_FLAG_ADD | Request.TNT_FLAG_RETURN, *args)
        self.transport.write(bytes(packet))
        return d.addCallback(self.handle_reply, self.charset, self.errors, field_types)

    def select(self, space_no, index_no, field_types, *args):
        """
        select tuple(s)
        """
        d = self.replyQueue.get()
        packet = RequestSelect(self.charset, self.errors, d._ipro_request_id, space_no, index_no, 0, 0xffffffff, *args)
        self.transport.write(bytes(packet))
        return d.addCallback(self.handle_reply, self.charset, self.errors, field_types)

    def select_ext(self, space_no, index_no, offset, limit, field_types, *args):
        """
        select tuple(s), additional parameters are submitted: offset and limit
        """
        d = self.replyQueue.get()
        packet = RequestSelect(self.charset, self.errors, d._ipro_request_id, space_no, index_no, offset, limit, *args)
        self.transport.write(bytes(packet))
        return d.addCallback(self.handle_reply, self.charset, self.errors, field_types)

    def update(self, space_no, key_tuple, op_list):
        """
        send update command(s)
        """
        d = self.replyQueue.get()
        packet = RequestUpdate(self.charset, self.errors, d._ipro_request_id, space_no, 0, key_tuple, op_list)
        self.transport.write(bytes(packet))
        return d.addCallback(self.handle_reply, self.charset, self.errors, None)

    def update_ret(self, space_no, field_types, key_tuple, op_list):
        """
        send update command(s), updated tuple(s) is(are) sent back
        """
        d = self.replyQueue.get()
        packet = RequestUpdate(self.charset, self.errors, d._ipro_request_id,
                               space_no, Request.TNT_FLAG_RETURN, key_tuple, op_list)
        self.transport.write(bytes(packet))
        return d.addCallback(self.handle_reply, self.charset, self.errors, field_types)

    def delete(self, space_no, *args):
        """
        delete tuple by primary key
        """
        d = self.replyQueue.get()
        packet = RequestDelete(self.charset, self.errors, d._ipro_request_id, space_no, 0, *args)
        self.transport.write(bytes(packet))
        return d.addCallback(self.handle_reply, self.charset, self.errors, None)

    def delete_ret(self, space_no, field_types, *args):
        """
        delete tuple by primary key, deleted tuple is sent back
        """
        d = self.replyQueue.get()
        packet = RequestDelete(self.charset, self.errors, d._ipro_request_id, space_no, Request.TNT_FLAG_RETURN, *args)
        self.transport.write(bytes(packet))
        return d.addCallback(self.handle_reply, self.charset, self.errors, field_types)

    def replace(self, space_no, *args):
        """
        insert tuple, if primary key exists it will be rewritten
        """
        d = self.replyQueue.get()
        packet = RequestInsert(self.charset, self.errors, d._ipro_request_id, space_no, 0, *args)
        self.transport.write(bytes(packet))
        return d.addCallback(self.handle_reply, self.charset, self.errors, None)

    def replace_ret(self, space_no, field_types, *args):
        """
        insert tuple, inserted tuple is sent back, if primary key exists it will be rewritten
        """
        d = self.replyQueue.get()
        packet = RequestInsert(self.charset, self.errors, d._ipro_request_id, space_no, Request.TNT_FLAG_RETURN, *args)
        self.transport.write(bytes(packet))
        return d.addCallback(self.handle_reply, self.charset, self.errors, field_types)

    def replace_req(self, space_no, *args):
        """
        insert tuple, if tuple with same primary key doesn't exist server will return error
        """
        d = self.replyQueue.get()
        packet = RequestInsert(self.charset, self.errors, d._ipro_request_id, space_no, Request.TNT_FLAG_REPLACE, *args)
        self.transport.write(bytes(packet))
        return d.addCallback(self.handle_reply, self.charset, self.errors, None)

    def replace_req_ret(self, space_no, field_types, *args):
        """
        insert tuple, inserted tuple is sent back, if tuple with same primary key doesn't exist server will return error
        """
        d = self.replyQueue.get()
        packet = RequestInsert(self.charset, self.errors, d._ipro_request_id,
                               space_no, Request.TNT_FLAG_REPLACE | Request.TNT_FLAG_RETURN, *args)
        self.transport.write(bytes(packet))
        return d.addCallback(self.handle_reply, self.charset, self.errors, field_types)

    def call(self, proc_name, field_types, *args):
        """
        call server procedure
        """
        d = self.replyQueue.get()
        packet = RequestCall(self.charset, self.errors, d._ipro_request_id, proc_name, 0, *args)
        self.transport.write(bytes(packet))
        return d.addCallback(self.handle_reply, self.charset, self.errors, field_types)


class ConnectionHandler(object):

    def __init__(self, factory):
        self._factory = factory
        self._connected = factory.deferred

    def _wait_pool_cleanup(self, deferred):
        if self._factory.size == 0:
            deferred.callback(True)

    def disconnect(self):
        self._factory.continueTrying = 0
        for conn in self._factory.pool:
            try:
                conn.transport.loseConnection()
            except:
                pass

        d = defer.Deferred()
        t = task.LoopingCall(self._wait_pool_cleanup, d)
        d.addCallback(lambda _: t.stop())
        t.start(.5)
        return d

    def __getattr__(self, method):
        def wrapper(*args, **kwargs):
            d = self._factory.getConnection()

            def callback(connection):
                protocol_method = getattr(connection, method)
                try:
                    d = protocol_method(*args, **kwargs)
                except:
                    self._factory.connectionQueue.put(connection)
                    raise

                def put_back(reply):
                    self._factory.connectionQueue.put(connection)
                    return reply

                def switch_to_errback(reply):
                    if isinstance(reply, Exception):
                        raise reply
                    return reply

                d.addBoth(put_back)
                d.addCallback(switch_to_errback)

                return d

            d.addCallback(callback)

            return d

        return wrapper

    def __repr__(self):
        try:
            cli = self._factory.pool[0].transport.getPeer()
        except:
            return "<Tarantool Connection: Not connected>"
        else:
            return "<Tarantool Connection: %s:%s - %d connection%s>" % \
                   (cli.host, cli.port, self._factory.size, 's' if self._factory.size > 1 else '')


class UnixConnectionHandler(ConnectionHandler):

    def __repr__(self):
        try:
            cli = self._factory.pool[0].transport.getPeer()
        except:
            return "<Tarantool Connection: Not connected>"
        else:
            return "<Tarantool Unix Connection: %s - %d connection%s>" % \
                   (cli.name, self._factory.size, 's' if self._factory.size > 1 else '')


class TarantoolFactory(protocol.ReconnectingClientFactory):

    maxDelay = 10
    protocol = TarantoolProtocol

    def __init__(self, poolsize, isLazy=False, handler=ConnectionHandler):
        if not isinstance(poolsize, int):
            raise ValueError("Tarantool poolsize must be an integer, not %s" % type(poolsize).__name__)

        self.poolsize = poolsize
        self.isLazy = isLazy

        self.idx = 0
        self.size = 0
        self.pool = []
        self.deferred = defer.Deferred()
        self.handler = handler(self)
        self.connectionQueue = defer.DeferredQueue()

    def addConnection(self, conn):
        self.connectionQueue.put(conn)
        self.pool.append(conn)
        self.size = len(self.pool)
        if self.deferred:
            if self.size == self.poolsize:
                self.deferred.callback(self.handler)
                self.deferred = None

    def delConnection(self, conn):
        try:
            self.pool.remove(conn)
        except Exception, e:
            log.msg("Could not remove connection from pool: %s" % str(e))

        self.size = len(self.pool)

    def connectionError(self, why):
        if self.deferred:
            self.deferred.errback(ValueError(why))
            self.deferred = None

    @defer.inlineCallbacks
    def getConnection(self, put_back=False):
        if not self.size:
            raise ConnectionError("Not connected")

        while True:
            conn = yield self.connectionQueue.get()
            if conn.connected == 0:
                log.msg('Discarding dead connection.')
            else:
                if put_back:
                    self.connectionQueue.put(conn)
                defer.returnValue(conn)


def makeConnection(host, port, poolsize, reconnect, isLazy):
    factory = TarantoolFactory(poolsize, isLazy, ConnectionHandler)
    factory.continueTrying = reconnect
    for x in xrange(poolsize):
        reactor.connectTCP(host, port, factory)

    if isLazy:
        return factory.handler
    else:
        return factory.deferred


def Connection(host="localhost", port=33013, reconnect=True):
    return makeConnection(host, port, 1, reconnect, False)


def lazyConnection(host="localhost", port=33013, reconnect=True):
    return makeConnection(host, port, 1, reconnect, True)


def ConnectionPool(host="localhost", port=33013, poolsize=10, reconnect=True):
    return makeConnection(host, port, poolsize, reconnect, False)


def lazyConnectionPool(host="localhost", port=33013, poolsize=10, reconnect=True):
    return makeConnection(host, port, poolsize, reconnect, True)


def makeUnixConnection(path, poolsize, reconnect, isLazy):
    factory = TarantoolFactory(poolsize, isLazy, UnixConnectionHandler)
    factory.continueTrying = reconnect
    for x in xrange(poolsize):
        reactor.connectUNIX(path, factory)

    if isLazy:
        return factory.handler
    else:
        return factory.deferred


def UnixConnection(path="/tmp/tarantool.sock", reconnect=True):
    return makeUnixConnection(path, 1, reconnect, False)


def lazyUnixConnection(path="/tmp/tarantool.sock", reconnect=True):
    return makeUnixConnection(path, 1, reconnect, True)


def UnixConnectionPool(path="/tmp/tarantool.sock", poolsize=10, reconnect=True):
    return makeUnixConnection(path, poolsize, reconnect, False)


def lazyUnixConnectionPool(path="/tmp/tarantool.sock", poolsize=10, reconnect=True):
    return makeUnixConnection(path, poolsize, reconnect, True)


__all__ = [
    Connection, lazyConnection,
    ConnectionPool, lazyConnectionPool,
    UnixConnection, lazyUnixConnection,
    UnixConnectionPool, lazyUnixConnectionPool,
]

__author__ = "Alexander V. Panfilov"
__version__ = version = "0.6"
