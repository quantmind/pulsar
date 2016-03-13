from itertools import starmap

cdef class Task

cdef bytes CRLF = b"\r\n"
cdef bytes RESPONSE_INTEGER  = b':'
cdef bytes RESPONSE_STRING  = b'$'
cdef bytes RESPONSE_ARRAY = b'*'
cdef bytes RESPONSE_STATUS = b'+'
cdef bytes RESPONSE_ERROR = b'-'
cdef bytes nil = b'$-1\r\n'
cdef bytes null_array = b'*-1\r\n'


cdef class RedisParser:
    cdef object _protocolError
    cdef object _responseError
    cdef object _encoding
    cdef object _inbuffer
    cdef Task _current

    def __cinit__(self, object perr, object rerr):
        self._protocolError = perr
        self._responseError = rerr
        self._inbuffer = bytearray()

    def on_connect(self, connection):
        if connection.decode_responses:
            self._encoding = connection.encoding

    def on_disconnect(self):
        pass

    # DECODER
    def get(self):
        if self._current:
            return self._resume(self._current, False)
        else:
            return self._get(None)

    def feed(self, stream):
        self._inbuffer.extend(stream)

    def buffer(self):
        return self._inbuffer

    # CLIENT ENCODERS
    def pack_command(self, args):
        return b''.join(self._pack_command(args))

    def pack_pipeline(self, commands):
        pack = lambda *args: b''.join(self._pack_command(args))
        return b''.join(starmap(pack, (args for args, _ in commands)))

    # SERVER ENCODERS
    def bulk(self, bytes value=None):
        if value is None:
            return nil
        else:
            return ('$%d\r\n' % len(value)).encode('utf-8') + value + b'\r\n'

    def multi_bulk_len(self, len):
        return ('*%s\r\n' % len).encode('utf-8')

    def multi_bulk(self, args):
        return null_array if args is None else b''.join(self._pack(args))

    # INTERNALS
    def _pack_command(self, args):
        yield ('*%d\r\n' % len(args)).encode('utf-8')
        for value in args:
            if isinstance(value, str):
                value = value.encode('utf-8')
            elif not isinstance(value, bytes):
                value = str(value).encode('utf-8')
            yield ('$%d\r\n' % len(value)).encode('utf-8')
            yield value
            yield CRLF

    def _pack(self, args):
        yield ('*%d\r\n' % len(args)).encode('utf-8')
        for value in args:
            if value is None:
                yield nil
            elif isinstance(value, bytes):
                yield ('$%d\r\n' % len(value)).encode('utf-8')
                yield value
                yield CRLF
            elif isinstance(value, str):
                value = value.encode('utf-8')
                yield ('$%d\r\n' % len(value)).encode('utf-8')
                yield value
                yield CRLF
            elif hasattr(value, 'items'):
                for value in self._pack(tuple(self._lua_dict(value))):
                    yield value
            elif hasattr(value, '__len__'):
                for value in self._pack(value):
                    yield value
            else:
                value = str(value).encode('utf-8')
                yield ('$%d\r\n' % len(value)).encode('utf-8')
                yield value
                yield CRLF

    def _lua_dict(self, d):
        index = 0
        while True:
            index += 1
            v = d.get(index)
            if v is None:
                break
            yield v

    cdef object _get(self, Task next):
        b = self._inbuffer
        cdef int length = b.find(b'\r\n')
        if length >= 0:
            self._inbuffer, response = b[length+2:], bytes(b[:length])
            rtype, response = response[:1], response[1:]
            if rtype == RESPONSE_ERROR:
                return self._responseError(response.decode('utf-8'))
            elif rtype == RESPONSE_INTEGER:
                return long(response)
            elif rtype == RESPONSE_STATUS:
                return response
            elif rtype == RESPONSE_STRING:
                task = Task(long(response), next)
                return task.decode(self, False)
            elif rtype == RESPONSE_ARRAY:
                task = ArrayTask(long(response), next)
                return task.decode(self, False)
            else:
                # Clear the buffer and raise
                self._inbuffer = bytearray()
                raise self._protocolError('Protocol Error')
        else:
            return False

    cdef object _resume(self, Task task, object result):
        result = task.decode(self, result)
        if result is not False and task._next:
            return self._resume(task._next, result)
        else:
            return result


cdef class Task:
    cdef long _length
    cdef Task _next

    def __cinit__(self, long length, Task next):
        self._length = length
        self._next = next

    cdef object decode(self, RedisParser parser, object result):
        cdef long length = self._length
        cdef bytes chunk
        parser._current = None
        if length >= 0:
            b = parser._inbuffer
            if len(b) >= length+2:
                parser._inbuffer, chunk = b[length+2:], bytes(b[:length])
                if parser._encoding:
                    return chunk.decode(parser._encoding)
                else:
                    return chunk
            else:
                parser._current = self
                return False


cdef class ArrayTask(Task):
    cdef list _response

    cdef object decode(self, RedisParser parser, object result):
        cdef long length = self._length
        cdef list response = self._response
        parser._current = None
        if response is None:
            self._response = response = []
        if length >= 0:
            if result is not False:
                response.append(result)
            while len(response) < length:
                result = parser._get(self)
                if result is False:
                    break
                response.append(result)
            if len(response) == length:
                parser._current = None
                return response
            elif not parser._current:
                parser._current = self
            return False
