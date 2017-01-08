from lib cimport _http_date

cdef str CRLFS = '\r\n'
cdef str CHARSET = 'ISO-8859-1'
cdef int MAX_CHUNK_SIZE = 65536


cpdef str http_date(int timestamp):
    return _http_date(timestamp)


cpdef bytes http_headers_message(object headers, str version, str status):
    cdef object k
    cdef str v
    cdef list buf = ['HTTP/%s %s' % (version, status)]

    for k, v in headers.items():
        buf.append('%s: %s' % (k, v))
    buf.append(CRLFS)
    return (CRLFS.join(buf)).encode(CHARSET)


cdef class http_write:

    cdef object protocol

    def __cinit__(self, object protocol):
        self.protocol = protocol

    cpdef object write(self, bytes data, object force=False):
        '''The write function returned by the :meth:`start_response` method.

        Required by the WSGI specification.

        :param data: bytes to write
        :param force: Optional flag used internally
        :return: a :class:`~asyncio.Future` or the number of bytes written
        '''
        cdef list chunks = None
        cdef object proto = self.protocol
        if not proto._headers_sent:
            tosend = proto.get_headers()
            proto._headers_sent = http_headers_message(
                tosend, proto.version, proto.status
            )
            proto.event('on_headers').fire()
            chunks = [proto._headers_sent]
        if data:
            if not chunks:
                chunks = []
            if proto.chunked:
                chunks.extend(http_chunks(data))
            else:
                chunks.append(data)
        elif force and proto.chunked:
            if not chunks:
                chunks = []
            chunks.extend(http_chunks(data, True))
        if chunks:
            return proto._connection.write(b''.join(chunks))


cdef bytes chunk_encoding(bytes chunk):
    '''Write a chunk::

        chunk-size(hex) CRLF
        chunk-data CRLF

    If the size is 0, this is the last chunk, and an extra CRLF is appended.
    '''
    cdef bytes head = ("%X\r\n" % len(chunk)).encode('utf-8')
    return head + chunk + b'\r\n'


def http_chunks(data, finish=False):
    while len(data) >= MAX_CHUNK_SIZE:
        chunk, data = data[:MAX_CHUNK_SIZE], data[MAX_CHUNK_SIZE:]
        yield chunk_encoding(chunk)
    if data:
        yield chunk_encoding(data)
    if finish:
        yield chunk_encoding(data)
