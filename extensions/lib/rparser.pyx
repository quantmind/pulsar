from itertools import starmap

cimport common


cdef bytes nil = b'$-1\r\n'


cdef class RedisParser:
    '''Cython wrapper for Hiredis protocol parser.'''

    cdef common.RedisParser *_parser
    cdef object _protocolError

    def __cinit__(self, object perr, object rerr):
        self._protocolError = perr
        self._parser = new common.RedisParser(perr, rerr)

    def __dealloc__(self):
        if self._parser is not NULL:
            del self._parser

    def feed(self, object stream):
        self._parser.feed(stream, len(stream))

    def get(self):
        result = self._parser.get()
        if isinstance(result, self._protocolError):
            raise result
        return result

    def buffer(self):
        return self._parser.get_buffer()

    def on_connect(self, connection):
        if connection.decode_responses:
            encoding = connection.encoding.encode('utf-8')
            self._parser.set_encoding(encoding)

    def on_disconnect(self):
        pass

    def bulk(self, bytes value=None):
        if value is None:
            return nil
        else:
            return ('$%d\r\n' % len(value)).encode('utf-8') + value + b'\r\n'

    def multi_bulk_len(self, len):
        return ('*%s\r\n' % len).encode('utf-8')

    def multi_bulk(self, *args):
        return common.pack_command(args)

    def pack_pipeline(self, commands):
        pack = lambda *args: common.pack_command(args)
        return b''.join(starmap(pack, (args for args, _ in commands)))
