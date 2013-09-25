from itertools import starmap

cimport common


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

    def pack_command(self, *args):
        return common.pack_command(args)

    def pack_pipeline(self, commands):
        pack = lambda *args: common.pack_command(args)
        return b''.join(starmap(pack, (args for args, _ in commands)))

