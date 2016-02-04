'''A parser for redis messages
'''
from itertools import starmap

nil = b'$-1\r\n'
null_array = b'*-1\r\n'
REPLAY_TYPE = frozenset((b'$',   # REDIS_REPLY_STRING,
                         b'*',   # REDIS_REPLY_ARRAY,
                         b':',   # REDIS_REPLY_INTEGER,
                         b'+',   # REDIS_REPLY_STATUS,
                         b'-'))  # REDIS_REPLY_ERROR


class String:
    __slots__ = ('_length', 'next')

    def __init__(self, length, next):
        self._length = length
        self.next = next

    def decode(self, parser, result):
        parser._current = None
        length = self._length
        if length >= 0:
            b = parser._inbuffer
            if len(b) >= length+2:
                parser._inbuffer, chunk = b[length+2:], bytes(b[:length])
                if parser.encoding:
                    return chunk.decode(parser.encoding)
                else:
                    return chunk
            else:
                parser._current = self
                return False


class ArrayTask:
    __slots__ = ('_length', '_response', 'next')

    def __init__(self, length, next):
        self._length = length
        self._response = []
        self.next = next

    def decode(self, parser, result):
        parser._current = None
        length = self._length
        if length >= 0:
            response = self._response
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


class Parser:
    '''A python parser for redis.'''
    encoding = None

    def __init__(self, protocolError, responseError):
        self.protocolError = protocolError
        self.responseError = responseError
        self._current = None
        self._inbuffer = bytearray()

    def on_connect(self, connection):
        if connection.decode_responses:
            self.encoding = connection.encoding

    def on_disconnect(self):
        pass

    def feed(self, buffer):
        '''Feed new data into the buffer'''
        self._inbuffer.extend(buffer)

    def get(self):
        '''Called by the protocol consumer'''
        if self._current:
            return self._resume(self._current, False)
        else:
            return self._get(None)

    def bulk(self, value):
        if value is None:
            return nil
        else:
            return ('$%d\r\n' % len(value)).encode('utf-8') + value + b'\r\n'

    def multi_bulk_len(self, len):
        return ('*%s\r\n' % len).encode('utf-8')

    def multi_bulk(self, args):
        '''Multi bulk encoding for list/tuple ``args``
        '''
        return null_array if args is None else b''.join(self._pack(args))

    def pack_command(self, args):
        '''Encode a command to send to the server.

        Used by redis clients
        '''
        return b''.join(self._pack_command(args))

    def pack_pipeline(self, commands):
        '''Packs pipeline commands into bytes.'''
        return b''.join(
            starmap(lambda *args: b''.join(self._pack_command(args)),
                    (a for a, _ in commands)))

    #    INTERNALS
    def _pack_command(self, args):
        crlf = b'\r\n'
        yield ('*%d\r\n' % len(args)).encode('utf-8')
        for value in args:
            if isinstance(value, str):
                value = value.encode('utf-8')
            elif not isinstance(value, bytes):
                value = str(value).encode('utf-8')
            yield ('$%d\r\n' % len(value)).encode('utf-8')
            yield value
            yield crlf

    def _pack(self, args):
        crlf = b'\r\n'
        yield ('*%d\r\n' % len(args)).encode('utf-8')
        for value in args:
            if value is None:
                yield nil
            elif isinstance(value, bytes):
                yield ('$%d\r\n' % len(value)).encode('utf-8')
                yield value
                yield crlf
            elif isinstance(value, str):
                value = value.encode('utf-8')
                yield ('$%d\r\n' % len(value)).encode('utf-8')
                yield value
                yield crlf
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
                yield crlf

    def _lua_dict(self, d):
        index = 0
        while True:
            index += 1
            v = d.get(index)
            if v is None:
                break
            yield v

    def _get(self, next):
        b = self._inbuffer
        length = b.find(b'\r\n')
        if length >= 0:
            self._inbuffer, response = b[length+2:], bytes(b[:length])
            rtype, response = response[:1], response[1:]
            if rtype == b'-':
                return self.responseError(response.decode('utf-8'))
            elif rtype == b':':
                return int(response)
            elif rtype == b'+':
                return response
            elif rtype == b'$':
                task = String(int(response), next)
                return task.decode(self, False)
            elif rtype == b'*':
                task = ArrayTask(int(response), next)
                return task.decode(self, False)
            else:
                # Clear the buffer and raise
                self._inbuffer = bytearray()
                raise self.protocolError('Protocol Error')
        else:
            return False

    def buffer(self):
        '''Current buffer'''
        return bytes(self._inbuffer)

    def _resume(self, task, result):
        result = task.decode(self, result)
        if result is not False and task.next:
            return self._resume(task.next, result)
        else:
            return result
