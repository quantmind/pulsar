# -*- coding: utf-8 -*-
'''WebSocket_ Protocol is implemented via the :class:`Frame` and
:class:`FrameParser` classes.

To obtain a frame parser one should use the :func:`frame_parser` function.

frame parser
~~~~~~~~~~~~~~~~~~~

.. autofunction:: frame_parser


Frame
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: Frame
   :members:
   :member-order: bysource


Frame Parser
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: FrameParser
   :members:
   :member-order: bysource


parse_close
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autofunction:: parse_close


.. _WebSocket: http://tools.ietf.org/html/rfc6455'''
import os
from struct import pack, unpack
from array import array
from base64 import b64encode

from .pep import to_bytes
from .exceptions import ProtocolError
from .httpurl import DEFAULT_CHARSET

try:
    from .lib import FrameParser as CFrameParser
except:     # pragma    nocover
    CFrameParser = None


CLOSE_CODES = {
    1000: "OK",
    1001: "going away",
    1002: "protocol error",
    1003: "unsupported type",
    # 1004: - (reserved)
    # 1005: no status code (internal)
    # 1006: connection closed abnormally (internal)
    1007: "invalid data",
    1008: "policy violation",
    1009: "message too big",
    1010: "extension required",
    1011: "unexpected error",
    # 1015: TLS failure (internal)
}


DEFAULT_VERSION = 13
SUPPORTED_VERSIONS = (DEFAULT_VERSION,)
WS_EXTENSIONS = {}
WS_PROTOCOLS = {}


def get_version(version):
    try:
        version = int(version or DEFAULT_VERSION)
    except Exception:
        pass
    if version not in SUPPORTED_VERSIONS:
        raise ProtocolError('Version %s not supported.' % version)
    return version


def websocket_key():
    return b64encode(os.urandom(16)).decode(DEFAULT_CHARSET)


class Extension:

    def receive(self, data):
        return data

    def send(self, data):
        return data


def frame_parser(version=None, kind=0, extensions=None, protocols=None,
                 pyparser=False):
    '''Create a new :class:`FrameParser` instance.

    :param version: protocol version, the default is 13
    :param kind: the kind of parser, and integer between 0 and 3 (check the
        :class:`FrameParser` documentation for details)
    :param extensions: not used at the moment
    :param protocols: not used at the moment
    :param pyparser: if ``True`` (default ``False``) uses the python frame
        parser implementation rather than the much faster cython
        implementation.
    '''
    version = get_version(version)
    Parser = FrameParser if pyparser else CFrameParser
    # extensions, protocols
    return Parser(version, kind, ProtocolError, close_codes=CLOSE_CODES)


def websocket_mask(data, masking_key):
    mask_size = len(masking_key)
    key = array('B', masking_key)
    data = array('B', data)
    for i in range(len(data)):
        data[i] ^= key[i % mask_size]
    return data.tobytes()


class Frame:
    _body = None
    _masking_key = None

    def __init__(self, opcode, final, payload_length):
        self._opcode = opcode
        self._final = final
        self._payload_length = payload_length

    @property
    def final(self):
        return self._final

    @property
    def opcode(self):
        return self._opcode

    @property
    def body(self):
        return self._body

    @property
    def masking_key(self):
        return self._masking_key

    @property
    def is_message(self):
        return self._opcode == 1

    @property
    def is_bytes(self):
        return self._opcode == 2

    @property
    def is_close(self):
        return self._opcode == 8

    @property
    def is_ping(self):
        return self._opcode == 9

    @property
    def is_pong(self):
        return self._opcode == 10


class FrameParser:
    '''Decoder and encoder for the websocket protocol.

    .. attribute:: version

        Optional protocol version (Default 13).

    .. attribute:: kind

        * 0 for parsing client's frames and sending server frames (to be used
          in the server)
        * 1 for parsing server frames and sending client frames (to be used
          by the client)
        * 2 Assumes always unmasked data
    '''
    def __init__(self, version, kind, ProtocolError, extensions=None,
                 protocols=None, close_codes=None):
        self.version = version
        self.kind = kind
        self.frame = None
        self.buffer = bytearray()
        self._opcodes = (0, 1, 2, 8, 9, 10)
        self._encode_mask_length = 0
        self._decode_mask_length = 0
        if kind == 0:
            self._decode_mask_length = 4
        elif kind == 1:
            self._encode_mask_length = 4
        elif kind == 3:
            self._decode_mask_length = 4
            self._encode_mask_length = 4
        self._max_payload = 1 << 63
        self._extensions = extensions
        self._protocols = protocols
        self._close_codes = close_codes or CLOSE_CODES

    @property
    def max_payload(self):
        return self._max_payload

    @property
    def decode_mask_length(self):
        return self._decode_mask_length

    @property
    def encode_mask_length(self):
        return self._encode_mask_length

    @property
    def extensions(self):
        return self._extensions

    @property
    def protocols(self):
        return self._protocols

    def ping(self, body=None):
        '''return a `ping` :class:`Frame`.'''
        return self.encode(body, opcode=0x9)

    def pong(self, body=None):
        '''return a `pong` :class:`Frame`.'''
        return self.encode(body, opcode=0xA)

    def close(self, code=None):
        '''return a `close` :class:`Frame`.
        '''
        code = code or 1000
        body = pack('!H', code)
        body += self._close_codes.get(code, '').encode('utf-8')
        return self.encode(body, opcode=0x8)

    def continuation(self, body=None, final=True):
        '''return a `continuation` :class:`Frame`.'''
        return self.encode(body, opcode=0, final=final)

    def encode(self, message, final=True, masking_key=None,
               opcode=None, rsv1=0, rsv2=0, rsv3=0):
        '''Encode a ``message`` for writing into the wire.

        To produce several frames for a given large message use
        :meth:`multi_encode` method.
        '''
        fin = 1 if final else 0
        opcode, masking_key, data = self._info(message, opcode, masking_key)
        return self._encode(data, opcode, masking_key, fin,
                            rsv1, rsv2, rsv3)

    def multi_encode(self, message, masking_key=None, opcode=None,
                     rsv1=0, rsv2=0, rsv3=0, max_payload=0):
        '''Encode a ``message`` into several frames depending on size.

        Returns a generator of bytes to be sent over the wire.
        '''
        max_payload = max(2, max_payload or self._max_payload)
        opcode, masking_key, data = self._info(message, opcode, masking_key)
        #
        while data:
            if len(data) >= max_payload:
                chunk, data, fin = (data[:max_payload],
                                    data[max_payload:], 0)
            else:
                chunk, data, fin = data, b'', 1
            yield self._encode(chunk, opcode, masking_key, fin,
                               rsv1, rsv2, rsv3)

    def decode(self, data=None):
        frame = self.frame
        mask_length = self._decode_mask_length

        if data:
            self.buffer.extend(data)
        if frame is None:
            if len(self.buffer) < 2:
                return
            chunk = self._chunk(2)
            first_byte, second_byte = unpack("BB", chunk)
            fin = (first_byte >> 7) & 1
            # rsv1 = (first_byte >> 6) & 1
            # rsv2 = (first_byte >> 5) & 1
            # rsv3 = (first_byte >> 4) & 1
            opcode = first_byte & 0xf
            if fin not in (0, 1):
                raise ProtocolError('FIN must be 0 or 1')
            if bool(mask_length) != bool(second_byte & 0x80):
                if mask_length:
                    raise ProtocolError('unmasked client frame.')
                else:
                    raise ProtocolError('masked server frame.')
            payload_length = second_byte & 0x7f
            # All control frames MUST have a payload length of 125 bytes
            # or less
            if opcode > 7:
                if payload_length > 125:
                    raise ProtocolError(
                        'WEBSOCKET control frame too large')
                elif not fin:
                    raise ProtocolError(
                        'WEBSOCKET control frame fragmented')
            self.frame = frame = Frame(opcode, bool(fin), payload_length)

        if frame._masking_key is None:
            if frame._payload_length == 0x7e:  # 126
                if len(self.buffer) < 2 + mask_length:  # 2 + 4 for mask
                    return
                chunk = self._chunk(2)
                frame._payload_length = unpack("!H", chunk)[0]
            elif frame._payload_length == 0x7f:  # 127
                if len(self.buffer) < 8 + mask_length:  # 8 + 4 for mask
                    return
                chunk = self._chunk(8)
                frame._payload_length = unpack("!Q", chunk)[0]
            elif len(self.buffer) < mask_length:
                return
            if mask_length:
                frame._masking_key = self._chunk(mask_length)
            else:
                frame._masking_key = b''

        if len(self.buffer) >= frame._payload_length:
            self.frame = None
            chunk = self._chunk(frame._payload_length)
            if self._extensions:
                for extension in self._extensions:
                    chunk = extension.receive(frame, self.buffer)
            if frame._masking_key:
                chunk = websocket_mask(chunk, frame._masking_key)
            if frame.opcode == 1:
                frame._body = chunk.decode("utf-8", "replace")
            else:
                frame._body = chunk
            return frame

    def _encode(self, data, opcode, masking_key, fin, rsv1, rsv2, rsv3):
        buffer = bytearray()
        length = len(data)
        mask_bit = 128 if masking_key else 0

        buffer.append(((fin << 7) | (rsv1 << 6) | (rsv2 << 5) |
                       (rsv3 << 4) | opcode))

        if length < 126:
            buffer.append(mask_bit | length)
        elif length < 65536:
            buffer.append(mask_bit | 126)
            buffer.extend(pack('!H', length))
        elif length < self._max_payload:
            buffer.append(mask_bit | 127)
            buffer.extend(pack('!Q', length))
        else:
            raise ProtocolError('WEBSOCKET frame too large')
        if masking_key:
            buffer.extend(masking_key)
            buffer.extend(websocket_mask(data, masking_key))
        else:
            buffer.extend(data)
        return bytes(buffer)

    def _info(self, message, opcode, masking_key):
        mask_length = self._encode_mask_length

        if mask_length:
            masking_key = to_bytes(masking_key or os.urandom(4))
            assert len(masking_key) == mask_length, "bad masking key"
        else:
            masking_key = b''
        if opcode is None:
            opcode = 1 if isinstance(message, str) else 2
        data = to_bytes(message or b'', 'utf-8')
        if opcode not in self._opcodes:
            raise ProtocolError('WEBSOCKET opcode a reserved value')
        elif opcode > 7:
            if len(data) > 125:
                raise ProtocolError('WEBSOCKET control frame too large')
            if opcode == 8:
                # TODO CHECK CLOSE FRAME STATUS CODE
                pass
        return opcode, masking_key, data

    def _chunk(self, length):
        chunk = bytes(self.buffer[:length])
        self.buffer = self.buffer[length:]
        return chunk


def parse_close(data):
    '''Parse the body of a close :class:`Frame`.

    Returns a tuple (``code``, ``reason``) if successful otherwise
    raise :class:`.ProtocolError`.
    '''
    length = len(data)
    if length == 0:
        return 1005, ''
    elif length == 1:
        raise ProtocolError("Close frame too short")
    else:
        code, = unpack('!H', data[:2])
        if not (code in CLOSE_CODES or 3000 <= code < 5000):
            raise ProtocolError("Invalid status code for websocket")
        reason = data[2:].decode('utf-8')
        return code, reason


if CFrameParser is None:     # pragma    nocover
    CFrameParser = FrameParser
