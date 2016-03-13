import os
from struct import pack, unpack

cimport cython
from websocket cimport websocket_mask, to_bytes


cdef class Frame:
    cdef int _opcode
    cdef bint _final
    cdef int _payload_length
    cdef bytes _masking_key
    cdef object _body

    def __cinit__(self, int opcode, bint final, int payload_length):
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


cdef class FrameParser:
    '''Encoder and decoder of WebSocket frames.

    * kind = 0, server (decode masked messages, encode unmasked messages)
    * kind = 1, client (decode unmasked messages, encode masked messages)
    * kind = 2, unmasked both encoding and decoding
    * kind = 3, masked both encoding and decoding
    '''
    cdef Frame frame
    cdef object buffer
    cdef object ProtocolError
    cdef int kind
    cdef int _version
    cdef int _decode_mask_length
    cdef int _encode_mask_length
    cdef tuple _opcodes
    cdef object _extensions
    cdef object _protocols
    cdef object _close_codes

    def __cinit__(self, int version, int kind, object ProtocolError,
                  extensions=None, protocols=None, close_codes=None):
        self._version = version
        self.kind = kind
        self.frame = None
        self.buffer = bytearray()
        self.ProtocolError = ProtocolError
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
        self._extensions = extensions
        self._protocols = protocols
        self._close_codes = close_codes

    @property
    def version(self):
        return self._version

    @property
    def max_payload(self):
        return 1 << 63

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
        '''return a `close` :class:`Frame`.'''
        code = code or 1000
        body = pack('!H', code)
        if self._close_codes is not None:
            body += self._close_codes.get(code, '').encode('utf-8')
        return self.encode(body, opcode=0x8)

    def continuation(self, body=None, final=True):
        '''return a `continuation` :class:`Frame`.'''
        return self.encode(body, opcode=0, final=final)

    def encode(self, message, final=True, bytes masking_key=None,
               opcode=None, int rsv1=0, int rsv2=0, int rsv3=0):
        '''Encode a ``message`` for writing into the wire.

        The message length cannot exceed :attr:`max_payload`
        To produce several frames for a given large message use
        :meth:`multi_encode'.
        '''
        cdef bytes data
        cdef int fin = 1 if final else 0
        opcode, masking_key, data = self._info(message, opcode, masking_key)
        return self._encode(data, opcode, masking_key, fin,
                            rsv1, rsv2, rsv3)

    def multi_encode(self, message, bytes masking_key=None, opcode=None,
                     int rsv1=0, int rsv2=0, int rsv3=0,
                     cython.ulonglong max_payload=0):
        '''Encode a ``message`` into several frames depending on size.

        Returns a generator of bytes to be sent over the wire.
        '''
        cdef bytes data
        cdef bytes chunk
        cdef int fin
        max_payload = max(2, max_payload or 1 << 63)
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

    def decode(self, bytes data=None):
        cdef int fin, rsv1, rsv2, rsv3, opcode, payload_length
        cdef Frame frame = self.frame
        cdef int mask_length = self._decode_mask_length
        cdef bytes chunk
        #
        if data:
            self.buffer.extend(data)
        if frame is None:
            if len(self.buffer) < 2:
                return
            first_byte, second_byte = unpack("BB", self.buffer[:2])
            fin = (first_byte >> 7) & 1
            rsv1 = (first_byte >> 6) & 1
            rsv2 = (first_byte >> 5) & 1
            rsv3 = (first_byte >> 4) & 1
            opcode = first_byte & 0xf
            if fin not in (0, 1):
                raise self.ProtocolError('FIN must be 0 or 1')
            if bool(mask_length) != bool(second_byte & 0x80):
                if mask_length:
                    raise self.ProtocolError('unmasked client frame.')
                else:
                    raise self.ProtocolError('masked server frame.')
            payload_length = second_byte & 0x7f
            # All control frames MUST have a payload length of 125 bytes
            # or less
            if opcode > 7:
                if payload_length > 125:
                    raise self.ProtocolError(
                        'WEBSOCKET control frame too large')
                elif not fin:
                    raise self.ProtocolError(
                        'WEBSOCKET control frame fragmented')
            chunk = self._chunk(2)
            self.frame = frame = Frame(opcode, <bint>fin, payload_length)

        if frame._masking_key is None:
            if frame._payload_length == 126:
                if len(self.buffer) < 2 + mask_length:  # 2 + 4 for mask
                    return
                chunk = self._chunk(2)
                frame._payload_length = unpack("!H", chunk)[0]
            elif frame._payload_length == 127:
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
                chunk = websocket_mask(chunk, frame._masking_key,
                                       len(chunk), len(frame._masking_key))
            if frame.opcode == 1:
                frame._body = chunk.decode("utf-8", "replace")
            else:
                frame._body = chunk
            return frame

    cdef bytes _encode(self, bytes data, int opcode, bytes masking_key,
                       int fin, int rsv1, int rsv2, int rsv3):
        cdef object buffer = bytearray()
        cdef cython.longlong length = len(data)
        cdef int mask_bit = 128 if masking_key else 0

        buffer.append(((fin << 7) | (rsv1 << 6) | (rsv2 << 5) |
                       (rsv3 << 4) | opcode))

        if length < 126:
            buffer.append(mask_bit | length)
        elif length < 65536:
            buffer.append(mask_bit | 126)
            buffer.extend(pack('!H', length))
        elif length < (1 << 63):
            buffer.append(mask_bit | 127)
            buffer.extend(pack('!Q', length))
        else:
            raise self.ProtocolError('WEBSOCKET frame too large')
        if masking_key:
            buffer.extend(masking_key)
            buffer.extend(websocket_mask(data, masking_key,
                                         length, len(masking_key)))
        else:
            buffer.extend(data)
        return bytes(buffer)

    cdef tuple _info(self, message, opcode, bytes masking_key):
        cdef int mask_length = self._encode_mask_length
        #
        if mask_length:
            masking_key = masking_key or os.urandom(4)
            assert len(masking_key) == mask_length, "bad masking key"
        else:
            masking_key = b''
        if opcode is None:
            opcode = 1 if isinstance(message, str) else 2
        data = to_bytes(message or b'', 'utf-8')
        if opcode not in self._opcodes:
            raise self.ProtocolError('WEBSOCKET opcode a reserved value')
        elif opcode > 7:
            if len(data) > 125:
                raise self.ProtocolError('WEBSOCKET control frame too large')
            if opcode == 8:
                #TODO CHECK CLOSE FRAME STATUS CODE
                pass
        return opcode, masking_key, data

    cdef bytes _chunk(self, int length):
        cdef bytes chunk = bytes(self.buffer[:length])
        self.buffer = self.buffer[length:]
        return chunk
