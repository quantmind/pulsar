import os
from struct import pack, unpack

from common cimport websocket_mask


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


cdef class EncodedFrame:

    def __init__(self, message, opcode=None, version=None,
                 masking_key=None, final=False, rsv1=0, rsv2=0, rsv3=0):
        pass


cdef class FrameParser:
    '''Cython wrapper for websocket FrameParser.'''
    cdef Frame frame
    cdef object buffer
    cdef object ProtocolError
    cdef int _mask_length

    def __cinit__(self, int version, int kind, object ProtocolError):
        self.version = version
        self.kind = kind
        self.frame
        self.buffer = bytearray()
        self.ProtocolError = ProtocolError
        self._mask_length = 4 if self.kind == 0 else 0

    def mask_length(self):
        return self._mask_length

    def ping(self, body=None):
        '''return a `ping` :class:`Frame`.'''
        return self.encode(body, opcode=0x9)

    def encode(self, data, final=True, masking_key=None, **params):
        if self._mask_length:
            masking_key = masking_key or os.urandom(4)
            assert len(masking_key) == 4, "bad masking key"
            params['masking_key'] = None
        return EncodedFrame(data, final=final, **params)

    def decode(self, bytes data):
        cdef int fin, rsv1, rsv2, rsv3, opcode, payload_length
        cdef Frame frame = self.frame
        cdef object buffer = self.buffer
        cdef int mask_length = self._mask_length
        cdef bytes chunk
        #
        if data:
            buffer.extend(data)
        if frame is None:
            if len(buffer) < 2:
                return
            first_byte, second_byte = unpack("BB", data[:2])
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
            if opcode > 0x7 and payload_length > 125:
                raise self.ProtocolError('WEBSOCKET frame too large')
            chunk = self._chunk(2)
            self.frame = frame = Frame(opcode, <bint>fin, payload_length)

        if frame._masking_key is None:
            # All control frames MUST have a payload length of 125 bytes
            # or less
            d = None
            if frame._payload_length == 0x7e:  # 126
                if len(buffer) < 2 + mask_length:  # 2 + 4 for mask
                    return self.save_buf(frame, buffer)
                chunk = self._chunk(2)
                frame._payload_length = unpack("!H", chunk)[0]
            elif frame._payload_length == 0x7f:  # 127
                if len(buffer) < 8 + mask_length:  # 8 + 4 for mask
                    return self.save_buf(frame, buffer)
                chunk = self._chunk(8)
                frame._payload_length = unpack("!Q", chunk)[0]
            elif len(buffer) < mask_length:
                return
            if mask_length:
                frame._masking_key = self._chunk(mask_length)
            else:
                frame._masking_key = b''

        if len(buffer) >= frame._payload_length:
            self.frame = None
            chunk = self._chunk(frame._payload_length)
            #for extension in self.extensions:
            #    data = extension.receive(frame, self.buffer)
            if frame._masking_key:
                chunk = websocket_mask(chunk, frame._masking_key,
                                       len(chunk), len(frame._masking_key))
            if frame.opcode == 0x1:
                frame.body = chunk.decode("utf-8", "replace")
            else:
                frame.body = chunk
            return frame

    cdef bytes _chunk(self, int length):
        cdef bytes chunk = bytes(self.buffer[:length])
        self.buffer = self.buffer[length:]
        return chunk
