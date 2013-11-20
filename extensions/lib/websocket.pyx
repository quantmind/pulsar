import os
from struct import pack, unpack


cdef class Frame:

    def __init__(self):
        pass


cdef class FrameParser:
    '''Cython wrapper for websocket FrameParser.'''
    cdef Frame frame
    cdef object buffer
    cdef object ProtocolError

    def __cinit__(self, int version, int kind, object ProtocolError):
        self.version = version
        self.kind = kind
        self.frame
        self.buffer = bytearray()
        self.ProtocolError = ProtocolError

    def encode(self, object data, final=True, masking_key=None, opcode=None,
               rsv1=0, rsv2=0, rsv3=0):
        if self.kind == 1:
            masking_key = masking_key or os.urandom(4)
            return websocket

    def decode(self, bytes data):
        cdef fin, rsv1, rsv2, rsv3, opcode
        cdef Frame frame = self.frame
        cdef object buffer = self.buffer
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
            is_masked = bool(second_byte & 0x80)
