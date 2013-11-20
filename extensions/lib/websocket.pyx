import os


cdef class FrameParser:
    '''Cython wrapper for websocket FrameParser.'''

    def __cinit__(self, int version, int kind):
        self.version = version
        self.kind = kind

    def encode(self, object data, final=True, masking_key=None, opcode=None,
               rsv1=0, rsv2=0, rsv3=0):
        if self.kind == 1:
            masking_key = masking_key or os.urandom(4)
            return websocket
