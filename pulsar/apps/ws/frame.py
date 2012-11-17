# -*- coding: utf-8 -*-
import array
import os
from struct import pack, unpack
from io import BytesIO

import pulsar
from pulsar.utils.httpurl import ispy3k, range, to_bytes, is_string


__all__ = ['WebSocketError',
           'WebSocketProtocolError',
           'Frame',
           'FrameParser',
           'i2b',
           'int2bytes']

DEFAULT_VERSION = 13
SUPPORTED_VERSIONS = (DEFAULT_VERSION,)

if ispy3k:
    i2b = lambda n : bytes((n,))
    
    def is_text_data(data):
        return isinstance(data, str)
            
else: # pragma : nocover
    i2b = lambda n : chr(n)
    
    def is_text_data(data):
        return True

def int2bytes(*ints):
    '''convert a series of integers into bytes'''
    return b''.join((i2b(i) for i in ints))

def get_version(version):
    version = int(version or DEFAULT_VERSION)
    if version not in SUPPORTED_VERSIONS:
        raise WebSocketProtocolError('Version %s not supported.' % version)
    return version


class WebSocketError(pulsar.HttpException):
    pass


class WebSocketProtocolError(WebSocketError):
    pass
    
    
class Frame(object):
    __slots__ = ('fin', 'opcode', 'masking_key', 'rsv1', 'rsv2', 'rsv3',
                 'msg', 'version', 'payload_length', 'body')
    def __init__(self, message=None, opcode=None, version=None,
                 masking_key=None, final=False, rsv1=0, rsv2=0, rsv3=0):
        """Implements the framing protocol as defined by hybi_
specification supporting protocol version 13::
    
    >>> f = Frame(opcode, 'hello world', os.urandom(4), fin=1)
    >>> data = f.build()
    >>> f = Frame()
    >>> f.parser.send(bytes[1])
    >>> f.parser.send(bytes[2])
    >>> f.parser.send(bytes[2:])
"""
        self.version = get_version(version)
        if opcode is None and message is not None:
            opcode = 0x1 if is_text_data(message) else 0x2
        message = to_bytes(message or b'')
        self.payload_length = len(message)
        if opcode is None:
            raise WebSocketProtocolError('opcode not available')
        self.version = version
        self.opcode = opcode
        if masking_key:
            masking_key = to_bytes(masking_key)
            if len(masking_key) != 4:
                raise WebSocketProtocolError('Masking key must be 4 bytes long')
        self.masking_key = masking_key
        self.fin = 0x1 if final else 0
        self.rsv1 = rsv1
        self.rsv2 = rsv2
        self.rsv3 = rsv3
        self.body = message
        self.msg = self.build_frame(message)
        
    @classmethod
    def ping(cls, body=None, version=None):
        return cls(body, 0x9, final=True, version=version)
    
    @classmethod
    def pong(cls, body=None, version=None):
        return cls(body, 0xA, final=True, version=version)
    
    @classmethod
    def close(cls, body=None, version=None):
        return cls(body, 0x8, final=True, version=version)
    
    @classmethod
    def continuation(cls, body=None, **kwargs):
        return cls(body, 0, **kwargs)
    
    @property
    def final(self):
        return bool(self.fin)
    
    @property
    def masked(self):
        return bool(self.masking_key)
    
    @property
    def is_data(self):
        return self.opcode in (0x1, 0x2)
    
    @property
    def is_close(self):
        return self.opcode == 0x8
    
    def on_received(self):
        if self.opcode == 0x9:
            return self.pong(self.body, self.version)
    
    def build_frame(self, message):
        """Builds a frame from the instance's attributes."""
        header = BytesIO()
        if 0x3 <= self.opcode <= 0x7 or 0xB <= self.opcode:
            raise WebSocketProtocolError('Opcode cannot be a reserved opcode')
        ## +-+-+-+-+-------+
        ## |F|R|R|R| opcode|
        ## |I|S|S|S|  (4)  |
        ## |N|V|V|V|       |
        ## | |1|2|3|       |
        ## +-+-+-+-+-------+
        header.write(i2b(((self.fin << 7)
                       | (self.rsv1 << 6)
                       | (self.rsv2 << 5)
                       | (self.rsv3 << 4)
                       | self.opcode)))
        ##                 +-+-------------+-------------------------------+
        ##                 |M| Payload len |    Extended payload length    |
        ##                 |A|     (7)     |             (16/63)           |
        ##                 |S|             |   (if payload len==126/127)   |
        ##                 |K|             |                               |
        ## +-+-+-+-+-------+-+-------------+ - - - - - - - - - - - - - - - +
        ## |     Extended payload length continued, if payload len == 127  |
        ## + - - - - - - - - - - - - - - - +-------------------------------+
        if self.masking_key:
            mask_bit = 1 << 7
        else:
            mask_bit = 0
        length = self.payload_length 
        if length < 126:
            header.write(i2b(mask_bit | length))
        elif length < (1 << 16):
            header.write(i2b(mask_bit | 126))
            header.write(pack('!H', length))
        elif length < (1 << 63):
            header.write(i2b(mask_bit | 127))
            header.write(pack('!Q', length))
        else:
            raise WebSocketProtocolError('Frame too large')
        ## + - - - - - - - - - - - - - - - +-------------------------------+
        ## |                               |Masking-key, if MASK set to 1  |
        ## +-------------------------------+-------------------------------+
        ## | Masking-key (continued)       |          Payload Data         |
        ## +-------------------------------- - - - - - - - - - - - - - - - +
        ## :                     Payload Data continued ...                :
        ## + - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - +
        ## |                     Payload Data continued ...                |
        ## +---------------------------------------------------------------+
        if not self.masking_key:
            header.write(message)
        else:
            header.write(self.masking_key)
            header.write(self.mask(message))
        return header.getvalue()
                
    def mask(self, data):
        """Performs the masking or unmasking operation on data
using the simple masking algorithm::

    j                   = i MOD 4
    transformed-octet-i = original-octet-i XOR masking-key-octet-j
"""
        masked = bytearray(data)
        key = self.masking_key
        if not ispy3k:  #pragma    nocover
            key = map(ord, key)
        for i in range(len(data)):
            masked[i] = masked[i] ^ key[i%4]
        return masked
    unmask = mask


class ParsedFrame(Frame):
    def build_frame(self, msg):
        return bytearray(msg)
    
    
class FrameParser(object):
    '''Parser for the version 8 protocol.
    
.. attribute:: kind

    0 for parsing client's frames and sending server frames (to be used in the server),
    1 for parsing server frames and sending client frames (to be used by the client)
'''    
    def __init__(self, version=None, kind=0):
        self.version = get_version(version)
        self._frame = None
        self._buf = None
        self.kind = kind
    
    def decode(self, data):
        return self.execute(data), bytearray()
    
    def encode(self, data):
        # Encode data into a frame
        if not isinstance(data, Frame):
            if self.kind == 1:
                data = Frame(data, masking_key=os.urandom(4))
            else:
                data = Frame(data)
        return data.msg
    
    def execute(self, data, length=None):
        # end of body can be passed manually by putting a length of 0
        if not data:
            return None
        data = bytes(data)
        frame = self._frame
        if self._buf:
            data = self._buf + data
        # No opcode yet
        if frame is None:
            if len(data) < 2:
                return self.save_buf(frame, data)
            first_byte, second_byte = unpack("BB", data[:2])
            fin = (first_byte >> 7) & 1
            rsv1 = (first_byte >> 6) & 1
            rsv2 = (first_byte >> 5) & 1
            rsv3 = (first_byte >> 4) & 1
            opcode = first_byte & 0xf
            if fin not in (0, 1):
                raise WebSocketProtocolError('FIN must be 0 or 1')
            if not (second_byte & 0x80):
                if not self.kind:
                    raise WebSocketProtocolError(\
                                'Unmasked client frame. Abort connection')
            else:
                if self.kind:
                    raise WebSocketProtocolError(\
                            'Masked server frame. Abort connection')
            payload_length = second_byte & 0x7f
            # All control frames MUST have a payload length of 125 bytes or less
            if opcode > 0x7 and payload_length > 125:
                raise WebSocketProtocolError('Frame too large')
            msg, data = data[:2], data[2:]
            frame = ParsedFrame(msg, opcode, self.version, final=fin,
                                rsv1=rsv1, rsv2=rsv2, rsv3=rsv3)
            frame.payload_length = payload_length
        #   
        if frame.masking_key is None:
            # All control frames MUST have a payload length of 125 bytes or less
            d = None
            mask_length = 4 if not self.kind else 0
            if frame.payload_length == 126:
                if len(data) < 2 + mask_length: # 2 + 4 for mask
                     return self.save_buf(frame, data)
                d, data = data[:2] , data[2:]
                frame.payload_length = unpack("!H", d)[0]
            elif frame.payload_length == 127:
                if len(data) < 8 + mask_length:  # 8 + 4 for mask
                     return self.save_buf(frame, data)
                d, data = data[:8] , data[8:]
                frame.payload_length = unpack("!Q", d)[0]
            elif len(data) < mask_length:
                return self.save_buf(frame, data)
            if d:
                frame.msg.extend(d)
            if mask_length:
                frame.masking_key, data = data[:mask_length], data[mask_length:]
            else:
                frame.masking_key = b''
            frame.msg.extend(frame.masking_key)
                
        if len(data) < frame.payload_length:
            return self.save_buf(frame, data)
        # We have a frame
        else:
            data = data[:frame.payload_length]
            frame.msg.extend(data)
            self.save_buf(None, data[frame.payload_length:])
            if frame.masking_key:
                data = bytes(frame.unmask(data))
            if frame.opcode == 0x1:
                data = data.decode("utf-8", "replace")
            frame.body = data
            return frame
            
    def save_buf(self, frame, data):
        self._frame = frame
        self._buf = data
        