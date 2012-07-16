# -*- coding: utf-8 -*-
import array
import os
import struct
from io import BytesIO

import pulsar
from pulsar.utils.httpurl import ispy3k, range, to_bytes, is_string


__all__ = ['WebSocketError',
           'WebSocketProtocolError',
           'Frame',
           'FrameParser',
           'frame',
           'frame_ping',
           'frame_close']

if ispy3k:
    i2b = lambda n : bytes((n,))
else: # pragma : nocover
    i2b = lambda n : chr(n)


class WebSocketError(pulsar.HttpException):
    pass


class WebSocketProtocolError(WebSocketError):
    pass


def frame(version=None, message='', opcode=None, **kwargs):
    '''Return a websocket :class:`Frame` instance.
    
:rtype: binary data'''
    v = int(version or 13)
    if v not in (8, 13):
        raise WebSocketProtocolError('Version %s' % version)
    if not opcode and message is not None:
        if is_string(message):
            message = to_bytes(message)
            opcode = 0x1
        else:
            opcode = 0x2
    return Frame(version, opcode, message, **kwargs)
    
def frame_ping(version=None):
    return frame(opcode=0xA, final=True)
    
def frame_close(version=None):
    return frame(opcode=0x8, final=True)


class Frame(object):
    msg = None
    def __init__(self, version, opcode=None, body=None, masking_key=None,
                 final=False, rsv1=0, rsv2=0, rsv3=0):
        """Implements the framing protocol as defined by hybi_
specification supporting protocol version 13::
    
    >>> f = Frame(opcode, 'hello world', os.urandom(4), fin=1)
    >>> data = f.build()
    >>> f = Frame()
    >>> f.parser.send(bytes[1])
    >>> f.parser.send(bytes[2])
    >>> f.parser.send(bytes[2:])
"""
        self.version = version
        self.opcode = opcode
        self.masking_key = masking_key
        self.fin = 0x1 if final else 0
        self.rsv1 = rsv1
        self.rsv2 = rsv2
        self.rsv3 = rsv3
        self.set_body(body)
    
    @property
    def final(self):
        return bool(self.fin)
    
    def on_complete(self, handler):
        opcode = self.opcode
        if opcode == 0x1:
            # UTF-8 data
            msg = self.body.decode("utf-8", "replace")
            return handler.on_message(msg)
        elif opcode == 0x2:
            # Binary data
            return handler.on_message(self.body)
        elif opcode == 0x8:
            # Close
            return handler.close()
        elif opcode == 0x9:
            # Ping
            return self.__class__(0xA,data,fin=True).msg
        elif opcode == 0xA:
            # Pong
            pass
        else:
            return handler.abort()
    
    def set_body(self, body):
        self.body = body
        self.payload_length = None
        if body is not None:
            self.payload_length = len(body)
            if self.opcode:
                self.msg = self._build()
    
    def is_complete(self):
        return self.body is not None
    
    def _build(self):
        """Builds a frame from the instance's attributes.
        """
        header = BytesIO()

        if self.fin > 0x1:
            raise WebSocketProtocolError('FIN bit parameter must be 0 or 1')

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
            header.write(struct.pack('!H', length))
        elif length < (1 << 63):
            header.write(i2b(mask_bit | 127))
            header.write(struct.pack('!Q', length))
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
            header.write(self.body)
        else:
            header.write(self.masking_key)
            header.write(self.mask(self.body))
        
        return header.getvalue()
                
    def mask(self, data):
        """Performs the masking or unmasking operation on data
using the simple masking algorithm::

    j                   = i MOD 4
    transformed-octet-i = original-octet-i XOR masking-key-octet-j
"""
        masked = bytearray(data)
        key = self.masking_key
        if not ispy3k:
            key = map(ord, key)
            
        for i in range(len(data)):
            masked[i] = masked[i] ^ key[i%4]
        return masked

    unmask = mask


class FrameParser(object):
    '''Parser for the version 8 protocol'''
    __slots__ = ('_buf','_frame')
    
    def __init__(self, version):
        self._buf = None
        self._frame = frame(version, None)
        
    def get_frame(self):
        if self._frame.is_complete():
            self._frame = frame(self._frame.version, None)        
        return self._frame
    
    def execute(self, data):
        # end of body can be passed manually by putting a length of 0
        frame = self.get_frame()
        if not data:
            return frame
        if self._buf:
            data = self._buf + data
        # No opcode yet
        if frame.opcode is None:
            first_byte, second_byte = struct.unpack("BB", data[:2])
            frame.fin = (first_byte >> 7) & 1
            frame.rsv1 = (first_byte >> 6) & 1
            frame.rsv2 = (first_byte >> 5) & 1
            frame.rsv3 = (first_byte >> 4) & 1
            frame.opcode = first_byte & 0xf
            if frame.fin not in (0, 1):
                raise WebSocketProtocolError('FIN must be 0 or 1')
            if frame.rsv1 or frame.rsv2 or frame.rsv3:
                pass
                #raise WebSocketProtocolError('RSV must be 0')
            if not (second_byte & 0x80):
                raise WebSocketProtocolError(\
                            'Unmasked frame. Abort connection')
            payload_length = second_byte & 0x7f
            
            # All control frames MUST have a payload length of 125 bytes or less
            if frame.opcode > 0x7 and payload_length > 125:
                raise WebSocketProtocolError('Frame too large')
            
            frame.payload_length = payload_length
            frame.body, data = bytearray(data[:2]), data[2:]
        
        if frame.masking_key is None:
            # All control frames MUST have a payload length of 125 bytes or less
            d = None
            if frame.payload_length == 126:
                if len(data) < 6: # 2 + 4 for mask
                     return self.save_buf(frame, data)
                d, data = d[:2] , d[2:]
                frame.payload_length = struct.unpack("!H", d)[0]
            elif frame.payload_length == 127:
                if len(data) < 12:  # 8 + 4 for mask
                     return self.save_buf(frame, data)
                d, data = d[:8] , d[8:]
                frame.payload_length = struct.unpack("!Q", d)[0]
            elif len(data) < 4:
                return self.save_buf(frame, data)
            
            # The mask is 4 bits
            if d:
                frame.body.extend(d)
            frame.masking_key, data = data[:4], data[4:]
            frame.body.extend(frame.masking_key)
                
        if len(data) < frame.payload_length:
            return self.save_buf(frame, data)
        # We have a frame
        else:
            data = data[:frame.payload_length]
            frame.body.extend(data)
            self.save_buf(frame, data[frame.payload_length:])
            frame.msg = frame.unmask(data)
            return frame
            
    def save_buf(self, frame, data):
        self._buf = data
        return frame
        