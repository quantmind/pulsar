# -*- coding: utf-8 -*-
'''WebSocket_ Protocol :class:`Frame` and :class:`FrameParser` classes.
These two classes can be used for both clients and server protocols.

.. _WebSocket: http://tools.ietf.org/html/rfc6455'''
import os
from struct import pack, unpack
from array import array
from io import BytesIO

from .pep import ispy3k, range, to_bytes
from .exceptions import ProtocolError

DEFAULT_VERSION = 13
SUPPORTED_VERSIONS = (DEFAULT_VERSION,)
WS_EXTENSIONS = {}
WS_PROTOCOLS = {}

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
    try:
        version = int(version or DEFAULT_VERSION)
    except Exception:
        pass
    if version not in SUPPORTED_VERSIONS:
        raise ProtocolError('Version %s not supported.' % version)
    return version

    
class Extension(object):
    
    def receive(self, data):
        return data
        
    def send(self, data):
        return data
    
    
class Frame(object):
    """Implements the framing protocol as defined by
specification supporting protocol version 13::
    
    >>> f = Frame('hello world', final=1)
    >>> data = f.build()
    >>> f = Frame()
    >>> f.parser.send(bytes[1])
    >>> f.parser.send(bytes[2])
    >>> f.parser.send(bytes[2:])

.. attribute:: version

    The version of the frame protocol.
        
.. attribute:: masking_key

    The frame masking key used for encoding and decoding data.
"""
    __slots__ = ('fin', 'opcode', 'masking_key', 'rsv1', 'rsv2', 'rsv3',
                 'msg', 'version', 'payload_length', 'body')
    def __init__(self, message=None, opcode=None, version=None,
                 masking_key=None, final=False, rsv1=0, rsv2=0, rsv3=0,
                 build_frame=True):
        self.version = get_version(version)
        if opcode is None and message is not None:
            opcode = 0x1 if is_text_data(message) else 0x2
        message = to_bytes(message or b'')
        self.payload_length = len(message)
        if opcode is None:
            raise ProtocolError('WEBSOCKET opcode not available')
        self.version = version
        self.opcode = opcode
        if masking_key:
            masking_key = to_bytes(masking_key)
            if len(masking_key) != 4:
                raise ProtocolError('WEBSOCKET masking key must be 4 bytes long')
        self.masking_key = masking_key
        self.fin = 0x1 if final else 0
        self.rsv1 = rsv1
        self.rsv2 = rsv2
        self.rsv3 = rsv3
        if build_frame:
            self.body = message
            self.msg = self._build_frame(message)
        else:
            self.body = None
            self.msg = bytearray(message)
        
    @property
    def final(self):
        '''Flag indicating if this is a final frame or not.'''
        return bool(self.fin)
    
    @property
    def masked(self):
        '''Indicate if the frame is masked.'''
        return bool(self.masking_key)
    
    @property
    def is_message(self):
        '''Indicate if the frame is a ``string`` message.'''
        return self.opcode == 0x1
    
    @property
    def is_bytes(self):
        '''Indicate if the frame is a ``string`` message.'''
        return self.opcode == 0x2
    
    @property
    def is_data(self):
        '''Indicate if the frame is a ``string`` or ``bytes``.'''
        return self.opcode in (0x1, 0x2)
    
    @property
    def is_ping(self):
        '''A :class:`Frame` representing a ping.'''
        return self.opcode == 0x9
    
    @property
    def is_close(self):
        '''A :class:`Frame` representing a close request.'''
        return self.opcode == 0x8
    
    def _build_frame(self, message):
        #Builds a frame from the instance's attributes
        header = BytesIO()
        if 0x3 <= self.opcode <= 0x7 or 0xB <= self.opcode:
            raise ProtocolError('WEBSOCKET opcode cannot be a reserved opcode')
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
            raise ProtocolError('WEBSOCKET frame too large')
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
        '''Performs the masking or un-masking operation on data using the
simple masking algorithm::
    
    j = i MOD 4
    transformed-octet-i = original-octet-i XOR masking-key-octet-j
    
This method is invoked when encoding/decoding frames with a :attr:`masking_key`
attribute set.'''
        mask_size = len(self.masking_key)
        key = array('B', self.masking_key)
        data = array('B', data)
        for i in range(len(data)):
            data[i] ^= key[i % mask_size]
        return data.tobytes() if ispy3k else data.tostring()
    unmask = mask

    
class FrameParser(object):
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
    def __init__(self, version=None, kind=0, extensions=None, protocols=None):
        self.version = get_version(version)
        self._ext_middleware, self._extensions =\
            self.ws_middleware(extensions, WS_EXTENSIONS)
        self._pro_middleware, self._protocols =\
            self.ws_middleware(extensions, WS_PROTOCOLS)
        self._frame = None  #current frame
        self._buf = None
        self._kind = kind

    def ws_middleware(self, names, group):
        mw = []
        av = []
        if names:
            for name in names:
                if name in group:
                    av.append(name)
                    mw.append(group[name]())
        return mw, tuple(av)
    
    @property
    def kind(self):
        return self._kind
    
    @property
    def extensions(self):
        return self._extensions
    
    @property
    def protocols(self):
        return self._protocols
    
    @property
    def masked(self):
        return self.kind == 1
    
    @property
    def expect_masked(self):
        return True if self.kind == 0 else False
    
    def encode(self, data, final=True, masking_key=None, **params):
        '''Encode data into a :class:`Frame`.
        
:parameter final: Indicating if this is a final Frame.
:parameter masking_key: Optional making key used only if :attr:`kind` is 1 
    (Client frames).
    '''
        if self.masked:
            masking_key = masking_key or os.urandom(4)
            return Frame(data, masking_key=masking_key, final=final, **params)
        else:
            return Frame(data, final=final, **params)
    
    def ping(self, body=None):
        '''return a `ping` :class:`Frame`.'''
        return self.encode(body, opcode=0x9)
    
    def pong(self, body=None):
        '''return a `pong` :class:`Frame`.'''
        return self.encode(body, opcode=0xA)
    
    def close(self, body=None):
        '''return a `close` :class:`Frame`.'''
        return self.encode(body, opcode=0x8)
    
    def continuation(self, body=None, final=True):
        '''return a `continuation` :class:`Frame`.'''
        return self.encode(body, opcode=0, final=final)
    
    def replay_to(self, frame):
        '''Build a :class:`Frame` as a reply to *frame*.'''
        if frame.opcode == 0x9:
            return self.pong(frame.body)
        
    def decode(self, data=None):
        '''Decode bytes data into a :class:`Frame`. If :attr:`kind` is 0
it decodes into a client frame (masked frame) while if is 1 or 2 it decodes
into a server frame (unmasked).'''
        if data:
            if self._buf:
                data = self._buf + data
        else:
            data = self._buf
        if not data: 
            return None
        masked_frame = self.expect_masked
        frame = self._frame
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
                raise ProtocolError('FIN must be 0 or 1')
            is_masked = bool(second_byte & 0x80)
            if masked_frame != is_masked:
                if masked_frame:
                    raise ProtocolError('WEBSOCKET unmasked client frame.')
                else:
                    raise ProtocolError('WEBSOCKET masked server frame.')
            payload_length = second_byte & 0x7f
            # All control frames MUST have a payload length of 125 bytes or less
            if opcode > 0x7 and payload_length > 125:
                raise ProtocolError('WEBSOCKET frame too large')
            msg, data = data[:2], data[2:]
            frame = Frame(msg, opcode, self.version, final=fin,
                          rsv1=rsv1, rsv2=rsv2, rsv3=rsv3, build_frame=False)
            frame.payload_length = payload_length
        #   
        if frame.masking_key is None:
            # All control frames MUST have a payload length of 125 bytes or less
            d = None
            mask_length = 4 if masked_frame else 0
            if frame.payload_length == 0x7e: #126
                if len(data) < 2 + mask_length: # 2 + 4 for mask
                     return self.save_buf(frame, data)
                d, data = data[:2] , data[2:]
                frame.payload_length = unpack("!H", d)[0]
            elif frame.payload_length == 0x7f: #127
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
            self.save_buf(frame, data)
        # We have a frame
        else:
            payload = data[:frame.payload_length] # payload data
            frame.msg.extend(payload)
            self.save_buf(None, data[frame.payload_length:])
            for extension in self.extensions:
                data = extension.receive(frame, data)
            if frame.masking_key:
                payload = frame.unmask(payload)
            if frame.opcode == 0x1:
                payload = payload.decode("utf-8", "replace")
            frame.body = payload
            return frame
            
    def save_buf(self, frame, data):
        self._frame = frame
        self._buf = data
        