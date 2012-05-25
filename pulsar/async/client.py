import io
import logging

from pulsar import create_connection, CouldNotParse

from .defer import make_async

from .iostream import AsyncIOStream

__all__ = ['SocketClient']


LOGGER = logging.getLogger('pulsar.client')

class SocketClient(object):
    parsercls = None
    blocking = True
    
    def __init__(self, address=None, sock=None, parsercls=None, blocking=None):
        parsercls = parsercls or self.parsercls
        self.parser = parsercls()
        self.sock = sock
        if isinstance(sock, AsyncIOStream):
            blocking = 0
        self.blocking = blocking if blocking is not None else self.blocking
        self.buffer = bytearray()
        self.on_connect = None
        if address:
            self.connect(address)
        
    def __repr__(self):
        return '%s %s' % (self.__class__.__name__,self.address or '')
    __str__ = __repr__
    
    @property
    def address(self):
        if self.sock:
            return self.sock.getsockname()
        
    def connect(self, address):
        if not self.sock:
            if self.blocking:
                self.sock = create_connection(address, blocking=True)
            else:
                self.sock = AsyncIOStream()
                self.on_connect = self.sock.connect(address)
                return self.on_connect
    
    def fileno(self):
        if self.sock:
            return self.sock.fileno()
    
    def close(self):
        if self.sock:
            self.sock.close()
            self.sock = None
    
    def send(self, data):
        if self.sock:
            data = self.parser.encode(data)
            if self.blocking:
                self.sock.sendall(data)
            elif self.on_connect is None:
                return self.sock.write(data)
            else:
                return self.on_connect.add_callback(
                                lambda r: self._async_write(data))
            
    def read(self, bytes_sent=None):
        if self.sock:
            if self.blocking:
                return self._read_sync()
            else:
                if self.on_connect is None:
                    return self._async_read()
                else:
                    return self.on_connect.add_callback(self._async_read)
    
    def execute(self, data):
        r = make_async(self.send(data)).add_callback(self.read)
        return r.result_or_self()
        
    def parsedata(self, data):
        buffer = self.buffer
        if data:
            buffer.extend(data)
        parsed_data = None
        try:
            parsed_data, buffer = self.parser.decode(buffer)
        except CouldNotParse:
            self.warning('Could not parse data', exc_info=True)
            buffer = bytearray()
        self.buffer = buffer
        return self.on_parsed_data(parsed_data)
    
    def on_parsed_data(self, data):
        return data
    
    def on_read_error(self):
        pass
    
    # INTERNALS

    def _read_sync(self):
        length = io.DEFAULT_BUFFER_SIZE
        data = True
        while data:
            data = self.sock.recv(length)
            if not data:
                self.close()
            else:
                msg = self.parsedata(data)
                if msg:
                    return msg
        
    def _async_write(self, data):
        self.on_connect = None
        return self.sock.write(data)
    
    def _async_read(self, result=None):
        self.on_connect = None
        r = self.sock.read()
        if not self.sock.closed:
            return r.add_callback(self.parsedata, self._read_error)
        else:
            return self.parsedata(r)
            
    def _read_error(self, failure):
        failure.log()
        self.sock.close()
        self.on_read_error()
        return failure

