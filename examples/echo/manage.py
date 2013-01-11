'''A very simple Echo server in Pulsar::

    python manage.py
    
To see options type::

    python manage.py -h
'''
try:
    import pulsar
except ImportError: #pragma nocover
    import sys
    sys.path.append('../../')
    
import pulsar
from pulsar.apps.socket import SocketServer


class EchoProtocol(pulsar.ProtocolResponse):
    separator = '\r\n\r\n\r\n'  #CRLF
    
    @property
    def buffer(self):
        if not hasattr(self, '_buffer'):
            self._buffer = bytearray()
        return self._buffer
    
    def finished(self):
        return not self.buffer
        
    def feed(self, data):
        idx = buffer.find(separator)
        idx = buffer.find(separator)
        if idx < 0:
            self.buffer.extend(buffer)
        else:   # got the full message
            idx += len(self.separator)
            self.buffer.extend(data[:idx])
            self.write(bytes(self.buffer))
            self.buffer = bytearray()
            return data[idx:]
        

def server(description=None, **kwargs):
    description = description or 'Pulsar Redis Server'
    return SocketServer(callable=EchoProtocol,
                        description=description,
                        **kwargs)
    

if __name__ == '__main__':  #pragma nocover
    server().start()