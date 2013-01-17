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


class EchoServerConsumer(pulsar.ProtocolConsumer):
    '''Protocol consumer for the server'''
    separator = b'\r\n'
    def feed(self, data):
        idx = data.find(self.separator)
        if idx: # we have the message
            idx += len(self.separator)
            data, rest = data[:idx], data[idx:]
            if rest:
                raise pulsar.ProtocolError
            self.write(data)
            self.finished()
        else:
            self.write(data)
    
    
class EchoProtocol(pulsar.ClientProtocolConsumer):
    separator = b'\r\n'
    @property
    def buffer(self):
        if not hasattr(self, '_buffer'):
            self._buffer = b''
        return self._buffer

    def feed(self, data):
        idx = data.find(self.separator)
        if idx: # we have the message
            data, rest = data[:idx], data[idx+len(self.separator):]
            if rest:
                raise pulsar.ProtocolError
            self.consumer(data)
            self.message = self.buffer + data
            self.finished() # done with this response
        else:
            self.buffer = self.buffer + data

    def send(self, *args):
        message = self.request.message.encode('utf-8') + self.separator
        self.write(message)
        

class Echo(pulsar.Client):
    response_factory = EchoProtocol

    def __init__(self, address, **params):
        super(Echo, self).__init__(**params)
        self.address = address

    def request(self, message, consumer=None):
        request = pulsar.Request(self.address, self.timeout)
        request.message = message
        return self.response(request, consumer)
        


def server(description=None, **kwargs):
    description = description or 'Echo Server'
    return SocketServer(callable=EchoServerConsumer,
                        description=description,
                        **kwargs)
    

if __name__ == '__main__':  #pragma nocover
    server().start()