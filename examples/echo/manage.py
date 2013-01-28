'''A very simple Echo server/client using Pulsar. To run the server::

    python manage.py
    
Open a new shell, in this directory, launch python and type::

    >>> from manage import Echo
    >>> p = Echo('localhost:8060')
    >>> response = p.request('Hello')
'''
try:
    import pulsar
except ImportError: #pragma nocover
    import sys
    sys.path.append('../../')
    
import pulsar
from pulsar.apps.socket import SocketServer


class EchoProtocol(pulsar.ProtocolConsumer):
    '''Protocol consumer for the client'''
    separator = b'\r\n\r\n'
    
    def data_received(self, data):
        idx = data.find(self.separator)
        if idx: # we have a full message
            idx += len(self.separator)
            data, rest = data[:idx], data[idx:]
            self.response(data)
            self.finished()
            return rest
        
    def response(self, data):
        # We store the result
        self.result = data[:len(self.separator)]
        

class EchoServerProtocol(EchoProtocol):
    
    def response(self, data):
        # write it back
        self.transport.write(data)
    
    
class Echo(pulsar.Client):
    consumer_factory = EchoProtocol

    def __init__(self, address, **params):
        super(Echo, self).__init__(**params)
        self.address = address

    def request(self, message):
        request = pulsar.Request(self.address, self.timeout)
        request.message = message
        return self.response(request, consumer)
        


def server(description=None, **kwargs):
    description = description or 'Echo Server'
    return SocketServer(callable=EchoServerProtocol,
                        description=description,
                        **kwargs)
    

if __name__ == '__main__':  #pragma nocover
    server().start()