
.. _tutorials-writing-clients:

=====================
Writing Clients
=====================

Pulsar provides several classes for writing clients. The first step
is to sublcass :class:`pulsar.ClientProtocolConsumer` which controls how::


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
            

The ``Echo`` client is built by subclassing :class:`pulsar.Client`::

    class Echo(pulsar.Client):
        response_factory = EchoProtocol
        
        def __init__(self, address, **params):
            super(Echo, self).__init__(**params)
            self.address = address
            
        def request(self, message, consumer=None):
            request = pulsar.Request(self.address, self.timeout)
            request.message = message
            return self.response(request, consumer)
        

To send requests::

    >>> echo = Echo('localhost:8060')
    >>> response = echo.request('Hello!')
    

