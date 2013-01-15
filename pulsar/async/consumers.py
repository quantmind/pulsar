from .protocols import ProtocolConsumer

__all__ = ['ServerChunkConsumer']


class ServerChunkConsumer(ProtocolConsumer):
    '''A consumer of chunks on the server.'''
    def __init__(self, connection):
        super(ServerChunkConsumer, self).__init__(connection)
        self._buffer = bytearray()
    
    def feed(self, data):
        self._buffer.extend(data)
        message, data = self.decode(bytes(self._buffer))
        if message:
            self._buffer = bytearray()
            self.responde(message)
        return data  
        
    def decode(self, data):
        raise NotImplementedError
    
    def responde(self, message):
        '''Write back to the client or server'''
        self.write(message)
        self.finished()