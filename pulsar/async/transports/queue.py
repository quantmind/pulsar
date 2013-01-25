from . import transport


class QueueTransport(transport.Transport):
    default_timeout = 0.5
    
    def __init__(self, queue):
        self.queue = queue
        
    def write(self, data):
        self.queue.put(data)
        
    def read(self):
        try:
            return self.queue.poll(self.timeout)
        except (Empty, IOError, TypeError, EOFError):
            return () 