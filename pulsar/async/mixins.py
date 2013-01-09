

class ConcurrentServer(object):
    
    def __new__(cls, *args, **kwargs):
        o = super(ConcurrentServer, cls).__new__(cls)
        o.received = 0
        o.concurrent_requests = set()
        return o
        
    @property
    def concurrent_request(self):
        return len(self.concurrent_requests)