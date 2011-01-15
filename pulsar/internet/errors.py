from pulsar.core.errors import PulsarException

class PulsarInternetException(PulsarException):
    '''base class of all Pulsar Internet Exception'''
    
    
class HaltServer(PulsarInternetException):
    
    def __init__(self, reason, exit_status=1):
        self.reason = reason
        self.exit_status = exit_status
    
    def __str__(self):
        return "<HaltServer %r %d>" % (self.reason, self.exit_status)
