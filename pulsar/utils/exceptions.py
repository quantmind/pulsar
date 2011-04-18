
class PulsarException(Exception):
    '''base class of all Pulsar Exception'''


class PulsarInternetException(PulsarException):
    '''base class of all Pulsar Internet Exception'''
    
    
class PulsarPoolAlreadyStarted(PulsarException):
    '''A :class:`PulsarException` raised when trying to start a worker pool already started'''
    
class HaltServer(PulsarInternetException):
    
    def __init__(self, reason, signal=None):
        self.reason = reason
        self.signal = signal
    
    def __str__(self):
        return "<HaltServer %r %d>" % (self.reason, self.exit_status)
