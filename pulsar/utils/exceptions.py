
class PulsarException(Exception):
    '''Base class of all Pulsar Exception'''
    
class Timeout(PulsarException):
    '''Raised when a timeout occurs'''

class ImproperlyConfigured(PulsarException):
    '''A :class:`PulsarException` raised when pulsar has inconsistent
configuration.'''
    pass

class PulsarInternetException(PulsarException):
    '''base class of all Pulsar Internet Exception'''
    pass

class MailboxError(PulsarException):
    pass
    
class ActorAlreadyStarted(PulsarException):
    '''A :class:`PulsarException` raised when trying to start an actor already started'''
    
class HaltServer(PulsarInternetException):
    
    def __init__(self, reason, signal=None):
        super(HaltServer,self).__init__(reason)
        self.signal = signal


class ConnectionError(PulsarInternetException):
    pass
    

class DeferredFailure(PulsarException):
    pass

class AlreadyCalledError(PulsarException):
    '''Raised when a :class:`Deferred` instance receives more than
one :meth:`Deferred.callback`.'''

class AlreadyRegistered(PulsarException):
    pass

class NotRegisteredWithServer(PulsarException):
    pass


class BadHttpRequest(PulsarInternetException):
    status = 500
    def __init__(self, status = None, reason = ''):
        self.status = status or self.status
        super(BadHttpRequest,self).__init__(reason)
        
        
class BadHttpResponse(BadHttpRequest):
    pass