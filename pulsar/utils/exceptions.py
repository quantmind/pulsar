
class PulsarException(Exception):
    '''Base class of all Pulsar Exception'''
    
class Timeout(PulsarException):
    '''Raised when a timeout occurs'''
    def __init__(self, msg, timeout = None):
        self.timeout = timeout
        if timeout:
            msg = msg + ' Timeout {0} surpassed.'.format(self.timeout)
        super(Timeout,self).__init__(msg)

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


class CannotCallBackError(PulsarException):
    pass


class AlreadyRegistered(PulsarException):
    pass

class NotRegisteredWithServer(PulsarException):
    pass


class BadHttpRequest(PulsarInternetException):
    status_code = 500
    def __init__(self, status_code = None, reason = ''):
        self.status_code = status_code or self.status_code
        super(BadHttpRequest,self).__init__(reason)
        
        
class BadHttpResponse(BadHttpRequest):
    pass