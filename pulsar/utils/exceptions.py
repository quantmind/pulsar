
class PulsarException(Exception):
    '''Base class of all Pulsar Exception'''

class Timeout(PulsarException):
    '''Raised when a timeout occurs'''
    def __init__(self, msg, timeout=None):
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


class CommandError(PulsarException):
    '''Exception raised when executing a Command'''
    pass


class CommandNotFound(PulsarException):

    def __init__(self, name):
        super(CommandNotFound, self).__init__(
                            'Command "%s" not available' % name)

class AuthenticationError(PulsarException):
    pass


class ConnectionError(PulsarInternetException):
    pass


class CouldNotParse(PulsarInternetException):
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


##################################################################### HTTP
class HttpException(Exception):
    status = 500
    def __init__(self, msg='', status=None, handler=None, strict=False,
                 headers=None):
        self.status = status or self.status
        self.handler = handler
        self.strict = strict
        self.headers = headers
        super(HttpException,self).__init__(msg)


class Http404(HttpException):
    status = 404


class HttpRedirect(HttpException):
    status = 302
    def __init__(self, location):
        self.location = location


class PermissionDenied(HttpException):
    status = 403