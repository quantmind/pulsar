'''
A list of all Exception specific to pulsar library.
'''
class PulsarException(Exception):
    '''Base class of all Pulsar exceptions.'''


class ImproperlyConfigured(PulsarException):
    '''A :class:`PulsarException` raised when an inconsistent configuration
has occured.'''
    pass


class CommandError(PulsarException):
    pass


class CommandNotFound(CommandError):

    def __init__(self, name):
        super(CommandNotFound, self).__init__(
                            'Command "%s" not available' % name)
        
class ProtocolError(Exception):
    '''Raised when the protocol encounter unexpected data. It will close
the socket connection.'''


class TooManyConnections(RuntimeError):
    '''Raised when there are too many concurrent connections.'''
    
    
class EventAlreadyRegistered(RuntimeError):
    pass
    

class ConnectionError(Exception):
    pass



class StopEventLoop(BaseException):
    ''':class:`BaseException` raised to cleanly stop a
    :class:`pulsar.EventLoop`.'''
    
    
class HaltServer(BaseException):
    ''':class:`BaseException` raised to stop a running server.
    
    When ``exit_code`` is greater than 1, it is considered an expected
    failure and therefore the full stack trace is not logged.'''
    def __init__(self, reason='Exiting server.', exit_code=1):
        super(HaltServer, self).__init__(reason)
        self.exit_code = exit_code


class CannotCallBackError(PulsarException):
    pass


class NotRegisteredWithServer(PulsarException):
    pass


##################################################################### HTTP
class HTTPError(PulsarException):
    "Base for all HTTP related errors."
    pass

class SSLError(HTTPError):
    "Raised when SSL certificate fails in an HTTPS connection."
    pass

class HttpException(HTTPError):
    '''The base class of all ``HTTP`` server exceptions
    
Introduces the following attributes:

.. attribute:: status

    The numeric status code for the exception (ex 500 for server error).
    
    Default: ``500``.
    
.. attribute:: headers

    Additional headers to add to the client response.
'''
    status = 500
    def __init__(self, msg='', status=None, handler=None, strict=False,
                 headers=None, content_type=None):
        self.status = status or self.status
        self.code = self.status # for compatibility with HTTPError
        self.handler = handler
        self.strict = strict
        self.headers = headers
        self.content_type = content_type
        super(HttpException, self).__init__(msg)
        
    @property
    def hdrs(self):
        return self.headers


class HttpRedirect(HttpException):
    '''An :class:`HttpException` for redirects.
    
The :attr:`HttpException.status` is set to ``302`` by default.
'''    
    status = 302
    def __init__(self, location, status=None, headers=None, **kw):
        headers = [] if headers is None else headers
        headers.append(('location', location))
        super(HttpRedirect, self).__init__(status=status or self.status,
                                           headers=headers, **kw)

    @property
    def location(self):
        '''The value in the ``Location`` header entry.
        
It is a proxy for ``self.headers['location'].'''
        return self.headers['location']
    

class PermissionDenied(HttpException):
    '''An :class:`HttpException` with default ``403`` status code.'''
    status = 403


class Http404(HttpException):
    '''An :class:`HttpException` with default ``404`` status code.'''
    status = 404

    
class MethodNotAllowed(HttpException):
    '''An :class:`HttpException` with default ``405`` status code.'''
    status = 405
    
    
class ContentNotAccepted(HttpException):
    '''An :class:`HttpException` with default ``406`` status code.'''
    status = 406