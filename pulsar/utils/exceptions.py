'''
A list of all Exception specific to pulsar library.
'''


class PulsarException(Exception):
    '''Base class of all Pulsar exceptions.'''


class MonitorStarted(PulsarException):
    pass


class ImproperlyConfigured(PulsarException):
    '''A :class:`PulsarException` raised when an inconsistent configuration
has occured.'''
    pass


class CommandError(PulsarException):
    pass


class CommandNotFound(CommandError):

    def __init__(self, name):
        super(CommandNotFound, self).__init__('Command "%s" not available' %
                                              name)


class ProtocolError(PulsarException):
    '''Raised when the protocol encounter unexpected data. It will close
the socket connection.'''
    status_code = None

    def ProtocolError(self, msg=None, status_code=None):
        super(ProtocolError, self).__init__(msg)
        self.status_code = status_code


class TooManyConsecutiveWrite(PulsarException):
    '''Raise when too many consecutive writes are attempted.'''


class TooManyConnections(PulsarException):
    '''Raised when there are too many concurrent connections.'''


class EventAlreadyRegistered(PulsarException):
    pass


class InvalidOperation(PulsarException):
    '''An invalid operation in pulsar'''
    pass


class HaltServer(BaseException):
    ''':class:`BaseException` raised to stop a running server.

    When ``exit_code`` is greater than 1, it is considered an expected
    failure and therefore the full stack trace is not logged.'''
    def __init__(self, reason='Exiting server.', exit_code=2):
        super(HaltServer, self).__init__(reason)
        self.exit_code = exit_code


# #################################################################### HTTP
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
        self.code = self.status  # for compatibility with HTTPError
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

    The :attr:`~HttpException.status` is set to ``302`` by default.
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

        Equivalent to ``self.headers['location']``.
        '''
        return self.headers['location']


class BadRequest(HttpException):
    status = 400


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
