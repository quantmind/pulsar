'''
A list of all Exception specific to pulsar library.
'''
import traceback

from .httpurl import Headers


__all__ = ['PulsarException',
           'MonitorStarted',
           'ImproperlyConfigured',
           'CommandError',
           'CommandNotFound',
           'ProtocolError',
           'EventAlreadyRegistered',
           'InvalidOperation',
           'HaltServer',
           # HTTP client exception
           'HttpRequestException',
           'HttpConnectionError',
           'HttpProxyError',
           'SSLError',
           #
           # HTTP Exceptions
           'HttpException',
           'HttpRedirect',
           'BadRequest',
           'Http401',
           'PermissionDenied',
           'Http404',
           'MethodNotAllowed',
           'HttpGone',
           'Unsupported',
           'UnprocessableEntity',
           #
           'format_traceback']


class PulsarException(Exception):
    '''Base class of all Pulsar exceptions.'''


class MonitorStarted(PulsarException):
    exit_code = 0


class ImproperlyConfigured(PulsarException):
    '''A :class:`PulsarException` raised when an inconsistent configuration
    has occured.

    .. attribute:: exit_code

        the exit code when rising this exception is set to 2. This will
        cause pulsar to log the error rather than the full stack trace.
    '''
    exit_code = 2


class CommandError(PulsarException):
    pass


class CommandNotFound(CommandError):

    def __init__(self, name):
        super().__init__('Command "%s" not available' % name)


class ProtocolError(PulsarException):
    '''A :class:`PulsarException` raised when the protocol encounter
    unexpected data.

    It will close the socket connection
    '''
    status_code = None

    def ProtocolError(self, msg=None, status_code=None):
        super().__init__(msg)
        self.status_code = status_code


class EventAlreadyRegistered(PulsarException):
    pass


class InvalidOperation(PulsarException):
    '''An invalid operation in pulsar'''
    pass


class HaltServer(BaseException):
    ''':class:`BaseException` raised to stop a running server.

    When ``exit_code`` is greater than 1, it is considered an expected
    failure and therefore the full stack trace is not logged.'''
    def __init__(self, reason='Exiting server.', exit_code=3):
        super().__init__(reason)
        self.exit_code = exit_code


# #################################################################### HTTP
class HttpRequestException(IOError):
    "Base for all HTTP related errors"
    def __init__(self, *args, **kwargs):
        """
        Initialize RequestException with `request` and `response` objects.
        """
        response = kwargs.pop('response', None)
        self.response = response
        self.request = kwargs.pop('request', None)
        if (response is not None and not self.request and
                hasattr(response, 'request')):
            self.request = self.response.request
        super().__init__(*args, **kwargs)


class HttpConnectionError(HttpRequestException):
    """A Connection error occurred."""


class HttpProxyError(HttpConnectionError):
    """A proxy error occurred."""


class SSLError(HttpConnectionError):
    """An SSL error occurred."""


# #################################################################### HTTP
http_errors = {}


def httperror(c):
    http_errors[c.status] = c
    return c


@httperror
class HttpException(PulsarException):
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
        self.content_type = content_type
        self._headers = Headers.make(headers)
        super().__init__(msg)

    @property
    def headers(self):
        return list(self._headers)


@httperror
class HttpRedirect(HttpException):
    '''An :class:`HttpException` for redirects.

    The :attr:`~HttpException.status` is set to ``302`` by default.
    '''
    status = 302

    def __init__(self, location, status=None, headers=None, **kw):
        headers = Headers.make(headers)
        headers['location'] = location
        super().__init__(status=status or self.status, headers=headers, **kw)

    @property
    def location(self):
        '''The value in the ``Location`` header entry.
        '''
        return self._headers['location']


@httperror
class BadRequest(HttpException):
    status = 400


@httperror
class Http401(HttpException):
    status = 401

    def __init__(self, auth, msg=''):
        headers = [('WWW-Authenticate', auth)]
        super().__init__(msg=msg, headers=headers)


@httperror
class PermissionDenied(HttpException):
    '''An :class:`HttpException` with default ``403`` status code.'''
    status = 403


@httperror
class Http404(HttpException):
    '''An :class:`HttpException` with default ``404`` status code.'''
    status = 404


@httperror
class MethodNotAllowed(HttpException):
    '''An :class:`HttpException` with default ``405`` status code.'''
    status = 405


@httperror
class HttpGone(HttpException):
    status = 410


@httperror
class Unsupported(HttpException):
    status = 415


@httperror
class UnprocessableEntity(HttpException):
    status = 422


def format_traceback(exc):
    return traceback.format_exception(exc.__class__, exc, exc.__traceback__)
