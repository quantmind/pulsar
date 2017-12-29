"""This section introduces classes used by pulsar
:ref:`wsgi application <apps-wsgi>` to pass a request/response state
during an HTTP request.

.. contents::
    :local:

The :class:`WsgiRequest` is a thin wrapper around a WSGI ``environ``
dictionary.
It contains only the ``environ`` as its private data.
The :class:`WsgiResponse`, which is available in the
:class:`WsgiRequest.response` attribute, is an iterable over bytes with
several utility methods for manipulating headers and asynchronous content.


.. _app-wsgi-request:

Wsgi Request
=====================

.. autoclass:: WsgiRequest
   :members:
   :member-order: bysource


.. _wsgi-response:

Wsgi Response
=====================

.. autoclass:: WsgiResponse
   :members:
   :member-order: bysource



Wsgi File Wrapper
=====================

.. autoclass:: FileWrapper
   :members:
   :member-order: bysource


.. _WSGI: http://www.wsgi.org
.. _AJAX: http://en.wikipedia.org/wiki/Ajax_(programming)
.. _TLS: http://en.wikipedia.org/wiki/Transport_Layer_Security
"""
from functools import partial
from inspect import isawaitable
from http.cookies import SimpleCookie

from pulsar.api import chain_future, HttpException, create_future
from pulsar.utils.lib import WsgiResponse, wsgi_cached
from pulsar.utils.system import json
from pulsar.utils.httpurl import (
    REDIRECT_CODES, ENCODE_URL_METHODS,
    remove_double_slash, iri_to_uri, is_absolute_uri, parse_options_header
)

from .content import HtmlDocument
from .utils import (set_wsgi_request_class, query_dict,
                    parse_accept_header, LOGGER, PULSAR_CACHE)
from .structures import ContentAccept, CharsetAccept, LanguageAccept
from .formdata import parse_form_data
from .headers import LOCATION


HEAD = 'HEAD'
MAX_BUFFER_SIZE = 2**16
ONEMB = 2**20
TEXT_PLAIN = 'text/plain'


def redirect(path, code=None, permanent=False):
    if code is None:
        code = 301 if permanent else 302

    assert code in REDIRECT_CODES, 'Invalid redirect status code.'
    return WsgiResponse(code, response_headers=[(LOCATION, path)])


class WsgiRequest:
    """A wsgi request
    """
    __slots__ = ('environ',)

    def __init__(self, environ, app_handler=None, urlargs=None):
        self.environ = environ
        if app_handler:
            self.cache.app_handler = app_handler
            self.cache.urlargs = urlargs

    def __repr__(self):
        return self.path

    def __str__(self):
        return self.__repr__()

    @property
    def cache(self):
        """The protocol consumer used as a cache for
        pulsar-specific data. Stored in the :attr:`environ` at
        the wsgi-extension key ``pulsar.cache``
        """
        return self.environ[PULSAR_CACHE]

    @property
    def connection(self):
        """The :class:`.Connection` handling the request
        """
        return self.environ.get('pulsar.connection')

    @property
    def _loop(self):
        """Event loop if :attr:`connection` is available.
        """
        c = self.connection
        if c:
            return c._loop

    @wsgi_cached
    def first_line(self):
        env = self.environ
        try:
            return '%s %s' % (env['REQUEST_METHOD'], self.absolute_uri())
        except Exception:
            return '%s %s' % (env.get('REQUEST_METHOD'), env.get('PATH'))

    @wsgi_cached
    def content_types(self):
        """List of content types this client supports as a
        :class:`.ContentAccept` object.

        Obtained form the ``Accept`` request header.
        """
        return parse_accept_header(self.environ.get('HTTP_ACCEPT'),
                                   ContentAccept)

    @wsgi_cached
    def charsets(self):
        """List of charsets this client supports as a
        :class:`.CharsetAccept` object.

        Obtained form the ``Accept-Charset`` request header.
        """
        return parse_accept_header(self.environ.get('HTTP_ACCEPT_CHARSET'),
                                   CharsetAccept)

    @wsgi_cached
    def encodings(self):
        """List of encodings this client supports as
        :class:`.Accept` object.

        Obtained form the ``Accept-Charset`` request header.
        Encodings in a HTTP term are compression encodings such as gzip.
        For charsets have a look at :attr:`charsets` attribute.
        """
        return parse_accept_header(self.environ.get('HTTP_ACCEPT_ENCODING'))

    @wsgi_cached
    def languages(self):
        """List of languages this client accepts as
        :class:`.LanguageAccept` object.

        Obtained form the ``Accept-Language`` request header.
        """
        return parse_accept_header(self.environ.get('HTTP_ACCEPT_LANGUAGE'),
                                   LanguageAccept)

    @wsgi_cached
    def cookies(self):
        """Container of request cookies
        """
        cookies = SimpleCookie()
        cookie = self.environ.get('HTTP_COOKIE')
        if cookie:
            cookies.load(cookie)
        return cookies

    @property
    def app_handler(self):
        """The WSGI application handling this request.

        The WSGI handler is responsible for setting this value in the
        same way as the :class:`.Router` does.
        """
        return self.cache.app_handler

    @property
    def urlargs(self):
        """Dictionary of url parameters obtained when matching a
        :ref:`router <wsgi-router>` with this request :attr:`path`."""
        args = self.cache.urlargs
        return args if args is not None else {}

    @property
    def cfg(self):
        """The :ref:`config container <settings>` of the server
        """
        return self.cache.cfg

    @property
    def logger(self):
        """logger

        Allow for injection of different logger
        """
        return self.cache.logger

    @wsgi_cached
    def response(self):
        """The :class:`WsgiResponse` for this client request.
        """
        return WsgiResponse()

    #######################################################################
    #    environ shortcuts
    @property
    def is_xhr(self):
        """``True`` if this is an AJAX_ request
        """
        return self.environ.get('HTTP_X_REQUESTED_WITH') == 'XMLHttpRequest'

    @property
    def is_secure(self):
        """``True`` if this request is via a TLS_ connection
        """
        return self.environ.get('HTTPS') == 'on'

    @property
    def path(self):
        """Shortcut to the :attr:`~.environ` ``PATH_INFO`` value.
        """
        return self.environ.get('PATH_INFO', '/')

    @property
    def uri(self):
        return self.absolute_uri()

    @property
    def method(self):
        """The request method (uppercase)."""
        return self.environ['REQUEST_METHOD']

    @property
    def input(self):
        return self.environ.get('wsgi.input')

    @wsgi_cached
    def encoding(self):
        return self.content_type_options[1].get('charset', 'utf-8')

    @wsgi_cached
    def content_type_options(self):
        content_type = self.environ.get('CONTENT_TYPE')
        if content_type:
            return parse_options_header(content_type)
        else:
            return None, {}

    def get(self, key, default=None):
        """Shortcut to the :attr:`environ` get method."""
        return self.environ.get(key, default)

    def data_and_files(self, data=True, files=True, stream=None):
        """Retrieve body data.

        Returns a two-elements tuple of a
        :class:`~.MultiValueDict` containing data from
        the request body, and data from uploaded files.

        If the body data is not ready, return a :class:`~asyncio.Future`
        which results in the tuple.

        The result is cached.
        """
        if self.method in ENCODE_URL_METHODS:
            value = {}, None
        else:
            value = self.cache.get('data_and_files')

        if not value:
            return self._data_and_files(data, files, stream)
        elif data and files:
            return value
        elif data:
            return value[0]
        elif files:
            return value[1]
        else:
            return None

    def body_data(self):
        """A :class:`~.MultiValueDict` containing data from the request body.
        """
        return self.data_and_files(files=False)

    def _data_and_files(self, data=True, files=True, stream=None, result=None):
        if result is None:
            data_files = parse_form_data(self, stream=stream)
            if isawaitable(data_files):
                return chain_future(
                    data_files,
                    partial(self._data_and_files, data, files, stream))
        else:
            data_files = result

        self.cache.data_and_files = data_files
        return self.data_and_files(data, files, stream)

    @wsgi_cached
    def url_data(self):
        """A (cached) dictionary containing data from the ``QUERY_STRING``
        in :attr:`~.environ`.
        """
        return query_dict(self.environ.get('QUERY_STRING', ''),
                          encoding=self.encoding)

    @wsgi_cached
    def html_document(self):
        """Return a cached instance of :class:`.HtmlDocument`."""
        return HtmlDocument()

    def get_host(self, use_x_forwarded=True):
        """Returns the HTTP host using the environment or request headers."""
        # We try three options, in order of decreasing preference.
        if use_x_forwarded and ('HTTP_X_FORWARDED_HOST' in self.environ):
            host = self.environ['HTTP_X_FORWARDED_HOST']
            port = self.environ.get('HTTP_X_FORWARDED_PORT')
            if port and port != ('443' if self.is_secure else '80'):
                host = '%s:%s' % (host, port)
            return host
        elif 'HTTP_HOST' in self.environ:
            host = self.environ['HTTP_HOST']
        else:
            # Reconstruct the host using the algorithm from PEP 333.
            host = self.environ['SERVER_NAME']
            server_port = str(self.environ['SERVER_PORT'])
            if server_port != ('443' if self.is_secure else '80'):
                host = '%s:%s' % (host, server_port)
        return host

    def get_client_address(self, use_x_forwarded=True):
        """Obtain the client IP address
        """
        xfor = self.environ.get('HTTP_X_FORWARDED_FOR')
        if use_x_forwarded and xfor:
            return xfor.split(',')[-1].strip()
        else:
            return self.environ['REMOTE_ADDR']

    def full_path(self, *args, **query):
        """Return a full path"""
        path = None
        if args:
            if len(args) > 1:
                raise TypeError("full_url() takes exactly 1 argument "
                                "(%s given)" % len(args))
            path = args[0]
        if not path:
            path = self.path
        elif not path.startswith('/'):
            path = remove_double_slash('%s/%s' % (self.path, path))
        return iri_to_uri(path, query)

    def absolute_uri(self, location=None, scheme=None, **query):
        """Builds an absolute URI from ``location`` and variables
        available in this request.

        If no ``location`` is specified, the relative URI is built from
        :meth:`full_path`.
        """
        if not is_absolute_uri(location):
            if location or location is None:
                location = self.full_path(location, **query)
            if not scheme:
                scheme = self.is_secure and 'https' or 'http'
            base = '%s://%s' % (scheme, self.get_host())
            return '%s%s' % (base, location)
        elif not scheme:
            return iri_to_uri(location)
        else:
            raise ValueError('Absolute location with scheme not valid')

    def redirect(self, path, **kw):
        """Redirect to a different ``path``
        """
        return redirect(path, **kw)

    def set_response_content_type(self, response_content_types=None):
        '''Evaluate the content type for the response to a client ``request``.

        The method uses the :attr:`response_content_types` parameter of
        accepted content types and the content types accepted by the client
        ``request`` and figures out the best match.
        '''
        request_content_types = self.content_types
        if request_content_types:
            ct = request_content_types.best_match(response_content_types)
            if ct and '*' in ct:
                ct = None
            if not ct and response_content_types:
                raise HttpException(status=415, msg=request_content_types)
            self.response.content_type = ct

    def json_response(self, data, status_code=None):
        ct = 'application/json'
        content_types = self.content_types
        if not content_types or ct in content_types:
            response = self.response
            response.content_type = ct
            response.content = json.dumps(data)
            if status_code:
                response.status_code = status_code
            return response
        else:
            raise HttpException(status=415, msg=content_types)


set_wsgi_request_class(WsgiRequest)


def close_object(iterator):
    if hasattr(iterator, 'close'):
        try:
            iterator.close()
        except Exception:
            LOGGER.exception('Error while closing wsgi iterator')


class FileWrapper:
    """WSGI File wrapper class.

    Available directly from the ``wsgi.file_wrapper`` key in the WSGI environ
    dictionary. Alternatively one can use the :func:`~.file_response`
    high level function for serving local files.
    """
    def __init__(self, file, block=None):
        self.file = file
        self.block = max(block or ONEMB, MAX_BUFFER_SIZE)

    def __iter__(self):
        while True:
            data = self.file.read(self.block)
            if not data:
                break
            future = create_future()
            future.set_result(data)
            yield future

    def close(self):
        close_object(self.file)
