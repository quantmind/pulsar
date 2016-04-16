"""This section introduces classes used by pulsar
:ref:`wsgi application <apps-wsgi>` to pass a request/response state
during an HTTP request.

.. contents::
    :local:

The :class:`WsgiRequest` is a thin wrapper around a WSGI ``environ``
dictionary.
It contains only the ``environ`` as its private data.
The :class:`WsgiResponse`, which is available in the
:class:`WsgiRequest.response` attribute, is an iterable over bytestring with
several utility methods for manipulating headers and asynchronous content.


Environ Mixin
=====================

.. autoclass:: EnvironMixin
   :members:
   :member-order: bysource


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
from functools import reduce, partial
from http.client import responses
import asyncio

from pulsar import isawaitable, chain_future, HttpException
from pulsar.utils.structures import AttributeDictionary
from pulsar.utils.httpurl import (Headers, SimpleCookie,
                                  has_empty_content, REDIRECT_CODES,
                                  ENCODE_URL_METHODS, JSON_CONTENT_TYPES,
                                  remove_double_slash, iri_to_uri,
                                  is_absolute_uri, parse_options_header)

from .content import HtmlDocument
from .utils import (set_wsgi_request_class, set_cookie, query_dict,
                    parse_accept_header, LOGGER)
from .structures import ContentAccept, CharsetAccept, LanguageAccept
from .formdata import parse_form_data


__all__ = ['EnvironMixin', 'WsgiResponse',
           'WsgiRequest', 'cached_property']

MAX_BUFFER_SIZE = 2**16
ONEMB = 2**20


def redirect(path, code=None, permanent=False):
    if code is None:
        code = 301 if permanent else 302

    assert code in REDIRECT_CODES, 'Invalid redirect status code.'
    return WsgiResponse(code, response_headers=[('location', path)])


def cached_property(method):
    name = method.__name__

    def _(self):
        if name not in self.cache:
            self.cache[name] = method(self)
        return self.cache[name]

    return property(_, doc=method.__doc__)


def wsgi_encoder(gen, encoding):
    for data in gen:
        if isinstance(data, str):
            yield data.encode(encoding)
        else:
            yield data


class WsgiResponse:
    """A WSGI response.

    Instances are callable using the standard WSGI call and, importantly,
    iterable::

        response = WsgiResponse(200)

    A :class:`WsgiResponse` is an iterable over bytes to send back to the
    requesting client.

    .. attribute:: status_code

        Integer indicating the HTTP status, (i.e. 200)

    .. attribute:: response

        String indicating the HTTP status (i.e. 'OK')

    .. attribute:: status

        String indicating the HTTP status code and response (i.e. '200 OK')

    .. attribute:: content_type

        The content type of this response. Can be ``None``.

    .. attribute:: headers

        The :class:`.Headers` container for this response.

    .. attribute:: environ

        The dictionary of WSGI environment if passed to the constructor.

    .. attribute:: cookies

        A python :class:`SimpleCookie` container of cookies included in the
        request as well as cookies set during the response.
    """
    _iterated = False
    _started = False
    DEFAULT_STATUS_CODE = 200

    def __init__(self, status=None, content=None, response_headers=None,
                 content_type=None, encoding=None, environ=None,
                 can_store_cookies=True):
        self.environ = environ
        self.status_code = status or self.DEFAULT_STATUS_CODE
        self.encoding = encoding
        self.cookies = SimpleCookie()
        self.headers = Headers(response_headers, kind='server')
        self.content = content
        self._can_store_cookies = can_store_cookies
        if content_type is not None:
            self.content_type = content_type

    @property
    def started(self):
        return self._started

    @property
    def iterated(self):
        return self._iterated

    @property
    def path(self):
        if self.environ:
            return self.environ.get('PATH_INFO', '')

    @property
    def method(self):
        if self.environ:
            return self.environ.get('REQUEST_METHOD')

    @property
    def connection(self):
        if self.environ:
            return self.environ.get('pulsar.connection')

    @property
    def content(self):
        return self._content

    @content.setter
    def content(self, content):
        if not self._iterated:
            if content is None:
                content = ()
            else:
                if isinstance(content, str):
                    if not self.encoding:   # use utf-8 if not set
                        self.encoding = 'utf-8'
                    content = content.encode(self.encoding)

            if isinstance(content, bytes):
                content = (content,)
            self._content = content
        else:
            raise RuntimeError('Cannot set content. Already iterated')

    def _get_content_type(self):
        return self.headers.get('content-type')

    def _set_content_type(self, typ):
        if typ:
            self.headers['content-type'] = typ
        else:
            self.headers.pop('content-type', None)
    content_type = property(_get_content_type, _set_content_type)

    @property
    def response(self):
        return responses.get(self.status_code)

    @property
    def status(self):
        return '%s %s' % (self.status_code, self.response)

    def __str__(self):
        return self.status

    def __repr__(self):
        return '%s(%s)' % (self.__class__.__name__, self)

    @property
    def is_streamed(self):
        """Check if the response is streamed.

        A streamed response is an iterable with no length information.
        In this case streamed means that there is no information about
        the number of iterations.

        This is usually `True` if a generator is passed to the response object.
        """
        try:
            len(self.content)
        except TypeError:
            return True
        return False

    def can_set_cookies(self):
        if self.status_code < 400:
            return self._can_store_cookies

    def length(self):
        if not self.is_streamed:
            return reduce(lambda x, y: x+len(y), self.content, 0)

    def start(self, start_response):
        assert not self._started
        self._started = True
        return start_response(self.status, self.get_headers())

    def __iter__(self):
        if self._iterated:
            raise RuntimeError('WsgiResponse can be iterated once only')
        self._started = True
        self._iterated = True
        if self.is_streamed:
            return wsgi_encoder(self.content, self.encoding or 'utf-8')
        else:
            return iter(self.content)

    def close(self):
        """Close this response, required by WSGI
        """
        if self.is_streamed:
            if hasattr(self.content, 'close'):
                self.content.close()

    def set_cookie(self, key, **kwargs):
        """
        Sets a cookie.

        ``expires`` can be a string in the correct format or a
        ``datetime.datetime`` object in UTC. If ``expires`` is a datetime
        object then ``max_age`` will be calculated.
        """
        set_cookie(self.cookies, key, **kwargs)

    def delete_cookie(self, key, path='/', domain=None):
        set_cookie(self.cookies, key, max_age=0, path=path, domain=domain,
                   expires='Thu, 01-Jan-1970 00:00:00 GMT')

    def get_headers(self):
        """The list of headers for this response
        """
        headers = self.headers
        if has_empty_content(self.status_code, self.method):
            headers.pop('content-type', None)
            headers.pop('content-length', None)
            self._content = ()
        else:
            if not self.is_streamed:
                cl = 0
                for c in self.content:
                    cl += len(c)
                if cl == 0 and self.content_type in JSON_CONTENT_TYPES:
                    self._content = (b'{}',)
                    cl = len(self._content[0])
                headers['Content-Length'] = str(cl)
            ct = self.content_type
            # content type encoding available
            if self.encoding:
                ct = ct or 'text/plain'
                if 'charset=' not in ct:
                    ct = '%s; charset=%s' % (ct, self.encoding)
            if ct:
                headers['Content-Type'] = ct
        if self.can_set_cookies():
            for c in self.cookies.values():
                headers.add_header('Set-Cookie', c.OutputString())
        return list(headers)

    def has_header(self, header):
        return header in self.headers
    __contains__ = has_header

    def __setitem__(self, header, value):
        self.headers[header] = value

    def __getitem__(self, header):
        return self.headers[header]


class EnvironMixin:
    """A wrapper around a WSGI_ environ.

    Instances of this class have the :attr:`environ` attribute as their
    only private data. Every other attribute is stored in the :attr:`environ`
    itself at the ``pulsar.cache`` wsgi-extension key.

    .. attribute:: environ

        WSGI_ environ dictionary
    """
    __slots__ = ('environ',)

    def __init__(self, environ, name=None):
        self.environ = environ
        if 'pulsar.cache' not in environ:
            environ['pulsar.cache'] = AttributeDictionary()
            self.cache.mixins = {}
        if name:
            self.cache.mixins[name] = self

    @property
    def cache(self):
        """An :ref:`attribute dictionary <attribute-dictionary>` of
        pulsar-specific data stored in the :attr:`environ` at
        the wsgi-extension key ``pulsar.cache``
        """
        return self.environ['pulsar.cache']

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

    def __getattr__(self, name):
        mixin = self.cache.mixins.get(name)
        if mixin is None:
            raise AttributeError("'%s' object has no attribute '%s'" %
                                 (self.__class__.__name__, name))
        return mixin

    def get(self, key, default=None):
        """Shortcut to the :attr:`environ` get method."""
        return self.environ.get(key, default)


class WsgiRequest(EnvironMixin):
    """An :class:`EnvironMixin` for wsgi requests."""
    def __init__(self, environ, app_handler=None, urlargs=None):
        super().__init__(environ)
        self.cache.cfg = environ.get('pulsar.cfg', {})
        if app_handler:
            self.cache.app_handler = app_handler
            self.cache.urlargs = urlargs

    def __repr__(self):
        return self.path

    def __str__(self):
        return self.__repr__()

    @cached_property
    def content_types(self):
        """List of content types this client supports as a
        :class:`.ContentAccept` object.

        Obtained form the ``Accept`` request header.
        """
        return parse_accept_header(self.environ.get('HTTP_ACCEPT'),
                                   ContentAccept)

    @cached_property
    def charsets(self):
        """List of charsets this client supports as a
        :class:`.CharsetAccept` object.

        Obtained form the ``Accept-Charset`` request header.
        """
        return parse_accept_header(self.environ.get('HTTP_ACCEPT_CHARSET'),
                                   CharsetAccept)

    @cached_property
    def encodings(self):
        """List of encodings this client supports as
        :class:`.Accept` object.

        Obtained form the ``Accept-Charset`` request header.
        Encodings in a HTTP term are compression encodings such as gzip.
        For charsets have a look at :attr:`charsets` attribute.
        """
        return parse_accept_header(self.environ.get('HTTP_ACCEPT_ENCODING'))

    @cached_property
    def languages(self):
        """List of languages this client accepts as
        :class:`.LanguageAccept` object.

        Obtained form the ``Accept-Language`` request header.
        """
        return parse_accept_header(self.environ.get('HTTP_ACCEPT_LANGUAGE'),
                                   LanguageAccept)

    @cached_property
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
        return self.cache.urlargs

    @property
    def cfg(self):
        """The :ref:`config container <settings>` of the server
        """
        return self.cache.cfg

    @cached_property
    def response(self):
        """The :class:`WsgiResponse` for this client request.
        """
        return WsgiResponse(environ=self.environ)

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
        """Shortcut to the :attr:`~EnvironMixin.environ` ``PATH_INFO`` value.
        """
        return self.environ.get('PATH_INFO', '/')

    @property
    def uri(self):
        return self.absolute_uri()

    @property
    def method(self):
        """The request method (uppercase)."""
        return self.environ['REQUEST_METHOD']

    @cached_property
    def encoding(self):
        return self.content_type_options[1].get('charset', 'utf-8')

    @cached_property
    def content_type_options(self):
        content_type = self.environ.get('CONTENT_TYPE')
        if content_type:
            return parse_options_header(content_type)
        else:
            return None, {}

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
            value = self.cache.data_and_files

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

    def _data_and_files(self, data=True, files=True, stream=None, future=None):
        if future is None:
            data_files = parse_form_data(self.environ, stream=stream)
            if isawaitable(data_files):
                return chain_future(
                    data_files,
                    partial(self._data_and_files, data, files, stream))
        else:
            data_files = future

        self.cache.data_and_files = data_files
        return self.data_and_files(data, files, stream)

    @cached_property
    def url_data(self):
        """A (cached) dictionary containing data from the ``QUERY_STRING``
        in :attr:`~.EnvironMixin.environ`.
        """
        return query_dict(self.environ.get('QUERY_STRING', ''),
                          encoding=self.encoding)

    @cached_property
    def html_document(self):
        """Return a cached instance of :class:`.HtmlDocument`."""
        return HtmlDocument()

    def get_host(self, use_x_forwarded=True):
        """Returns the HTTP host using the environment or request headers."""
        # We try three options, in order of decreasing preference.
        if use_x_forwarded and ('HTTP_X_FORWARDED_HOST' in self.environ):
            host = self.environ['HTTP_X_FORWARDED_HOST']
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

    def absolute_uri(self, location=None, scheme=None):
        """Builds an absolute URI from ``location`` and variables
        available in this request.

        If no ``location`` is specified, the relative URI is built from
        :meth:`full_path`.
        """
        if not is_absolute_uri(location):
            location = self.full_path(location)
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
    dictionary. Alternatively one can use the :func:`~file_response`
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
            future = asyncio.Future()
            future.set_result(data)
            yield future

    def close(self):
        close_object(self.file)
