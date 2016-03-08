"""Pulsar ships with a fully featured, :class:`.HttpClient`
class for multiple asynchronous HTTP requests.

To get started, one builds a client::

    from pulsar.apps import http
    client = http.HttpClient()

and than makes requests, in a coroutine::

    response = await http.get('http://www.bbc.co.uk')

Making requests
=================
Pulsar HTTP client has no dependencies and an API similar to requests_::

    from pulsar.apps import http
    client = http.HttpClient()
    response = await client.get('https://github.com/timeline.json')

``response`` is a :class:`HttpResponse` object which contains all the
information about the request and the result:

    >>> request = response.request
    >>> print(request.headers)
    Connection: Keep-Alive
    User-Agent: pulsar/0.8.2-beta.1
    Accept-Encoding: deflate, gzip
    Accept: */*
    >>> response.status_code
    200
    >>> print(response.headers)
    ...

The :attr:`~.ProtocolConsumer.request` attribute of :class:`HttpResponse`
is an instance of :class:`.HttpRequest`.

Posting data and Parameters
=============================

You can attach parameters to the ``url`` by passing the
``urlparams`` dictionary:

    response = await http.get('http://bla.com',
                              urlparams={'page': 2, 'key': 'foo'})
    response.request.full_url == 'http://bla.com?page=2&key=foo'


.. _http-cookie:

Cookie support
================

Cookies are handled by the client by storing cookies received with responses.
To disable cookie one can pass ``store_cookies=False`` during
:class:`HttpClient` initialisation.

If a response contains some Cookies, you can get quick access to them::

    response = await client.get(...)
    type(response.cookies)
    <type 'dict'>

To send your own cookies to the server, you can use the cookies parameter::

    response = await client.get(..., cookies={'sessionid': 'test'})


.. _http-authentication:

Authentication
======================

Authentication, either ``basic`` or ``digest``, can be added to a
client by invoking

* :meth:`HttpClient.add_basic_authentication` or
* :meth:`HttpClient.add_digest_authentication`

In either case the authentication is handled by adding additional headers
to your requests.

TLS/SSL
=================
Supported out of the box::

    client.get('https://github.com/timeline.json')

The :class:`.HttpClient` can verify SSL certificates for HTTPS requests,
just like a web browser. To check a host's SSL certificate, you can use the
``verify`` argument:

    client = HttpClient()
    client.verify is True
    client = HttpClient(verify=False)
    client.verify is False

By default, ``verify`` is set to True.

You can override the ``verify`` argument during requests too:

    res1 = await client.get('https://github.com/timeline.json')
    res2 = await client.get('https://locahost:8020', verify=False)

You can pass ``verify`` the path to a CA_BUNDLE file or directory with
certificates of trusted CAs:

    res3 = await client.get('https://locahost:8020',
                            verify='/path/to/ca_bundle')


.. _http-streaming:

Streaming
=========================

This is an event-driven client, therefore streaming support is native.

The easyiest way to use streaming is to pass the ``stream=True`` parameter
during a request and access the :attr:`HttpResponse.raw` attribute.
For example::

    response = await http.get(..., stream=True)
    for data in response.raw:
        # data can be a future or bytes

The ``raw`` attribute is an iterable over bytes or Futures resulting in bytes.

Another approach to streaming is to use the
:ref:`data_processed <http-many-time-events>` event handler.
For example::

    def new_data(response, **kw):
        if response.status_code == 200:
            data = response.recv_body()
            # do something with this data

    response = http.get(..., data_processed=new_data)

The response :meth:`~.HttpResponse.recv_body` method fetches the parsed body
of the response and at the same time it flushes it.
Check the :ref:`proxy server <tutorials-proxy-server>` example for an
application using the :class:`HttpClient` streaming capabilities.

.. _http-websocket:

WebSocket
==============

The http client support websocket upgrades. First you need to have a
websocket handler::

    from pulsar.apps import ws

    class Echo(ws.WS):

        def on_message(self, websocket, message):
            websocket.write(message)

The websocket response is obtained by::

    ws = await http.get('ws://...', websocket_handler=Echo())

.. _http-redirects:

Redirects & Decompression
=============================

[TODO]

Synchronous Mode
=====================

Can be used in :ref:`synchronous mode <tutorials-synchronous>`::

    client = HttpClient(loop=new_event_loop())

Events
==============
:ref:`Events <event-handling>` control the behaviour of the
:class:`.HttpClient` when certain conditions occur. They are useful for
handling standard HTTP event such as :ref:`redirects <http-redirects>`,
:ref:`websocket upgrades <http-websocket>`,
:ref:`streaming <http-streaming>` or anything your application
requires.

.. _http-one-time-events:

One time events
~~~~~~~~~~~~~~~~~~~

There are three :ref:`one time events <one-time-event>` associated with an
:class:`HttpResponse` object:

* ``pre_request``, fired before the request is sent to the server. Callbacks
  receive the *response* argument.
* ``on_headers``, fired when response headers are available. Callbacks
  receive the *response* argument.
* ``post_request``, fired when the response is done. Callbacks
  receive the *response* argument.

Adding event handlers can be done at client level::

    def myheader_handler(response, exc=None):
        if not exc:
            print('got headers!')

    client.bind_event('on_headers', myheader_handler)

or at request level::

    response = client.get(..., on_headers=myheader_handler)

By default, the :class:`HttpClient` has one ``pre_request`` callback for
handling `HTTP tunneling`_, three ``on_headers`` callbacks for
handling *100 Continue*, *websocket upgrade* and :ref:`cookies <http-cookie>`,
and one ``post_request`` callback for handling redirects.

.. _http-many-time-events:

Many time events
~~~~~~~~~~~~~~~~~~~

In addition to the three :ref:`one time events <http-one-time-events>`,
the Http client supports two additional
events which can occur several times while processing a given response:

* ``data_received`` is fired when new data has been received but not yet
  parsed
* ``data_processed`` is fired just after the data has been parsed by the
  :class:`.HttpResponse`. This is the event one should bind to when performing
  :ref:`http streaming <http-streaming>`.


both events support handlers with a signature::

    def handler(response, data=None):
        ...

where ``response`` is the :class:`.HttpResponse` handling the request and
``data`` is the **raw** data received.

API
==========

The main class here is the :class:`HttpClient` which is a subclass of
:class:`.AbstractClient`.


HTTP Client
~~~~~~~~~~~~~~~~~~

.. autoclass:: HttpClient
   :members:
   :member-order: bysource


HTTP Request
~~~~~~~~~~~~~~~~~~

.. autoclass:: HttpRequest
   :members:
   :member-order: bysource

HTTP Response
~~~~~~~~~~~~~~~~~~

.. autoclass:: HttpResponse
   :members:
   :member-order: bysource


.. _module:: pulsar.apps.http.oauth

OAuth1
~~~~~~~~~~~~~~~~~~

.. autoclass:: OAuth1
   :members:
   :member-order: bysource

OAuth2
~~~~~~~~~~~~~~~~~~

.. autoclass:: OAuth2
   :members:
   :member-order: bysource


.. _requests: http://docs.python-requests.org/
.. _`uri scheme`: http://en.wikipedia.org/wiki/URI_scheme
.. _`HTTP tunneling`: http://en.wikipedia.org/wiki/HTTP_tunnel
"""
import os
import platform
import logging
import asyncio
from functools import partial
from collections import namedtuple
from io import StringIO, BytesIO
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse
from http.client import responses
try:
    import ssl
    BaseSSLError = ssl.SSLError
except ImportError:
    ssl = None

    class BaseSSLError(Exception):
        pass

import pulsar
from pulsar import (AbortRequest, AbstractClient, Pool, Connection,
                    isawaitable, ProtocolConsumer, ensure_future,
                    HttpRequestException, HttpConnectionError, SSLError)
from pulsar.utils import websocket
from pulsar.utils.system import json
from pulsar.utils.pep import native_str, to_bytes
from pulsar.utils.structures import mapping_iterator
from pulsar.utils.httpurl import (http_parser, ENCODE_URL_METHODS,
                                  encode_multipart_formdata,
                                  Headers, get_environ_proxies,
                                  choose_boundary, request_host,
                                  is_succesful,
                                  get_hostport, cookiejar_from_dict,
                                  host_no_default_port, http_chunks,
                                  parse_options_header,
                                  parse_header_links,
                                  JSON_CONTENT_TYPES)

from .plugins import (handle_cookies, handle_100, WebSocket, Redirect,
                      Tunneling, TooManyRedirects, start_request,
                      response_content, keep_alive, HTTP11)

from .auth import Auth, HTTPBasicAuth, HTTPDigestAuth
from .oauth import OAuth1, OAuth2
from .stream import HttpStream, StreamConsumedError


__all__ = ['HttpRequest', 'HttpResponse', 'HttpClient', 'HTTPDigestAuth',
           'TooManyRedirects', 'Auth', 'OAuth1', 'OAuth2',
           'HttpStream', 'StreamConsumedError']


scheme_host = namedtuple('scheme_host', 'scheme netloc')
tls_schemes = ('https', 'wss')

LOGGER = logging.getLogger('pulsar.http')
FORM_URL_ENCODED = 'application/x-www-form-urlencoded'
MULTIPART_FORM_DATA = 'multipart/form-data'


def guess_filename(obj):
    """Tries to guess the filename of the given object."""
    name = getattr(obj, 'name', None)
    if name and name[0] != '<' and name[-1] != '>':
        return os.path.basename(name)


def is_streamed(data):
    try:
        len(data)
    except TypeError:
        return True
    return False


class RequestBase:
    inp_params = None
    release_connection = True
    history = None
    full_url = None
    scheme = None

    @property
    def unverifiable(self):
        """Unverifiable when a redirect.

        It is a redirect when :attr:`history` has past requests.
        """
        return bool(self.history)

    @property
    def origin_req_host(self):
        if self.history:
            return self.history[0].request.origin_req_host
        else:
            return request_host(self)

    @property
    def type(self):
        return self.scheme

    def get_full_url(self):
        return self.full_url

    def write_body(self, transport):
        pass


class HttpTunnel(RequestBase):
    first_line = None
    data = None

    def __init__(self, request, scheme, host):
        self.status = 0
        self.request = request
        self.scheme = scheme
        self.host, self.port = get_hostport(scheme, host)
        self.full_url = '%s://%s:%s' % (scheme, self.host, self.port)
        self.parser = request.parser
        request.new_parser()
        self.headers = request.client.tunnel_headers.copy()

    def __repr__(self):
        return 'Tunnel %s' % self.full_url
    __str__ = __repr__

    @property
    def key(self):
        return self.request.key

    @property
    def address(self):
        return (self.host, self.port)

    @property
    def client(self):
        return self.request.client

    @property
    def version(self):
        return self.request.version

    def encode(self):
        req = self.request
        self.headers['host'] = req.get_header('host')
        bits = req.target_address + (req.version,)
        self.first_line = 'CONNECT %s:%s %s\r\n' % bits
        return b''.join((self.first_line.encode('ascii'), bytes(self.headers)))

    def has_header(self, header_name):
        return header_name in self.headers

    def get_header(self, header_name, default=None):
        return self.headers.get(header_name, default)

    def remove_header(self, header_name):
        self.headers.pop(header_name, None)

    def apply(self, response, handler):
        """Tunnel the connection if needed
        """
        connection = response.connection
        tunnel_status = getattr(connection, '_tunnel_status', 0)
        if not tunnel_status:
            connection._tunnel_status = 1
            response.bind_event('pre_request', handler)
        elif tunnel_status == 1:
            connection._tunnel_status = 2
            response._request = self
            response.bind_event('on_headers', handler.on_headers)


class HttpRequest(RequestBase):
    """An :class:`HttpClient` request for an HTTP resource.

    This class has a similar interface to :class:`urllib.request.Request`.

    :param files: optional dictionary of name, file-like-objects.
    :param allow_redirects: allow the response to follow redirects.

    .. attribute:: method

        The request method

    .. attribute:: version

        HTTP version for this request, usually ``HTTP/1.1``

    .. attribute:: encode_multipart

        If ``True`` (default), defaults POST data as ``multipart/form-data``.
        Pass ``encode_multipart=False`` to default to
        ``application/x-www-form-urlencoded``. In any case, this parameter is
        overwritten by passing the correct content-type header.

    .. attribute:: history

        List of past :class:`.HttpResponse` (collected during redirects).

    .. attribute:: wait_continue

        if ``True``, the :class:`HttpRequest` includes the
        ``Expect: 100-Continue`` header.

    .. attribute:: stream

        Allow for streaming body

    """
    _proxy = None
    _ssl = None
    _tunnel = None
    _write_done = False

    def __init__(self, client, url, method, inp_params=None, headers=None,
                 data=None, files=None, history=None, auth=None,
                 charset=None, encode_multipart=True, multipart_boundary=None,
                 source_address=None, allow_redirects=False, max_redirects=10,
                 decompress=True, version=None, wait_continue=False,
                 websocket_handler=None, cookies=None, urlparams=None,
                 stream=False, proxies=None, verify=True, **ignored):
        self.client = client
        self._data = None
        self.files = files
        self.urlparams = urlparams
        self.inp_params = inp_params or {}
        self.unredirected_headers = Headers(kind='client')
        self.method = method.upper()
        self.full_url = url
        if urlparams:
            self._encode_url(urlparams)
        self.history = history
        self.wait_continue = wait_continue
        self.max_redirects = max_redirects
        self.allow_redirects = allow_redirects
        self.charset = charset or 'utf-8'
        self.version = version
        self.decompress = decompress
        self.encode_multipart = encode_multipart
        self.multipart_boundary = multipart_boundary
        self.websocket_handler = websocket_handler
        self.source_address = source_address
        self.stream = stream
        self.verify = verify
        self.new_parser()
        if self._scheme in tls_schemes:
            self._ssl = client.ssl_context(verify=self.verify, **ignored)
        if auth and not isinstance(auth, Auth):
            auth = HTTPBasicAuth(*auth)
        self.auth = auth
        self.headers = client.get_headers(self, headers)
        cookies = cookiejar_from_dict(client.cookies, cookies)
        if cookies:
            cookies.add_cookie_header(self)
        self.unredirected_headers['host'] = host_no_default_port(self._scheme,
                                                                 self._netloc)
        self.data = data
        self._set_proxy(proxies)

    @property
    def address(self):
        """``(host, port)`` tuple of the HTTP resource
        """
        return self._tunnel.address if self._tunnel else (self.host, self.port)

    @property
    def target_address(self):
        return (self.host, int(self.port))

    @property
    def ssl(self):
        """Context for TLS connections.

        If this is a tunneled request and the tunnel connection is not yet
        established, it returns ``None``.
        """
        if not self._tunnel:
            return self._ssl

    @property
    def key(self):
        tunnel = self._tunnel.full_url if self._tunnel else None
        return (self.scheme, self.host, self.port, tunnel, self.verify)

    @property
    def proxy(self):
        """Proxy server for this request.
        """
        return self._proxy

    @property
    def tunnel(self):
        """Tunnel for this request.
        """
        return self._tunnel

    @property
    def netloc(self):
        if self._proxy:
            return self._proxy.netloc
        else:
            return self._netloc

    def __repr__(self):
        return self.first_line()
    __str__ = __repr__

    @property
    def full_url(self):
        """Full url of endpoint
        """
        return urlunparse((self._scheme, self._netloc, self.path,
                           self.params, self.query, self.fragment))

    @full_url.setter
    def full_url(self, url):
        self._scheme, self._netloc, self.path, self.params,\
            self.query, self.fragment = urlparse(url)
        if not self._netloc and self.method == 'CONNECT':
            self._scheme, self._netloc, self.path, self.params,\
                self.query, self.fragment = urlparse('http://%s' % url)

    @property
    def data(self):
        """Body of request
        """
        return self._data

    @data.setter
    def data(self, data):
        self._data = self._encode_data(data)

    def first_line(self):
        if self._proxy:
            if self.method == 'CONNECT':
                url = self._netloc
            else:
                url = self.full_url
        else:
            url = urlunparse(('', '', self.path or '/', self.params,
                              self.query, self.fragment))
        return '%s %s %s' % (self.method, url, self.version)

    def new_parser(self):
        self.parser = self.client.http_parser(kind=1,
                                              decompress=self.decompress,
                                              method=self.method)

    def is_chunked(self):
        return self.data and 'content-length' not in self.headers

    def encode(self):
        """The bytes representation of this :class:`HttpRequest`.

        Called by :class:`HttpResponse` when it needs to encode this
        :class:`HttpRequest` before sending it to the HTTP resource.
        """
        # Call body before fist_line in case the query is changes.
        first_line = self.first_line()
        if self.data and self.wait_continue:
            self.headers['expect'] = '100-continue'
        headers = self.headers
        if self.unredirected_headers:
            headers = self.unredirected_headers.copy()
            headers.update(self.headers)
        buffer = [first_line.encode('ascii'), b'\r\n',  bytes(headers)]
        return b''.join(buffer)

    def add_header(self, key, value):
        self.headers[key] = value

    def has_header(self, header_name):
        """Check ``header_name`` is in this request headers.
        """
        return (header_name in self.headers or
                header_name in self.unredirected_headers)

    def get_header(self, header_name, default=None):
        """Retrieve ``header_name`` from this request headers.
        """
        return self.headers.get(
            header_name, self.unredirected_headers.get(header_name, default))

    def remove_header(self, header_name):
        """Remove ``header_name`` from this request.
        """
        val1 = self.headers.pop(header_name, None)
        val2 = self.unredirected_headers.pop(header_name, None)
        return val1 or val2

    def add_unredirected_header(self, header_name, header_value):
        self.unredirected_headers[header_name] = header_value

    def write_body(self, transport):
        assert not self._write_done, 'Body already sent'
        self._write_done = True
        if not self.data:
            return
        if is_streamed(self.data):
            ensure_future(self._write_streamed_data(transport),
                          loop=transport._loop)
        else:
            self._write_body_data(transport, self.data, True)

    # INTERNAL ENCODING METHODS
    def _encode_data(self, data):
        body = None
        if self.method in ENCODE_URL_METHODS:
            self.files = None
            self._encode_url(data)
        elif isinstance(data, bytes):
            assert self.files is None, ('data cannot be bytes when files are '
                                        'present')
            body = data
        elif isinstance(data, str):
            assert self.files is None, ('data cannot be string when files are '
                                        'present')
            body = to_bytes(data, self.charset)
        elif data and is_streamed(data):
            assert self.files is None, ('data cannot be an iterator when '
                                        'files are present')
            if 'content-type' not in self.headers:
                self.headers['content-type'] = 'application/octet-stream'
            if 'content-length' not in self.headers:
                self.headers['transfer-encoding'] = 'chunked'
            return data
        elif data or self.files:
            if self.files:
                body, content_type = self._encode_files(data)
            else:
                body, content_type = self._encode_params(data)
            # set files to None, Important!
            self.files = None
            self.headers['Content-Type'] = content_type
        if body:
            self.headers['content-length'] = str(len(body))
        elif 'expect' not in self.headers:
            self.headers.pop('content-length', None)
            self.headers.pop('content-type', None)
        return body

    def _encode_url(self, data):
        query = self.query
        if data:
            data = native_str(data)
            if isinstance(data, str):
                data = parse_qsl(data)
            else:
                data = mapping_iterator(data)
            query = parse_qsl(query)
            query.extend(data)
            query = urlencode(query)
        self.query = query

    def _encode_files(self, data):
        fields = []
        for field, val in mapping_iterator(data or ()):
            if (isinstance(val, str) or isinstance(val, bytes) or
                    not hasattr(val, '__iter__')):
                val = [val]
            for v in val:
                if v is not None:
                    if not isinstance(v, bytes):
                        v = str(v)
                    fields.append((field.decode('utf-8') if
                                   isinstance(field, bytes) else field,
                                   v.encode('utf-8') if isinstance(v, str)
                                   else v))
        for (k, v) in mapping_iterator(self.files):
            # support for explicit filename
            ft = None
            if isinstance(v, (tuple, list)):
                if len(v) == 2:
                    fn, fp = v
                else:
                    fn, fp, ft = v
            else:
                fn = guess_filename(v) or k
                fp = v
            if isinstance(fp, bytes):
                fp = BytesIO(fp)
            elif isinstance(fp, str):
                fp = StringIO(fp)
            if ft:
                new_v = (fn, fp.read(), ft)
            else:
                new_v = (fn, fp.read())
            fields.append((k, new_v))
        #
        return encode_multipart_formdata(fields, charset=self.charset)

    def _encode_params(self, data):
        content_type = self.headers.get('content-type')
        # No content type given, chose one
        if not content_type:
            if self.encode_multipart:
                content_type = MULTIPART_FORM_DATA
            else:
                content_type = FORM_URL_ENCODED

        if content_type in JSON_CONTENT_TYPES:
            body = json.dumps(data).encode(self.charset)
        elif content_type == FORM_URL_ENCODED:
            body = urlencode(data).encode(self.charset)
        elif content_type == MULTIPART_FORM_DATA:
            body, content_type = encode_multipart_formdata(
                data, boundary=self.multipart_boundary, charset=self.charset)
        else:
            raise ValueError("Don't know how to encode body for %s" %
                             content_type)
        return body, content_type

    def _write_body_data(self, transport, data, finish=False):
        if self.is_chunked():
            data = http_chunks(data, finish)
        elif data:
            data = (data,)
        else:
            return
        for chunk in data:
            transport.write(chunk)

    @asyncio.coroutine
    def _write_streamed_data(self, transport):
        for data in self.data:
            if isawaitable(data):
                data = yield from data
            self._write_body_data(transport, data)
        self._write_body_data(transport, b'', True)

    # PROXY INTERNALS
    def _set_proxy(self, proxies):
        request_proxies = self.client.proxies.copy()
        if proxies:
            request_proxies.update(proxies)
        self.proxies = request_proxies
        self.scheme = self._scheme
        self._set_hostport(self._scheme, self._netloc)
        #
        if self.scheme in request_proxies:
            hostonly = self.host
            no_proxy = [n for n in request_proxies.get('no', '').split(',')
                        if n]
            if not any(map(hostonly.endswith, no_proxy)):
                url = request_proxies[self.scheme]
                p = urlparse(url)
                if not p.scheme:
                    raise ValueError('Could not understand proxy %s' % url)
                scheme = p.scheme
                host = p.netloc
                if not self._ssl:
                    self.scheme = scheme
                    self._set_hostport(scheme, host)
                    self._proxy = scheme_host(scheme, host)
                else:
                    self._tunnel = HttpTunnel(self, scheme, host)

    def _set_hostport(self, scheme, host):
        self._tunnel = None
        self._proxy = None
        self.host, self.port = get_hostport(scheme, host)


class HttpResponse(ProtocolConsumer):
    """A :class:`.ProtocolConsumer` for the HTTP client protocol.

    Initialised by a call to the :class:`HttpClient.request` method.

    There are two events you can yield in a coroutine:

    .. attribute:: on_headers

        fired once the response headers are received.

    .. attribute:: on_finished

        Fired once the whole request has finished

    Public API:
    """
    _tunnel_host = None
    _has_proxy = False
    _content = None
    _data_sent = None
    _status_code = None
    _cookies = None
    _raw = None
    request_again = None
    ONE_TIME_EVENTS = ('pre_request', 'on_headers', 'post_request')

    @property
    def parser(self):
        request = self.request
        if request:
            return request.parser

    def __repr__(self):
        return '<Response [%s]>' % (self.status_code or 'None')
    __str__ = __repr__

    @property
    def status_code(self):
        """Numeric status code such as 200, 404 and so forth.

        Available once the :attr:`on_headers` has fired.
        """
        return self._status_code

    @property
    def url(self):
        """The request full url.
        """
        request = self.request
        if request:
            return request.full_url

    @property
    def history(self):
        request = self.request
        if request:
            return request.history

    @property
    def headers(self):
        if not hasattr(self, '_headers'):
            if self.parser and self.parser.is_headers_complete():
                self._headers = Headers(self.parser.get_headers())
        return getattr(self, '_headers', None)

    @property
    def is_error(self):
        if self.status_code:
            return not is_succesful(self.status_code)
        else:
            return False

    @property
    def ok(self):
        return not self.is_error

    @property
    def cookies(self):
        """Dictionary of cookies set by the server or ``None``.
        """
        return self._cookies

    @property
    def content(self):
        """Content of the response, in bytes
        """
        return response_content(self)

    @property
    def raw(self):
        """A raw asynchronous Http response
        """
        if self._raw is None:
            self._raw = HttpStream(self)
        return self._raw

    @property
    def links(self):
        """Returns the parsed header links of the response, if any
        """
        headers = self.headers or {}
        header = headers.get('link')
        l = {}
        if header:
            links = parse_header_links(header)
            for link in links:
                key = link.get('rel') or link.get('url')
                l[key] = link
        return l

    def recv_body(self):
        """Flush the response body and return it.
        """
        return self.parser.recv_body()

    def get_status(self):
        code = self.status_code
        if code:
            return '%d %s' % (code, responses.get(code, 'Unknown'))

    def text(self, charset=None, errors=None):
        """Decode content as a string.
        """
        data = self.content
        if data is not None:
            return data.decode(charset or 'utf-8', errors or 'strict')
    content_string = text

    def json(self, charset=None):
        """Decode content as a JSON object.
        """
        return json.loads(self.text(charset))

    def decode_content(self):
        """Return the best possible representation of the response body.
        """
        ct = self.headers.get('content-type')
        if ct:
            ct, options = parse_options_header(ct)
            charset = options.get('charset')
            if ct in JSON_CONTENT_TYPES:
                return self.json(charset)
            elif ct.startswith('text/'):
                return self.text(charset)
            elif ct == FORM_URL_ENCODED:
                return parse_qsl(self.content.decode(charset),
                                 keep_blank_values=True)
        return self.content

    def raise_for_status(self):
        """Raises stored :class:`HTTPError` or :class:`URLError`, if occured.
        """
        if self.is_error:
            if self.status_code:
                raise HttpRequestException(response=self)
            else:
                raise HttpConnectionError(response=self,
                                          msg=self.on_finished.result.error)

    def info(self):
        """Required by python CookieJar.

        Return :attr:`headers`.
        """
        return self.headers

    # #####################################################################
    # #    PROTOCOL IMPLEMENTATION
    def start_request(self):
        request = self._request
        self.transport.write(request.encode())
        if request.headers.get('expect') != '100-continue':
            self.write_body()

    def data_received(self, data):
        request = self.request
        # request.parser my change (100-continue)
        # Always invoke it via request
        try:
            if request.parser.execute(data, len(data)) == len(data):
                if request.parser.is_headers_complete():
                    self._status_code = request.parser.get_status_code()
                    if not self.event('on_headers').fired():
                        self.fire_event('on_headers')
                    if (not self.event('post_request').fired() and
                            request.parser.is_message_complete()):
                        self.finished()
            else:
                raise pulsar.ProtocolError('%s\n%s' % (self, self.headers))
        except Exception as exc:
            self.finished(exc=exc)

    def write_body(self):
        self.request.write_body(self.transport)


class HttpClient(AbstractClient):
    """A client for HTTP/HTTPS servers.

    It handles pool of asynchronous connections.

    :param encode_multipart: optional flag for setting the
        :attr:`encode_multipart` attribute
    :param pool_size: set the :attr:`pool_size` attribute.
    :param store_cookies: set the :attr:`store_cookies` attribute

    .. attribute:: headers

        Default headers for this :class:`HttpClient`.

        Default: :attr:`DEFAULT_HTTP_HEADERS`.

    .. attribute:: cookies

        Default cookies for this :class:`HttpClient`.

    .. attribute:: store_cookies

        If ``True`` it remebers response cookies and send them back to
        serves.

        Default: ``True``

    .. attribute:: timeout

        Default timeout for requests. If None or 0, no timeout on requests

    .. attribute:: encode_multipart

        Flag indicating if body data is by default encoded using the
        ``multipart/form-data`` or ``application/x-www-form-urlencoded``
        encoding.

        It can be overwritten during a :meth:`request`.

        Default: ``True``

    .. attribute:: proxies

        Dictionary of proxy servers for this client.

    .. attribute:: pool_size

        The size of a pool of connection for a given host.

    .. attribute:: connection_pools

        Dictionary of connection pools for different hosts

    .. attribute:: DEFAULT_HTTP_HEADERS

        Default headers for this :class:`HttpClient`

    """
    MANY_TIMES_EVENTS = ('connection_made', 'pre_request', 'on_headers',
                         'post_request', 'connection_lost')
    protocol_factory = partial(Connection, HttpResponse)
    max_redirects = 10
    """Maximum number of redirects.

    It can be overwritten on :meth:`request`.
    """
    connection_pool = Pool
    """Connection :class:`.Pool` factory
    """
    client_version = pulsar.SERVER_SOFTWARE
    """String for the ``User-Agent`` header.
    """
    version = HTTP11
    """Default HTTP request version for this :class:`HttpClient`.

    It can be overwritten on :meth:`request`.
    """
    DEFAULT_HTTP_HEADERS = Headers([
        ('Connection', 'Keep-Alive'),
        ('Accept', '*/*'),
        ('Accept-Encoding', 'deflate'),
        ('Accept-Encoding', 'gzip')],
        kind='client')
    DEFAULT_TUNNEL_HEADERS = Headers([
        ('Connection', 'Keep-Alive'),
        ('Proxy-Connection', 'Keep-Alive')],
        kind='client')
    request_parameters = ('encode_multipart', 'max_redirects', 'decompress',
                          'websocket_handler', 'multipart_boundary', 'version',
                          'verify', 'stream')
    # Default hosts not affected by proxy settings. This can be overwritten
    # by specifying the "no" key in the proxies dictionary
    no_proxy = set(('localhost', platform.node()))

    def __init__(self, proxies=None, cache=None, headers=None,
                 encode_multipart=True, multipart_boundary=None,
                 keyfile=None, certfile=None, verify=True,
                 ca_certs=None, cookies=None, store_cookies=True,
                 max_redirects=10, decompress=True, version=None,
                 websocket_handler=None, parser=None, trust_env=True,
                 loop=None, client_version=None, timeout=None, stream=False,
                 pool_size=10, frame_parser=None, logger=None):
        super().__init__(loop)
        self._logger = logger or LOGGER
        self.client_version = client_version or self.client_version
        self.connection_pools = {}
        self.pool_size = pool_size
        self.trust_env = trust_env
        self.timeout = timeout
        self.store_cookies = store_cookies
        self.max_redirects = max_redirects
        self.cookies = cookiejar_from_dict(cookies)
        self.decompress = decompress
        self.version = version or self.version
        self.verify = verify
        self.stream = stream
        dheaders = self.DEFAULT_HTTP_HEADERS.copy()
        dheaders['user-agent'] = self.client_version
        if headers:
            dheaders.override(headers)
        self.headers = dheaders
        self.tunnel_headers = self.DEFAULT_TUNNEL_HEADERS.copy()
        self.proxies = dict(proxies or ())
        if not self.proxies and self.trust_env:
            self.proxies = get_environ_proxies()
            if 'no' not in self.proxies:
                self.proxies['no'] = ','.join(self.no_proxy)
        self.encode_multipart = encode_multipart
        self.multipart_boundary = multipart_boundary or choose_boundary()
        self.websocket_handler = websocket_handler
        self.http_parser = parser or http_parser
        self.frame_parser = frame_parser or websocket.frame_parser
        # Add hooks
        self.bind_event('finish', self._close)
        self.bind_event('pre_request', Tunneling(self._loop))
        self.bind_event('pre_request', WebSocket())
        self.bind_event('on_headers', handle_100)
        self.bind_event('on_headers', handle_cookies)
        self.bind_event('post_request', Redirect())

    def connect(self, address):
        if isinstance(address, tuple):
            address = ':'.join(('%s' % v for v in address))
        return self.request('CONNECT', address)

    def get(self, url, **kwargs):
        """Sends a GET request and returns a :class:`HttpResponse` object.

        :params url: url for the new :class:`HttpRequest` object.
        :param \*\*kwargs: Optional arguments for the :meth:`request` method.
        """
        kwargs.setdefault('allow_redirects', True)
        return self.request('GET', url, **kwargs)

    def options(self, url, **kwargs):
        """Sends a OPTIONS request and returns a :class:`HttpResponse` object.

        :params url: url for the new :class:`HttpRequest` object.
        :param \*\*kwargs: Optional arguments for the :meth:`request` method.
        """
        kwargs.setdefault('allow_redirects', True)
        return self.request('OPTIONS', url, **kwargs)

    def head(self, url, **kwargs):
        """Sends a HEAD request and returns a :class:`HttpResponse` object.

        :params url: url for the new :class:`HttpRequest` object.
        :param \*\*kwargs: Optional arguments for the :meth:`request` method.
        """
        return self.request('HEAD', url, **kwargs)

    def post(self, url, **kwargs):
        """Sends a POST request and returns a :class:`HttpResponse` object.

        :params url: url for the new :class:`HttpRequest` object.
        :param \*\*kwargs: Optional arguments for the :meth:`request` method.
        """
        return self.request('POST', url, **kwargs)

    def put(self, url, **kwargs):
        """Sends a PUT request and returns a :class:`HttpResponse` object.

        :params url: url for the new :class:`HttpRequest` object.
        :param \*\*kwargs: Optional arguments for the :meth:`request` method.
        """
        return self.request('PUT', url, **kwargs)

    def patch(self, url, **kwargs):
        """Sends a PATCH request and returns a :class:`HttpResponse` object.

        :params url: url for the new :class:`HttpRequest` object.
        :param \*\*kwargs: Optional arguments for the :meth:`request` method.
        """
        return self.request('PATCH', url, **kwargs)

    def delete(self, url, **kwargs):
        """Sends a DELETE request and returns a :class:`HttpResponse` object.

        :params url: url for the new :class:`HttpRequest` object.
        :param \*\*kwargs: Optional arguments for the :meth:`request` method.
        """
        return self.request('DELETE', url, **kwargs)

    def request(self, method, url, timeout=None, **params):
        """Constructs and sends a request to a remote server.

        It returns a :class:`.Future` which results in a
        :class:`HttpResponse` object.

        :param method: request method for the :class:`HttpRequest`.
        :param url: URL for the :class:`HttpRequest`.
        :parameter response: optional pre-existing :class:`HttpResponse` which
            starts a new request (for redirects, digest authentication and
            so forth).
        :param params: optional parameters for the :class:`HttpRequest`
            initialisation.

        :rtype: a :class:`.Future`
        """
        assert not self.event('finish').fired(), "Http session is closed"
        response = self._request(method, url, **params)
        if timeout is None:
            timeout = self.timeout
        if timeout:
            response = asyncio.wait_for(response, timeout, loop=self._loop)
        if not self._loop.is_running():
            return self._loop.run_until_complete(response)
        else:
            return response

    def add_basic_authentication(self, username, password):
        """Add a :class:`HTTPBasicAuth` handler to the ``pre_requests`` hook.
        """
        self.bind_event('pre_request', HTTPBasicAuth(username, password))

    def add_digest_authentication(self, username, password):
        """Add a :class:`HTTPDigestAuth` handler to the ``pre_requests`` hook.
        """
        self.bind_event('pre_request', HTTPDigestAuth(username, password))

    # INTERNALS
    @asyncio.coroutine
    def _request(self, method, url, **params):
        nparams = params.copy()
        nparams.update(((name, getattr(self, name)) for name in
                        self.request_parameters if name not in params))
        request = HttpRequest(self, url, method, params, **nparams)
        pool = self.connection_pools.get(request.key)
        if pool is None:
            host, port = request.address
            pool = self.connection_pool(
                lambda: self.create_connection((host, port), ssl=request.ssl),
                pool_size=self.pool_size, loop=self._loop)
            self.connection_pools[request.key] = pool
        try:
            conn = yield from pool.connect()
        except BaseSSLError as e:
            raise SSLError(str(e), response=self) from None
        except ConnectionRefusedError as e:
            raise HttpConnectionError(str(e), response=self) from None

        with conn:
            try:
                response = yield from start_request(request, conn)
                headers = response.headers
            except AbortRequest:
                response = None
                headers = None

            if (not headers or
                    not keep_alive(response.request.version, headers) or
                    response.status_code == 101 or
                    # Streaming responses return before the response
                    # is finished - therefore we detach the connection
                    response.request.stream):
                conn = conn.detach()
                if self.debug:
                    self.logger.debug('Detached %s from pool', conn)

        # Handle a possible redirect
        if response and isinstance(response.request_again, tuple):
            method, url, params = response.request_again
            response = yield from self._request(method, url, **params)
        return response

    def get_headers(self, request, headers=None):
        # Returns a :class:`Header` obtained from combining
        # :attr:`headers` with *headers*. Can handle websocket requests.
        d = self.headers.copy()
        if headers:
            d.override(headers)
        return d

    def ssl_context(self, verify=True, cert_reqs=None,
                    check_hostname=False, certfile=None, keyfile=None,
                    cafile=None, capath=None, cadata=None, **kw):
        assert ssl, 'SSL not supported'
        if verify is True:
            cert_reqs = ssl.CERT_REQUIRED
            check_hostname = True
        elif isinstance(verify, str):
            if os.path.isfile(verify):
                cafile = verify
            elif os.path.isdir(verify):
                capath = verify
            cert_reqs = ssl.CERT_REQUIRED

        return ssl._create_unverified_context(cert_reqs=cert_reqs,
                                              check_hostname=check_hostname,
                                              certfile=certfile,
                                              keyfile=keyfile,
                                              cafile=cafile, capath=capath,
                                              cadata=cadata)

    def _close(self, _, exc=None):
        # Internal method, use self.close()
        # Close all connections.
        # Returns a future which can be used to wait for complete closure
        waiters = []
        for p in self.connection_pools.values():
            waiters.append(p.close())
        self.connection_pools.clear()
        return asyncio.gather(*waiters, loop=self._loop)
