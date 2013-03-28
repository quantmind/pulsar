'''This section introduces two classes used by pulsar
:ref:`wsgi application <apps-wsgi>` to pass a request/response state
during an HTTP request.
The :class:`WsgiRequest` is a thin wrapper around a WSGI ``environ`` dictionary.
It contains only the ``environ`` as its private data.
The :class:`WsgiResponse`, which is available in the
:class:`WsgiRequest.response` attribute, is an iterable over bytestring with
several utility methods for manipulating headers and asynchronous content.

.. _app-wsgi-request:

Wsgi Request
=====================

.. autoclass:: WsgiRequest
   :members:
   :member-order: bysource
   
   
Wsgi Response
=====================

.. autoclass:: WsgiResponse
   :members:
   :member-order: bysource
   
.. _WSGI: http://www.wsgi.org
'''
import os
import json
from functools import partial, reduce

import pulsar
from pulsar import maybe_async, is_async, is_failure, multi_async
from pulsar.utils.multipart import parse_form_data, parse_options_header
from pulsar.utils.structures import MultiValueDict, AttributeDictionary
from pulsar.utils.html import escape
from pulsar.utils.httpurl import Headers, SimpleCookie, responses,\
                                 has_empty_content, string_type, ispy3k,\
                                 to_bytes, REDIRECT_CODES, iteritems,\
                                 ENCODE_URL_METHODS, JSON_CONTENT_TYPES

from .middleware import is_streamed
from .route import Route
from .content import HtmlDocument
from .utils import LOGGER, set_wsgi_request_class, set_cookie, query_dict


__all__ = ['WsgiResponse',
           'WsgiRequest',
           'PulsarWsgiResponse',
           'wsgi_cache_property']

MAX_BUFFER_SIZE = 2**16

def wsgi_cache_property(f):
    name = f.__name__
    def _(self):
        if name not in self.cache:
            self.cache[name] = f(self)
        return self.cache[name]
    return property(_, doc=f.__doc__)

def wsgi_encoder(gen, encoding):
    for data in gen:
        if not isinstance(data, bytes):
            yield data.encode(encoding)
        else:
            yield data


class PulsarWsgiResponse(object):
    streaming = True
    
    def has_header(self, header):
        raise NotImplementedError
    
    def __contains__(self, header):
        return self.has_header(header)


class WsgiResponseGenerator(PulsarWsgiResponse):
    
    def __init__(self, request, callable):
        self.request = request
        self.callable = callable
        
    def __iter__(self):
        request = self.request
        cache = maybe_async(multi_async(request.cache, raise_on_error=True))
        while is_async(cache):
            yield b''
            cache = maybe_async(cache)
        if is_failure(cache):
            cache.raise_all()
        request.cache.update(cache)
        for data in self.callable(request):
            yield data
            
    @property
    def middleware(self):
        return self.request.response.middleware
    
    @property
    def status_code(self):
        return self.request.response.status_code
        
    def has_header(self, header):
        return self.request.response.has_header(header)
    
    def __setitem__(self, header, value):
        self.request.response[header] = value
        
    def __getitem__(self, header):
        return self.request.response[header]
    
    
class WsgiResponse(PulsarWsgiResponse):
    '''A WSGI response wrapper initialized by a
:ref:`pulsar WSGI application handler <apps-wsgi-handlers>`.
Instances are callable using the standard WSGI call and, importantly, iterable::

    response = WsgiResponse(200)
    for data in response(environ, start_response):
        ...

A :class:`WsgiResponse` is an iterable over bytes to send back to the requesting
client.

.. attribute:: status_code

    Integer indicating the HTTP status, (i.e. 200)

.. attribute:: response

    String indicating the HTTP status (i.e. 'OK')

.. attribute:: status

    String indicating the HTTP status code and response (i.e. '200 OK')

.. attribute:: environ

    The dictionary of WSGI environment if passed to the constructor.
    
.. attribute:: start_response

    The WSGI ``start_response`` callable.

.. attribute:: middleware

    A list of :ref:`response middleware <apps-wsgi-middleware>` callables
    which will be invoked  when this :class:`WsgiResponse`
    get called (the ``__call__`` or the :meth:`start` method of this
    :class:`WsgiResponse` is invoked).
'''
    _started = False
    DEFAULT_STATUS_CODE = 200
    def __init__(self, status=None, content=None, response_headers=None,
                 content_type=None, encoding=None, environ=None,
                 start_response=None):
        self.environ = environ
        self.start_response = start_response
        self.middleware = []
        self.status_code = status or self.DEFAULT_STATUS_CODE
        self.encoding = encoding
        self.cookies = SimpleCookie()
        self.headers = Headers(response_headers, kind='server') 
        self.content = content
        if content_type is not None:
            self.content_type = content_type

    @property
    def started(self):
        return self._started

    @property
    def path(self):
        if self.environ:
            return self.environ.get('PATH_INFO','')

    @property
    def method(self):
        if self.environ:
            return self.environ.get('REQUEST_METHOD')

    @property
    def connection(self):
        if self.environ:
            return self.environ.get('pulsar.connection')

    def _get_content(self):
        return self._content
    def _set_content(self, content):
        if not self._started:
            if content is None:
                content = ()
            elif ispy3k: #what a pain
                if isinstance(content, str):
                    content = content.encode(self.encoding or 'utf-8')
            else: #pragma    nocover
                if isinstance(content, unicode):
                    content = content.encode(self.encoding or 'utf-8')
            if isinstance(content, bytes):
                content = (content,)
            self._content = content
        else:
            raise RuntimeError('Cannot set content. Already iterated')
    content = property(_get_content, _set_content)
    
    def _get_content_type(self):
        return self.headers.get('content-type')
    def _set_content_type(self, typ):
        if typ:
            self.headers['content-type'] = typ
        else:
            self.headers.pop('content-type', None)
    content_type = property(_get_content_type, _set_content_type)

    def __call__(self, environ, start_response, exc_info=None):
        '''Make sure the headers are set.'''
        if not exc_info:
            for rm in self.middleware:
                try:
                    rm(environ, self)
                except Exception:
                    LOGGER.error('Exception in response middleware',
                                 exc_info=True)
        environ['wsgi.writer'] = start_response(self.status, self.get_headers(),
                                                exc_info)
        return self
    
    def start(self):
        '''Starts the response by applying :attr:`middleware` and
invoking :attr:`start_response`.'''
        return self.__call__(self.environ, self.start_response)

    def length(self):
        if not self.is_streamed:
            return reduce(lambda x,y: x+len(y), self.content, 0)

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
        """If the response is streamed (the response is not an iterable with
length information) this property is `True`.  In this case streamed
means that there is no information about the number of iterations.
This is usually `True` if a generator is passed to the response object."""
        return is_streamed(self.content)

    def __iter__(self):
        if self._started:
            raise RuntimeError('WsgiResponse can be iterated once only')
        self._started = True
        if is_streamed(self.content):
            return wsgi_encoder(self.content, self.encoding or 'utf-8')
        else:
            return iter(self.content)

    def __len__(self):
        return len(self.content)

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
        headers = self.headers
        if has_empty_content(self.status_code, self.method):
            headers.pop('content-type', None)
            headers.pop('content-length', None)
        else:
            if not self.is_streamed:
                cl = 0
                for c in self.content:
                    cl += len(c)
                headers['Content-Length'] = str(cl)
            if not self.content_type:
                headers['Content-Type'] = 'text/plain'
        for c in self.cookies.values():
            headers['Set-Cookie'] = c.OutputString()
        return list(headers)
    
    def has_header(self, header):
        return header in self.headers
    
    def __setitem__(self, header, value):
        self.headers[header] = value
        
    def __getitem__(self, header):
        return self.headers[header]
        
    
class WsgiRequest(object):
    '''A thin wrapper around a WSGI_ environ. Instances of this class
have the :attr:`environ` attribute as their only private data. Every
other attribute is stored in the :attr:`environ` itself at the
``pulsar.cache`` wsgi-extension key.

.. attribute:: environ

    WSGI_ environ dictionary
'''
    slots = ('environ',)
    
    def __init__(self, environ, start_response=None, app_handler=None,
                 urlargs=None):
        self.environ = environ
        if 'pulsar.cache' not in environ:
            environ['pulsar.cache'] = AttributeDictionary()
            self.cache.cfg = environ.get('pulsar.cfg', {})
            self.cache.response = WsgiResponse(environ=environ,
                                               start_response=start_response)
        if start_response:
            self.response.start_response = start_response
            self.cache.app_handler = app_handler
            self.cache.urlargs = urlargs
    
    def __repr__(self):
        return self.path
    
    def __str__(self):
        return self.__repr__()
    
    @property
    def cache(self):
        '''dictionary of pulsar-specific data stored in the :attr:`environ`
at the wsgi-extension key ``pulsar.cache``.'''
        return self.environ['pulsar.cache']
    
    @property
    def response(self):
        '''The :class:`WsgiResponse` for this request.'''
        return self.cache.response
    
    @property
    def app_handler(self):
        '''The WSGI application handling this request.'''
        return self.cache['app_handler']
    
    @property
    def urlargs(self):
        '''Dictionary of url parameters obtained when matching a
:ref:`router <apps-wsgi-router>` with this request :attr:`path`.'''
        return self.cache['urlargs']
    
    ############################################################################
    #    environ shortcuts
    @property
    def is_xhr(self):
        return self.environ.get('HTTP_X_REQUESTED_WITH') == 'XMLHttpRequest'
    
    @property
    def is_secure(self):
        return 'wsgi.url_scheme' in self.environ \
            and self.environ['wsgi.url_scheme'] == 'https'

    @property
    def path(self):
        '''Shortcut to the :attr:`environ` `PATH_INFO` value.'''
        return self.environ.get('PATH_INFO', '/')

    @property
    def method(self):
        '''The request method (uppercase).'''
        return self.environ['REQUEST_METHOD']      

    @wsgi_cache_property
    def encoding(self):
        return self.content_type_options[1].get('charset', 'utf-8')
    
    @wsgi_cache_property
    def content_type(self):
        return self.content_type_options[0]
    
    @wsgi_cache_property
    def content_type_options(self):
        content_type = self.environ.get('CONTENT_TYPE')
        if content_type:
            return parse_options_header(content_type)
        else:
            return None, {}
    
    @wsgi_cache_property
    def data_and_files(self):
        if self.method not in ENCODE_URL_METHODS:
            return parse_form_data(self.environ)
        else:
            return MultiValueDict(), None
    
    @wsgi_cache_property
    def json_data(self):
        if self.method not in ENCODE_URL_METHODS:
            if self.content_type in JSON_CONTENT_TYPES:
                data = self.environ['wsgi.input'].read(MAX_BUFFER_SIZE)
                return json.loads(data.decode(self.encoding))
        
    @property
    def body_data(self):
        '''A :class:`pulsar.utils.structures.MultiValueDict` containing
data from the request body.'''
        data, files = self.data_and_files
        return data
    
    @wsgi_cache_property
    def url_data(self):
        '''A :class:`pulsar.utils.structures.MultiValueDict` containing
data from the `QUERY_STRING` in :attr:`environ`.'''
        return query_dict(self.environ.get('QUERY_STRING', ''),
                          encoding=self.encoding)
    
    @wsgi_cache_property
    def html_document(self):
        '''Return a cached instance of an
:ref:`Html document <app-wsgi-html-document>`.'''
        return HtmlDocument()
    
    def get(self, key, default=None):
        '''Shortcut to the :attr:`environ` get method.'''
        return self.environ.get(key, default)
    

set_wsgi_request_class(WsgiRequest)

    