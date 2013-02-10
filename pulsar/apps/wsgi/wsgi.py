'''WSGI utilities and wrappers.'''
import os
import sys
import json
import textwrap
import logging
import time
from datetime import datetime, timedelta
from functools import partial, reduce
from email.utils import formatdate

import pulsar
from pulsar import is_failure, HttpException, maybe_async, is_async
from pulsar.utils.multipart import parse_form_data
from pulsar.utils.httpurl import Headers, SimpleCookie, responses,\
                                 has_empty_content, string_type, ispy3k,\
                                 to_bytes, REDIRECT_CODES, iteritems,\
                                 ENCODE_URL_METHODS

from .middleware import is_streamed
from .route import Route
from .content import HtmlDocument


__all__ = ['WsgiHandler',
           'WsgiResponse',
           'WsgiRequest',
           'handle_wsgi_error',
           'wsgi_error_msg',
           'async_wsgi']


LOGGER = logging.getLogger('pulsar.wsgi')


def wsgi_iterator(gen, encoding):
    for data in gen:
        if isinstance(data, bytes):
            yield data
        else:
            yield data.encode(encoding)


def async_wsgi(request, result, callback):
    result = maybe_async(result)
    while is_async(result):
        yield b''
        result = maybe_async(result)
    for b in callback(request, result):
        yield b
                    
                    
def cookie_date(epoch_seconds=None):
    """Formats the time to ensure compatibility with Netscape's cookie
    standard.

    Accepts a floating point number expressed in seconds since the epoch in, a
    datetime object or a timetuple.  All times in UTC.  The :func:`parse_date`
    function can be used to parse such a date.

    Outputs a string in the format ``Wdy, DD-Mon-YYYY HH:MM:SS GMT``.

    :param expires: If provided that date is used, otherwise the current.
    """
    rfcdate = formatdate(epoch_seconds)
    return '%s-%s-%s GMT' % (rfcdate[:7], rfcdate[8:11], rfcdate[12:25])

def set_cookie(cookies, key, value='', max_age=None, expires=None, path='/',
                domain=None, secure=False, httponly=False):
    '''Set a cookie key into the cookies dictionary *cookies*.'''
    cookies[key] = value
    if expires is not None:
        if isinstance(expires, datetime):
            delta = expires - expires.utcnow()
            # Add one second so the date matches exactly (a fraction of
            # time gets lost between converting to a timedelta and
            # then the date string).
            delta = delta + timedelta(seconds=1)
            # Just set max_age - the max_age logic will set expires.
            expires = None
            max_age = max(0, delta.days * 86400 + delta.seconds)
        else:
            cookies[key]['expires'] = expires
    if max_age is not None:
        cookies[key]['max-age'] = max_age
        # IE requires expires, so set it if hasn't been already.
        if not expires:
            cookies[key]['expires'] = cookie_date(time.time() + max_age)
    if path is not None:
        cookies[key]['path'] = path
    if domain is not None:
        cookies[key]['domain'] = domain
    if secure:
        cookies[key]['secure'] = True
    if httponly:
        cookies[key]['httponly'] = True


class WsgiResponse(object):
    '''A WSGI response wrapper initialized by a WSGI request middleware.
Instances are callable using the standard WSGI call::

    response = WsgiResponse(200)
    response(environ, start_response)

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
        start_response(self.status, self.get_headers(), exc_info)
        return self
    
    def start(self):
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
            raise RuntimeError('WsgiResponse can be iterated only once')
        self._started = True
        if self.is_streamed:
            return wsgi_iterator(self.content, self.encoding or 'utf-8')
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


class WsgiRequest(object):
    slots = ('environ',)
    
    def __init__(self, environ, start_response, urlargs=None):
        self.environ = environ
        if 'pulsar.cache' not in environ:
            environ['pulsar.cache'] = {}
            self.cache['response'] = WsgiResponse(environ=environ,
                                                  start_response=start_response)
        self.cache['urlargs'] = urlargs
    
    @property
    def cache(self):
        return self.environ['pulsar.cache']
    
    @property
    def response(self):
        return self.cache['response']
    
    @property
    def urlargs(self):
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
        return self.environ.get('PATH_INFO', '/')

    @property
    def method(self):
        return self.environ['REQUEST_METHOD'].lower()      

    def body_data(self):
        cache = self.cache
        if 'body_data' not in cache:
            if self.method not in ENCODE_URL_METHODS:
                cache['body_data'] = {}, None
            else:
                cache['body_data'] = parse_form_data(environ)
        return cache['body_data']
     
    ############################################################################
    #    Html tools
    @property
    def html_document(self):
        cache = self.cache
        if 'html_document' not in cache:
            cache['html_document'] = HtmlDocument()
        return cache['html_document']
    
    
class WsgiHandler(object):
    '''An handler for application conforming to python WSGI_.

.. attribute:: middleware

    List of callable WSGI middleware callable which accept
    ``environ`` and ``start_response`` as arguments.
    The order matter, since the response returned by the callable
    is the non ``None`` value returned by a middleware.

.. attribute:: response_middleware

    List of functions of the form::

        def ..(environ, start_response, response):
            ...

    where ``response`` is the first not ``None`` value returned by
    the middleware.

'''
    def __init__(self, middleware=None, response_middleware=None, **kwargs):
        if middleware:
            middleware = list(middleware)
        self.middleware = middleware or []
        self.response_middleware = response_middleware or []

    def __call__(self, environ, start_response):
        '''The WSGI callable'''
        response = None
        for middleware in self.middleware:
            response = middleware(environ, start_response)
            if response is not None:
                break
        if response is None:
            raise pulsar.Http404(environ.get('PATH_INFO','/'))
        if hasattr(response, 'middleware'):
            response.middleware.extend(self.response_middleware)
        return response

    def get(self, route='/'):
        '''Fetch a middleware with the given *route*. If it is not found
 return ``None``.'''
        for m in self.middleware:
            if getattr(m,'route',None) == route:
                return m


error_messages = {
    500: 'An exception has occurred while evaluating your request.',
    404: 'Cannot find what you are looking for.'
}

def wsgi_error_msg(response, msg):
    if response.content_type == 'application/json':
        return json.dumps({'status': response.status_code,
                           'message': msg})
    else:
        return msg
    
class dump_environ(object):
    __slots__ = ('environ',)
    
    def __init__(self, environ):
        self.environ = environ
        
    def __str__(self):
        env = iteritems(self.environ)
        return '\n%s\n' % '\n'.join(('%s = %s' % (k, v) for k, v in env))
    
    
def handle_wsgi_error(environ, trace=None, content_type=None,
                        encoding=None):
    '''The default handler for errors while serving an Http requests.

:parameter environ: The WSGI environment.
:parameter trace: the error traceback. If not avaiable it will be obtained from
    ``sys.exc_info()``.
:parameter content_type: Optional content type.
:parameter encoding: Optional charset.
:return: a :class:`WsgiResponse`
'''
    content_type = content_type or environ.get('CONTENT_TYPE')
    if not trace:
        trace = sys.exc_info()
    error = trace[1]
    if not content_type:
        content_type = getattr(error, 'content_type', content_type)
    response = WsgiResponse(content_type=content_type,
                            environ=environ,
                            encoding=encoding)
    content = None
    response.status_code = getattr(error, 'status', 500)
    response.headers.update(getattr(error, 'headers', None) or ())
    path = ' @ path "%s"' % environ.get('PATH_INFO','/')
    e = dump_environ(environ)
    if response.status_code == 500:
        LOGGER.critical('Unhandled exception during WSGI response %s.%s',
                        path, e, exc_info=trace)
    else:
        LOGGER.warn('WSGI %s status code %s.', response.status_code, path)
        LOGGER.debug('%s', e, exc_info=trace)
    if has_empty_content(response.status_code) or\
       response.status_code in REDIRECT_CODES:
        content = ()
        response.content_type = None
    else:
        renderer = environ.get('wsgi_error_handler')
        if renderer:
            try:
                content = renderer(environ, response, trace)
                if is_failure(content):
                    content.log()
                    content = None
            except Exception:
                LOGGER.critical('Error while rendering error', exc_info=True)
                content = None
    if content is None:
        msg = error_messages.get(response.status_code) or ''
        if response.content_type == 'text/html':
            content = textwrap.dedent("""\
            <!DOCTYPE html>
            <html>
              <head>
                <title>{0[reason]}</title>
              </head>
              <body>
                <h1>{0[reason]}</h1>
                {0[msg]}
                <h3>{0[version]}</h3>
              </body>
            </html>
            """).format({"reason": response.status, "msg": msg,
                         "version": pulsar.SERVER_SOFTWARE})
        else:
            content = wsgi_error_msg(response, msg)
    response.content = content
    return response


def render_trace(environ, response, exc_info):
    '''Render the traceback into the content type in *response*.'''
    if exc_info:
        request = Request(environ)
        trace = exc_info[2]
        if istraceback(trace):
            trace = traceback.format_exception(*exc_info)
        is_html = response.content_type == 'text/html'
        if is_html:
            html = request.html(error=True)
            #html.title = response.response
            error = Widget('div', cn='section traceback error')
            html.body.append(error)
        else:
            error = []
        for traces in trace:
            counter = 0
            for trace in traces.split('\n'):
                if trace.startswith('  '):
                    counter += 1
                    trace = trace[2:]
                if not trace:
                    continue
                if is_html:
                    trace = Widget('p', escape(trace))
                    if counter:
                        trace.css({'margin-left':'%spx' % (20*counter)})
                error.append(trace)
        if is_html:
            return html.render(request)
        else:
            return wsgi_error_msg(response, '\n'.join(error))