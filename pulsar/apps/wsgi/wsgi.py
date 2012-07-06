import os
import textwrap
import logging
from functools import partial

import pulsar
from pulsar import make_async, Deferred, is_failure, NOT_DONE
from pulsar.utils.httpurl import Headers, SimpleCookie, set_cookie, responses,\
                                    iteritems, has_empty_content, string_type,\
                                    ispy3k

from .middleware import is_streamed

if ispy3k:
    from functools import reduce

__all__ = ['WsgiHandler', 'WsgiResponse',
           'WsgiResponseGenerator',
           'wsgi_iterator', 'handle_http_error']


default_logger = logging.getLogger('pulsar.apps.wsgi')

EMPTY_DICT = {}
EMPTY_TUPLE = ()


def wsgi_iterator(gen, encoding=None):
    encoding = encoding or 'utf-8'
    for data in gen:
        if data is NOT_DONE:
            yield b''
        elif isinstance(data, bytes):
            yield data
        elif isinstance(data, string_type):
            yield data.encode(encoding)
        else:
            for b in wsgi_iterator(data, encoding):
                yield b
                

class WsgiResponseGenerator(object):
    
    def __init__(self, environ=None, start_response=None):
        self.environ = environ
        self.start_response = start_response
        self.middleware = []
        
    def __iter__(self):
        raise NotImplementedError()
    
    def __call__(self, response):
        '''Apply response middleware '''
        response.middleware.extend(self.middleware)
        for b in response(self.environ, self.start_response):
            yield b
    
        
class WsgiResponse(WsgiResponseGenerator):
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
    DEFAULT_CONTENT_TYPE = 'text/plain'
    ENCODED_CONTENT_TYPE = ('text/plain', 'text/html', 'application/json',
                            'application/javascript')
    def __init__(self, status=None, content=None, response_headers=None,
                 content_type=None, encoding=None, environ=None,
                 start_response=None):
        super(WsgiResponse, self).__init__(environ, start_response)
        self.status_code = status or self.DEFAULT_STATUS_CODE
        self.encoding = encoding
        self.cookies = SimpleCookie()
        self.content_type = content_type or self.DEFAULT_CONTENT_TYPE
        self.headers = Headers(response_headers, kind='server')
        self._sent_headers = None
        self.content = content
    
    @property
    def started(self):
        return self._started
    
    @property
    def sent_headers(self):
        return self._sent_headers
    
    @property
    def method(self):
        if self.environ:
            return self.environ.get('REQUEST_METHOD')
        
    def _get_content(self):
        return self._content
    def _set_content(self, content):
        if not self._started:
            if content is None:
                # no content, get the default content
                content = self.default_content()
            elif isinstance(content, bytes):
                content = (content,)
            self._content = content
        else:
            raise RuntimeError('Cannot set content. Already iterated')
    content = property(_get_content, _set_content)
            
    def default_content(self):
        '''Called during initialization when the content given is ``None``.
By default it returns an empty tuple. Overrides if you need to.'''
        return ()
    
    def __call__(self, environ, start_response):
        '''This method should be called when ready to start the response.
Return ``self`` for convenience::

    for data in response(environ, start_response):
        yield data
'''
        if self._sent_headers is None:
            self.environ = environ
            for rm in self.middleware:
                try:
                    rm(environ, self)
                except:
                    log = pulsar.get_actor().log
                    log.error('Exception in response middleware', exc_info=True)
            self._sent_headers = self.get_headers()
            start_response(self.status, self._sent_headers)
        return self
        
    def length(self):
        if not self.is_streamed:
            return reduce(lambda x,y: x+len(y), self.content, 0)
        
    @property
    def response(self):
        return responses.get(self.status_code)
    
    @property
    def status(self):
        return '{0} {1}'.format(self.status_code, self.response)
    
    def __str__(self):
        return self.status
            
    def __repr__(self):
        return '{0}({1})'.format(self.__class__.__name__,self)
        
    @property
    def is_streamed(self):
        """If the response is streamed (the response is not an iterable with
length information) this property is `True`.  In this case streamed
means that there is no information about the number of iterations.
This is usually `True` if a generator is passed to the response object."""
        return is_streamed(self.content)
        
    def _generator(self):
        #Called by the __iter__ method when the response is streamed.
        content = []
        try:
            for b in wsgi_iterator(self.content, self.encoding):
                if b:
                    content.append(b)
                    if len(content) == 1:
                        # We make sure we send the headers
                        self(self.environ, self.start_response)
                yield b
        except Exception as e:
            if not content:
                pulsar.get_actor().cfg.handle_http_error(self, e)
        self.generated_content = content
                
    def __iter__(self):
        self._started = True
        if self.is_streamed:
            return self._generator()
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
            headers.pop('content-type',None)
        elif self.content_type:
            headers['Content-type'] = self.content_type
        if not self.is_streamed:
            cl = 0
            for c in self.content:
                cl += len(c)
            headers['Content-Length'] = str(cl)
        for c in self.cookies.values():
            headers['Set-Cookie'] = c.OutputString()
        return list(iteritems(headers))
    
    
class WsgiHandler(pulsar.LogginMixin):
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
    def __init__(self, middleware=None, **kwargs):
        self.setlog(**kwargs)
        if middleware:
            middleware = list(middleware)
        self.middleware = middleware or []
        self.response_middleware = []
        
    def __call__(self, environ, start_response):
        '''The WSGI callable'''
        response = None
        for middleware in self.middleware:
            try:
                response = middleware(environ, start_response)
            except Exception as e:
                response = WsgiResponse(500)
                pulsar.get_actor().cfg.handle_http_error(response, e)
            if response is not None:
                break
        if response is None:
            response = WsgiResponse(404)
            pulsar.get_actor().cfg.handle_http_error(response)
        if hasattr(response, 'middleware'):
            response.middleware.extend(self.response_middleware)
        return response
    
    def get(self, route='/'):
        '''Fetch a middleware with the given *route*. If it is not found
 return ``None``.'''
        for m in self.middleware:
            if getattr(m,'route',None) == route:
                return m


def handle_http_error(response, e=None):
    '''The default handler for errors while serving an Http requests.
:parameter response: an instance of :class:`WsgiResponse`.
:parameter e: the exception instance.
'''
    actor = pulsar.get_actor()
    code = 500
    if e:
        code = getattr(e, 'status_code', 500)
    response.content_type = 'text/html'
    if code == 500:
        actor.log.critical('Unhandled exception during WSGI response',
                           exc_info=True)
        msg = 'An exception has occured while evaluating your request.'
    else:
        actor.log.info('WSGI {0} status code'.format(code))
        if code == 404:
            msg = 'Cannot find what you are looking for.'
        else:
            msg = ''
    response.status_code = code
    encoding = 'utf-8'
    reason = response.status
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
    """).format({"reason": reason, "msg": msg,
                 "version": pulsar.SERVER_SOFTWARE})
    response.content = content.encode(encoding, 'replace')
    return response