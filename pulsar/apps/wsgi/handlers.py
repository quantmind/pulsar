'''Pulsar is shipped with four WSGI application handlers which facilitate the
development of server-side python web applications.

.. note::

    A **WSGI application handler** is always a callable, either a function
    or a callable instance, which accepts two positional arguments:
    *environ* and *start_response*.

WsgiHandler
======================

The first and most basic handler is the :class:`WsgiHandler` which is
a step above the hello callable above. It accepts two iterables,
a list of wsgi middleware and an optional list of response middleware.

Response middleware is a callable of the form::

    def my_response_middleware(environ, response):
        ...
        
where *environ* is the WSGI environ dictionary and *response* is an instance
of :class:`WsgiResponse`. 

.. autoclass:: WsgiHandler
   :members:
   :member-order: bysource
   
   
Router
======================

Next up is routing. Routing is the process of match and
parse the URL to something we can use. Pulsar provides a flexible integrated
routing system you can use for that. It works by creating a
:class:`Router` instance with its own ``rule`` and, optionally, additional
sub-routers for handling additional urls::

    class Page(Router):
        
        def get(self, request):
            "This method handle request with get-method" 
            ...
            
        def post(self, request):
            "This method handle request with post-method" 
            ...
            
    middleware = Page('/bla')
    
The ``middleware`` constructed can be used to serve ``get`` and ``post`` methods
at ``/bla``.
The :class:`Router` introduces a new element into pulsar WSGI handlers, the
:class:`WsgiRequest` instance ``request``, which is a light-weight
wrapper of the WSGI environ.

.. autoclass:: Router
   :members:
   :member-order: bysource


Media Router
======================

The :class:`MediaRouter` is a spcialised :class:`Router` for serving static
files such ass ``css``, ``javascript``, images and so forth.

.. autoclass:: MediaRouter
   :members:
   :member-order: bysource
   
   
Lazy Wsgi Handler
======================

.. autoclass:: LazyWsgi
   :members:
   :member-order: bysource
'''
import os
import re
import stat
import mimetypes
from email.utils import parsedate_tz, mktime_tz

from pulsar.utils.httpurl import http_date, CacheControl, remove_double_slash
from pulsar.utils.log import LocalMixin
from pulsar import Http404, PermissionDenied, HttpException

from .route import Route
from .wrappers import WsgiRequest
from .content import Html

__all__ = ['WsgiHandler', 'LazyWsgi', 'Router',
           'MediaRouter', 'FileRouter', 'MediaMixin']


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
            raise Http404(environ.get('PATH_INFO','/'))
        if hasattr(response, 'middleware'):
            response.middleware.extend(self.response_middleware)
        return response
    

class LazyWsgi(LocalMixin):
    '''A :ref:`wsgi handler <wsgi-handlers>` which loads its middleware the
first time it is called. Subclasses must implement the :meth:`setup` method.'''
    def __call__(self, environ, start_response):
        handler = self.local.handler
        if handler is None:
            self.local.handler = handler = self.setup()
        return handler(environ, start_response)
    
    def setup(self):
        raise NotImplementedError
    
    
class Router(object):
    '''A WSGI application which handle multiple :class:`Route`.
    
.. attribute:: route

    The class:`Route` served by this :class:`Router`.

.. attribute:: routes

    List of children :class.:`Router` of this :class:`Router`.
    
.. attribute:: parameters

    Dictionary of parameters
'''
    default_content_type=None
    request_class = WsgiRequest
    routes = []
    def __init__(self, rule, *routes, **parameters):
        if not isinstance(rule, Route):
            rule = Route(rule)
        self.route = rule
        self.routes = list(self.routes)
        self.routes.extend(routes)
        self.parameters = {}
        for name, callable in self.__class__.__dict__.items():
            rule_method = getattr(callable, 'rule_method', None)
            if isinstance(rule_method, tuple):
                rule, method, params = rule_method
                parameters = params.copy()
                parameters[method] = getattr(self, name)
                router = Router(rule, **parameters)
                self.routes.append(router)
        for name, value in parameters.items():
            if not hasattr(self, name) and hasattr(value, '__call__'):
                setattr(self, name, value)
            else:
                self.parameters[name] = value
        
    def __repr__(self):
        return self.route.__repr__()
        
    def __call__(self, environ, start_response):
        path = environ.get('PATH_INFO') or '/'
        path = path[1:]
        router_args = self.resolve(path)
        if router_args:
            router, args = router_args
            request = self.request_class(environ, start_response, args)
            method = request.method.lower()
            callable = getattr(router, method, None)
            if callable is None:
                raise HttpException(status=405,
                                    msg='Method "%s" not allowed' % method)
            return callable(request)
        
    def resolve(self, path, urlargs=None):
        urlargs = urlargs if urlargs is not None else {}
        match = self.route.match(path)
        if match is None:
            return
        if '__remaining__' in match:
            for handler in self.routes:
                view_args = handler.resolve(path, urlargs)
                if view_args is None:
                    continue
                #remaining_path = match.pop('__remaining__','')
                #urlargs.update(match)
                return view_args
        else:
            return self, match
    
    def link(self, *args, **urlargs):
        if len(args) > 1:
            raise ValueError
        url = self.route.url(**urlargs)
        if len(args) == 1:
            text = args[0]
        else:
            text = url
        return Html('a', text, href=url)
    

class MediaMixin(Router):
    default_content_type = 'application/octet-stream'
    cache_control = CacheControl(maxage=86400)
    
    def serve_file(self, request, fullpath):
        # Respect the If-Modified-Since header.
        statobj = os.stat(fullpath)
        mimetype, encoding = mimetypes.guess_type(fullpath)
        mimetype = mimetype or self.default_content_type
        response = request.response
        response.content_type = mimetype
        response.encoding = encoding
        if not self.was_modified_since(request.environ.get(
                                            'HTTP_IF_MODIFIED_SINCE'),
                                       statobj[stat.ST_MTIME],
                                       statobj[stat.ST_SIZE]):
            response.status_code = 304
        else:
            response.content = open(fullpath, 'rb').read()
            response.headers["Last-Modified"] = http_date(statobj[stat.ST_MTIME])
        return response.start()

    def was_modified_since(self, header=None, mtime=0, size=0):
        '''Check if an item was modified since the user last downloaded it

:param header: the value of the ``If-Modified-Since`` header. If this is None,
    simply return ``True``.
:param mtime: the modification time of the item in question.
:param size: the size of the item.
'''
        try:
            if header is None:
                raise ValueError
            matches = re.match(r"^([^;]+)(; length=([0-9]+))?$", header,
                               re.IGNORECASE)
            header_mtime = mktime_tz(parsedate_tz(matches.group(1)))
            header_len = matches.group(3)
            if header_len and int(header_len) != size:
                raise ValueError()
            if mtime > header_mtime:
                raise ValueError()
        except (AttributeError, ValueError, OverflowError):
            return True
        return False
    
    def directory_index(self, request, fullpath):
        names = [Html('a', '../', href='../', cn='folder')]
        files = []
        for f in sorted(os.listdir(fullpath)):
            if not f.startswith('.'):
                if os.path.isdir(os.path.join(fullpath, f)):
                    names.append(Html('a', f, href=f+'/', cn='folder'))
                else:
                    files.append(Html('a', f, href=f))
        names.extend(files)
        return self.static_index(request, names)
    
    def html_title(self, request):
        return 'Index of %s' % request.path
    
    def static_index(self, request, links):
        title = Html('h2', self.html_title(request))
        list = Html('ul', *[Html('li', a) for a in links])
        body = Html('div', title, list)
        doc = request.html_document(title=title, body=body)
        return doc.http_response(request)
    
    
class MediaRouter(MediaMixin):
    '''A :class:`Router` for serving static media files from a given 
directory.

:param rute: The top-level url for this router. For example ``/media``
    will serve the ``/media/<path:path>`` :class:`Route`.
:param path: Check the :attr:`path` attribute.
:param show_indexes: Check the :attr:`show_indexes` attribute.

.. attribute::    path

    The file-system path of the media files to serve.
    
.. attribute::    show_indexes

    If ``True`` (default), the router will serve media file directories as
    well as media files.
'''
    def __init__(self, rute, path, show_indexes=True):
        super(MediaRouter, self).__init__('%s/<path:path>' % rute)
        self._show_indexes = show_indexes
        self._file_path = path
        
    def get(self, request):
        paths = request.urlargs['path'].split('/')
        if len(paths) == 1 and paths[0] == '':
            paths.pop(0)
        fullpath = os.path.join(self._file_path, *paths)
        if os.path.isdir(fullpath):
            if self._show_indexes:
                return self.directory_index(request, fullpath)
            else:
                raise PermissionDenied()
        elif os.path.exists(fullpath):
            return self.serve_file(request, fullpath)
        else:
            raise Http404


class FileRouter(MediaMixin):
    
    def __init__(self, route, file_path):
        super(FileRouter, self).__init__(route)
        self._file_path = file_path
        
    def get(self, request):
        return self.serve_file(request, self._file_path)
    
