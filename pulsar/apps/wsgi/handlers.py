'''Pulsar is shipped with four WSGI application handlers which facilitate the
development of server-side python web applications.

.. note::

    A **WSGI application handler** is always a callable, either a function
    or a callable object, which accepts two positional arguments:
    ``environ`` and ``start_response``. When called by the server,
    the application object must return an iterable yielding zero or more bytes. 


WsgiHandler
======================

The first and most basic application handler is the :class:`WsgiHandler`
which is a step above the :ref:`hello callable <tutorials-hello-world>`
in the tutorial. It accepts two iterables, a list of
:ref:`wsgi middleware <wsgi-middleware>` and an optional list of
:ref:`response middleware <wsgi-response-middleware>`.

.. autoclass:: WsgiHandler
   :members:
   :member-order: bysource
   
.. _wsgi-lazy-handler:

Lazy Wsgi Handler
======================

.. autoclass:: LazyWsgi
   :members:
   :member-order: bysource
   
   
.. _wsgi-middleware:

WSGI Middleware
=====================

Middleware are function or callable objects similar to :ref:`wsgi-handlers`
with the only difference that they can return ``None``. Middleware is used
in conjunction with both :class:`WsgiHandler`.
Here we introduce the :class:`Router` and :class:`MediaRouter` which handle
requests on given urls. Pulsar is shipped with
:ref:`additional wsgi middleware <wsgi-additional-middleware>` for manipulating
the environment before a client response is returned.

.. _apps-wsgi-router:

Router
~~~~~~~~~~~~~~~~~~

Next up is routing. Routing is the process of match and
parse the URL to something we can use. Pulsar provides a flexible integrated
routing system you can use for that. It works by creating a
:class:`Router` instance with its own ``rule`` and, optionally, additional
sub-routers for handling additional urls::

    class Page(Router):
        accept_content_types = ('text/html',
                                'text/plain',
                                'application/json')
        
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
:ref:`wsgi request <app-wsgi-request>`, a light-weight wrapper of the
WSGI environ.

.. autoclass:: Router
   :members:
   :member-order: bysource


Media Router
~~~~~~~~~~~~~~~~~~~~~~~~

The :class:`MediaRouter` is a spcialised :class:`Router` for serving static
files such ass ``css``, ``javascript``, images and so forth.

.. autoclass:: MediaRouter
   :members:
   :member-order: bysource
   
   
.. _WSGI: http://www.wsgi.org
'''
import os
import re
import stat
import mimetypes
from functools import partial
from email.utils import parsedate_tz, mktime_tz

from pulsar.utils.httpurl import http_date, CacheControl, remove_double_slash
from pulsar.utils.structures import AttributeDictionary, OrderedDict
from pulsar.utils.log import LocalMixin, local_property
from pulsar import Http404, PermissionDenied, HttpException, HttpRedirect, async, is_async

from .route import Route
from .utils import wsgi_request
from .content import Html
from .wrappers import WsgiResponse

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

        def ..(environ, response):
            ...

    where ``response`` is a :ref:`WsgiResponse <wsgi-response>`.
    Pulsar contains some :ref:`response middlewares <wsgi-response-middleware>`.

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
        if is_async(response):
            return response.add_callback(
                        partial(self.finish, environ, start_response))
        else:
            return self.finish(environ, start_response, response)
    
    def finish(self, environ, start_response, response):
        if isinstance(response, WsgiResponse):
            for middleware in self.response_middleware:
                response = middleware(environ, response)
            start_response(response.status, response.get_headers())
        return response
    

class LazyWsgi(LocalMixin):
    '''A :ref:`wsgi handler <wsgi-handlers>` which loads the actual
handler the first time it is called. Subclasses must implement
the :meth:`setup` method.
Useful when working in multiprocessing mode when the application
handler must be a ``pickable`` instance. This handler can rebuild
its wsgi :attr:`handler` every time is pickled and un-pickled without
causing serialisation issues.'''        
    def __call__(self, environ, start_response):
        return self.handler(environ, start_response)
    
    @local_property
    def handler(self):
        '''The :ref:`wsgi application handler <wsgi-handlers>` which
is loaded via the :meth:`setup` method, once only, when first accessed.'''
        return self.setup()
    
    def setup(self):
        '''The setup function for this :class:`LazyWsgi`. Called once only
the first time this application handler is invoked. This **must** be implemented
by subclasses and **must** return a
:ref:`wsgi application handler <wsgi-handlers>`.'''
        raise NotImplementedError
    
    
def get_roule_methods(attrs):
    rule_methods = []
    for code, callable in attrs:
        if code.startswith('__') or not hasattr(callable, '__call__'):
            continue
        rule_method = getattr(callable, 'rule_method', None)
        if isinstance(rule_method, tuple):
            rule_methods.append((code, rule_method))
    return sorted(rule_methods, key=lambda x: x[1][3])
    
    
class RouterType(type):
    
    def __new__(cls, name, bases, attrs):
        rule_methods = get_roule_methods(attrs.items())
        no_rule = set(attrs) - set((x[0] for x in rule_methods))
        for base in bases[::-1]:
            if hasattr(base, 'rule_methods'):
                items = base.rule_methods.items()
            else:
                g = ((name, getattr(base, name)) for name in dir(base))
                items = get_roule_methods(g)
            base_rules = [pair for pair in items if pair[0] not in no_rule]
            rule_methods = base_rules + rule_methods
        #                
        attrs['rule_methods'] = OrderedDict(rule_methods)
        return super(RouterType, cls).__new__(cls, name, bases, attrs)
    

class Redirect(object):
    
    def __init__(self, path):
        self.path = path
        
    def response(self, environ, start_response, args):
        request = wsgi_request(environ)
        raise HttpRedirect(request.full_path(self.path))
    
    
class Router(RouterType('RouterBase', (object,), {})):
    '''A WSGI application which handle multiple
:ref:`routes <apps-wsgi-route>`. A user must implement the HTTP method
required by her application. For example if the route needs to serve a ``GET``
request, the ``get(self, request)`` method must be implemented.

:param rule: String used for creating the :attr:`route` of this :class:`Router`.
:param routes: Optionals :class:`Router` instances which are added to the
    children :attr:`routes` of this router.
:param parameters: Optional parameters for this router. They are stored in the
    :attr:`parameters` attribute.
    
.. attribute:: route

    The :ref:`Route <apps-wsgi-route>` served by this :class:`Router`.

.. attribute:: routes

    List of children :class:`Router` of this :class:`Router`.

.. attribute:: parent

    The parent :class:`Router` of this :class:`Router`.
        
.. attribute:: accept_content_types

    Tuple of content type accepted by this :class:`Router`. It can be specified
    as class attribute or during initialisation. This is an important parameter.
    It is used to set the content type of the response instance.
    Default ``None``.
    
.. attribute:: parameters

    A :class:`pulsar.utils.structures.AttributeDictionary` of parameters.
'''
    creation_count = 0
    accept_content_types = None
    _parent = None
    
    def __init__(self, rule, *routes, **parameters):
        self.__class__.creation_count += 1
        self.creation_count = self.__class__.creation_count
        if not isinstance(rule, Route):
            rule = Route(rule)
        self.route = rule
        self.routes = []
        for router in routes:
            self.add_child(router)
        self.parameters = AttributeDictionary()
        for name, rule_method in self.rule_methods.items():
            rule, method, params, count = rule_method
            rparameters = params.copy()
            handler = getattr(self, name)
            if rparameters.pop('async', False): # asynchronous method
                handler = async()(handler)
                handler.rule_method = rule_method
            router = self.add_child(Router(rule, **rparameters))
            setattr(router, method, handler)
        if 'accept_content_types' in parameters:
            self.accept_content_types = parameters.pop('accept_content_types')
        self.setup(**parameters)
        
    def setup(self, content_type=None, **parameters):
        for name, value in parameters.items():
            if not hasattr(self, name) and hasattr(value, '__call__'):
                setattr(self, name, value)
            else:
                self.parameters[name] = value
    
    @property
    def root(self):
        '''The root :class:`Router` for this :class:`Router`.'''
        if self.parent:
            return self.parent.root
        else:
            return self
     
    @property
    def parent(self):
        return self._parent
    
    def parent_property(self, name):
        value = getattr(self, name, None)
        if value is None and self._parent:
            return self._parent.parent_property(name)
        else:
            return value
        
    def content_type(self, request):
        '''Evaluate the content type for the response to the client ``request``.
The method uses the :attr:`accept_content_types` tuple of accepted content
types and the content types accepted by the client and figure out
the best match.'''
        accept = self.parent_property('accept_content_types')
        if accept:
            return request.content_types.best_match(accept)
       
    def __repr__(self):
        return self.route.__repr__()
        
    def __call__(self, environ, start_response):
        path = environ.get('PATH_INFO') or '/'
        path = path[1:]
        router_args = self.resolve(path)
        if router_args:
            router, args = router_args
            return router.response(environ, start_response, args)
        
    def resolve(self, path, urlargs=None):
        '''Resolve a path and return a ``(handler, urlargs)`` tuple or
``None`` if the path could not be resolved.'''
        urlargs = urlargs if urlargs is not None else {}
        match = self.route.match(path)
        if match is None:
            if self.route.is_leaf:
                if path.endswith('/'):
                    match = self.route.match(path[:-1])
                    if match is not None and not match:
                        return Redirect(path[:-1]), None
            else:
                if not path.endswith('/'):
                    match = self.route.match('%s/' % path)
                    if match is not None:
                        return Redirect('%s/' % path), None
            return
        if '__remaining__' in match:
            remaining_path = match['__remaining__']
            for handler in self.routes:
                view_args = handler.resolve(remaining_path, urlargs)
                if view_args is None:
                    continue
                #remaining_path = match.pop('__remaining__','')
                #urlargs.update(match)
                return view_args
        else:
            return self, match
    
    def response(self, environ, start_response, args):
        '''Once the :meth:`resolve` method has matched the correct
:class:`Router` for serving the request, this matched roter invokes this method
to actually produce the WSGI response.'''
        request = wsgi_request(environ, self, args)
        # Set the response content type
        request.response.content_type = self.content_type(request)
        method = request.method.lower()
        callable = getattr(self, method, None)
        if callable is None:
            raise HttpException(status=405,
                                msg='Method "%s" not allowed' % method)
        return callable(request)
    
    def add_child(self, router):
        '''Add a new :class:`Router` to the :attr:`routes` list. If this
:class:`Router` is a leaf route, add a slash to the url.'''
        assert isinstance(router, Router), 'Not a valid Router'
        assert router is not self, 'cannot add self to children'
        if self.route.is_leaf:
            self.route = Route('%s/' % self.route.rule)
        for r in self.routes:
            if r.route == router.route:
                r.parameters.update(router.parameters)
                return r
        if router.parent:
            router.parent.remove_child(router)
        router._parent = self
        self.routes.append(router)
        return router
        
    def remove_child(self, router):
        '''remove a :class:`Router` from the :attr:`routes` list.'''
        if router in self.routes:
             self.routes.remove(router)
             router._parent = None
        
    def link(self, *args, **urlargs):
        '''Return an anchor :class:`Html` element with the `href` attribute
set to the url of this :class:`Router`.'''
        if len(args) > 1:
            raise ValueError
        url = self.route.url(**urlargs)
        if len(args) == 1:
            text = args[0]
        else:
            text = url
        return Html('a', text, href=url)
    
    def sitemap(self, root=None):
        '''This utility method returns a sitemap starting at root.
If *root* is ``None`` it starts from this :class:`Router`.

:param request: a :ref:`wsgi request wrapper <app-wsgi-request>`
:param root: Optional url path where to start the sitemap.
    By default it starts from this :class:`Router`. Pass `"/"` to
    start from the root :class:`Router`.
:param levels: Number of nested levels to include.
:return: A list of children
'''
        if not root:
            root = self
        else:
            handler_urlargs = self.root.resolve(root[1:])
            if handler_urlargs:
                root, urlargs = handler_urlargs
            else:
                return []
        return list(self.routes)    
    
    def encoding(self, request):
        '''The encoding to use for the response. By default it
returns ``utf-8``.'''
        return 'utf-8'
    

class MediaMixin(Router):
    accept_content_types = ('application/octet-stream', 'text/css')
    cache_control = CacheControl(maxage=86400)
    
    def serve_file(self, request, fullpath):
        # Respect the If-Modified-Since header.
        statobj = os.stat(fullpath)
        content_type, encoding = mimetypes.guess_type(fullpath)
        response = request.response
        if content_type: 
            response.content_type = content_type
        response.encoding = encoding
        if not self.was_modified_since(request.environ.get(
                                            'HTTP_IF_MODIFIED_SINCE'),
                                       statobj[stat.ST_MTIME],
                                       statobj[stat.ST_SIZE]):
            response.status_code = 304
        else:
            response.content = open(fullpath, 'rb').read()
            response.headers["Last-Modified"] = http_date(statobj[stat.ST_MTIME])
        return response

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
    
