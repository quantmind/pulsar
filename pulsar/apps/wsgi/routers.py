'''
Routing is the process of matching and parsing a URL to something we can use.
Pulsar provides a flexible integrated
routing system you can use for that. It works by creating a
:class:`Router` instance with its own ``rule`` and, optionally, additional
sub-routers for handling additional urls::

    class Page(Router):
        response_content_types = RouterParam(('text/html',
                                              'text/plain',
                                              'application/json'))

        def get(self, request):
            "This method handle requests with get-method"
            ...

        def post(self, request):
            "This method handle requests with post-method"
            ...

        def delete(self, request):
            "This method handle requests with delete-method"
            ...

        ...


    middleware = Page('/bla')


.. _wsgi-router:

Router
=====================

The :ref:`middleware <wsgi-middleware>` constructed in the snippet above
handles ``get`` and ``post`` methods at the ``/bla`` url.
The :class:`Router` introduces a new element into pulsar WSGI handlers, the
:ref:`wsgi request <app-wsgi-request>`, a light-weight wrapper of the
WSGI environ.

For an exaustive example on how to use the :class:`Router` middleware make sure
you check out the :ref:`HttpBin example <tutorials-httpbin>`.

.. autoclass:: Router
   :members:
   :member-order: bysource


.. _wsgi-media-router:

Media Router
=====================

The :class:`MediaRouter` is a specialised :class:`Router` for serving static
files such ass ``css``, ``javascript``, images and so forth.

.. autoclass:: MediaRouter
   :members:
   :member-order: bysource


RouterParam
=================

.. autoclass:: RouterParam
   :members:
   :member-order: bysource

.. _WSGI: http://www.wsgi.org
'''
import os
import re
import stat
import mimetypes
from email.utils import parsedate_tz, mktime_tz

from pulsar.utils.httpurl import http_date, CacheControl
from pulsar.utils.structures import OrderedDict
from pulsar.utils.slugify import slugify
from pulsar import Http404, HttpException

from .route import Route
from .utils import wsgi_request
from .content import Html


__all__ = ['Router', 'MediaRouter', 'FileRouter', 'MediaMixin',
           'RouterParam']


def get_roule_methods(attrs):
    rule_methods = []
    for code, callable in attrs:
        if code.startswith('__') or not hasattr(callable, '__call__'):
            continue
        rule_method = getattr(callable, 'rule_method', None)
        if isinstance(rule_method, tuple):
            rule_methods.append((code, rule_method))
    return sorted(rule_methods, key=lambda x: x[1].order)


def update_args(urlargs, args):
    if urlargs:
        urlargs.update(args)
        return urlargs
    return args


class RouterParam(object):
    '''A :class:`RouterParam` is a way to flag a :class:`Router` parameter
    so that children can inherit the value if they don't define their own.

    A :class:`RouterParam` is always defined as a class attribute and it
    is processed by the :class:`Router` metaclass and stored in a dictionary
    available as ``parameter`` class attribute.

    .. attribute:: value

        The value associated with this :class:`RouterParam`. THis is the value
        stored in the :class:`Router.parameters` dictionary at key given by
        the class attribute specified in the class definition.
    '''
    def __init__(self, value=None):
        self.value = value


class RouterType(type):
    ''':class:`Router` metaclass.'''
    def __new__(cls, name, bases, attrs):
        rule_methods = get_roule_methods(attrs.items())
        defaults = set()
        for key, value in list(attrs.items()):
            if isinstance(value, RouterParam):
                defaults.add(key)
                attrs[key] = value.value
        no_rule = set(attrs) - set((x[0] for x in rule_methods))
        base_rules = []
        for base in reversed(bases):
            if hasattr(base, 'defaults'):
                params = base.defaults.copy()
                params.update(defaults)
                defaults = params
            if hasattr(base, 'rule_methods'):
                items = base.rule_methods.items()
            else:
                g = ((key, getattr(base, key)) for key in dir(base))
                items = get_roule_methods(g)
            rules = [pair for pair in items if pair[0] not in no_rule]
            base_rules = base_rules + rules

        if base_rules:
            all = base_rules + rule_methods
            rule_methods = {}
            for namerule, rule in all:
                if namerule in rule_methods:
                    rule = rule.override(rule_methods[namerule])
                rule_methods[namerule] = rule
            rule_methods = sorted(rule_methods.items(),
                                  key=lambda x: x[1].order)
        attrs['rule_methods'] = OrderedDict(rule_methods)
        attrs['defaults'] = defaults
        return super(RouterType, cls).__new__(cls, name, bases, attrs)


class Router(metaclass=RouterType):
    '''A :ref:`WSGI middleware <wsgi-middleware>` to handle client requests
    on multiple :ref:`routes <apps-wsgi-route>`.

    The user must implement the HTTP methods
    required by the application. For example if the route needs to
    serve a ``GET`` request, the ``get(self, request)`` method must
    be implemented.

    :param rule: String used for creating the :attr:`route` of this
        :class:`Router`.
    :param routes: Optional :class:`Router` instances which are added to the
        children :attr:`routes` of this router.
    :param parameters: Optional parameters for this router.

    .. attribute:: rule_methods

        A class attribute built during class creation. It is an ordered
        dictionary mapping method names with a five-elements tuple
        containing information
        about a child route (See the :class:`.route` decorator).

    .. attribute:: routes

        List of children :class:`Router` of this :class:`Router`.

    .. attribute:: parent

        The parent :class:`Router` of this :class:`Router`.

    .. attribute:: response_content_types

        A list/tuple of possible content types of a response to a
        client request.

        The client request must accept at least one of the response content
        types, otherwise an HTTP ``415`` exception occurs.

    .. attribute:: response_wrapper

        Optional function which wraps all handlers of this :class:`.Router`.
        The function must accept two parameters, the original handler
        and the :class:`.WsgiRequest`::

            def response_wrapper(handler, request):
                ...
                return handler(request)

    '''
    _creation_count = 0
    _parent = None
    name = None

    response_content_types = RouterParam(None)
    response_wrapper = RouterParam(None)

    def __init__(self, rule, *routes, **parameters):
        Router._creation_count += 1
        self._creation_count = Router._creation_count
        if not isinstance(rule, Route):
            rule = Route(rule)
        self._route = rule
        parameters.setdefault('name', rule.name or self.name or '')
        self._set_params(parameters)
        self.routes = []
        # add routes specified via the initialiser first
        for router in routes:
            self.add_child(router)

        for name, rule_method in self.rule_methods.items():
            rule, method, params, _, _ = rule_method
            rparameters = params.copy()
            handler = getattr(self, name)
            self.add_child(self.make_router(rule, method=method,
                                            handler=handler, **rparameters))

    @property
    def route(self):
        '''The relative :class:`.Route` served by this
        :class:`Router`.
        '''
        parent = self._parent
        if parent and parent._route.is_leaf:
            return parent.route + self._route
        else:
            return self._route

    @property
    def full_route(self):
        '''The full :attr:`route` for this :class:`.Router`.

        It includes the :attr:`parent` portion of the route if a parent
        router is available.
        '''
        if self._parent:
            return self._parent.full_route + self._route
        else:
            return self._route

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

    @property
    def creation_count(self):
        '''Integer for sorting :class:`Router` by creation.

        Auto-generated during initialisation.'''
        return self._creation_count

    @property
    def rule(self):
        '''The full ``rule`` string for this :class:`Router`.

        It includes the :attr:`parent` portion of the rule if a :attr:`parent`
        router is available.
        '''
        return self.full_route.rule

    def path(self, **urlargs):
        '''The full path of this :class:`Router`.

        It includes the :attr:`parent` portion of url if a parent router
        is available.
        '''
        return self.full_route.url(**urlargs)

    def getparam(self, name, default=None):
        '''A parameter in this :class:`.Router`
        '''
        try:
            return getattr(self, name)
        except AttributeError:
            return default

    def __getattr__(self, name):
        '''Get the value of the ``name`` attribute.

        If the ``name`` is not available, retrieve it from the
        :attr:`parent` :class:`Router` if it exists.
        '''
        if self._parent:
            return getattr(self._parent, name)

        raise AttributeError("'%s' object has no attribute '%s'" %
                             (self.__class__.__name__, name))

    def content_type(self, request):
        '''Evaluate the content type for the response to a client ``request``.

        The method uses the :attr:`response_content_types` parameter of
        accepted content types and the content types accepted by the client
        ``request`` and figures out the best match.
        '''
        response_content_types = self.response_content_types
        request_content_types = request.content_types
        if request_content_types:
            ct = request_content_types.best_match(response_content_types)
            if ct and '*' in ct:
                ct = None
            if not ct and response_content_types:
                raise HttpException(status=415, msg=request_content_types)
            return ct

    def __repr__(self):
        return self.full_route.__repr__()

    def __call__(self, environ, start_response=None):
        path = environ.get('PATH_INFO') or '/'
        path = path[1:]
        router_args = self.resolve(path)
        if router_args:
            router, args = router_args
            return router.response(environ, args)

    def resolve(self, path, urlargs=None):
        '''Resolve a path and return a ``(handler, urlargs)`` tuple or
        ``None`` if the path could not be resolved.
        '''
        match = self.route.match(path)
        if match is None:
            if not self.route.is_leaf:  # no match
                return
        elif '__remaining__' in match:
            path = match.pop('__remaining__')
            urlargs = update_args(urlargs, match)
        else:
            return self, update_args(urlargs, match)
        #
        for handler in self.routes:
            view_args = handler.resolve(path, urlargs)
            if view_args is None:
                continue
            return view_args

    def response(self, environ, args):
        '''Once the :meth:`resolve` method has matched the correct
        :class:`Router` for serving the request, this matched router invokes
        this method to produce the WSGI response.
        '''
        request = wsgi_request(environ, self, args)
        request.response.content_type = self.content_type(request)
        method = request.method.lower()
        callable = getattr(self, method, None)
        if callable is None:
            raise HttpException(status=405)
        response_wrapper = self.response_wrapper
        if response_wrapper:
            return response_wrapper(callable, request)
        return callable(request)

    def add_child(self, router):
        '''Add a new :class:`Router` to the :attr:`routes` list.
        '''
        assert isinstance(router, Router), 'Not a valid Router'
        assert router is not self, 'cannot add self to children'

        for r in self.routes:
            if r == router:
                return r
            elif r._route == router._route:
                raise ValueError('Cannot add route %s. Already avalable' %
                                 r._route)
        #
        # Remove from previous parent
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

    def get_route(self, name):
        '''Get a child :class:`Router` by its :attr:`name`.

        This method search child routes recursively.
        '''
        for route in self.routes:
            if route.name == name:
                return route
        for child in self.routes:
            route = child.get_route(name)
            if route:
                return route

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

    def has_parent(self, router):
        '''Check if ``router`` is ``self`` or a parent or ``self``
        '''
        parent = self
        while parent and parent is not router:
            parent = parent._parent
        return parent is not None

    def make_router(self, rule, method=None, handler=None, cls=None,
                    name=None, **params):
        '''Create a new :class:`.Router` from a ``rule`` and parameters.

        This method is used during initialisation when building child
        Routers from the :attr:`rule_methods`.
        '''
        cls = cls or Router
        router = cls(rule, name=name, **params)
        for r in self.routes:
            if r._route == router._route:
                if isinstance(r, cls):
                    router = r
                    router._set_params(params)
                    break
        if method and handler:
            if isinstance(method, tuple):
                for m in method:
                    setattr(router, m, handler)
            else:
                setattr(router, method, handler)
        return router

    # INTERNALS
    def _set_params(self, parameters):
        for name, value in parameters.items():
            if name not in self.defaults:
                name = slugify(name, separator='_')
            setattr(self, name, value)


class MediaMixin(object):

    def serve_file(self, request, fullpath, status_code=None):
        # Respect the If-Modified-Since header.
        statobj = os.stat(fullpath)
        content_type, encoding = mimetypes.guess_type(fullpath)
        response = request.response
        if content_type:
            response.content_type = content_type
        if encoding:
            response.encoding = encoding
        if not (status_code or self.was_modified_since(
                request.environ.get('HTTP_IF_MODIFIED_SINCE'),
                statobj[stat.ST_MTIME],
                statobj[stat.ST_SIZE])):
            response.status_code = 304
        else:
            response.content = open(fullpath, 'rb').read()
            if status_code:
                response.status_code = status_code
            else:
                response.headers["Last-Modified"] = http_date(
                    statobj[stat.ST_MTIME])
        return response

    def was_modified_since(self, header=None, mtime=0, size=0):
        '''Check if an item was modified since the user last downloaded it

        :param header: the value of the ``If-Modified-Since`` header.
            If this is ``None``, simply return ``True``
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

    def static_index(self, request, links):
        doc = request.html_document
        doc.title = 'Index of %s' % request.path
        title = Html('h2', doc.title)
        list = Html('ul', *[Html('li', a) for a in links])
        doc.body.append(Html('div', title, list))
        return doc.http_response(request)


class MediaRouter(Router, MediaMixin):
    '''A :class:`Router` for serving static media files from a given
    directory.

    :param rute: The top-level url for this router. For example ``/media``
        will serve the ``/media/<path:path>`` :class:`Route`.
    :param path: Check the :attr:`path` attribute.
    :param show_indexes: Check the :attr:`show_indexes` attribute.

    .. attribute::    path

        The file-system path of the media files to serve.

    .. attribute::    show_indexes

        If ``True``, the router will serve media file directories as
        well as media files.

    .. attribute:: default_file

        The default file to serve when a directory is requested.
    '''
    cache_control = CacheControl(maxage=86400)

    def __init__(self, rule, path, show_indexes=False,
                 default_suffix=None, default_file='index.html',
                 raise_404=True, **params):
        super().__init__('%s/<path:path>' % rule, **params)
        self._default_suffix = default_suffix
        self._default_file = default_file
        self._show_indexes = show_indexes
        self._file_path = path
        self._raise_404 = raise_404

    def filesystem_path(self, request):
        path = request.urlargs['path']
        bits = [bit for bit in path.split('/') if bit]
        return os.path.join(self._file_path, *bits)

    def get(self, request):
        fullpath = self.filesystem_path(request)
        if os.path.isdir(fullpath) and self._default_file:
            file = os.path.join(fullpath, self._default_file)
            if os.path.isfile(file):
                if not request.path.endswith('/'):
                    return request.redirect('%s/' % request.path)
                fullpath = file
        #
        if os.path.isdir(fullpath):
            if self._show_indexes:
                return self.directory_index(request, fullpath)
            else:
                raise Http404
        #
        filename = os.path.basename(fullpath)
        if '.' not in filename and self._default_suffix:
            fullpath = '%s.%s' % (fullpath, self._default_suffix)
        #
        if os.path.isfile(fullpath):
            return self.serve_file(request, fullpath)
        elif self._raise_404:
            raise Http404


class FileRouter(Router, MediaMixin):
    '''A Router for a single file
    '''
    response_content_types = RouterParam(('application/octet-stream',
                                          'text/css',
                                          'application/javascript',
                                          'text/html'))
    cache_control = CacheControl(maxage=86400)

    def __init__(self, route, file_path, status_code=None, raise_404=True):
        super().__init__(route)
        self._status_code = status_code
        self._file_path = file_path
        self._raise_404 = raise_404

    def filesystem_path(self, request):
        return self._file_path

    def get(self, request):
        fullpath = self.filesystem_path(request)
        if os.path.isfile(fullpath):
            return self.serve_file(request, fullpath,
                                   status_code=self._status_code)
        elif self._raise_404:
            raise Http404
