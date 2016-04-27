'''

.. contents::
    :local:

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


File Response
=====================

High level, battery included function for serving small and large files
concurrently. Cavet, you app does not need to be asynchronous to use this
method.

.. autofunction:: file_response


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
from pulsar.utils.security import digest
from pulsar import Http404, MethodNotAllowed

from .route import Route
from .utils import wsgi_request
from .content import Html


__all__ = ['Router',
           'MediaRouter',
           'MediaMixin',
           'RouterParam',
           'file_response']


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


def _get_default(parent, name):
    if name in parent.defaults:
        return getattr(parent, name)
    elif parent._parent:
        return _get_default(parent._parent, name)
    else:
        raise AttributeError


class RouterParam:
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
        defaults = {}
        for key, value in list(attrs.items()):
            if isinstance(value, RouterParam):
                defaults[key] = attrs.pop(key).value

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
        return super().__new__(cls, name, bases, attrs)


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

    def getparam(self, name, default=None, parents=False):
        '''A parameter in this :class:`.Router`
        '''
        value = getattr(self, name, None)
        if value is None:
            if parents and self._parent:
                return self._parent.getparam(name, default, parents)
            else:
                return default
        else:
            return value

    def __getattr__(self, name):
        '''Get the value of the ``name`` attribute.

        If the ``name`` is not available, retrieve it from the
        :attr:`parent` :class:`Router` if it exists.
        '''
        available = False
        value = None

        if name in self.defaults:
            available = True
            value = self.defaults[name]

        if self._parent and value is None:
            try:
                return _get_default(self._parent, name)
            except AttributeError:
                pass

        if available:
            return value

        raise AttributeError("'%s' object has no attribute '%s'" %
                             (self.__class__.__name__, name))

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
        method = request.method.lower()
        request.set_response_content_type(self.response_content_types)

        callable = getattr(self, method, None)
        if callable is None:
            raise MethodNotAllowed

        response_wrapper = self.response_wrapper
        if response_wrapper:
            return response_wrapper(callable, request)
        return callable(request)

    def add_child(self, router, index=None):
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
        if index is None:
            self.routes.append(router)
        else:
            self.routes.insert(index, router)
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


class MediaMixin:
    cache_control = CacheControl(maxage=86400)

    def serve_file(self, request, fullpath, status_code=None):
        return file_response(request, fullpath, status_code=status_code,
                             cache_control=self.cache_control)

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
    def __init__(self, rule, path, show_indexes=False,
                 default_suffix=None, default_file='index.html',
                 **params):
        super().__init__('%s/<path:path>' % rule, **params)
        self._default_suffix = default_suffix
        self._default_file = default_file
        self._show_indexes = show_indexes
        self._file_path = path or ''

    def filesystem_path(self, request):
        return self.get_full_path(request.urlargs['path'])

    def get_full_path(self, path):
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
        # Check for missing suffix
        if self._default_suffix:
            ext = '.%s' % self._default_suffix
            if not fullpath.endswith(ext):
                file = '%s%s' % (fullpath, ext)
                if os.path.isfile(file):
                    fullpath = file

        if os.path.isdir(fullpath):
            if self._show_indexes:
                return self.directory_index(request, fullpath)
            else:
                raise Http404
        #
        try:
            return self.serve_file(request, fullpath)
        except Http404:
            file404 = self.get_full_path('404.html')
            if os.path.isfile(file404):
                return self.serve_file(request, file404, status_code=404)
            else:
                raise


def modified_since(header, size=0):
    try:
        if header is None:
            raise ValueError
        matches = re.match(r"^([^;]+)(; length=([0-9]+))?$",
                           header,
                           re.IGNORECASE)
        header_mtime = mktime_tz(parsedate_tz(matches.group(1)))
        header_len = matches.group(3)
        if header_len and int(header_len) != size:
            raise ValueError
        return header_mtime
    except (AttributeError, ValueError, OverflowError):
        pass


def was_modified_since(header=None, mtime=0, size=0):
    '''Check if an item was modified since the user last downloaded it

    :param header: the value of the ``If-Modified-Since`` header.
        If this is ``None``, simply return ``True``
    :param mtime: the modification time of the item in question.
    :param size: the size of the item.
    '''
    header_mtime = modified_since(header, size)
    if header_mtime and header_mtime <= mtime:
        return False
    return True


def file_response(request, filepath, block=None, status_code=None,
                  content_type=None, encoding=None, cache_control=None):
    """Utility for serving a local file

    Typical usage::

        from pulsar.apps import wsgi

        class MyRouter(wsgi.Router):

            def get(self, request):
                return wsgi.file_response(request, "<filepath>")

    :param request: Wsgi request
    :param filepath: full path of file to serve
    :param block: Optional block size (deafult 1MB)
    :param status_code: Optional status code (default 200)
    :return: a :class:`~.WsgiResponse` object
    """
    file_wrapper = request.get('wsgi.file_wrapper')
    if os.path.isfile(filepath):
        response = request.response
        info = os.stat(filepath)
        size = info[stat.ST_SIZE]
        modified = info[stat.ST_MTIME]
        header = request.get('HTTP_IF_MODIFIED_SINCE')
        if not was_modified_since(header, modified, size):
            response.status_code = 304
        else:
            if not content_type:
                content_type, encoding = mimetypes.guess_type(filepath)
            file = open(filepath, 'rb')
            response.headers['content-length'] = str(size)
            response.content = file_wrapper(file, block)
            response.content_type = content_type
            response.encoding = encoding
            if status_code:
                response.status_code = status_code
            else:
                response.headers["Last-Modified"] = http_date(modified)
            if cache_control:
                etag = digest('modified: %d - size: %d' % (modified, size))
                cache_control(response.headers, etag=etag)
        return response
    raise Http404
