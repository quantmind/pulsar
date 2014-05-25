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
            "This method handle request with get-method"
            ...

        def post(self, request):
            "This method handle request with post-method"
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
from pulsar.utils.structures import AttributeDictionary, OrderedDict
from pulsar import Http404, HttpException

from .route import Route
from .utils import wsgi_request
from .content import Html
from .structures import ContentAccept
from .wrappers import WsgiResponse


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
so that children can retrieve the value if they don't define their own.

A :class:`RouterParam` is always defined as a class attribute and it
is processed by the :class:`Router` metaclass and stored in a dictionary
available as ``parameter`` class attribute.

.. attribute:: value

    The value associated with this :class:`RouterParam`. THis is the value
    stored in the :class:`Router.parameters` dictionary at key given by
    the class attribute specified in the class definition.
'''
    def __init__(self, value):
        self.value = value


class RouterType(type):
    ''':class:`Router` metaclass.'''
    def __new__(cls, name, bases, attrs):
        rule_methods = get_roule_methods(attrs.items())
        parameters = {}
        for key, value in list(attrs.items()):
            if isinstance(value, RouterParam):
                parameters[key] = attrs.pop(key).value
        no_rule = set(attrs) - set((x[0] for x in rule_methods))
        base_rules = []
        for base in reversed(bases):
            if hasattr(base, 'parameters'):
                params = base.parameters.copy()
                params.update(parameters)
                parameters = params
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
            for name, rule in all:
                if name in rule_methods:
                    rule = rule.override(rule_methods[name])
                rule_methods[name] = rule
            rule_methods = sorted(rule_methods.items(),
                                  key=lambda x: x[1].order)
        attrs['rule_methods'] = OrderedDict(rule_methods)
        attrs['parameters'] = parameters
        return super(RouterType, cls).__new__(cls, name, bases, attrs)


class Router(RouterType('RouterBase', (object,), {})):
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
        They are stored in the :attr:`parameters` attribute.
        If a ``response_content_types`` value is
        passed, it overrides the :attr:`response_content_types` attribute.

    .. attribute:: routes

        List of children :class:`Router` of this :class:`Router`.

    .. attribute:: parent

        The parent :class:`Router` of this :class:`Router`.

    .. attribute:: response_content_types

        a list/tuple of possible content types of a response to a
        client request.

        The client request must accept at least one of the response content
        types, otherwise an HTTP ``415`` exception occurs.

    .. attribute:: allows_redirects

        boolean indicating if this router can redirect requests to valid urls
        within this router and its children. For example, if a router serves
        the '/echo' url but not the ``/echo/`` one, a request on ``/echo/``
        will be redirected to ``/echo``.

        Default: ``False``

    .. attribute:: parameters

        A :class:`.AttributeDictionary` of parameters for
        this :class:`Router`. Parameters are created at initialisation from
        the ``parameters`` class attribute and the key-valued parameters
        passed to the ``__init__`` method for which the value is not callable.
    '''
    _creation_count = 0
    _parent = None
    _name = None

    response_content_types = RouterParam(None)
    allows_redirects = RouterParam(False)

    def __init__(self, rule, *routes, **parameters):
        Router._creation_count += 1
        self._creation_count = Router._creation_count
        if not isinstance(rule, Route):
            rule = Route(rule)
        self._route = rule
        self._name = parameters.pop('name', rule.rule)
        self.routes = []
        # add routes specified via the initialiser
        for router in routes:
            self.add_child(router)
        # copy parameters
        self.parameters = AttributeDictionary(self.parameters)
        for name, rule_method in self.rule_methods.items():
            rule, method, params, _, _ = rule_method
            rparameters = params.copy()
            handler = getattr(self, name)
            router = self.add_child(Router(rule, **rparameters))
            setattr(router, method, handler)
        for name, value in parameters.items():
            if name in self.parameters:
                self.parameters[name] = value
            else:
                setattr(self, name, value)

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
    def name(self):
        '''The name of this :class:`Router`.

        This attribute can be specified during initialisation.
        If available, it can be used to retrieve a child router
        by name via the :meth:`get_route` method.
        '''
        return self._name

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
    def default_content_type(self):
        '''The default content type for responses.

        This is the first element in the :attr:`response_content_types` list.
        '''
        ct = self.response_content_types
        return ct[0] if ct else None

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

    def __getattr__(self, name):
        '''Check the value of a :attr:`parameters` ``name``.

        If the parameter is not available, retrieve the parameter from the
        :attr:`parent` :class:`Router` if it exists.
        '''
        if not name.startswith('_'):
            return self.get_parameter(name, False)
        self.no_param(name)

    def get_parameter(self, name, safe=True):
        value = self.parameters.get(name)
        if value is None:
            if self._parent:
                return self._parent.get_parameter(name, safe)
            elif name in self.parameters:
                return value
            elif not safe:
                self.no_param(name)
        else:
            return value

    def no_param(self, name):
        raise AttributeError("'%s' object has no attribute '%s'" %
                             (self.__class__.__name__, name))

    def content_type(self, request):
        '''Evaluate the content type for the response to a client ``request``.

        The method uses the :attr:`response_content_types` parameter of
        accepted content types and the content types accepted by the client
        and figure out the best match.
        '''
        return request.content_types.best_match(self.response_content_types)

    def accept_content_type(self, content_type):
        '''Check if ``content_type`` is accepted by this :class:`Router`.

        Return the best mach or ``None`` if not accepted.'''
        response_content_types = self.response_content_types
        if response_content_types:
            return ContentAccept(
                [(content_type, 1)]).best_match(response_content_types)

    def __repr__(self):
        return self.route.__repr__()

    def __call__(self, environ, start_response=None):
        path = environ.get('PATH_INFO') or '/'
        path = path[1:]
        router_args = self.resolve(path)
        if router_args:
            router, args = router_args
            return router.response(environ, args)
        elif self.allows_redirects:
            if self.route.is_leaf:
                if path.endswith('/'):
                    router_args = self.resolve(path[:-1])
                    if router_args is not None:
                        return self.redirect(environ, '/%s' % path[:-1])
            else:
                if not path.endswith('/'):
                    router_args = self.resolve('%s/' % path)
                    if router_args is not None:
                        return self.redirect(environ, '/%s/' % path)

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
        # Set the response content type
        request.response.content_type = self.content_type(request)
        method = request.method.lower()
        callable = getattr(self, method, None)
        if callable is None:
            raise HttpException(status=405,
                                msg='Method "%s" not allowed' % method)
        return callable(request)

    def redirect(self, path):
        '''Redirect to a different ``path``
        '''
        return WsgiResponse(302, response_headers=[('location', path)])

    def add_child(self, router):
        '''Add a new :class:`Router` to the :attr:`routes` list.
        '''
        assert isinstance(router, Router), 'Not a valid Router'
        assert router is not self, 'cannot add self to children'
        # Loop over available routers to check it the router
        # is already available
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

    def get_route(self, name):
        '''Get a child :class:`Router` by its :attr:`name`.'''
        for route in self.routes:
            if route.name == name:
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
        '''The encoding to use for the response.

        By default it returns ``utf-8``.'''
        return 'utf-8'


class MediaMixin(Router):
    response_content_types = RouterParam(('application/octet-stream',
                                          'text/css',
                                          'application/javascript',
                                          'text/html'))
    cache_control = CacheControl(maxage=86400)
    _file_path = ''

    def serve_file(self, request, fullpath, status_code=None):
        # Respect the If-Modified-Since header.
        statobj = os.stat(fullpath)
        content_type, encoding = mimetypes.guess_type(fullpath)
        response = request.response
        if content_type:
            response.content_type = content_type
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

    def html_title(self, request):
        return 'Index of %s' % request.path

    def static_index(self, request, links):
        doc = request.html_document
        doc.title = 'Index of %s' % request.path
        title = Html('h2', doc.title)
        list = Html('ul', *[Html('li', a) for a in links])
        doc.body.append(Html('div', title, list))
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

        If ``True``, the router will serve media file directories as
        well as media files.

    .. attribute:: default_file

        The default file to serve when a directory is requested.
    '''
    def __init__(self, rute, path, show_indexes=False, mapping=None,
                 default_suffix=None, default_file='index.html',
                 raise_404=True):
        rute = '%s/<path:path>' % rute
        if rute.startswith('/'):
            rute = rute[1:]
        super(MediaRouter, self).__init__(rute)
        self._mapping = mapping or {}
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


class FileRouter(MediaMixin):
    '''A Router for a single file.'''
    def __init__(self, route, file_path, status_code=None, raise_404=True):
        super(FileRouter, self).__init__(route)
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
