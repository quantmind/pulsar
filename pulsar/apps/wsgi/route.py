'''Routing classes for matching and parsing urls.

.. note::

    The :mod:`~pulsar.apps.wsgi.route` module was originally from the routing
    module in werkzeug_. Original License:

    copyright (c) 2011 by the Werkzeug Team. License BSD

.. _werkzeug: https://github.com/mitsuhiko/werkzeug

A :class:`Route` is a class for relative url paths::

    r1 = Route('bla')
    r2 = Route('bla/foo')

Integers::

    # accept any integer
    Route('<int:size>')
    # accept an integer between 1 and 200 only
    Route('<int(min=1,max=200):size>')


Paths::

    # accept any path (including slashes)
    Route('<path:pages>')
    Route('<path:pages>/edit')



.. _wsgi-route-decorator:

Route decorator
==================

.. autoclass:: route
   :members:
   :member-order: bysource

.. _apps-wsgi-route:

Route
================

.. autoclass:: Route
   :members:
   :member-order: bysource

'''
import re
from collections import namedtuple

from pulsar import Http404
from pulsar.utils.httpurl import (iri_to_uri, remove_double_slash,
                                  ENCODE_URL_METHODS, ENCODE_BODY_METHODS)
from pulsar.utils.pep import to_string
from pulsar.utils.slugify import slugify


__all__ = ['route', 'Route']


class rule_info(namedtuple('rinfo', 'rule method parameters position order')):

    def override(self, parent):
        if self.position is None:
            return rule_info(self.rule, self.method, self.parameters,
                             parent.position, parent.order)
        else:
            return self


_rule_re = re.compile(r'''
    (?:
        (?P<converter>[a-zA-Z_][a-zA-Z0-9_]*)   # converter name
        (?:\((?P<args>.*?)\))?                  # converter parameters
        \:                                      # variable delimiter
    )?
    (?P<variable>[a-zA-Z_][a-zA-Z0-9_]*)        # variable name
''', re.VERBOSE)

_converter_args_re = re.compile(r'''
    ((?P<name>\w+)\s*=\s*)?
    (?P<value>
        True|False|
        \d+.\d+|
        \d+.|
        \d+|
        \w+|
        [urUR]?(?P<stringval>"[^"]*?"|'[^']*')
    )\s*,
''', re.VERBOSE | re.UNICODE)


_PYTHON_CONSTANTS = {
    'None':     None,
    'True':     True,
    'False':    False
}


def _pythonize(value):
    if value in _PYTHON_CONSTANTS:
        return _PYTHON_CONSTANTS[value]
    for convert in int, float:
        try:
            return convert(value)
        except ValueError:
            pass
    if value[:1] == value[-1:] and value[0] in '"\'':
        value = value[1:-1]
    return str(value)


def parse_rule(rule):
    """Parse a rule and return it as generator. Each iteration yields tuples
    in the form ``(converter, parameters, variable)``. If the converter is
    `None` it's a static url part, otherwise it's a dynamic one.

    :internal:
    """
    m = _rule_re.match(rule)
    if m is None or m.end() < len(rule):
        raise ValueError('Error while parsing rule {0}'.format(rule))
    data = m.groupdict()
    converter = data['converter'] or 'default'
    return converter, data['args'] or None, data['variable']


class route:
    '''Decorator to create a child route from a :class:`.Router` method.

    Typical usage::

        from pulsar.apps.wsgi import Router, route

        class View(Router):

            def get(self, request):
                ...

            @route()
            def foo(self, request):
                ...

            @route('/bla', method='post')
            def handle2(self, request):
                ...


    In this example, ``View`` is the **parent router** which handle get
    requests only.

    The decorator injects the :attr:`rule_method` attribute to the
    method it decorates. The attribute is a **five elements tuple**
    which contains the :class:`Route`, the HTTP ``method``, a
    dictionary of additional ``parameters``, ``position`` and
    the ``order``. Position and order are internal integers used by the
    :class:`.Router` when deciding the order of url matching.
    If ``position`` is not passed,
    the order will be given by the position of each method in the
    :class:`.Router` class.

    The decorated method are stored in the :attr:`.Router.rule_methods` class
    attribute.

        >>> len(View.rule_methods)
        2
        >>> View.rule_methods['foo'].rule
        /foo
        >>> View.rule_methods['foo'].method
        'get'
        >>> View.rule_methods['handle2'].rule
        /bla
        >>> View.rule_methods['handle2'].method
        'post'

    Check the :ref:`HttpBin example <tutorials-httpbin>`
    for a sample usage.

    :param rule: Optional string for the relative url served by the method
        which is decorated. If not supplied, the method name is used.
    :param method: Optional HTTP method name. Default is `get`.
    :param defaults: Optional dictionary of default variable values used when
        initialising the :class:`Route` instance.
    :param position: Optional positioning of the router within the
        list of child routers of the parent router
    :param parameters: Additional parameters used when initialising
        the :class:`.Router` created by this decorator

    '''
    creation_count = 0

    def __init__(self, rule=None, method=None, defaults=None,
                 position=None, re=False, **parameters):
        self.__class__.creation_count += 1
        self.position = position
        self.creation_count = self.__class__.creation_count
        self.rule = rule
        self.re = re
        self.defaults = defaults
        self.method = method
        self.parameters = parameters

    @property
    def order(self):
        return self.creation_count if self.position is None else self.position

    def __call__(self, callable):
        bits = callable.__name__.split('_')
        method = None
        if len(bits) > 1:
            m = bits[0].upper()
            if m in ENCODE_URL_METHODS or m in ENCODE_BODY_METHODS:
                method = m
                bits = bits[1:]
        name = self.parameters.get('name', '_'.join(bits))
        self.parameters['name'] = name
        method = self.method or method or 'get'
        if isinstance(method, (list, tuple)):
            method = tuple((m.lower() for m in method))
        else:
            method = method.lower()
        rule = Route(self.rule or name, defaults=self.defaults, is_re=self.re)
        callable.rule_method = rule_info(rule, method, self.parameters,
                                         self.position, self.order)
        return callable


class Route:
    '''A Route is a class with a relative :attr:`path`.

    :parameter rule: a normal URL path with ``placeholders`` in the
        format ``<converter(parameters):name>``
        where both the ``converter`` and the ``parameters`` are optional.
        If no ``converter`` is defined the `default` converter is used which
        means ``string``, ``name`` is the variable name.
    :parameter defaults: optional dictionary of default values for the rule
        variables.

    .. attribute:: is_leaf

        If ``True``, the route is equivalent to a file.
        For example ``/bla/foo``

    .. attribute:: rule

        The rule string, does not include the initial ``'/'``

    .. attribute:: path

        The full rule for this route including initial ``'/'``.

    .. attribute:: variables

        a set of  variable names for this route. If the route has no
        variables, the set is empty.

    .. _werkzeug: https://github.com/mitsuhiko/werkzeug
    '''
    def __init__(self, rule, defaults=None, is_re=False):
        rule = remove_double_slash('/%s' % rule)
        self.defaults = defaults if defaults is not None else {}
        self.is_leaf = not rule.endswith('/')
        self.rule = rule[1:]
        self.variables = set(map(str, self.defaults))
        breadcrumbs = []
        self._converters = {}
        regex_parts = []
        if self.rule:
            for bit in self.rule.split('/'):
                if not bit:
                    continue
                s = bit[0]
                e = bit[-1]
                if s == '<' or e == '>':
                    if s + e != '<>':
                        raise ValueError(
                            'malformed rule {0}'.format(self.rule))
                    converter, parameters, variable = parse_rule(bit[1:-1])
                    if variable in self._converters:
                        raise ValueError('variable name {0} used twice in '
                                         'rule {1}.'.format(variable,
                                                            self.rule))
                    convobj = get_converter(converter, parameters)
                    regex_parts.append('(?P<%s>%s)' % (variable,
                                                       convobj.regex))
                    breadcrumbs.append((True, variable))
                    self._converters[variable] = convobj
                    self.variables.add(str(variable))
                else:
                    variable = bit if is_re else re.escape(bit)
                    regex_parts.append(variable)
                    breadcrumbs.append((False, bit))

        self.breadcrumbs = tuple(breadcrumbs)
        self._regex_string = '/'.join(regex_parts)
        if self._regex_string and not self.is_leaf:
            self._regex_string += '/'
        self._regex = re.compile(self.regex, re.UNICODE)

    @property
    def level(self):
        return len(self.breadcrumbs)

    @property
    def path(self):
        return '/' + self.rule

    @property
    def name(self):
        '''A nice name for the route.

        Derived from :attr:`rule` replacing underscores and dashes.
        '''
        return slugify(self.rule, separator='_')

    @property
    def regex(self):
        if self.is_leaf:
            return '^' + self._regex_string + '$'
        else:
            return '^' + self._regex_string

    @property
    def bits(self):
        return tuple((b[1] for b in self.breadcrumbs))

    @property
    def ordered_variables(self):
        '''Tuple of ordered url :attr:`variables`
        '''
        return tuple((b for dyn, b in self.breadcrumbs if dyn))

    def __hash__(self):
        return hash(self.rule)

    def __repr__(self):
        return self.path

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return str(self) == str(other)
        else:
            return False

    def __lt__(self, other):
        if isinstance(other, self.__class__):
            return to_string(self) < to_string(other)
        else:
            raise TypeError('Cannot compare {0} with {1}'.format(self, other))

    def _url_generator(self, values):
        for is_dynamic, val in self.breadcrumbs:
            if is_dynamic:
                val = self._converters[val].to_url(values[val])
            yield val

    def url(self, **urlargs):
        '''Build a ``url`` from ``urlargs`` key-value parameters
        '''
        if self.defaults:
            d = self.defaults.copy()
            d.update(urlargs)
            urlargs = d
        url = '/'.join(self._url_generator(urlargs))
        if not url:
            return '/'
        else:
            url = '/' + url
            return url if self.is_leaf else url + '/'

    def safe_url(self, params=None):
        try:
            if params:
                return self.url(**params)
            else:
                return self.url()
        except KeyError:
            return None

    def match(self, path):
        '''Match a path and return ``None`` if no matching, otherwise
        a dictionary of matched variables with values. If there is more
        to be match in the path, the remaining string is placed in the
        ``__remaining__`` key of the dictionary.'''
        match = self._regex.search(path)
        if match is not None:
            remaining = path[match.end():]
            groups = match.groupdict()
            result = {}
            for name, value in groups.items():
                try:
                    value = self._converters[name].to_python(value)
                except Http404:
                    return
                result[str(name)] = value
            if remaining:
                result['__remaining__'] = remaining
            return result

    def split(self):
        '''Return a two element tuple containing the parent route and
        the last url bit as route. If this route is the root route, it returns
        the root route and ``None``. '''
        rule = self.rule
        if not self.is_leaf:
            rule = rule[:-1]
        if not rule:
            return Route('/'), None
        bits = ('/'+rule).split('/')
        last = Route(bits[-1] if self.is_leaf else bits[-1] + '/')
        if len(bits) > 1:
            return Route('/'.join(bits[:-1]) + '/'), last
        else:
            return last, None

    def __add__(self, other):
        cls = self.__class__
        defaults = self.defaults.copy()
        is_re = False
        if isinstance(other, cls):
            rule = other.rule
            defaults.update(other.defaults)
            is_re = True
        else:
            rule = str(other)
        return cls('%s/%s' % (self.rule, rule), defaults, is_re=is_re)


class BaseConverter:
    """Base class for all converters."""
    regex = '[^/]+'

    def to_python(self, value):
        return value

    def to_url(self, value):
        return iri_to_uri(value)


class StringConverter(BaseConverter):
    """This converter is the default converter and accepts any string but
    only one path segment.  Thus the string can not include a slash.

    This is the default validator.

    Example::

        Rule('/pages/<page>'),
        Rule('/<string(length=2):lang_code>')

    :param minlength: the minimum length of the string.  Must be greater
                      or equal 1.
    :param maxlength: the maximum length of the string.
    :param length: the exact length of the string.
    """

    def __init__(self, minlength=1, maxlength=None, length=None):
        if length is not None:
            length = '{%d}' % int(length)
        else:
            if maxlength is None:
                maxlength = ''
            else:
                maxlength = int(maxlength)
            length = '{%s,%s}' % (
                int(minlength),
                maxlength
            )
        self.regex = '[^/]' + length


class AnyConverter(BaseConverter):
    """Matches one of the items provided.  Items can either be Python
    identifiers or strings::

        Rule('/<any(about, help, imprint, class, "foo,bar"):page_name>')

    :param items: this function accepts the possible items as positional
                  parameters.
    """

    def __init__(self, *items):
        self.regex = '(?:%s)' % '|'.join([re.escape(x) for x in items])


class PathConverter(BaseConverter):
    """Like the default :class:`StringConverter`, but it also matches
    slashes.  This is useful for wikis and similar applications::

        Rule('/<path:wikipage>')
        Rule('/<path:wikipage>/edit')
    """
    regex = '.*'


class NumberConverter(BaseConverter):
    """Baseclass for `IntegerConverter` and `FloatConverter`.

    :internal:
    """
    def __init__(self, fixed_digits=0, min=None, max=None):
        self.fixed_digits = fixed_digits
        self.min = min
        self.max = max

    def to_python(self, value):
        if (self.fixed_digits and len(value) != self.fixed_digits):
            raise Http404()
        value = self.num_convert(value)
        if (self.min is not None and value < self.min) or \
           (self.max is not None and value > self.max):
            raise Http404()
        return value

    def to_url(self, value):
        if (self.fixed_digits and len(str(value)) > self.fixed_digits):
            raise ValueError()
        value = self.num_convert(value)
        if (self.min is not None and value < self.min) or \
           (self.max is not None and value > self.max):
            raise ValueError()
        if self.fixed_digits:
            value = ('%%0%sd' % self.fixed_digits) % value
        return str(value)


class IntegerConverter(NumberConverter):
    """This converter only accepts integer values::

        Rule('/page/<int:page>')

    This converter does not support negative values.

    :param fixed_digits: the number of fixed digits in the URL.  If you set
                         this to ``4`` for example, the application will
                         only match if the url looks like ``/0001/``.  The
                         default is variable length.
    :param min: the minimal value.
    :param max: the maximal value.
    """
    regex = r'\d+'
    num_convert = int


class FloatConverter(NumberConverter):
    """This converter only accepts floating point values::

        Rule('/probability/<float:probability>')

    This converter does not support negative values.

    :param min: the minimal value.
    :param max: the maximal value.
    """
    regex = r'\d+\.\d+'
    num_convert = float

    def __init__(self, min=None, max=None):
        super().__init__(0, min, max)


def parse_converter_args(argstr):
    argstr += ','
    args = []
    kwargs = {}

    for item in _converter_args_re.finditer(argstr):
        value = item.group('stringval')
        if value is None:
            value = item.group('value')
        value = _pythonize(value)
        if not item.group('name'):
            args.append(value)
        else:
            name = item.group('name')
            kwargs[name] = value

    return tuple(args), kwargs


def get_converter(name, parameters):
    c = _CONVERTERS.get(name)
    if not c:
        raise LookupError('Route converter {0} not available'.format(name))
    if parameters:
        args, kwargs = parse_converter_args(parameters)
        return c(*args, **kwargs)
    else:
        return c()


#: the default converter mapping for the map.
_CONVERTERS = {
    'default':          StringConverter,
    'string':           StringConverter,
    'any':              AnyConverter,
    'path':             PathConverter,
    'int':              IntegerConverter,
    'float':            FloatConverter
}
