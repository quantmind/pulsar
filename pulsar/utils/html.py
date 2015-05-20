'''Utilities for HTML and text manipulation.
'''
from .system import json
from .pep import to_string


NOTHING = ('', b'', None)
'''Tuple of elements considered as null.'''
INLINE_TAGS = set(('input', 'link', 'meta', 'hr'))
HTML_CHILDREN_TAG = {'ul': 'li',
                     'ol': 'li'}

# COMMON HTML ENTITIES
# Check http://www.w3schools.com/tags/ref_symbols.asp for more
############################################################################
HTML_NON_BREACKING_SPACE = '&nbsp;'
'''HTML non breaking space symbol.'''
HTML_LESS_THEN = '&lt;'
'''HTML < symbol.'''
HTML_GREATER_THEN = '&gt;'
'''HTML > symbol.'''
HTML_AMPERSAND = '&amp;'
'''HTML & symbol.'''
HTML_ENDASH = '&ndash;'
'''HTML - symbol.'''
HTML_EMDASH = '&mdash;'
'''HTML -- symbol.'''


def child_tag(tag):
    '''The default children ``tag`` for a given ``tag``.'''
    return HTML_CHILDREN_TAG.get(tag)


def mark_safe(v):
    '''Mar a string ``v`` as safe. A safe string won't be escaped by the
:func:`escape` function.'''
    return SafeString(v)


def is_safe(v):
    return getattr(v, '__html__', False)


def escape(html, force=False):
    """Returns the given HTML with ampersands,
    quotes and angle brackets encoded."""
    if hasattr(html, '__html__') and not force:
        return html
    if html in NOTHING:
        return ''
    else:
        return to_string(html).replace('&', '&amp;').replace(
            '<', '&lt;').replace('>', '&gt;').replace("'", '&#39;').replace(
            '"', '&quot;')


def dump_data_value(v):
    if not isinstance(v, str):
        if isinstance(v, bytes):
            v = v.decode('utf-8')
        else:
            v = json.dumps(v)
    return mark_safe(v)


def lazy_string(f):
    def _(*args, **kwargs):
        return _lazy(f, args, kwargs)
    return _


def capfirst(x):
    '''Capitalise the first letter of ``x``.
    '''
    x = to_string(x).strip()
    if x:
        return x[0].upper() + x[1:].lower()
    else:
        return x


def nicename(name):
    '''Make ``name`` a more user friendly string.

    Capitalise the first letter and replace dash and underscores with a space
    '''
    name = to_string(name)
    return capfirst(' '.join(name.replace('-', ' ').replace('_', ' ').split()))


def plural(n, text, plural=None):
    if n != 1:
        text = plural or '%ss' % text
    return '%d %s' % (n, text)


class SafeString(str):
    __html__ = True


class _lazy:

    def __init__(self, f, args, kwargs):
        self._value = None
        self._f = f
        self.args = args
        self.kwargs = kwargs

    def __str__(self):
        if self._value is None:
            self._value = str(self._f(*self.args, **self.kwargs) or '')
        return self._value
    __repr__ = __str__
