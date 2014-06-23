'''Utilities for HTML and text manipulation.
'''
import re
from unicodedata import normalize

from .system import json
from .pep import (ispy3k, native_str, to_string, iteritems, is_string,
                  string_type)

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


def slugify(value, rtx='_'):
    '''Normalizes string, removes non-alpha characters,
and converts spaces to ``rtx`` character (hyphens or underscore).'''
    value = normalize('NFKD', to_string(value)).encode('ascii', 'ignore')
    value = to_string(re.sub('[^\w\s-]', rtx, value.decode()).strip())
    return re.sub('[-\s]+', rtx, value)


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
    if not is_string(v):
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


class SafeString(string_type):
    __html__ = True


class _lazy:

    def __init__(self, f, args, kwargs):
        self._value = None
        self._f = f
        self.args = args
        self.kwargs = kwargs

    def __str__(self):
        if self._value is None:
            self._value = native_str(self._f(*self.args, **self.kwargs) or '')
        return self._value
    __repr__ = __str__


if ispy3k:

    class UnicodeMixin(object):

        def __unicode__(self):
            return '%s object' % self.__class__.__name__

        def __str__(self):
            return self.__unicode__()

        def __repr__(self):
            return '%s: %s' % (self.__class__.__name__, self)

else:  # pragma nocover

    class UnicodeMixin(object):

        def __unicode__(self):
            return unicode('%s object' % self.__class__.__name__)

        def __str__(self):
            return self.__unicode__().encode()

        def __repr__(self):
            return '%s: %s' % (self.__class__.__name__, self)
