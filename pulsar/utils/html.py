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
# The global attributes below can be used on any HTML element
# class and style are misssing here since they are treated separately
GLOBAL_HTML_ATTRIBUTES = ('accesskey', 'contenteditable', 'contextmenu', 'dir',
                          'draggable', 'dropzone', 'hidden', 'id', 'lang',
                          'spellcheck', 'tabindex', 'title', 'translate')
HTML_ATTRIBUTES = {}
HTML_CHILDREN_TAG = {}

# Extend GLOBAL ATTRIBUTES (which can be used in any HTML element).
e = lambda *t: GLOBAL_HTML_ATTRIBUTES + t

# Input atg attributes
# http://www.w3schools.com/tags/tag_input.asp
input_attr = lambda *t: e('type', 'autocomplete', 'autofocus', 'disabled',
                          'form', 'formnovalidate', 'list', 'max', 'maxlength',
                          'min', 'multiple', 'name', 'pattern', 'placeholder',
                          'readonly', 'required', 'size', 'step', 'value', *t)

# HTML TAG ATTRIBUTES
############################################################################
HTML_ATTRIBUTES['a'] = e('href', 'name', 'target')
HTML_ATTRIBUTES['form'] = e('accept-charset', 'action', 'autocomplete',
                            'enctype', 'method', 'name', 'novalidate',
                            'target')
HTML_ATTRIBUTES['img'] = e('align', 'alt', 'broder', 'crossorigin',
                           'height', 'hspace', 'ismap', 'longdesc',
                           'src', 'usemap', 'vspace', 'width')
HTML_ATTRIBUTES['input'] = input_attr()
HTML_ATTRIBUTES['input[type="checkbox"]'] = input_attr('checked')
HTML_ATTRIBUTES['input[type="file"]'] = input_attr('accept')
HTML_ATTRIBUTES['input[type="image"]'] = input_attr(
    'alt', 'formaction', 'formenctype', 'formmethod', 'formtarget',
    'height', 'src', 'width')
HTML_ATTRIBUTES['input[type="radio"]'] = input_attr('checked')
HTML_ATTRIBUTES['input[type="submit"]'] = input_attr(
    'formaction', 'formenctype', 'formmethod', 'formtarget')
HTML_ATTRIBUTES['link'] = e('href', 'hreflang', 'media', 'rel',
                            'sizes', 'type')
HTML_ATTRIBUTES['meta'] = e('charset', 'content', 'http-equiv', 'name',
                            'scheme')
HTML_ATTRIBUTES['option'] = e('disabled', 'label', 'selected', 'value')
HTML_ATTRIBUTES['script'] = e('async', 'charset', 'defer', 'src', 'type')
HTML_ATTRIBUTES['style'] = e('media', 'scoped', 'type')
HTML_ATTRIBUTES['select'] = e('autofocus', 'disabled', 'form', 'multiple',
                              'name', 'required', 'size')
HTML_ATTRIBUTES['textarea'] = e('autofocus', 'cols', 'disabled', 'maxlength',
                                'name', 'placeholder', 'readonly', 'required',
                                'rows', 'wrap')
HTML_ATTRIBUTES['th'] = e('colspan', 'headers', 'rowspan', 'scope')

# DEFAULT HTML TAG CHILDREN
############################################################################
HTML_CHILDREN_TAG['ul'] = 'li'
HTML_CHILDREN_TAG['ol'] = 'li'
HTML_CHILDREN_TAG['select'] = 'option'

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


def tag_attributes(tag, type=None):
    '''Return a tuple of valid attributes for the HTML ``tag`` and optional
``type``. If the ``tag`` is not found in the global ``HTML_ATTRIBUTES``
dictionary, the ``GLOBAL_HTML_ATTRIBUTES`` set is returned.'''
    if type:
        ntag = '%s[type="%s"]' % (tag, type)
        if ntag in HTML_ATTRIBUTES:
            return HTML_ATTRIBUTES[ntag]
    return HTML_ATTRIBUTES.get(tag, GLOBAL_HTML_ATTRIBUTES)


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


def attr_iter(attrs):
    for k, v in iteritems(attrs):
        if v is not None:
            yield " %s='%s'" % (k, escape(v, force=True))


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
