'''Utilities for HTML and text manipulation.

.. autofunction:: escape

.. autofunction:: slugify

.. autofunction:: tag_attributes
'''
import re
import json
from unicodedata import normalize
from collections import namedtuple

from .pep import to_string, iteritems, is_string, string_type

NOTHING = ('', b'', None)
INLINE_TAGS = set(('input', 'meta', 'hr'))
DEFAULT_HTML_ATTRIBUTES = ('id', 'title')
HTML_ATTRIBUTES = {}
HTML_CHILDREN_TAG = {}

e = lambda *t: DEFAULT_HTML_ATTRIBUTES + t
input_attr = lambda *t: e('type', 'autocomplete', 'autofocus', 'disabled', 'form',
                          'formnovalidate', 'list', 'max', 'maxlength', 'min',
                          'multiple', 'name', 'pattern', 'placeholder',
                          'required', 'size', 'step', 'value', *t)

HTML_ATTRIBUTES['a'] = e('href', 'name', 'target')
HTML_ATTRIBUTES['meta'] = e('name', 'charset', 'content')
HTML_ATTRIBUTES['th'] = e('colspan', 'headers', 'rowspan', 'scope')
HTML_ATTRIBUTES['select'] = e('autofocus', 'disabled', 'form', 'multiple',
                              'name', 'required', 'size')
HTML_ATTRIBUTES['form'] = e('accept-charset', 'action', 'autocomplete',
                            'enctype', 'method', 'name', 'novalidate', 'target')
HTML_ATTRIBUTES['input'] = input_attr()
HTML_ATTRIBUTES['input[type="checkbox"]'] = input_attr('checked')
HTML_ATTRIBUTES['input[type="file"]'] = input_attr('accept')
HTML_ATTRIBUTES['input[type="image"]'] = input_attr('alt', 'formaction',
                                    'formenctype', 'formmethod', 'formtarget',
                                    'height', 'src', 'width')
HTML_ATTRIBUTES['input[type="radio"]'] = input_attr('checked')
HTML_ATTRIBUTES['input[type="submit"]'] = input_attr('formaction',
                                    'formenctype', 'formmethod', 'formtarget')

HTML_CHILDREN_TAG['ul'] = 'li'
HTML_CHILDREN_TAG['ol'] = 'li'
HTML_CHILDREN_TAG['select'] = 'option'

csslink = namedtuple('cssentry', 'link condition')

def tag_attributes(tag, type=None):
    '''Return a tuple of valid attributes for the HTML ``tag`` and optional
``type``. If the ``tag`` is not found in the global ``HTML_ATTRIBUTES``
dictionary, the ``DEFAULT_HTML_ATTRIBUTES`` set is returned.'''
    if type:
        ntag = '%s[type="%s"]' % (tag, type)
        if ntag in HTML_ATTRIBUTES:
            return HTML_ATTRIBUTES[ntag]
    return HTML_ATTRIBUTES.get(tag, DEFAULT_HTML_ATTRIBUTES)
    
def child_tag(tag):
    return HTML_CHILDREN_TAG.get(tag)

def slugify(value, rtx='_'):
    '''Normalizes string, removes non-alpha characters,
and converts spaces to ``rtx`` character (hyphens or underscore).'''
    value = normalize('NFKD', to_string(value)).encode('ascii', 'ignore')
    value = to_string(re.sub('[^\w\s-]', rtx, value.decode()).strip())
    return re.sub('[-\s]+', rtx, value)

def mark_safe(v):
    return SafeString(v)

def escape(html, force=False):
    """Returns the given HTML with ampersands,
quotes and angle brackets encoded."""
    if hasattr(html, '__html__') and not force:
        return html
    if html in NOTHING:
        return ''
    else:
        return to_string(html).replace('&', '&amp;').replace('<', '&lt;')\
            .replace('>', '&gt;').replace('"', '&quot;').replace("'", '&#39;')
            
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

class SafeString(string_type):
    __html__ = True