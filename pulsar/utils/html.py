'''HTML utilities'''
import re
import json
from unicodedata import normalize
from collections import namedtuple

from .pep import to_string, iteritems, is_string, string_type

NOTHING = ('', None)
INLINE_TAGS = set(('input', 'meta', 'hr'))
DEFAULT_HTML_ATTRIBUTES = ('id', 'title', 'dir', 'style')
HTML_ATTRIBUTES = {}

e = lambda *t: DEFAULT_HTML_ATTRIBUTES + t

HTML_ATTRIBUTES['a'] = e('href', 'name', 'target')
HTML_ATTRIBUTES['meta'] = e('name', 'charset', 'content')


csslink = namedtuple('cssentry', 'link condition')

def tag_attributes(tag):
    return HTML_ATTRIBUTES.get(tag, DEFAULT_HTML_ATTRIBUTES)
    
def slugify(value, rtx='_'):
    '''Normalizes string, removes non-alpha characters,
and converts spaces to hyphens *rtx* character'''
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