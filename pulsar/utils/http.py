import time
from datetime import datetime, timedelta
from email.utils import formatdate

from .py2py3 import ispy3k, iteritems
from .collections import DictPropertyMixin


__all__ = ['urlparse',
           'unquote',
           'urlsplit',
           'Headers',
           'bytes_to_str',
           'to_string',
           'parse_authorization_header',
           'parse_dict_header',
           'set_cookie']


if ispy3k:
    from urllib.parse import urlparse, unquote, urlsplit
    from http.cookies import SimpleCookie
    
    def bytes_to_str(b):
        return str(b, 'latin-1')
    
    def to_string(data):
        if isinstance(data, bytes):
            return str(data, 'latin-1')
        elif not isinstance(data,str):
            return str(data)
        return data
    
else:
    from urlparse import urlparse, unquote, urlsplit
    from Cookie import SimpleCookie
    
    def bytes_to_str(b):
        return b
    
    def to_string(data):
        return str(data)

    
class Headers(object):
    '''Utility for managing HTTP headers. It has a dictionaru like interface
with few extra functions to facilitate the insertion of multiple values.'''
    def __init__(self, defaults=None):
        self._dict = {}
        self._keys = []
        if defaults is not None:
            self.extend(defaults)
    
    def __repr__(self):
        return self._dict.__repr__()
    __str__ = __repr__
    
    def extend(self, iterable):
        """Extend the headers with a dict or an iterable yielding keys and
        values.
        """
        if isinstance(iterable, dict):
            iterable = iteritems(iterable)
        for key, value in iterable:
            self.__setitem__(key, value)
    
    def __contains__(self, key):
        return key.lower() in self._dict
        
    def __iter__(self):
        d = self._dict
        for k in self._keys:
            yield k,d[k.lower()]

    def __len__(self):
        return len(self._keys)
    
    def __getitem__(self, key):
        return self._dict[key.lower()]
    
    def __delitem__(self, key):
        key = key.lower()
        del self._dict[key]
        for k in self._keys:
            if k.lower() == key:
                self._keys.remove(k)
                break

    def __setitem__(self, key, value):
        lkey = key.lower()
        if value:
            if isinstance(value, (tuple, list)):
                value = ','.join(value)
            if lkey not in self._dict:
                self._keys.append(key)
            self._dict[lkey] = value
    
    def as_list(self, key):
        '''Return the value at *key* as a list of values.'''
        value = self._dict.get(key.lower(),None)
        return value.split(',') if value else []
        
    def add(self, key, value):
        '''Add *value* to *key* header. If the header is already available,
append the value to the list.'''
        lkey = key.lower()
        values = self.as_list(key)
        if value not in values:
            values.append(value)
            self[key] = values
        
    def get(self, key, default=None):
        try:
            return self.__getitem__(key)
        except KeyError:
            return default
        
    def flat(self, version, status):
        vs = version + (status,)
        h = 'HTTP/{0}.{1} {2}'.format(*vs) 
        f = ''.join(("{0}: {1}\r\n".format(n, v) for n, v in self))
        return '{0}\r\n{1}\r\n'.format(h,f)
    
    def pop(self, key, *arg):
        if len(arg) > 1:
            raise TypeError('pop expected at most 2 arguments, got {0}'\
                            .format(len(arg)))
        try:
            del self[key]
        except KeyError:
            if arg:
                return arg[0]
            else:
                raise
    
    @property
    def vary_headers(self):
        return self.get('vary',[])
        
    def has_vary(self, header_query):
        """Checks to see if the has a given header name in its Vary header.
        """
        return header_query.lower() in set(self.vary_headers)
        
        
def parse_dict_header(value):
    """Parse lists of key, value pairs as described by RFC 2068 Section 2 and
    convert them into a python dict:

    >>> d = parse_dict_header('foo="is a fish", bar="as well"')
    >>> type(d) is dict
    True
    >>> sorted(d.items())
    [('bar', 'as well'), ('foo', 'is a fish')]

    If there is no value for a key it will be `None`:

    >>> parse_dict_header('key_without_value')
    {'key_without_value': None}

    To create a header from the :class:`dict` again, use the
    :func:`dump_header` function.

    :param value: a string with a dict header.
    :return: :class:`dict`
    """
    result = {}
    for item in _parse_list_header(value):
        if '=' not in item:
            result[item] = None
            continue
        name, value = item.split('=', 1)
        if value[:1] == value[-1:] == '"':
            value = unquote_header_value(value[1:-1])
        result[name] = value
    return result


class Authorization(DictPropertyMixin):
    """Represents an `Authorization` header sent by the client."""

    def __init__(self, auth_type, data=None):
        super(Authorization,self).__init__(data = data)
        self.type = auth_type
        
        
def parse_authorization_header(value):
    """Parse an HTTP basic/digest authorization header transmitted by the web
browser.  The return value is either `None` if the header was invalid or
not given, otherwise an :class:`Authorization` object.

:param value: the authorization header to parse.
:return: a :class:`Authorization` object or `None`."""
    if not value:
        return
    try:
        auth_type, auth_info = value.split(None, 1)
        auth_type = auth_type.lower()
    except ValueError:
        return
    if auth_type == 'basic':
        try:
            username, password = auth_info.decode('base64').split(':', 1)
        except Exception as e:
            return
        return Authorization('basic', {'username': username,
                                       'password': password})
    elif auth_type == 'digest':
        auth_map = parse_dict_header(auth_info)
        for key in 'username', 'realm', 'nonce', 'uri', 'nc', 'cnonce', \
                   'response':
            if not key in auth_map:
                return
        return Authorization('digest', auth_map)
    

def cookie_date(epoch_seconds=None):
    """Formats the time to ensure compatibility with Netscape's cookie
    standard.

    Accepts a floating point number expressed in seconds since the epoch in, a
    datetime object or a timetuple.  All times in UTC.  The :func:`parse_date`
    function can be used to parse such a date.

    Outputs a string in the format ``Wdy, DD-Mon-YYYY HH:MM:SS GMT``.

    :param expires: If provided that date is used, otherwise the current.
    """
    rfcdate = formatdate(epoch_seconds)
    return '%s-%s-%s GMT' % (rfcdate[:7], rfcdate[8:11], rfcdate[12:25])


def set_cookie(cookies, key, value='', max_age=None, expires=None, path='/',
               domain=None, secure=False, httponly=False):
    cookies[key] = value
    if expires is not None:
        if isinstance(expires, datetime):
            delta = expires - expires.utcnow()
            # Add one second so the date matches exactly (a fraction of
            # time gets lost between converting to a timedelta and
            # then the date string).
            delta = delta + timedelta(seconds=1)
            # Just set max_age - the max_age logic will set expires.
            expires = None
            max_age = max(0, delta.days * 86400 + delta.seconds)
        else:
            cookies[key]['expires'] = expires
    if max_age is not None:
        cookies[key]['max-age'] = max_age
        # IE requires expires, so set it if hasn't been already.
        if not expires:
            cookies[key]['expires'] = cookie_date(time.time() + max_age)
    if path is not None:
        cookies[key]['path'] = path
    if domain is not None:
        cookies[key]['domain'] = domain
    if secure:
        cookies[key]['secure'] = True
    if httponly:
        cookies[key]['httponly'] = True
