import os
import re
import string
import mimetypes
from hashlib import sha1, md5
from uuid import uuid4
from io import BytesIO
from urllib import request as urllibr
from http import client as httpclient
from urllib.parse import quote, splitport
from http.cookiejar import CookieJar, Cookie
from http.cookies import SimpleCookie

from .structures import mapping_iterator
from .string import to_bytes, to_string


getproxies_environment = urllibr.getproxies_environment
ascii_letters = string.ascii_letters
HTTPError = urllibr.HTTPError
URLError = urllibr.URLError
parse_http_list = urllibr.parse_http_list

tls_schemes = ('https', 'wss')

# ###################################################    URI & IRI SUFF
#
# The reserved URI characters (RFC 3986 - section 2.2)
# Default is charset is "iso-8859-1" (latin-1) from section 3.7.1
# http://www.ietf.org/rfc/rfc2616.txt
CHARSET = 'ISO-8859-1'
URI_GEN_DELIMS = frozenset(':/?#[]@')
URI_SUB_DELIMS = frozenset("!$&'()*+,;=")
URI_RESERVED_SET = URI_GEN_DELIMS.union(URI_SUB_DELIMS)
URI_RESERVED_CHARS = ''.join(URI_RESERVED_SET)
# The unreserved URI characters (RFC 3986 - section 2.3)
URI_UNRESERVED_SET = frozenset(ascii_letters + string.digits + '-._~')
URI_SAFE_CHARS = URI_RESERVED_CHARS + '%~'
HEADER_TOKEN_CHARS = frozenset("!#$%&'*+-.0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                               '^_`abcdefghijklmnopqrstuvwxyz|~')
MAX_CHUNK_SIZE = 65536

# ###################################################    CONTENT TYPES
JSON_CONTENT_TYPES = ('application/json',
                      'application/javascript',
                      'text/json',
                      'text/x-json')
# ###################################################    REQUEST METHODS
GET = 'GET'
DELETE = 'DELETE'
HEAD = 'HEAD'
OPTIONS = 'OPTIONS'
PATCH = 'PATCH'
POST = 'POST'
PUT = 'PUT'
TRACE = 'TRACE'


ENCODE_URL_METHODS = frozenset((DELETE, GET, HEAD, OPTIONS))
ENCODE_BODY_METHODS = frozenset((PATCH, POST, PUT, TRACE))
REDIRECT_CODES = (301, 302, 303, 305, 307)
NO_CONTENT_CODES = frozenset((204, 304))

CRLF = '\r\n'
LWS = '\r\n '
SEP = ': '


def escape(s):
    return quote(s, safe='~')


def urlquote(iri):
    return quote(iri, safe=URI_RESERVED_CHARS)


def _gen_unquote(uri):
    unreserved_set = URI_UNRESERVED_SET
    for n, part in enumerate(to_string(uri, 'latin1').split('%')):
        if not n:
            yield part
        else:
            h = part[0:2]
            if len(h) == 2:
                c = chr(int(h, 16))
                if c in unreserved_set:
                    yield c + part[2:]
                else:
                    yield '%' + part
            else:
                yield '%' + part


def unquote_unreserved(uri):
    """Un-escape any percent-escape sequences in a URI that are unreserved
characters. This leaves all reserved, illegal and non-ASCII bytes encoded."""
    return ''.join(_gen_unquote(uri))


def requote_uri(uri):
    """Re-quote the given URI.

    This function passes the given URI through an unquote/quote cycle to
    ensure that it is fully and consistently quoted.
    """
    # Unquote only the unreserved characters
    # Then quote only illegal characters (do not quote reserved, unreserved,
    # or '%')
    return quote(unquote_unreserved(uri), safe=URI_SAFE_CHARS)


def iri_to_uri(iri, kwargs=None):
    '''Convert an Internationalised Resource Identifier (IRI) portion
    to a URI portion that is suitable for inclusion in a URL.
    This is the algorithm from section 3.1 of RFC 3987.
    Returns an ASCII native string containing the encoded result.
    '''
    if iri is None:
        return iri
    if kwargs:
        iri = '%s?%s' % (to_string(iri, 'latin1'),
                         '&'.join(('%s=%s' % kv for kv in kwargs.items())))
    return urlquote(unquote_unreserved(iri))


def host_and_port(host):
    host, port = splitport(host)
    return host, int(port) if port else None


def default_port(scheme):
    if scheme in ("http", "ws"):
        return '80'
    elif scheme in ("https", "wss"):
        return '443'


def host_and_port_default(scheme, host):
    host, port = splitport(host)
    if not port:
        port = default_port(scheme)
    return host, port


def host_no_default_port(scheme, netloc):
    host, port = splitport(netloc)
    if port and port == default_port(scheme):
        return host
    else:
        return netloc


def get_hostport(scheme, full_host):
    host, port = host_and_port(full_host)
    if port is None:
        i = host.rfind(':')
        j = host.rfind(']')         # ipv6 addresses have [...]
        if i > j:
            try:
                port = int(host[i+1:])
            except ValueError:
                if host[i+1:] == "":  # http://foo.com:/ == http://foo.com/
                    port = default_port(scheme)
                else:
                    raise httpclient.InvalidURL("nonnumeric port: '%s'"
                                                % host[i+1:])
            host = host[:i]
        else:
            port = default_port(scheme)
        if host and host[0] == '[' and host[-1] == ']':
            host = host[1:-1]
    return host, int(port)


def remove_double_slash(route):
    if '//' in route:
        route = re.sub('/+', '/', route)
    return route


def is_succesful(status):
    '''2xx status is succesful'''
    return status >= 200 and status < 300


def split_comma(value):
    return [v for v in (v.strip() for v in value.split(',')) if v]


def parse_cookies(value):
    return [c.OutputString() for c in SimpleCookie(value).values()]


header_parsers = {'Connection': split_comma,
                  'Cookie': parse_cookies}


def quote_header_value(value, extra_chars='', allow_token=True):
    """Quote a header value if necessary.

    :param value: the value to quote.
    :param extra_chars: a list of extra characters to skip quoting.
    :param allow_token: if this is enabled token values are returned
        unchanged.
    """
    value = to_string(value)
    if allow_token:
        token_chars = HEADER_TOKEN_CHARS | set(extra_chars)
        if set(value).issubset(token_chars):
            return value
    return '"%s"' % value.replace('\\', '\\\\').replace('"', '\\"')


def unquote_header_value(value, is_filename=False):
    """Unquotes a header value.

    Reversal of :func:`quote_header_value`. This does not use the real
    un-quoting but what browsers are actually using for quoting.

    :param value: the header value to unquote.
    """
    if value and value[0] == value[-1] == '"':
        # this is not the real unquoting, but fixing this so that the
        # RFC is met will result in bugs with internet explorer and
        # probably some other browsers as well.  IE for example is
        # uploading files with "C:\foo\bar.txt" as filename
        value = value[1:-1]
        # if this is a filename and the starting characters look like
        # a UNC path, then just return the value without quotes.  Using the
        # replace sequence below on a UNC path has the effect of turning
        # the leading double slash into a single slash and then
        # _fix_ie_filename() doesn't work correctly.  See #458.
        if not is_filename or value[:2] != '\\\\':
            return value.replace('\\\\', '\\').replace('\\"', '"')
    return value


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
    for item in parse_http_list(value):
        if '=' not in item:
            result[item] = None
            continue
        name, value = item.split('=', 1)
        if value[:1] == value[-1:] == '"':
            value = unquote_header_value(value[1:-1])
        result[name] = value
    return result


_special = re.escape('()<>@,;:\\"/[]?={} \t')
_re_special = re.compile('[%s]' % _special)
_qstr = '"(?:\\\\.|[^"])*"'  # Quoted string
_value = '(?:[^%s]+|%s)' % (_special, _qstr)  # Save or quoted string
_option = '(?:;|^)\s*([^%s]+)\s*=\s*(%s)' % (_special, _value)
_re_option = re.compile(_option)  # key=value part of an Content-Type header


def header_unquote(val, filename=False):
    if val[0] == val[-1] == '"':
        val = val[1:-1]
        if val[1:3] == ':\\' or val[:2] == '\\\\':
            val = val.split('\\')[-1]  # fix ie6 bug: full path --> filename
        return val.replace('\\\\', '\\').replace('\\"', '"')
    return val


def parse_options_header(header, options=None):
    if ';' not in header:
        return header.lower().strip(), {}
    ctype, tail = header.split(';', 1)
    options = options or {}
    for match in _re_option.finditer(tail):
        key = match.group(1).lower()
        value = header_unquote(match.group(2), key == 'filename')
        options[key] = value
    return ctype, options


# ############################################    UTILITIES, ENCODERS, PARSERS
absolute_http_url_re = re.compile(r"^https?://", re.I)


def is_absolute_uri(location):
    '''Check if a ``location`` is absolute, i.e. it includes the scheme
    '''
    return location and absolute_http_url_re.match(location)


def get_environ_proxies():
    """Return a dict of environment proxies. From requests_."""

    proxy_keys = [
        'all',
        'http',
        'https',
        'ftp',
        'socks',
        'ws',
        'wss',
        'no'
    ]

    def get_proxy(k):
        return os.environ.get(k) or os.environ.get(k.upper())

    proxies = [(key, get_proxy(key + '_proxy')) for key in proxy_keys]
    return dict([(key, val) for (key, val) in proxies if val])


def appendslash(url):
    '''Append a slash to *url* if it does not have one.'''
    if not url.endswith('/'):
        url = '%s/' % url
    return url


def choose_boundary():
    """Our embarassingly-simple replacement for mimetools.choose_boundary."""
    return uuid4().hex


def get_content_type(filename):
    return mimetypes.guess_type(filename)[0] or 'application/octet-stream'


def encode_multipart_formdata(fields, boundary=None, charset=None):
    """Encode a dictionary of ``fields`` using the multipart/form-data format.

    :param fields:
        Dictionary of fields or list of (key, value) field tuples.  The key is
        treated as the field name, and the value as the body of the form-data
        bytes. If the value is a tuple of two elements, then the first element
        is treated as the filename of the form-data section.

        Field names and filenames must be unicode.

    :param boundary:
        If not specified, then a random boundary will be generated using
        :func:`mimetools.choose_boundary`.
    """
    charset = charset or 'utf-8'
    body = BytesIO()
    if boundary is None:
        boundary = choose_boundary()
    for fieldname, value in mapping_iterator(fields):
        body.write(('--%s\r\n' % boundary).encode(charset))
        if isinstance(value, tuple):
            filename, data = value
            body.write(('Content-Disposition: form-data; name="%s"; '
                        'filename="%s"\r\n' % (fieldname, filename))
                       .encode(charset))
            body.write(('Content-Type: %s\r\n\r\n' %
                       (get_content_type(filename))).encode(charset))
        else:
            data = value
            body.write(('Content-Disposition: form-data; name="%s"\r\n'
                        % (fieldname)).encode(charset))
            body.write(b'Content-Type: text/plain\r\n\r\n')
        body.write(to_bytes(data))
        body.write(b'\r\n')
    body.write(('--%s--\r\n' % (boundary)).encode(charset))
    content_type = 'multipart/form-data; boundary=%s' % boundary
    return body.getvalue(), content_type


def hexmd5(x):
    return md5(to_bytes(x)).hexdigest()


def hexsha1(x):
    return sha1(to_bytes(x)).hexdigest()


# ################################################################# COOKIES
def create_cookie(name, value, **kwargs):
    """Make a cookie from underspecified parameters.

    By default, the pair of `name` and `value` will be set for the domain ''
    and sent on every request (this is sometimes called a "supercookie").
    """
    result = dict(
        version=0,
        name=name,
        value=value,
        port=None,
        domain='',
        path='/',
        secure=False,
        expires=None,
        discard=True,
        comment=None,
        comment_url=None,
        rest={'HttpOnly': None},
        rfc2109=False,)
    badargs = set(kwargs) - set(result)
    if badargs:
        err = 'create_cookie() got unexpected keyword arguments: %s'
        raise TypeError(err % list(badargs))
    result.update(kwargs)
    result['port_specified'] = bool(result['port'])
    result['domain_specified'] = bool(result['domain'])
    result['domain_initial_dot'] = result['domain'].startswith('.')
    result['path_specified'] = bool(result['path'])
    return Cookie(**result)


def cookiejar_from_dict(*cookie_dicts):
    """Returns a CookieJar from a key/value dictionary.

    :param cookie_dict: Dict of key/values to insert into CookieJar.
    """
    cookie_dicts = tuple((d for d in cookie_dicts if d))
    if len(cookie_dicts) == 1 and isinstance(cookie_dicts[0], CookieJar):
        return cookie_dicts[0]
    cookiejar = CookieJar()
    for cookie_dict in cookie_dicts:
        if isinstance(cookie_dict, CookieJar):
            for cookie in cookie_dict:
                cookiejar.set_cookie(cookie)
        else:
            for name in cookie_dict:
                cookiejar.set_cookie(create_cookie(name, cookie_dict[name]))
    return cookiejar


# ################################################################# VARY HEADER
cc_delim_re = re.compile(r'\s*,\s*')


def patch_vary_headers(response, newheaders):
    """Adds (or updates) the "Vary" header in the given HttpResponse object.

    newheaders is a list of header names that should be in "Vary". Existing
    headers in "Vary" aren't removed.

    For information on the Vary header, see:

        http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.44
    """
    # Note that we need to keep the original order intact, because cache
    # implementations may rely on the order of the Vary contents in, say,
    # computing an MD5 hash.
    if 'Vary' in response:
        vary_headers = cc_delim_re.split(response['Vary'])
    else:
        vary_headers = []
    # Use .lower() here so we treat headers as case-insensitive.
    existing_headers = set([header.lower() for header in vary_headers])
    additional_headers = [newheader for newheader in newheaders
                          if newheader.lower() not in existing_headers]
    response['Vary'] = ', '.join(vary_headers + additional_headers)


def has_vary_header(response, header_query):
    """
    Checks to see if the response has a given header name in its Vary header.
    """
    if not response.has_header('Vary'):
        return False
    vary_headers = cc_delim_re.split(response['Vary'])
    existing_headers = set([header.lower() for header in vary_headers])
    return header_query.lower() in existing_headers


class CacheControl:
    '''
    http://www.mnot.net/cache_docs/

.. attribute:: maxage

    Specifies the maximum amount of time that a representation will be
    considered fresh.
    '''
    def __init__(self, maxage=None, private=False,
                 must_revalidate=False, proxy_revalidate=False,
                 nostore=False):
        self.maxage = maxage
        self.private = private
        self.must_revalidate = must_revalidate
        self.proxy_revalidate = proxy_revalidate
        self.nostore = nostore

    def __call__(self, headers, etag=None):
        if self.nostore:
            headers['cache-control'] = ('no-store, no-cache, must-revalidate,'
                                        ' max-age=0')
        elif self.maxage:
            headers['cache-control'] = 'max-age=%s' % self.maxage
            if etag:
                headers['etag'] = '"%s"' % etag
            if self.private:
                headers.add('cache-control', 'private')
            else:
                headers.add('cache-control', 'public')
            if self.must_revalidate:
                headers.add('cache-control', 'must-revalidate')
            elif self.proxy_revalidate:
                headers.add('cache-control', 'proxy-revalidate')
        else:
            headers['cache-control'] = 'no-cache'
        return headers


def chunk_encoding(chunk):
    '''Write a chunk::

        chunk-size(hex) CRLF
        chunk-data CRLF

    If the size is 0, this is the last chunk, and an extra CRLF is appended.
    '''
    head = ("%X\r\n" % len(chunk)).encode('utf-8')
    return head + chunk + b'\r\n'


def http_chunks(data, finish=False):
    while len(data) >= MAX_CHUNK_SIZE:
        chunk, data = data[:MAX_CHUNK_SIZE], data[MAX_CHUNK_SIZE:]
        yield chunk_encoding(chunk)
    if data:
        yield chunk_encoding(data)
    if finish:
        yield chunk_encoding(data)


def parse_header_links(value):
    """Return a dict of parsed link headers proxies

    i.e. Link: <http:/.../front.jpeg>; rel=front; type="image/jpeg",
    <http://.../back.jpeg>; rel=back;type="image/jpeg"

    Original code from https://github.com/kennethreitz/requests

    Copyright 2016 Kenneth Reitz
    """
    links = []
    replace_chars = " '\""

    for val in re.split(", *<", value):
        try:
            url, params = val.split(";", 1)
        except ValueError:
            url, params = val, ''
        link = {}
        link["url"] = url.strip("<> '\"")
        for param in params.split(";"):
            try:
                key, value = param.split("=")
            except ValueError:
                break

            link[key.strip(replace_chars)] = value.strip(replace_chars)
        links.append(link)
    return links
