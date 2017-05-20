import time
from http.client import responses
from http.cookies import SimpleCookie
from functools import reduce
from datetime import datetime, timedelta
from wsgiref.handlers import format_date_time as http_date

from multidict import CIMultiDict

from .wsgi import has_empty_content, HEAD


PULSAR_CACHE = 'pulsar.cache'
nocache = object()


def wsgi_cached(method):
    name = method.__name__

    def _(self):
        cache = self.environ[PULSAR_CACHE]
        value = getattr(cache, name, nocache)
        if value is nocache:
            setattr(cache, name, method(self))
            value = getattr(cache, name)
        return value

    return property(_, doc=method.__doc__)


def count_len(a, b):
    return a + len(b)


class WsgiResponse:
    """A WSGI response.

    Instances are callable using the standard WSGI call and, importantly,
    iterable::

        response = WsgiResponse(200)

    A :class:`WsgiResponse` is an iterable over bytes to send back to the
    requesting client.

    .. attribute:: status_code

        Integer indicating the HTTP status, (i.e. 200)

    .. attribute:: response

        String indicating the HTTP status (i.e. 'OK')

    .. attribute:: status

        String indicating the HTTP status code and response (i.e. '200 OK')

    .. attribute:: content_type

        The content type of this response. Can be ``None``.

    .. attribute:: headers

        The :class:`.Headers` container for this response.

    .. attribute:: cookies

        A python :class:`SimpleCookie` container of cookies included in the
        request as well as cookies set during the response.
    """
    _iterated = False
    __wsgi_started__ = False

    def __init__(self, status_code=200, content=None, response_headers=None,
                 content_type=None, encoding=None, can_store_cookies=True):
        self.status_code = status_code
        self.encoding = encoding
        self.headers = CIMultiDict(response_headers or ())
        self.content = content
        self._cookies = None
        self._can_store_cookies = can_store_cookies
        if content_type is not None:
            self.content_type = content_type

    @property
    def started(self):
        return self.__wsgi_started__

    @property
    def iterated(self):
        return self._iterated

    @property
    def cookies(self):
        if self._cookies is None:
            self._cookies = SimpleCookie()
        return self._cookies

    @property
    def content(self):
        return self._content

    @content.setter
    def content(self, content):
        self.set_content(content)

    def set_content(self, content):
        if self._iterated:
            raise RuntimeError('Cannot set content. Already iterated')
        if content is None:
            self._content = ()
        elif isinstance(content, str):
            if not self.encoding:  # use utf-8 if not set
                self.encoding = 'utf-8'
            self._content = content.encode(self.encoding),
        elif isinstance(content, bytes):
            self._content = content,
        else:
            self._content = content

    def _get_content_type(self):
        return self.headers.get('content-type')

    def _set_content_type(self, typ):
        if typ:
            self.headers['content-type'] = typ
        else:
            self.headers.pop('content-type', None)
    content_type = property(_get_content_type, _set_content_type)

    @property
    def response(self):
        return responses.get(self.status_code)

    @property
    def status(self):
        return '%s %s' % (self.status_code, responses.get(self.status_code))

    def __str__(self):
        return self.status

    def __repr__(self):
        return '%s(%s)' % (self.__class__.__name__, self)

    def is_streamed(self):
        """Check if the response is streamed.

        A streamed response is an iterable with no length information.
        In this case streamed means that there is no information about
        the number of iterations.

        This is usually `True` if a generator is passed to the response object.
        """
        try:
            len(self._content)
        except TypeError:
            return True
        return False

    def length(self):
        try:
            len(self._content)
        except TypeError:
            return
        return reduce(count_len, self._content, 0)

    def can_set_cookies(self):
        return self.status_code < 400 and self._can_store_cookies

    def start(self, environ, start_response, exc_info=None):
        self.__wsgi_started__ = True
        headers = self._get_headers(environ)
        return start_response(self.status, headers, exc_info)

    def __iter__(self):
        if self._iterated:
            raise RuntimeError('WsgiResponse can be iterated once only')
        self.__wsgi_started__ = True
        self._iterated = True
        iterable = iter(self._content)
        self._content = None
        return iterable

    def close(self):
        """Close this response, required by WSGI
        """
        if hasattr(self._content, 'close'):
            self._content.close()

    def set_cookie(self, key, **kwargs):
        """
        Sets a cookie.

        ``expires`` can be a string in the correct format or a
        ``datetime.datetime`` object in UTC. If ``expires`` is a datetime
        object then ``max_age`` will be calculated.
        """
        set_cookie(self.cookies, key, **kwargs)

    def delete_cookie(self, key, path='/', domain=None):
        set_cookie(self.cookies, key, max_age=0, path=path, domain=domain,
                   expires='Thu, 01-Jan-1970 00:00:00 GMT')

    def has_header(self, header):
        return header in self.headers
    __contains__ = has_header

    def __setitem__(self, header, value):
        self.headers[header] = value

    def __getitem__(self, header):
        return self.headers[header]

    def _get_headers(self, environ):
        """The list of headers for this response
        """
        headers = self.headers
        method = environ['REQUEST_METHOD']

        if has_empty_content(self.status_code, method) and method != HEAD:
            headers.pop('content-type', None)
            headers.pop('content-length', None)
            self._content = ()
        else:
            if not self.is_streamed():
                cl = reduce(count_len, self._content, 0)
                headers['content-length'] = str(cl)
            ct = headers.get('content-type')
            # content type encoding available
            if self.encoding:
                ct = ct or 'text/plain'
                if ';' not in ct:
                    ct = '%s; charset=%s' % (ct, self.encoding)
            if ct:
                headers['content-type'] = ct
            if method == HEAD:
                self._content = ()
        # Cookies
        if (self.status_code < 400 and self._can_store_cookies and
                self._cookies):
            for c in self.cookies.values():
                headers.add('set-cookie', c.OutputString())
        return headers.items()


def set_cookie(cookies, key, value='', max_age=None, expires=None, path='/',
               domain=None, secure=False, httponly=False):
    '''Set a cookie key into the cookies dictionary *cookies*.'''
    cookies[key] = value
    if expires is not None:
        if isinstance(expires, datetime):
            now = (expires.now(expires.tzinfo) if expires.tzinfo else
                   expires.utcnow())
            delta = expires - now
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
            cookies[key]['expires'] = http_date(time.time() + max_age)
    if path is not None:
        cookies[key]['path'] = path
    if domain is not None:
        cookies[key]['domain'] = domain
    if secure:
        cookies[key]['secure'] = True
    if httponly:
        cookies[key]['httponly'] = True
