'''
The :mod:`pulsar.apps.wsgi.utils` module include several utilities used
by various components in the :ref:`wsgi application <apps-wsgi>`
'''
import time
import re
import textwrap
import logging
from datetime import datetime, timedelta
from email.utils import formatdate
from urllib.parse import parse_qsl

from pulsar import format_traceback
from pulsar.utils.system import json
from pulsar.utils.structures import MultiValueDict
from pulsar.utils.html import escape
from pulsar.utils.pep import to_string
from pulsar.utils.httpurl import (has_empty_content, REDIRECT_CODES,
                                  HTTPError, parse_dict_header,
                                  JSON_CONTENT_TYPES)

from .structures import Accept, RequestCacheControl
from .content import Html, HtmlDocument

__all__ = ['handle_wsgi_error',
           'render_error_debug',
           'wsgi_request',
           'set_wsgi_request_class',
           'dump_environ',
           'HOP_HEADERS']

DEFAULT_RESPONSE_CONTENT_TYPES = ('text/html', 'text/plain'
                                  ) + JSON_CONTENT_TYPES
HOP_HEADERS = frozenset(('connection',
                         'keep-alive',
                         'proxy-authenticate',
                         'proxy-authorization',
                         'te',
                         'trailers',
                         'transfer-encoding',
                         'upgrade')
                        )

LOGGER = logging.getLogger('pulsar.wsgi')
error_css = '''
.pulsar-error {
    width: 500px;
    margin: 50px auto;
}
'''

_RequestClass = None


def wsgi_request(environ, app_handler=None, urlargs=None):
    global _RequestClass
    return _RequestClass(environ, app_handler=app_handler, urlargs=urlargs)


def set_wsgi_request_class(RequestClass):
    global _RequestClass
    _RequestClass = RequestClass


def log_wsgi_info(log, environ, status, exc=None):
    if not environ.get('pulsar.logged'):
        environ['pulsar.logged'] = True
        msg = '' if not exc else ' - %s' % exc
        log('%s %s %s - %s%s',
            environ.get('REQUEST_METHOD'),
            environ.get('RAW_URI'),
            environ.get('SERVER_PROTOCOL'),
            status, msg)


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
            cookies[key]['expires'] = cookie_date(time.time() + max_age)
    if path is not None:
        cookies[key]['path'] = path
    if domain is not None:
        cookies[key]['domain'] = domain
    if secure:
        cookies[key]['secure'] = True
    if httponly:
        cookies[key]['httponly'] = True


_accept_re = re.compile(r'([^\s;,]+)(?:[^,]*?;\s*q=(\d*(?:\.\d+)?))?')


def parse_accept_header(value, cls=None):
    """Parses an HTTP Accept-* header.  This does not implement a complete
    valid algorithm but one that supports at least value and quality
    extraction.

    Returns a new :class:`Accept` object (basically a list of
    ``(value, quality)`` tuples sorted by the quality with some additional
    accessor methods).

    The second parameter can be a subclass of :class:`Accept` that is created
    with the parsed values and returned.

    :param value: the accept header string to be parsed.
    :param cls: the wrapper class for the return value (can be
                         :class:`Accept` or a subclass thereof)
    :return: an instance of `cls`.
    """
    if cls is None:
        cls = Accept
    if not value:
        return cls(None)
    result = []
    for match in _accept_re.finditer(value):
        quality = match.group(2)
        if not quality:
            quality = 1
        else:
            quality = max(min(float(quality), 1), 0)
        result.append((match.group(1), quality))
    return cls(result)


def parse_cache_control_header(value, on_update=None, cls=None):
    """Parse a cache control header.  The RFC differs between response and
    request cache control, this method does not.  It's your responsibility
    to not use the wrong control statements.

    :param value: a cache control header to be parsed.
    :param on_update: an optional callable that is called every time a value
                      on the :class:`~werkzeug.datastructures.CacheControl`
                      object is changed.
    :param cls: the class for the returned object.  By default
                :class:`pulsar.apps.wsgi.structures.RequestCacheControl` is
                used.
    :return: a `cls` object.
    """
    if cls is None:
        cls = RequestCacheControl
    if not value:
        return cls(None, on_update)
    return cls(parse_dict_header(value), on_update)


def _gen_query(query_string, encoding):
    # keep_blank_values=True
    for key, value in parse_qsl((query_string or ''), True):
        yield (to_string(key, encoding, errors='replace'),
               to_string(value, encoding, errors='replace'))


def query_dict(query_string, encoding='utf-8'):
    if query_string:
        return dict(MultiValueDict(_gen_query(query_string, encoding)).items())
    else:
        return {}


error_messages = {
    500: 'An exception has occurred while evaluating your request.',
    404: 'Cannot find what you are looking for.'
}


class dump_environ:
    __slots__ = ('environ',)

    def __init__(self, environ):
        self.environ = environ

    def __str__(self):
        def _():
            for k, v in self.environ.items():
                try:
                    v = str(v)
                except Exception as e:
                    v = str(e)
                yield '%s=%s' % (k, v)
        return '\n%s\n' % '\n'.join(_())


def handle_wsgi_error(environ, exc):
    '''The default error handler while serving a WSGI request.

    :param environ: The WSGI environment.
    :param exc: the exception
    :return: a :class:`.WsgiResponse`
    '''
    if isinstance(exc, tuple):
        exc_info = exc
        exc = exc[1]
    else:
        exc_info = True
    request = wsgi_request(environ)
    request.cache.handle_wsgi_error = True
    response = request.response
    if isinstance(exc, HTTPError):
        response.status_code = exc.code or 500
    else:
        response.status_code = getattr(exc, 'status', 500)
        response.headers.update(getattr(exc, 'headers', None) or ())
    path = '@ %s "%s"' % (request.method, request.path)
    status = response.status_code
    if status >= 500:
        LOGGER.critical('%s - %s.\n%s', exc, path,
                        dump_environ(environ), exc_info=exc_info)
    else:
        log_wsgi_info(LOGGER.warning, environ, response.status, exc)
    if has_empty_content(status, request.method) or status in REDIRECT_CODES:
        response.content_type = None
        response.content = None
    else:
        request.cache.pop('html_document', None)
        renderer = environ.get('error.handler') or render_error
        try:
            content = renderer(request, exc)
        except Exception:
            LOGGER.critical('Error while rendering error', exc_info=True)
            response.content_type = 'text/plain'
            content = 'Critical server error'
        if content is not response:
            response.content = content
    return response


def render_error(request, exc):
    '''Default renderer for errors.'''
    cfg = request.get('pulsar.cfg')
    debug = cfg.debug if cfg else False
    response = request.response
    if not response.content_type:
        content_type = request.get('default.content_type')
        response.content_type = request.content_types.best_match(
            content_type or DEFAULT_RESPONSE_CONTENT_TYPES)
    content_type = None
    if response.content_type:
        content_type = response.content_type.split(';')[0]
    is_html = content_type == 'text/html'

    if debug:
        msg = render_error_debug(request, exc, is_html)
    else:
        msg = escape(error_messages.get(response.status_code) or exc)
        if is_html:
            msg = textwrap.dedent("""
                <h1>{0[reason]}</h1>
                {0[msg]}
                <h3>{0[version]}</h3>
            """).format({"reason": response.status, "msg": msg,
                         "version": request.environ['SERVER_SOFTWARE']})
    #
    if content_type == 'text/html':
        doc = HtmlDocument(title=response.status)
        doc.head.embedded_css.append(error_css)
        doc.body.append(Html('div', msg, cn='pulsar-error'))
        return doc.render(request)
    elif content_type in JSON_CONTENT_TYPES:
        return json.dumps({'status': response.status_code,
                           'message': msg})
    else:
        return '\n'.join(msg) if isinstance(msg, (list, tuple)) else msg


def render_error_debug(request, exception, is_html):
    '''Render the ``exception`` traceback
    '''
    error = Html('div', cn='well well-lg') if is_html else []
    for trace in format_traceback(exception):
        counter = 0
        for line in trace.split('\n'):
            if line.startswith('  '):
                counter += 1
                line = line[2:]
            if line:
                if is_html:
                    line = Html('p', escape(line), cn='text-danger')
                    if counter:
                        line.css({'margin-left': '%spx' % (20*counter)})
                error.append(line)
    if is_html:
        error = Html('div', Html('h1', request.response.status), error)
    return error
