import time
import re
import textwrap
import logging
from datetime import datetime, timedelta
from email.utils import formatdate


from pulsar.utils.system import json
from pulsar.utils.structures import MultiValueDict
from pulsar.utils.html import escape
from pulsar.utils.pep import to_string
from pulsar.utils.httpurl import (has_empty_content, REDIRECT_CODES, iteritems,
                                  parse_qsl, HTTPError, parse_dict_header,
                                  JSON_CONTENT_TYPES)

from .structures import Accept, RequestCacheControl
from .content import Html

__all__ = ['handle_wsgi_error',
           'wsgi_error_msg',
           'render_error_debug',
           'wsgi_request',
           'set_wsgi_request_class',
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
                         'upgrade',
                         'server',
                         'date')
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


def wsgi_error_msg(response, msg):
    if response.content_type == 'application/json':
        return json.dumps({'status': response.status_code,
                           'message': msg})
    else:
        return msg


class dump_environ(object):
    __slots__ = ('environ',)

    def __init__(self, environ):
        self.environ = environ

    def __str__(self):
        env = iteritems(self.environ)
        return '\n%s\n' % '\n'.join(('%s = %s' % (k, v) for k, v in env))


def handle_wsgi_error(environ, failure):
    '''The default handler for errors while serving an Http requests.

:parameter environ: The WSGI environment.
:parameter failure: a :class:`Failure`.
:return: a :class:`WsgiResponse`
'''
    request = wsgi_request(environ)
    response = request.response
    error = failure.error
    if isinstance(error, HTTPError):
        response.status_code = error.code or 500
    else:
        response.status_code = getattr(error, 'status', 500)
        response.headers.update(getattr(error, 'headers', None) or ())
    path = '@ path "%s"' % environ.get('PATH_INFO', '/')
    status = response.status_code
    if status == 500:
        failure.log(msg='Unhandled exception during WSGI response %s.%s' %
                    (path, dump_environ(environ)), level='critical')
    else:
        failure.log(msg='WSGI %s status code %s' % (status, path),
                    level='warning')
    if has_empty_content(status, request.method) or status in REDIRECT_CODES:
        content = None
    else:
        request.cache.pop('html_document', None)
        renderer = environ.get('error.handler') or render_error
        try:
            content = renderer(request, failure)
        except Exception:
            LOGGER.critical('Error while rendering error')
            content = None
    response.content = content
    return response


def render_error(request, failure):
    '''Default renderer for errors.'''
    cfg = request.get('pulsar.cfg')
    debug = cfg.debug if cfg else False
    response = request.response
    if not response.content_type:
        response.content_type = request.content_types.best_match(
            DEFAULT_RESPONSE_CONTENT_TYPES)
    if response.content_type == 'text/html':
        request.html_document.head.title = response.status
    if debug:
        msg = render_error_debug(request, failure)
    else:
        msg = error_messages.get(response.status_code) or ''
        if response.content_type == 'text/html':
            msg = textwrap.dedent("""
                <h1>{0[reason]}</h1>
                {0[msg]}
                <h3>{0[version]}</h3>
            """).format({"reason": response.status, "msg": msg,
                         "version": request.environ['SERVER_SOFTWARE']})
    #
    if response.content_type == 'text/html':
        doc = request.html_document
        doc.head.embedded_css.append(error_css)
        doc.body.append(Html('div', msg, cn='pulsar-error'))
        return doc.render(request)
    else:
        return wsgi_error_msg(response, msg)


def render_error_debug(request, failure):
    '''Render the traceback into the content type in *response*.'''
    response = request.response
    is_html = response.content_type == 'text/html'
    error = Html('div', cn='section traceback error') if is_html else []
    for traces in failure.exc_info[2]:
        counter = 0
        for trace in traces.split('\n'):
            if trace.startswith('  '):
                counter += 1
                trace = trace[2:]
            if trace:
                if is_html:
                    trace = Html('p', escape(trace))
                    if counter:
                        trace.css({'margin-left': '%spx' % (20*counter)})
                error.append(trace)
    if not is_html:
        error = '\n'.join(error)
    return error
