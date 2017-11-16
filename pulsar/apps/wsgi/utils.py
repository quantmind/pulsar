import re
import textwrap
import logging
from urllib.parse import parse_qsl

from multidict import MultiDict

from pulsar.utils.lib import has_empty_content
from pulsar.utils.exceptions import format_traceback
from pulsar.utils.system import json
from pulsar.utils.html import escape
from pulsar.utils.string import to_string
from pulsar.utils.structures import as_tuple
from pulsar.utils.httpurl import (
    REDIRECT_CODES, HTTPError, parse_dict_header, JSON_CONTENT_TYPES
)

from .structures import Accept, RequestCacheControl
from .content import Html, HtmlDocument


DEFAULT_RESPONSE_CONTENT_TYPES = ('text/plain', 'text/html',
                                  ) + JSON_CONTENT_TYPES


PULSAR_CACHE = 'pulsar.cache'
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
            status,
            msg)


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
        on the :class:`~.CacheControl`
        object is changed.
    :param cls: the class for the returned object.
        By default :class:`~.RequestCacheControl` is used.
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
        return MultiDict(_gen_query(query_string, encoding))
    else:
        return MultiDict()


error_messages = {
    500: 'An exception has occurred while evaluating your request.',
    404: 'Cannot find what you are looking for.'
}


class dump_environ:
    __slots__ = ('environ',)
    FILTERED_KEYS = ['HTTP_COOKIE']

    def __init__(self, environ):
        self.environ = environ

    def __str__(self):
        def _():
            for k, v in self.environ.items():
                if k in self.FILTERED_KEYS:
                    continue
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
    old_response = request.cache.pop('response', None)
    response = request.response
    if old_response:
        response.content_type = old_response.content_type
    logger = request.logger
    #
    if isinstance(exc, HTTPError):
        response.status_code = exc.code or 500
    else:
        response.status_code = getattr(exc, 'status', 500)
    response.headers.update(getattr(exc, 'headers', None) or ())
    status = response.status_code
    if status >= 500:
        logger.critical('%s - @ %s.\n%s', exc, request.first_line,
                        dump_environ(environ), exc_info=exc_info)
    else:
        log_wsgi_info(logger.warning, environ, response.status, exc)
    if has_empty_content(status, request.method) or status in REDIRECT_CODES:
        response.content_type = None
        response.content = None
    else:
        request.cache.pop('html_document', None)
        renderer = environ.get('error.handler') or render_error
        try:
            content = renderer(request, exc)
        except Exception:
            logger.critical('Error while rendering error', exc_info=True)
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
            as_tuple(content_type or DEFAULT_RESPONSE_CONTENT_TYPES)
        )
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
        return doc.to_bytes(request)
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
