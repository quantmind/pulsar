'''
HTTP Protocol Consumer
==============================

.. autoclass:: HttpServerResponse
   :members:
   :member-order: bysource


Testing WSGI Environ
=========================

.. autofunction:: test_wsgi_environ
'''
import sys
import os
import io
from urllib.parse import urlparse

from async_timeout import timeout
from multidict import CIMultiDict

import pulsar
from pulsar import BadRequest
from pulsar.utils.httpurl import iri_to_uri
from pulsar.async.protocols import ProtocolConsumer
from pulsar.utils.structures import AttributeDictionary
from pulsar.utils import http

from .utils import (handle_wsgi_error, wsgi_request,
                    log_wsgi_info, LOGGER, get_logger)
from .formdata import HttpBodyReader
from .wrappers import FileWrapper, close_object
from .headers import CONTENT_LENGTH

try:
    from pulsar.utils.lib import WsgiProtocol
except ImportError:
    WsgiProtocol = None


CHARSET = http.CHARSET
MAX_TIME_IN_LOOP = 0.3
HTTP_1_0 = '1.0'
URL_SCHEME = os.environ.get('wsgi.url_scheme', 'http')
ENVIRON = {
    "wsgi.errors": sys.stderr,
    "wsgi.file_wrapper": FileWrapper,
    "wsgi.version": (1, 0),
    "wsgi.run_once": False,
    "wsgi.multithread": True,
    "wsgi.multiprocess": True,
    "SCRIPT_NAME": os.environ.get("SCRIPT_NAME", ""),
    "CONTENT_TYPE": ''
}


class AbortWsgi(Exception):
    pass


def test_wsgi_environ(path=None, method=None, headers=None, extra=None,
                      https=False, loop=None, body=None, **params):
    '''An function to create a WSGI environment dictionary for testing.

    :param url: the resource in the ``PATH_INFO``.
    :param method: the ``REQUEST_METHOD``.
    :param headers: optional request headers
    :params https: a secure connection?
    :param extra: additional dictionary of parameters to add to ``params``
    :param params: key valued parameters
    :return: a valid WSGI environ dictionary.
    '''
    parser = http.HttpRequestParser()
    method = (method or 'GET').upper()
    path = iri_to_uri(path or '/')
    request_headers = CIMultiDict(headers)
    # Add Host if not available
    parsed = urlparse(path)
    if 'host' not in request_headers:
        if not parsed.netloc:
            scheme = ('https' if https else 'http')
            path = '%s://127.0.0.1%s' % (scheme, path)
        else:
            request_headers['host'] = parsed.netloc
    #
    data = '%s %s HTTP/1.1\r\n\r\n' % (method, path)
    data = data.encode('latin1')
    parser.execute(data, len(data))
    #
    stream = io.BytesIO(body or b'')
    if extra:
        params.update(extra)
    return wsgi_environ(stream, parser, request_headers,
                        ('127.0.0.1', 8060), '255.0.1.2:8080',
                        CIMultiDict(), https=https, extra=params)


class HttpServerResponse(ProtocolConsumer):
    '''Server side WSGI :class:`.ProtocolConsumer`.

    .. attribute:: wsgi_callable

        The wsgi callable handling requests.
    '''
    logger = LOGGER
    SERVER_SOFTWARE = pulsar.SERVER_SOFTWARE
    ONE_TIME_EVENTS = ProtocolConsumer.ONE_TIME_EVENTS + ('on_headers',)

    def __init__(self, wsgi_callable, cfg, server_software=None, loop=None):
        self._loop = loop
        self.wsgi_callable = wsgi_callable
        self.cfg = cfg
        self.body_reader = HttpBodyReader()
        self.wsgi = WsgiProtocol(
            self,
            server_software or self.SERVER_SOFTWARE,
            AttributeDictionary(logger=LOGGER, cfg=cfg),
            FileWrapper
        )
        self.parse_url = http.parse_url
        self.parser = http.HttpRequestParser(self.wsgi)
        self.data_received = self.parser.feed_data

    ########################################################################
    #    INTERNALS
    async def _response(self):
        wsgi = self.wsgi
        environ = wsgi.environ
        exc_info = None
        response = None
        done = False
        alive = self.cfg.keep_alive or 15
        #
        while not done:
            done = True
            try:
                with timeout(alive, loop=self._loop):
                    if exc_info is None:
                        if (not environ.get('HTTP_HOST') and
                                environ['SERVER_PROTOCOL'] != 'HTTP/1.0'):
                            raise BadRequest
                        response = self.wsgi_callable(environ,
                                                      wsgi.start_response)
                        try:
                            response = await response
                        except TypeError:
                            pass
                    else:
                        response = handle_wsgi_error(environ, exc_info)
                        try:
                            response = await response
                        except TypeError:
                            pass
                    #
                    if exc_info:
                        wsgi.start_response(response.status,
                                            response.get_headers(), exc_info)
                    #
                    # Do the actual writing
                    loop = self._loop
                    # start = loop.time()
                    for chunk in response:
                        try:
                            chunk = await chunk
                        except TypeError:
                            pass
                        try:
                            await wsgi.write(chunk)
                        except TypeError:
                            pass
                        # time_in_loop = loop.time() - start
                        # if time_in_loop > MAX_TIME_IN_LOOP:
                        #     get_logger(environ).debug(
                        #         'Released the event loop after %.3f seconds',
                        #         time_in_loop)
                        #     await sleep(0.1, loop=self._loop)
                        #     start = loop.time()
                    #
                    # make sure we write headers and last chunk if needed
                    try:
                        await wsgi.write(b'', True)
                    except TypeError:
                        pass

            # client disconnected, end this connection
            except (IOError, AbortWsgi, RuntimeError):
                self.event('post_request').fire()
            except Exception:
                if wsgi_request(environ).cache.handle_wsgi_error:
                    wsgi.keep_alive = False
                    self._write_headers()
                    self.connection.close()
                    self.event('post_request').fire()
                else:
                    done = False
                    exc_info = sys.exc_info()
            else:
                if loop.get_debug():
                    logger = get_logger(environ)
                    log_wsgi_info(logger.info, environ, self.wsgi.status)
                    if not wsgi.keep_alive:
                        logger.debug('No keep alive, closing connection %s',
                                     self.connection)
                self.event('post_request').fire()
                if not wsgi.keep_alive:
                    self._connection.close()
            finally:
                close_object(response)

    def _write_headers(self):
        wsgi = self.wsgi
        if not wsgi.headers_sent:
            if CONTENT_LENGTH in wsgi.headers:
                wsgi.headers[CONTENT_LENGTH] = '0'
            wsgi.write(b'')
