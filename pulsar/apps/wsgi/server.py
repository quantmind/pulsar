'''
HTTP Protocol Consumer
==============================

.. autoclass:: HttpServerResponse
   :members:
   :member-order: bysource

'''
import sys
import os

from async_timeout import timeout

from pulsar.api import BadRequest, ProtocolConsumer
from pulsar.utils.lib import WsgiProtocol
from pulsar.utils import http

from .utils import handle_wsgi_error, wsgi_request, log_wsgi_info, get_logger
from .formdata import HttpBodyReader
from .wrappers import FileWrapper, close_object
from .headers import CONTENT_LENGTH


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


class HttpServerResponse(ProtocolConsumer):
    '''Server side WSGI :class:`.ProtocolConsumer`.

    .. attribute:: wsgi_callable

        The wsgi callable handling requests.
    '''
    ONE_TIME_EVENTS = ProtocolConsumer.ONE_TIME_EVENTS + ('on_headers',)

    def create_request(self):
        producer = self.producer
        self.body_reader = HttpBodyReader()
        self.parse_url = http.parse_url
        self.create_parser = http.HttpRequestParser
        self.cfg = producer.cfg
        wsgi = WsgiProtocol(self, producer.cfg, FileWrapper)
        return wsgi

    ########################################################################
    #    INTERNALS
    def feed_data(self, data):
        try:
            self.request.parser.feed_data(data)
        except http.HttpParserUpgrade:
            pass

    async def _response(self):
        wsgi = self.request
        producer = self.producer
        wsgi_callable = producer.wsgi_callable
        keep_alive = producer.keep_alive or None
        environ = wsgi.environ
        exc_info = None
        response = None
        done = False
        #
        try:
            while not done:
                done = True
                try:
                    with timeout(keep_alive, loop=self._loop):
                        if exc_info is None:
                            if (not environ.get('HTTP_HOST') and
                                    environ['SERVER_PROTOCOL'] != 'HTTP/1.0'):
                                raise BadRequest
                            response = wsgi_callable(environ,
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
                            wsgi.start_response(
                                response.status,
                                response.get_headers(),
                                exc_info
                            )
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
                            #         'Released loop after %.3f seconds',
                            #         time_in_loop)
                            #     await sleep(0.1, loop=self._loop)
                            #     start = loop.time()
                        #
                        # make sure we write headers and last chunk if needed
                        wsgi.write(b'', True)

                # client disconnected, end this connection
                except (IOError, AbortWsgi, RuntimeError):
                    self.event('post_request').fire()
                except Exception:
                    if wsgi_request(environ).cache.get('handle_wsgi_error'):
                        get_logger(environ).exception(
                            'Exception while handling WSGI error'
                        )
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
                        log_wsgi_info(logger.info, environ, wsgi.status)
                        if not wsgi.keep_alive:
                            logger.debug(
                                'No keep alive, closing connection %s',
                                self.connection
                            )
                    self.event('post_request').fire()
                    if not wsgi.keep_alive:
                        self.connection.close()
                finally:
                    close_object(response)
        finally:
            self = None

    def _write_headers(self):
        wsgi = self.request
        if not wsgi.headers_sent:
            if CONTENT_LENGTH in wsgi.headers:
                wsgi.headers[CONTENT_LENGTH] = '0'
            wsgi.write(b'')
