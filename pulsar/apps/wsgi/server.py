"""
HTTP Protocol Consumer
==============================

.. autoclass:: HttpServerResponse
   :members:
   :member-order: bysource

"""
import os
import sys

from pulsar.api import BadRequest, ProtocolConsumer, isawaitable
from pulsar.utils.lib import WsgiProtocol
from pulsar.utils import http
from pulsar.async.timeout import timeout

from .utils import handle_wsgi_error, log_wsgi_info, LOGGER
from .formdata import HttpBodyReader
from .wrappers import FileWrapper, close_object
from .headers import CONTENT_LENGTH


PULSAR_TEST = 'PULSAR_TEST'


class AbortWsgi(Exception):
    pass


class HttpServerResponse(ProtocolConsumer):
    '''Server side WSGI :class:`.ProtocolConsumer`.

    .. attribute:: wsgi_callable

        The wsgi callable handling requests.
    '''
    ONE_TIME_EVENTS = ProtocolConsumer.ONE_TIME_EVENTS + ('on_headers',)

    def create_request(self):
        self.parse_url = http.parse_url
        self.create_parser = http.HttpRequestParser
        self.cfg = self.producer.cfg
        self.logger = LOGGER
        return WsgiProtocol(self, self.producer.cfg, FileWrapper)

    def body_reader(self, environ):
        return HttpBodyReader(
            self.connection.transport,
            self.producer.cfg.stream_buffer,
            environ)

    def __repr__(self):
        return '%s - %d - %s' % (
            self.__class__.__name__,
            self.connection.processed,
            self.connection
        )
    __str__ = __repr__

    ########################################################################
    #    INTERNALS
    def feed_data(self, data):
        try:
            return self.request.parser.feed_data(data)
        except http.HttpParserUpgrade:
            pass
        except Exception as exc:
            self.logger.exception(
                'Could not recover from "%s" - sending 500',
                exc
            )
            write = self.request.start_response(
                '500 Internal Server Error',
                [('content-length', '0')],
                sys.exc_info()
            )
            write(b'', True)
            self.connection.close()
            self.event('post_request').fire()

    async def write_response(self):
        loop = self._loop
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
                    if exc_info is None:
                        if (not environ.get('HTTP_HOST') and
                                environ['SERVER_PROTOCOL'] != 'HTTP/1.0'):
                            raise BadRequest
                        response = wsgi_callable(environ,
                                                 wsgi.start_response)
                        if isawaitable(response):
                            with timeout(loop, keep_alive):
                                response = await response
                    else:
                        response = handle_wsgi_error(environ, exc_info)
                        if isawaitable(response):
                            with timeout(loop, keep_alive):
                                response = await response
                    #
                    if exc_info:
                        response.start(
                            environ,
                            wsgi.start_response,
                            exc_info
                        )
                    #
                    # Do the actual writing
                    for chunk in response:
                        if isawaitable(chunk):
                            with timeout(loop, keep_alive):
                                chunk = await chunk
                        waiter = wsgi.write(chunk)
                        if waiter:
                            with timeout(loop, keep_alive):
                                await waiter
                    #
                    # make sure we write headers and last chunk if needed
                    wsgi.write(b'', True)

                # client disconnected, end this connection
                except (IOError, AbortWsgi, RuntimeError):
                    self.event('post_request').fire()
                except Exception:
                    if self.get('handle_wsgi_error'):
                        self.logger.exception(
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
                        log_wsgi_info(self.logger.info, environ, wsgi.status)
                        if not wsgi.keep_alive:
                            self.logger.debug(
                                'No keep alive, closing connection %s',
                                self.connection
                            )
                    self.event('post_request').fire()
                    if not wsgi.keep_alive:
                        self.connection.close()
                finally:
                    close_object(response)
        finally:
            # help GC
            if PULSAR_TEST not in os.environ:
                environ.clear()
            self = None

    def _cancel_task(self, task):
        task.cancel()

    def _write_headers(self):
        wsgi = self.request
        if not wsgi.headers_sent:
            if CONTENT_LENGTH in wsgi.headers:
                wsgi.headers[CONTENT_LENGTH] = '0'
            wsgi.write(b'')
