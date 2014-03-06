import base64
import hashlib
from functools import partial

from pulsar import HttpException, ProtocolError, ProtocolConsumer, maybe_async
from pulsar.utils.pep import to_bytes, native_str
from pulsar.utils.httpurl import DEFAULT_CHARSET
from pulsar.utils.websocket import frame_parser, parse_close
from pulsar.apps import wsgi

from . import extensions

WEBSOCKET_GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11'
TRANSPORTS = {}


def register_transport(klass):
    TRANSPORTS[klass.name] = klass
    return klass


@register_transport
class WebSocket(wsgi.Router):
    """A :ref:`Router <wsgi-router>` for a websocket handshake.

    Once the handshake is succesful, the protocol consumer
    is upgraded to :class:`WebSocketProtocol` and messages are handled by
    the :attr:`handle` attribute, an instance of :class:`.WS`.

    See http://tools.ietf.org/html/rfc6455 for the websocket server protocol
    and http://www.w3.org/TR/websockets/ for details on the JavaScript
    interface.

    .. attribute:: parser_factory

        A factory of websocket frame parsers
    """
    parser_factory = frame_parser
    _name = 'websocket'

    def __init__(self, route, handle, parser_factory=None, **kwargs):
        super(WebSocket, self).__init__(route, **kwargs)
        self.handle = handle
        self.parser_factory = parser_factory or frame_parser

    @property
    def name(self):
        return self._name

    def get(self, request):
        headers_parser = self.handle_handshake(request.environ)
        if not headers_parser:
            raise HttpException(status=404)
        headers, parser = headers_parser
        response = request.response
        response.status_code = 101
        response.content = b''
        response.headers.update(headers)
        connection = request.environ.get('pulsar.connection')
        if not connection:
            raise HttpException(status=404)
        factory = partial(WebSocketProtocol, request, self.handle, parser)
        connection.upgrade(factory)
        return request.response

    def handle_handshake(self, environ):
        connections = environ.get(
            "HTTP_CONNECTION", '').lower().replace(' ', '').split(',')
        if environ.get("HTTP_UPGRADE", '').lower() != "websocket" or \
           'upgrade' not in connections:
            raise HttpException(status=400)
        key = environ.get('HTTP_SEC_WEBSOCKET_KEY')
        if key:
            try:
                ws_key = base64.b64decode(key.encode(DEFAULT_CHARSET))
            except Exception:
                ws_key = ''
            if len(ws_key) != 16:
                raise HttpException(msg="WebSocket key's length is invalid",
                                    status=400)
        else:
            raise HttpException(msg='Not a valid HyBi WebSocket request. '
                                    'Missing Sec-Websocket-Key header.',
                                status=400)
        # Collect supported subprotocols
        subprotocols = environ.get('HTTP_SEC_WEBSOCKET_PROTOCOL')
        ws_protocols = []
        if subprotocols:
            for s in subprotocols.split(','):
                ws_protocols.append(s.strip())
        # Collect supported extensions
        ws_extensions = []
        extensions = environ.get('HTTP_SEC_WEBSOCKET_EXTENSIONS')
        if extensions:
            for ext in extensions.split(','):
                ws_extensions.append(ext.strip())
        # Build the frame parser
        version = environ.get('HTTP_SEC_WEBSOCKET_VERSION')
        try:
            parser = self.parser_factory(version=version,
                                         protocols=ws_protocols,
                                         extensions=ws_extensions)
        except ProtocolError as e:
            raise HttpException(str(e), status=400)
        headers = [('Sec-WebSocket-Accept', self.challenge_response(key))]
        if parser.protocols:
            headers.append(('Sec-WebSocket-Protocol',
                            ', '.join(parser.protocols)))
        if parser.extensions:
            headers.append(('Sec-WebSocket-Extensions',
                            ','.join(parser.extensions)))
        return headers, parser

    def challenge_response(self, key):
        sha1 = hashlib.sha1(to_bytes(key+WEBSOCKET_GUID))
        return native_str(base64.b64encode(sha1.digest()))


class WebSocketProtocol(ProtocolConsumer):
    '''A :class:`.ProtocolConsumer` for websocket servers and clients.

    .. attribute:: handshake

        The original handshake response/request.

    .. attribute:: handler

        A websocket handler :class:`.WS`.

    .. attribute:: parser

        A websocket :class:`.FrameParser`.

    .. attribute:: close_reason

        A tuple of (``code``, ``reason``) or ``None``.

        Available when a close frame is received.
    '''
    close_reason = None

    def __init__(self, handshake, handler, parser):
        super(WebSocketProtocol, self).__init__()
        self.bind_event('post_request', self._shut_down)
        self.handshake = handshake
        self.handler = handler
        self.parser = parser

    @property
    def cfg(self):
        '''The :class:`.Config` container for this protocol.
        '''
        return self.handshake.cfg

    def connection_made(self, connection):
        connection.set_timeout(0)
        maybe_async(self.handler.on_open(self), self._loop)

    def data_received(self, data):
        frame = self.parser.decode(data)
        while frame:
            if frame.is_close:
                try:
                    self.close_reason = parse_close(frame.body)
                finally:
                    self._connection.close()
                break
            if frame.is_message:
                maybe_async(self.handler.on_message(self, frame.body))
            elif frame.is_bytes:
                maybe_async(self.handler.on_bytes(self, frame.body))
            elif frame.is_ping:
                maybe_async(self.handler.on_ping(self, frame.body))
            elif frame.is_pong:
                maybe_async(self.handler.on_pong(self, frame.body))
            frame = self.parser.decode()

    def write(self, message, opcode=None, **kw):
        '''Write a new ``message`` into the wire.

        It uses the :meth:`~.FrameParser.encode` method of the
        websocket :attr:`parser`.

        :param message: message to send, must be a string or bytes
        :param opcode: optional ``opcode``, if not supplied it is set to 1
            if ``message`` is a string, otherwise ``2`` when the message
            are bytes.
         '''
        chunk = self.parser.encode(message, opcode=opcode, **kw)
        self.transport.write(chunk)
        if opcode == 8:
            self.finish()

    def ping(self, message=None):
        '''Write a ping ``frame``.
        '''
        self.transport.write(self.parser.ping(message))

    def pong(self, message=None):
        '''Write a pong ``frame``.
        '''
        self.transport.write(self.parser.pong(message))

    def write_close(self, code=None):
        '''Write a close ``frame`` with ``code``.
        '''
        self.transport.write(self.parser.close(code))
        self._connection.close()

    def _shut_down(self, result, exc=None):
        maybe_async(self.handler.on_close(self))
