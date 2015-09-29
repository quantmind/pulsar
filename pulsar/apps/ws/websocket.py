import base64
import hashlib
from functools import partial

from pulsar import HttpException, ProtocolError, ProtocolConsumer, maybe_async
from pulsar.utils.pep import to_bytes, native_str
from pulsar.utils.httpurl import DEFAULT_CHARSET
from pulsar.utils.websocket import frame_parser, parse_close
from pulsar.apps import wsgi

from . import extensions as ext    # noqa

WEBSOCKET_GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11'


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

    def __init__(self, handshake, handler, parser, loop=None):
        super().__init__(loop)
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
        connection.timeout = 0
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
                self._on(self.handler.on_message, frame)
            elif frame.is_bytes:
                self._on(self.handler.on_bytes, frame)
            elif frame.is_ping:
                self._on(self.handler.on_ping, frame)
            elif frame.is_pong:
                self._on(self.handler.on_pong, frame)
            frame = self.parser.decode()

    def write(self, message, opcode=None, encode=True, **kw):
        '''Write a new ``message`` into the wire.

        It uses the :meth:`~.FrameParser.encode` method of the
        websocket :attr:`parser`.

        :param message: message to send, must be a string or bytes
        :param opcode: optional ``opcode``, if not supplied it is set to 1
            if ``message`` is a string, otherwise ``2`` when the message
            are bytes.
         '''
        if encode:
            message = self.parser.encode(message, opcode=opcode, **kw)
        result = super().write(message)
        if opcode == 0x8:
            self.finished()
        return result

    def ping(self, message=None):
        '''Write a ping ``frame``.
        '''
        return self.write(self.parser.ping(message), encode=False)

    def pong(self, message=None):
        '''Write a pong ``frame``.
        '''
        return self.write(self.parser.pong(message), encode=False)

    def write_close(self, code=None):
        '''Write a close ``frame`` with ``code``.
        '''
        return self.write(self.parser.close(code), opcode=0x8, encode=False)

    def _on(self, handler, frame):
        maybe_async(handler(self, frame.body), loop=self._loop)

    def _shut_down(self, result, exc=None):
        maybe_async(self.handler.on_close(self))


class WebSocket(wsgi.Router):
    """A specialised :class:`.Router` for a websocket handshake.

    Once the handshake is successful, the protocol consumer
    is upgraded to :class:`.WebSocketProtocol` and messages are handled by
    the :attr:`handle` attribute, an instance of :class:`.WS`.

    See http://tools.ietf.org/html/rfc6455 for the websocket server protocol
    and http://www.w3.org/TR/websockets/ for details on the JavaScript
    interface.

    .. attribute:: parser_factory

        A factory of websocket frame parsers
    """
    protocol_class = WebSocketProtocol
    parser_factory = frame_parser

    def __init__(self, route, handle, parser_factory=None, **kwargs):
        super().__init__(route, **kwargs)
        self.handle = handle
        self.parser_factory = parser_factory or frame_parser

    def get(self, request):
        headers_parser = self.handle_handshake(request)
        if not headers_parser:
            raise HttpException(status=404)
        headers, parser = headers_parser
        response = request.response
        response.status_code = 101
        response.content = b''
        response.headers.update(headers)
        connection = request.get('pulsar.connection')
        if not connection:
            raise HttpException(status=404)
        factory = partial(self.protocol_class, request, self.handle, parser)
        connection.upgrade(factory)
        return request.response

    def handle_handshake(self, request):
        environ = request.environ
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
            for e in extensions.split(','):
                ws_extensions.append(e.strip())
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
