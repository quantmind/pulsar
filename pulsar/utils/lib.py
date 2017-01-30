import os

if os.environ.get('PULSARPY', 'no') == 'yes':
    HAS_C_EXTENSIONS = False
else:
    HAS_C_EXTENSIONS = True
    try:
        from .clib import (
            EventHandler, ProtocolConsumer, Protocol, Producer, WsgiProtocol,
            AbortEvent, RedisParser, WsgiResponse, wsgi_cached, http_date,
            FrameParser
        )
    except ImportError:
        HAS_C_EXTENSIONS = False


if not HAS_C_EXTENSIONS:

    from .pylib.events import EventHandler, AbortEvent
    from .pylib.protocols import  ProtocolConsumer, Protocol, Producer
    from .pylib.wsgi import WsgiProtocol
    from .pylib.wsgiresponse import WsgiResponse, wsgi_cached
    from .pylib.redisparser import RedisParser
    from .pylib.websocket import FrameParser
    from wsgiref.handlers import format_date_time as http_date


__all__ = [
    'HAS_C_EXTENSIONS',
    'AbortEvent',
    'EventHandler',
    'ProtocolConsumer',
    'Protocol',
    'WsgiProtocol',
    'WsgiResponse',
    'wsgi_cached',
    'http_date',
    'RedisParser'
]
