import os

if os.environ.get('pulsar_speedup') == 'no':
    HAS_C_EXTENSIONS = False
else:
    HAS_C_EXTENSIONS = True
    try:
        from .clib import (
            EventHandler, ProtocolConsumer, Protocol, Producer, WsgiProtocol,
            AbortEvent, RedisParser, WsgiResponse, wsgi_cached
        )
    except ImportError:
        HAS_C_EXTENSIONS = False


if not HAS_C_EXTENSIONS:

    from .pylib.events import EventHandler, AbortEvent
    from .pylib.protocols import  ProtocolConsumer, Protocol, Producer
    from .pylib.wsgi import WsgiProtocol
    from .pylib.wsgiresponse import WsgiResponse, wsgi_cached
    from .pylib.redisparser import RedisParser


__all__ = [
    'HAS_C_EXTENSIONS',
    'AbortEvent',
    'EventHandler',
    'ProtocolConsumer',
    'Protocol',
    'Connection',
    'WsgiProtocol',
    'WsgiResponse',
    'wsgi_cached',
    'RedisParser'
]
