import os

if os.environ.get('PULSARPY', 'no') == 'yes':
    HAS_C_EXTENSIONS = False
else:
    HAS_C_EXTENSIONS = True
    try:
        import httptools    # noqa
        from .clib import (
            EventHandler, ProtocolConsumer, Protocol, Producer, WsgiProtocol,
            AbortEvent, RedisParser, WsgiResponse, wsgi_cached, http_date,
            FrameParser, has_empty_content, isawaitable, Event
        )
    except ImportError:
        HAS_C_EXTENSIONS = False


if not HAS_C_EXTENSIONS:
    from inspect import isawaitable     # noqa

    from .pylib.protocols import  ProtocolConsumer, Protocol, Producer  # noqa
    from .pylib.events import EventHandler, AbortEvent, Event   # noqa
    from .pylib.wsgi import WsgiProtocol, http_date, has_empty_content  # noqa
    from .pylib.wsgiresponse import WsgiResponse, wsgi_cached   # noqa
    from .pylib.redisparser import RedisParser                  # noqa
    from .pylib.websocket import FrameParser                    # noqa


__all__ = [
    'HAS_C_EXTENSIONS',
    'AbortEvent',
    'EventHandler',
    'Event',
    'ProtocolConsumer',
    'Protocol',
    'WsgiProtocol',
    'WsgiResponse',
    'wsgi_cached',
    'http_date',
    'isawaitable',
    'has_empty_content',
    'RedisParser'
]
