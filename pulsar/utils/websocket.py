# -*- coding: utf-8 -*-
'''WebSocket_ Protocol is implemented via the :class:`Frame` and
:class:`FrameParser` classes.

To obtain a frame parser one should use the :func:`frame_parser` function.

frame parser
~~~~~~~~~~~~~~~~~~~

.. autofunction:: frame_parser


Frame
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: Frame
   :members:
   :member-order: bysource


Frame Parser
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: FrameParser
   :members:
   :member-order: bysource


parse_close
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autofunction:: parse_close


.. _WebSocket: http://tools.ietf.org/html/rfc6455'''
import os
from struct import unpack
from base64 import b64encode

from .exceptions import ProtocolError
from .httpurl import CHARSET
from .lib import FrameParser


CLOSE_CODES = {
    1000: "OK",
    1001: "going away",
    1002: "protocol error",
    1003: "unsupported type",
    # 1004: - (reserved)
    # 1005: no status code (internal)
    # 1006: connection closed abnormally (internal)
    1007: "invalid data",
    1008: "policy violation",
    1009: "message too big",
    1010: "extension required",
    1011: "unexpected error",
    # 1015: TLS failure (internal)
}


DEFAULT_VERSION = 13
SUPPORTED_VERSIONS = (DEFAULT_VERSION,)
WS_EXTENSIONS = {}
WS_PROTOCOLS = {}


def get_version(version):
    try:
        version = int(version or DEFAULT_VERSION)
    except Exception:
        pass
    if version not in SUPPORTED_VERSIONS:
        raise ProtocolError('Version %s not supported.' % version)
    return version


def websocket_key():
    return b64encode(os.urandom(16)).decode(CHARSET)


class Extension:

    def receive(self, data):
        return data

    def send(self, data):
        return data


def frame_parser(version=None, kind=0, extensions=None, protocols=None):
    '''Create a new :class:`FrameParser` instance.

    :param version: protocol version, the default is 13
    :param kind: the kind of parser, and integer between 0 and 3 (check the
        :class:`FrameParser` documentation for details)
    :param extensions: not used at the moment
    :param protocols: not used at the moment
    :param pyparser: if ``True`` (default ``False``) uses the python frame
        parser implementation rather than the much faster cython
        implementation.
    '''
    version = get_version(version)
    # extensions, protocols
    return FrameParser(version, kind, ProtocolError, close_codes=CLOSE_CODES)


def parse_close(data):
    '''Parse the body of a close :class:`Frame`.

    Returns a tuple (``code``, ``reason``) if successful otherwise
    raise :class:`.ProtocolError`.
    '''
    length = len(data)
    if length == 0:
        return 1005, ''
    elif length == 1:
        raise ProtocolError("Close frame too short")
    else:
        code, = unpack('!H', data[:2])
        if not (code in CLOSE_CODES or 3000 <= code < 5000):
            raise ProtocolError("Invalid status code for websocket")
        reason = data[2:].decode('utf-8')
        return code, reason
