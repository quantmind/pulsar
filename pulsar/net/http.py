from pulsar import lib

from .defer import make_async, is_async
from .iostream import Connection, AsyncSocketServer


def wsgi_iterator(result, callback, *args, **kwargs):
    result = make_async(result).get_result_or_self()
    while is_async(result):
        # yield empty bytes so that the loop is released
        yield b''
        result = result.get_result_or_self()
    # The result is ready
    for chunk in callback(result, *args, **kwargs):
        yield chunk


class HttpConnection(Connection):
    pass


class HttpServer(AsyncSocketServer):
    connection_class = HttpConnection
    parser_class = lib.Http_Parser