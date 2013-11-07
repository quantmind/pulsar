import pulsar

from .pyparser import Parser

try:
    import redis
    from redis.exceptions import NoScriptError, InvalidResponse
    from redis.connection import PythonParser as _p
    EXCEPTION_CLASSES = _p.EXCEPTION_CLASSES
except ImportError:     # pragma    nocover

    class ResponseError(pulsar.PulsarException):
        pass

    class InvalidResponse(pulsar.PulsarException):
        pass

    class NoScriptError(ResponseError):
        pass

    EXCEPTION_CLASSES = {
        'ERR': ResponseError,
        'NOSCRIPT': NoScriptError,
    }


def response_error(response):
    "Parse an error response"
    response = response.split(' ')
    error_code = response[0]
    if error_code not in EXCEPTION_CLASSES:
        error_code = 'ERR'
    response = ' '.join(response[1:])
    return EXCEPTION_CLASSES[error_code](response)


PyRedisParser = lambda: Parser(InvalidResponse, response_error)
HAS_C_EXTENSIONS = True

try:
    from . import cparser
    RedisParser = lambda: cparser.RedisParser(InvalidResponse, ResponseError)
except ImportError:     # pragma    nocover
    HAS_C_EXTENSIONS = False
    RedisParser = PyRedisParser


def redis_parser(py_redis_parser=False):
    return PyRedisParser if py_redis_parser else RedisParser

