from ...utils.exceptions import PulsarException
from ...utils.lib import RedisParser


class RedisError(PulsarException):
    '''Redis Error Base class'''
    pass


class CommandError(RedisError):
    pass


class ResponseError(RedisError):
    pass


class InvalidResponse(RedisError):
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


def redis_parser():
    return RedisParser(InvalidResponse, response_error)
