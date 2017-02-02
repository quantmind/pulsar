from ..lib import HAS_C_EXTENSIONS


if HAS_C_EXTENSIONS:
    try:
        from httptools import (
            HttpResponseParser, HttpRequestParser, HttpParserUpgrade,
            HttpParserError, parse_url
        )
        hasextensions = True
    except ImportError:
        hasextensions = False

else:
    hasextensions = False


if not hasextensions:
    from .parser import (   # noqa
        HttpRequestParser, HttpResponseParser, HttpParserUpgrade,
        HttpParserError, parse_url
    )


CHARSET = 'ISO-8859-1'


__all__ = [
    'HttpResponseParser',
    'HttpRequestParser',
    'HttpParserUpgrade',
    'HttpParserError',
    'parse_url',
    'hasextensions',
    'CHARSET'
]
