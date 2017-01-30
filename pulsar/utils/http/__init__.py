import os


if os.environ.get('PULSARPY', 'no') != 'yes':
    try:
        from httptools import (
            HttpResponseParser, HttpRequestParser, HttpParserUpgrade,
            parse_url
        )
        hasextensions = True
    except ImportError:
        hasextensions = False

else:
    hasextensions = False


if not hasextensions:
    from .parser import (   # noqa
        HttpRequestParser, HttpResponseParser, HttpParserUpgrade,
        parse_url
    )


CHARSET = 'ISO-8859-1'


__all__ = [
    'HttpResponseParser',
    'HttpRequestParser',
    'HttpParserUpgrade',
    'parse_url',
    'hasextensions',
    'CHARSET'
]
