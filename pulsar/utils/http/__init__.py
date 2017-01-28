import os


if os.environ.get('PULSARPY', 'no') == 'no':
    try:
        import httptools
        hasextensions = True
        from httptools import (
            HttpResponseParser, HttpRequestParser, HttpParserUpgrade,
            parse_url
        )
    except ImportError:
        hasextensions = False
        from .parser import (
            HttpRequestParser, HttpResponseParser, HttpParserUpgrade,
            parse_url
        )


CHARSET = 'ISO-8859-1'
