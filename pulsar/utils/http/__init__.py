from urllib.parse import urlparse

from . import parser

try:
    import httptools
    hasextensions = True
except ImportError:
    httptools = None
    hasextensions = False


CHARSET = 'ISO-8859-1'
HttpResponseParser = None
HttpRequestParser = None
parse_url = None


def setDefaultHttpParsers(best=True):   # pragma    nocover
    global HttpRequestParser, HttpResponseParser, parse_url
    if best and hasextensions:
        HttpRequestParser = httptools.HttpRequestParser
        HttpResponseParser = httptools.HttpResponseParser
        parse_url = httptools.parse_url
    else:
        HttpRequestParser = parser.HttpRequestParser
        HttpResponseParser = parser.HttpResponseParser
        parse_url = urlparse


setDefaultHttpParsers()
