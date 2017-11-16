from urllib.parse import urlparse
from unittest.mock import MagicMock

from pulsar.apps.wsgi import wsgi_request
from pulsar.apps.http import HttpWsgiClient


async def test_wsgi_request(url=None, method=None, headers=None, **kwargs):
    cli = HttpWsgiClient(ok, headers=headers)
    url = url or '/'
    if not urlparse(url).scheme:
        url = 'http://www.example.com/%s' % (
            url[1:] if url.startswith('/') else url)
    method = method or 'get'
    response = await cli.request(method, url, **kwargs)
    return wsgi_request(response.server_side.request.environ)


def ok(environ, start_response):
    request = wsgi_request(environ)
    request.cache.logger = MagicMock()
    response_headers = [
        ('Content-Length', '0')
    ]
    start_response('200 OK', response_headers)
    return iter([])
