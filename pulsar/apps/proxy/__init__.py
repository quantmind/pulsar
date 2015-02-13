from urllib.parse import urljoin

from pulsar import as_coroutine, task
from pulsar.utils.httpurl import Headers
from pulsar.utils.log import LocalMixin, local_property
from pulsar.apps.wsgi import Route, wsgi_request
from pulsar.apps.http import HttpClient


ENVIRON_HEADERS = ('content-type', 'content-length')


class Proxy(LocalMixin):
    '''Proxy requests to another server
    '''
    def __init__(self, route, url):
        self.route = Route(route)
        self.url = url

    @local_property
    def http_client(self):
        '''The :class:`.HttpClient` used by this proxy middleware for
        accessing upstream resources'''
        return HttpClient(decompress=False, store_cookies=False)

    def __call__(self, environ, start_response):
        request = wsgi_request(environ)
        path = request.path
        match = self.route.match(path[1:])
        if match is not None:
            query = request.get('QUERY_STRING', '')
            path = urljoin(self.url, match.pop('__remaining__', ''))
            if query:
                path = '%s?%s' % (path, query)
            return self._call(request, path, start_response)

    @task
    def _call(self, request, path, start_response):
        data, files = yield from as_coroutine(request.data_and_files())
        response = yield from self.http_client.request(
            request.method, path, data=data, files=files,
            headers=self.request_headers(request.environ),
            version=request.get('SERVER_PROTOCOL'))
        response.raise_for_status()
        start_response(response.get_status(), list(response.headers))
        return [response.get_content()]

    def request_headers(self, environ):
        '''Fill request headers from the environ dictionary and
        modify them via the list of :attr:`headers_middleware`.
        The returned headers will be sent to the target uri.
        '''
        headers = Headers(kind='client')
        for k in environ:
            if k.startswith('HTTP_'):
                head = k[5:].replace('_', '-')
                headers[head] = environ[k]
        for head in ENVIRON_HEADERS:
            k = head.replace('-', '_').upper()
            v = environ.get(k)
            if v:
                headers[head] = v
        return headers
