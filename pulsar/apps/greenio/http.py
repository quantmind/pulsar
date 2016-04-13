from pulsar.apps.http import HttpClient

from .utils import wait


class GreenHttp:
    """Wraps an http client so we can use it with greenlets
    """
    def __init__(self, http=None):
        self._http = http or HttpClient()

    def __getattr__(self, name):
        return getattr(self._http, name)

    def get(self, url, **kwargs):
        kwargs.setdefault('allow_redirects', True)
        return self.request('GET', url, **kwargs)

    def options(self, url, **kwargs):
        kwargs.setdefault('allow_redirects', True)
        return self.request('OPTIONS', url, **kwargs)

    def head(self, url, **kwargs):
        return self.request('HEAD', url, **kwargs)

    def post(self, url, **kwargs):
        return self.request('POST', url, **kwargs)

    def put(self, url, **kwargs):
        return self.request('PUT', url, **kwargs)

    def patch(self, url, **kwargs):
        return self.request('PATCH', url, **kwargs)

    def delete(self, url, **kwargs):
        return self.request('DELETE', url, **kwargs)

    def request(self, method, url, **kw):
        return wait(self._http.request(method, url, **kw), True)
