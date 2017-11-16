import os
import unittest

from tests.http import base, req


@unittest.skipIf(os.environ.get('CI'), 'Skip on CI see #288')
class TestHttpClientWithProxy(req.TestRequest, base.TestHttpClient):
    with_proxy = True

    def _check_server(self, response):
        pass
