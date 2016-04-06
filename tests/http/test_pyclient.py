import unittest

from pulsar.utils.httpurl import hasextensions, HttpParser

import base


@unittest.skipUnless(hasextensions, 'Requires C extensions')
class TestHttpClientWithPythonParser(base.TestHttpClient):

    @classmethod
    def parser(cls):
        return HttpParser
