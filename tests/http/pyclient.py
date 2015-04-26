import unittest

from pulsar.utils.httpurl import hasextensions, HttpParser

from . import base


@unittest.skipUnless(hasextensions, 'Requires C extensions')
class TestHttpClientWithPythonParser(base.TestHttpClient):

    @classmethod
    def parser(cls):
        return HttpParser
