from pulsar.utils.httpurl import hasextensions, HttpParser
from pulsar.apps.test import unittest

from . import client


@unittest.skipUnless(hasextensions, 'Requires C extensions')
class TestHttpClientWithPythonParser(client.TestHttpClient):

    @classmethod
    def parser(cls):
        return HttpParser
