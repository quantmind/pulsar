import unittest

from pulsar.utils.http import hasextensions
from pulsar.utils.http.parser import HttpRequestParser

from tests.http import base


@unittest.skipUnless(hasextensions, 'Requires C extensions')
class TestHttpClientWithPythonParser(base.TestHttpClient):

    @classmethod
    def parser(cls):
        return HttpRequestParser
