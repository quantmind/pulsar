import unittest
from time import time

from pulsar.utils.lib import http_date
from wsgiref.handlers import format_date_time


class TestPythonCode(unittest.TestCase):
    __benchmark__ = True
    __number__ = 100000

    def test_http_date_cython(self):
        http_date(time())

    def test_http_date_python(self):
        format_date_time(time())
