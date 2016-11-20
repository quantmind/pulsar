import unittest

from pulsar.apps.test.wsgi import HttpTestClient
from pulsar.utils.httpurl import HttpParser, CHttpParser


class TestPyParser(unittest.TestCase):
    __benchmark__ = True
    __number__ = 1000

    @classmethod
    async def setUpClass(cls):
        http = HttpTestClient()
        response = await http.post('http://bla.com/upload', data=b'g' * 2**20)
        message = response.message
        ip = len(message) // 2
        cls.messages = [message[:ip], message[ip:]]

    def startUp(self):
        self.server = self.parser()

    def parser(self, kind=0):
        return HttpParser(kind=kind)

    def test_server_parser(self):
        for msg in self.messages:
            assert self.server.execute(msg, len(msg)) == len(msg)
        assert self.server.is_message_complete()


class TestCParser(TestPyParser):

    def parser(self, kind=0):
        return CHttpParser(kind=kind)
