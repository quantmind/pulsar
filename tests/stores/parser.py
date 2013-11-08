from pulsar.apps.test import unittest

from . import client

if client.available:
    from redis.exceptions import InvalidResponse, NoScriptError, ResponseError


lua_nested_table = '''
local s = ''
for i=1,100 do
    s = s .. '1234567890'
end
local nesting = ARGV[1]
local pres = {100, s}
local result = pres
for i=1,nesting do
    local res = {-8, s}
    pres[3] = res
    pres[4] = res
    pres = res
end
return result
'''


class TestParser(client.RedisTest):

    def test_null(self):
        test = b'$-1\r\n'
        p = self.parser()
        p.feed(test)
        self.assertEqual(p.get(), None)

    def test_empty_string(self):
        test = b'$0\r\n\r\n'
        p = self.parser()
        p.feed(test)
        self.assertEqual(p.get(), b'')
        self.assertEqual(p.buffer(), b'')

    def test_empty_vector(self):
        test = b'*0\r\n'
        p = self.parser()
        p.feed(test)
        self.assertEqual(p.get(), [])
        self.assertEqual(p.buffer(), b'')

    def test_parseError(self):
        test = b'pxxxx\r\n'
        p = self.parser()
        p.feed(test)
        self.assertRaises(InvalidResponse, p.get)

    def test_responseError(self):
        test = b'-ERR random error\r\n'
        p = self.parser()
        p.feed(test)
        value = p.get()
        self.assertIsInstance(value, ResponseError)
        self.assertEqual(str(value), 'random error')

    def test_noscriptError(self):
        test = b'-NOSCRIPT random error\r\n'
        p = self.parser()
        p.feed(test)
        value = p.get()
        self.assertIsInstance(value, NoScriptError)
        self.assertEqual(str(value), 'random error')

    def test_binary(self):
        test = b'$31\r\n\x80\x02]q\x00(X\x04\x00\x00\x00ciaoq\x01X\x05\x00'\
               b'\x00\x00pippoq\x02e.\r\n'
        p = self.parser()
        p.feed(test)
        self.assertEqual(p.buffer(), test)
        value = p.get()
        self.assertTrue(value)
        self.assertEqual(p.buffer(), b'')

    def test_multi(self):
        test = b'+OK\r\n+QUEUED\r\n+QUEUED\r\n+QUEUED\r\n*3\r\n$-1\r\n:1\r\n:39\r\n'
        p = self.parser()
        p.feed(test)
        self.assertEqual(p.get(), b'OK')
        self.assertEqual(p.get(), b'QUEUED')
        self.assertEqual(p.get(), b'QUEUED')
        self.assertEqual(p.get(), b'QUEUED')
        self.assertEqual(p.get(), [None, 1, 39])

    def test_nested10(self):
        client = self.client()
        result = yield client.eval(lua_nested_table, 0, 10)
        self.assertEqual(len(result), 4)

    def test_nested2(self):
        client = self.client()
        result = yield client.eval(lua_nested_table, 0, 2)
        self.assertEqual(len(result), 4)

    def test_empty_string(self):
        client = self.client()
        yield client.set('ghghg', '')
        result = yield client.get('ghghg')
        self.assertEqual(result, b'')


@unittest.skipUnless(client.HAS_C_EXTENSIONS , 'Requires cython extensions')
class TestPythonParser(client.PythonParser, TestParser):
    pass
