from random import randint
import unittest

import pulsar
from pulsar.apps.ds import (redis_parser, ResponseError, NoScriptError,
                            InvalidResponse)


def lua_nested_table(nesting, s=100):
    s = b''.join((b'1234567890' for n in range(s)))
    pres = [100, s]
    result = pres
    for i in range(nesting):
        res = [-8, s, None]
        # dres = {1: 'valid', 'bla': 6}
        dres = 1
        pres.extend((res, res, dres))
        pres = res
    return result


class TestParser(unittest.TestCase):

    def parser(self):
        return redis_parser()()

    #    DECODER
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
        test = (b'$31\r\n\x80\x02]q\x00(X\x04\x00\x00\x00ciaoq\x01X\x05\x00'
                b'\x00\x00pippoq\x02e.\r\n')
        p = self.parser()
        p.feed(test)
        self.assertEqual(p.buffer(), test)
        value = p.get()
        self.assertTrue(value)
        self.assertEqual(p.buffer(), b'')

    def test_multi(self):
        test = (b'+OK\r\n+QUEUED\r\n+QUEUED\r\n+QUEUED\r\n*3\r\n$-1\r\n:1'
                b'\r\n:39\r\n')
        p = self.parser()
        p.feed(test)
        self.assertEqual(p.get(), b'OK')
        self.assertEqual(p.get(), b'QUEUED')
        self.assertEqual(p.get(), b'QUEUED')
        self.assertEqual(p.get(), b'QUEUED')
        self.assertEqual(p.get(), [None, 1, 39])

    def test_nested(self):
        p = self.parser()
        result = lua_nested_table(2)
        chunk = p.multi_bulk(result)
        p.feed(chunk)
        res2 = p.get()
        self.assertEqual(len(res2), len(result))
        self.assertEqual(res2[0], b'100')
        self.assertEqual(res2[1], result[1])

    def test_nested_bug_fixer(self):
        p = self.parser()
        result = [b'100',
                  b'1234567890',
                  [b'-8', b'1234567890', None],
                  [b'-8', b'1234567890', None],
                  b'1']
        data = p.multi_bulk(result)
        chunks = [b'*5\r\n$3\r\n100',
                  b'\r\n$10\r\n12345',
                  b'67890\r\n*3',
                  b'\r\n$2\r',
                  b'\n-8\r\n$10\r',
                  b'\n1234567890\r\n$-1\r\n',
                  b'*3\r\n$2\r\n-8\r\n$10\r\n1234567890\r\n$-1\r\n$1\r\n1\r\n']
        self.assertEqual(data, b''.join(chunks))
        for chunk in chunks[:-1]:
            p.feed(chunk)
            self.assertEqual(p.get(), False)
        p.feed(chunks[-1])
        self.assertEqual(p.get(), result)

    def test_nested_a_drop_at_a_time(self):
        p = self.parser()
        result = lua_nested_table(3)
        data = p.multi_bulk(result)
        while data:
            self.assertEqual(p.get(), False)
            i = randint(1, 30)
            chunk, data = data[:i], data[i:]
            p.feed(chunk)
        res2 = p.get()
        self.assertEqual(len(res2), len(result))
        self.assertEqual(res2[0], b'100')
        self.assertEqual(res2[1], result[1])

    # CLIENT ENCODERS
    def test_encode_commands(self):
        p = self.parser()
        chunk = p.pack_command(['whatever', None])
        self.assertEqual(chunk, b'*2\r\n$8\r\nwhatever\r\n$4\r\nNone\r\n')

    # SERVER ENCODERS

    def test_encode_empty_string(self):
        p = self.parser()
        chunk = p.bulk(None)
        self.assertEqual(chunk, b'$-1\r\n')

    def test_encode_multi_bulk(self):
        p = self.parser()
        self.assertEqual(p.multi_bulk([]), b'*0\r\n')
        self.assertEqual(p.multi_bulk(()), b'*0\r\n')


@unittest.skipUnless(pulsar.HAS_C_EXTENSIONS, 'Requires C extensions')
class TestPythonParser(TestParser):

    def parser(self):
        return redis_parser(True)()
