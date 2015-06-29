from random import randint
import struct
import unittest

from pulsar import ProtocolError, HAS_C_EXTENSIONS
from pulsar.utils.websocket import frame_parser, parse_close


def i2b(args):
    return bytes(bytearray(args))


class FrameTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.bdata = i2b((randint(0, 255) for v in range(256)))
        cls.large_bdata = i2b((randint(0, 255) for v in range(64*1024)))

    def parser(self, pyparser=False, **kw):
        return frame_parser(**kw)

    def test_version(self):
        self.assertRaises(ProtocolError, self.parser, version='bla')

    def test_server(self):
        server = self.parser()
        self.assertEqual(server.decode_mask_length, 4)
        self.assertEqual(server.encode_mask_length, 0)
        self.assertEqual(server.max_payload, 1 << 63)

    def test_both_masked(self):
        server = self.parser(kind=2)
        self.assertEqual(server.decode_mask_length, 0)
        self.assertEqual(server.encode_mask_length, 0)
        server = self.parser(kind=3)
        self.assertEqual(server.decode_mask_length, 4)
        self.assertEqual(server.encode_mask_length, 4)

    def testCloseFrame(self):
        parser = self.parser(kind=2)
        close_message = struct.pack('!H', 1000) + b'OK'
        f = parser.encode(close_message, opcode=0x8)
        self.assertEqual(close_message, f[2:])

    def testControlFrames(self):
        s = self.parser()
        c = self.parser(kind=1)
        #
        chunk = s.close(1001)
        frame = c.decode(chunk)
        self.assertTrue(frame.final)
        self.assertEqual(frame.opcode, 8)
        code, reason = parse_close(frame.body)
        self.assertEqual(code, 1001)
        #
        chunk = s.ping('Hello')
        frame = c.decode(chunk)
        self.assertTrue(frame.final)
        self.assertEqual(frame.opcode, 9)
        self.assertEqual(i2b((0x89, 0x05, 0x48, 0x65, 0x6c, 0x6c, 0x6f)),
                         chunk)
        self.assertEqual(frame.body, b'Hello')
        self.assertRaises(ProtocolError, s.ping, self.bdata)
        #
        chunk = s.pong('Hello')
        frame = c.decode(chunk)
        self.assertTrue(frame.final)
        self.assertEqual(frame.opcode, 10)
        self.assertEqual(frame.body, b'Hello')
        self.assertRaises(ProtocolError, s.pong, self.bdata)

    def test_conntrol_frames_fragmented(self):
        c = self.parser(kind=1)
        for opcode in (8, 9, 10):
            chunk = c.encode('test', opcode=opcode, final=False)
            s = self.parser()
            try:
                s.decode(chunk)
            except ProtocolError:
                pass
            else:
                raise Exception('Protocol error not raised')

    def testUnmaskedDataFrame(self):
        parser = self.parser(kind=2)
        data = parser.encode('Hello')
        f = parser.decode(data)
        self.assertEqual(f.opcode, 1)
        self.assertEqual(len(f.body), 5)
        self.assertFalse(f.masking_key)
        #
        self.assertEqual(i2b((0x81, 0x05, 0x48, 0x65, 0x6c, 0x6c, 0x6f)), data)
        f1 = parser.encode('Hel', final=False)
        f2 = parser.continuation('lo', final=True)
        self.assertEqual(i2b((0x01, 0x03, 0x48, 0x65, 0x6c)), f1)
        self.assertEqual(i2b((0x80, 0x02, 0x6c, 0x6f)), f2)

    def testBinaryDataFrame(self):
        s = self.parser()
        c = self.parser(kind=1)
        #
        chunk = s.encode(self.bdata, opcode=2)
        frame = c.decode(chunk)
        self.assertEqual(frame.opcode, 2)
        self.assertFalse(frame.masking_key)
        self.assertEqual(frame.body, self.bdata)
        self.assertEqual(struct.pack('!BBH', 0x82, 0x7E, 0x0100),
                         chunk[:4])
        #
        chunk = s.encode(self.large_bdata, opcode=2)
        frame = c.decode(chunk)
        self.assertEqual(frame.opcode, 2)
        self.assertFalse(frame.masking_key)
        self.assertEqual(frame.body, self.large_bdata)
        self.assertEqual(struct.pack('!BBQ', 0x82, 0x7F, 0x0000000000010000),
                         chunk[:10])

    def testMaskData(self):
        client = self.parser(kind=1)
        masking_key = i2b((0x37, 0xfa, 0x21, 0x3d))
        chunk = client.encode('Hello', masking_key=masking_key)
        msg = i2b((0x81, 0x85, 0x37, 0xfa, 0x21, 0x3d, 0x7f, 0x9f,
                   0x4d, 0x51, 0x58))
        self.assertEqual(chunk, msg)

    def testParserBinary(self):
        s = self.parser()
        c = self.parser(kind=1)
        chunk = c.encode(self.bdata, opcode=2)
        frame = s.decode(chunk)
        self.assertTrue(frame)
        self.assertEqual(frame.body, self.bdata)
        #
        # Now try different masking key
        chunk = c.encode(self.large_bdata, opcode=2, masking_key=b'ciao')
        frame = s.decode(chunk)
        self.assertTrue(frame)
        self.assertEqual(frame.body, self.large_bdata)

    def testPartialParsing(self):
        s = self.parser()
        c = self.parser(kind=1)
        chunk = s.encode(self.large_bdata, opcode=2)
        #
        self.assertEqual(c.decode(chunk[:1]), None)
        self.assertEqual(c.decode(chunk[1:5]), None)
        self.assertEqual(c.decode(chunk[5:50]), None)
        frame = c.decode(chunk[50:])
        self.assertTrue(frame)
        self.assertEqual(frame.body, self.large_bdata)
        self.assertEqual(frame.opcode, 2)

    def test_multi_encode(self):
        s = self.parser()
        c = self.parser(kind=1)
        chunks = list(s.multi_encode(self.large_bdata, opcode=2,
                                     max_payload=6500))
        self.assertEqual(len(chunks), 11)
        #
        # Now decode them
        frames = []
        for chunk in chunks:
            frames.append(c.decode(chunk))
        for frame in frames[:-1]:
            self.assertFalse(frame.final)
        self.assertTrue(frames[-1].final)
        msg = b''.join((f.body for f in frames))
        self.assertEqual(msg, self.large_bdata)

    def test_bad_mask(self):
        s = self.parser()
        chunk = s.encode('hello')
        self.assertRaises(ProtocolError, s.decode, chunk)
        #
        # and the client
        c = self.parser(kind=1)
        chunk = c.encode('hello')
        self.assertRaises(ProtocolError, c.decode, chunk)

    def test_symmetric_mask(self):
        s = self.parser(kind=2)
        chunk = s.encode('Hello')
        self.assertEqual(s.decode(chunk).body, 'Hello')
        s = self.parser(kind=3)
        chunk = s.encode('Hello')
        self.assertEqual(s.decode(chunk).body, 'Hello')

    def test_parse_close(self):
        self.assertRaises(ProtocolError, parse_close, b'o')


@unittest.skipUnless(HAS_C_EXTENSIONS, "Requires C extensions")
class PyFrameTest(FrameTest):

    def parser(self, pyparser=True, **kw):
        return frame_parser(pyparser=True, **kw)

    def test_parsers(self):
        import pulsar.utils.websocket as ws
        self.assertNotEqual(ws.CFrameParser, ws.FrameParser)
