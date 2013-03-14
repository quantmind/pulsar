from random import randint
import struct

from pulsar import ProtocolError
from pulsar.apps.test import unittest
from pulsar.utils.websocket import Frame, int2bytes, i2b, FrameParser
import pulsar.apps.ws


class FrameTest(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        cls.bdata = b''.join((i2b(randint(0,255)) for v in range(256)))
        SIZE = 64*1024
        cls.large_bdata = b''.join((i2b(randint(0,255)) for v in range(SIZE)))
        
    def testBadFrame(self):
        self.assertRaises(ProtocolError, Frame, version=809)
        self.assertRaises(ProtocolError, Frame, opcode=56)
        self.assertRaises(ProtocolError, Frame)
        self.assertRaises(ProtocolError, Frame, 'bla',
                          masking_key='foo')
        self.assertRaises(ProtocolError, Frame, 'bla',
                          masking_key='fooox')
        
    def testControlFrames(self):
        parser = FrameParser()
        f = parser.close()
        self.assertEqual(f.opcode, 0x8)
        self.assertTrue(f.payload_length <= 125)
        f = parser.ping('Hello')
        self.assertEqual(f.opcode, 0x9)
        self.assertEqual(int2bytes(0x89,0x05,0x48,0x65,0x6c,0x6c,0x6f), f.msg)
        self.assertTrue(f.payload_length <= 125)
        r = parser.replay_to(f)
        self.assertTrue(r)
        self.assertEqual(r.opcode, 0xA)
        f = parser.pong()
        self.assertEqual(f.opcode, 0xA)
        self.assertTrue(f.payload_length <= 125)
        
    def testUnmaskedDataFrame(self):
        parser = FrameParser(kind=2)
        f = parser.encode('Hello')
        self.assertEqual(f.opcode, 0x1)
        self.assertEqual(f.payload_length, 5)
        self.assertFalse(f.masked)
        self.assertEqual(len(f.msg), 7)
        self.assertEqual(int2bytes(0x81,0x05,0x48,0x65,0x6c,0x6c,0x6f), f.msg)
        f1 = parser.encode('Hel', final=False)
        f2 = parser.continuation('lo', final=True)
        self.assertEqual(int2bytes(0x01,0x03,0x48,0x65,0x6c), f1.msg)
        self.assertEqual(int2bytes(0x80,0x02,0x6c,0x6f), f2.msg)
        
    def testBinaryDataFrame(self):
        f = Frame(self.bdata, opcode=0x2, final=True)
        self.assertEqual(f.opcode, 0x2)
        self.assertEqual(f.payload_length, 256)
        self.assertFalse(f.masked)
        self.assertEqual(len(f.msg), 260)
        self.assertEqual(struct.pack('!BBH',0x82,0x7E,0x0100), f.msg[:4])
        f = Frame(self.large_bdata, opcode=0x2, final=True)
        self.assertEqual(f.opcode, 0x2)
        self.assertEqual(f.payload_length, len(self.large_bdata))
        self.assertFalse(f.masked)
        self.assertEqual(len(f.msg), len(self.large_bdata)+10)
        self.assertEqual(struct.pack('!BBQ',0x82,0x7F,0x0000000000010000),
                         f.msg[:10])
            
    def testMaskData(self):
        masking_key = int2bytes(0x37,0xfa,0x21,0x3d)
        f = Frame('Hello', masking_key=masking_key, final=True)
        self.assertTrue(f.final)
        msg = int2bytes(0x81,0x85,0x37,0xfa,0x21,0x3d,0x7f,0x9f,0x4d,0x51,0x58)
        self.assertTrue(f.masked)
        self.assertEqual(msg, f.msg)
        
    def testParser(self):
        p = FrameParser()
        self.assertEqual(p.decode(b''), None)
        frame = Frame('Hello', final=True)
        self.assertRaises(ProtocolError, p.decode, frame.msg)
        frame = Frame('Hello', masking_key='ciao', final=True)
        pframe = p.decode(frame.msg)
        self.assertTrue(pframe)
        self.assertEqual(pframe.body, 'Hello')
        self.assertEqual(pframe.payload_length, 5)
        self.assertEqual(pframe.masking_key, b'ciao')
    
    def testParserBinary(self):
        p = FrameParser()
        frame = Frame(self.bdata, opcode=0x2, final=True, masking_key='ciao')
        pframe = p.decode(frame.msg)
        self.assertTrue(pframe)
        self.assertEqual(pframe.payload_length, 256)
        self.assertEqual(pframe.body, self.bdata)
        frame = Frame(self.large_bdata, opcode=0x2, final=True,
                      masking_key='ciao')
        pframe = p.decode(frame.msg)
        self.assertTrue(pframe)
        self.assertEqual(pframe.payload_length, len(self.large_bdata))
        self.assertEqual(pframe.body, self.large_bdata)
        
    def testPartialParsing(self):
        p = FrameParser()
        frame = Frame(self.large_bdata, opcode=0x2, final=True,
                      masking_key='ciao')
        self.assertEqual(p.decode(frame.msg[:1]), None)
        self.assertEqual(p.decode(frame.msg[1:5]), None)
        self.assertEqual(p.decode(frame.msg[5:50]), None)
        pframe = p.decode(frame.msg[50:])
        self.assertTrue(pframe)
        self.assertEqual(pframe.payload_length, len(self.large_bdata))
        self.assertEqual(pframe.body, self.large_bdata)
        
        
class Extensions(unittest.TestCase):
    
    def testDeflate(self):
        parser = FrameParser(extensions=['x-webkit-deflate-frame'])
        pass