'''Tests the websocket middleware in pulsar.apps.ws.'''
from random import randint
import struct

from pulsar import send, HttpClient
from pulsar.apps.ws import Frame, WebSocket, int2bytes, i2b,\
                            WebSocketProtocolError, FrameParser
from pulsar.apps.test import unittest, dont_run_with_thread

from .manage import server


class WebSocketThreadTest(unittest.TestCase):
    app = None
    concurrency = 'thread'
    
    @classmethod
    def name(cls):
        return 'websocket_' + cls.concurrency
    
    @classmethod
    def setUpClass(cls):
        s = server(bind='127.0.0.1:0', name=cls.name(),
                   concurrency=cls.concurrency)
        outcome = send('arbiter', 'run', s)
        yield outcome
        cls.app = outcome.result
        cls.uri = 'http://{0}:{1}'.format(*cls.app.address)
        cls.ws_uri = 'ws://{0}:{1}/data'.format(*cls.app.address)
        
    @classmethod
    def tearDownClass(cls):
        if cls.app is not None:
            outcome = send('arbiter', 'kill_actor', cls.app.mid)
            yield outcome
    
    def headers(self, extensions=None, protocol=None):
        headers = Headers((('upgrade', 'websocket'),
                           ('sec-websocket-key', 'E3qAXpIDEniWJd59VoBALQ=='),
                           ('sec-websocket-version', '13')))
        if extensions:
            headers['sec-websocket-extensions'] = extensions
        if extensions:
            headers['sec-websocket-protocol'] = protocol
        return headers
    
    def testHyBiKey(self):
        w = WebSocket(None)
        v = w.challenge_response('dGhlIHNhbXBsZSBub25jZQ==')
        self.assertEqual(v, "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=")
        
    def testUpgrade(self):
        c = HttpClient()
        outcome = c.get(self.ws_uri)
        yield outcome
        response = outcome.result
        self.assertEqual(response.status_code, 101)
        #
        outcome = c.post(self.ws_uri)
        yield outcome
        response = outcome.result
        self.assertEqual(response.status_code, 400)
        #
        outcome = c.get(self.ws_uri, headers=[('Sec-Websocket-Key','')])
        yield outcome
        response = outcome.result
        self.assertEqual(response.status_code, 400)
        #
        outcome = c.get(self.ws_uri, headers=[('Sec-Websocket-Key','bla')])
        yield outcome
        response = outcome.result
        self.assertEqual(response.status_code, 400)
        #
        outcome = c.get(self.ws_uri, headers=[('Sec-Websocket-version','xxx')])
        yield outcome
        response = outcome.result
        self.assertEqual(response.status_code, 400)
    

@dont_run_with_thread
class WebSocketProcessTest(WebSocketThreadTest):
    concurrency = 'process'
    
    
class FrameTest(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        cls.bdata = b''.join((i2b(randint(0,255)) for v in range(256)))
        SIZE = 64*1024
        cls.large_bdata = b''.join((i2b(randint(0,255)) for v in range(SIZE)))
        
    def testBadFrame(self):
        self.assertRaises(WebSocketProtocolError, Frame, version=809)
        self.assertRaises(WebSocketProtocolError, Frame, opcode=56)
        self.assertRaises(WebSocketProtocolError, Frame)
        self.assertRaises(WebSocketProtocolError, Frame, 'bla',
                          masking_key='foo')
        self.assertRaises(WebSocketProtocolError, Frame, 'bla',
                          masking_key='fooox')
        
    def testControlFrames(self):
        f = Frame.close()
        self.assertEqual(f.opcode, 0x8)
        self.assertTrue(f.payload_length <= 125)
        f = Frame.ping('Hello')
        self.assertEqual(f.opcode, 0x9)
        self.assertEqual(int2bytes(0x89,0x05,0x48,0x65,0x6c,0x6c,0x6f), f.msg)
        self.assertTrue(f.payload_length <= 125)
        r = f.on_received()
        self.assertTrue(r)
        self.assertEqual(r.opcode, 0xA)
        f = Frame.pong()
        self.assertEqual(f.opcode, 0xA)
        self.assertTrue(f.payload_length <= 125)
        
    def testUnmaskedDataFrame(self):
        f = Frame('Hello', final=True)
        self.assertEqual(f.opcode, 0x1)
        self.assertEqual(f.payload_length, 5)
        self.assertFalse(f.masked)
        self.assertEqual(len(f.msg), 7)
        self.assertEqual(int2bytes(0x81,0x05,0x48,0x65,0x6c,0x6c,0x6f), f.msg)
        f1 = Frame('Hel')
        f2 = Frame.continuation('lo', final=True)
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
        self.assertEqual(p.execute(b''), None)
        frame = Frame('Hello', final=True)
        self.assertRaises(WebSocketProtocolError, p.execute, frame.msg)
        frame = Frame('Hello', masking_key='ciao', final=True)
        pframe = p.execute(frame.msg)
        self.assertTrue(pframe)
        self.assertEqual(pframe.body, 'Hello')
        self.assertEqual(pframe.payload_length, 5)
        self.assertEqual(pframe.masking_key, b'ciao')
    
    def testParserBinary(self):
        p = FrameParser()
        frame = Frame(self.bdata, opcode=0x2, final=True, masking_key='ciao')
        pframe = p.execute(frame.msg)
        self.assertTrue(pframe)
        self.assertEqual(pframe.payload_length, 256)
        self.assertEqual(pframe.body, self.bdata)
        frame = Frame(self.large_bdata, opcode=0x2, final=True,
                      masking_key='ciao')
        pframe = p.execute(frame.msg)
        self.assertTrue(pframe)
        self.assertEqual(pframe.payload_length, len(self.large_bdata))
        self.assertEqual(pframe.body, self.large_bdata)
        
    def testPartialParsing(self):
        p = FrameParser()
        frame = Frame(self.large_bdata, opcode=0x2, final=True,
                      masking_key='ciao')
        self.assertEqual(p.execute(frame.msg[:1]), None)
        self.assertEqual(p.execute(frame.msg[1:5]), None)
        self.assertEqual(p.execute(frame.msg[5:50]), None)
        pframe = p.execute(frame.msg[50:])
        self.assertTrue(pframe)
        self.assertEqual(pframe.payload_length, len(self.large_bdata))
        self.assertEqual(pframe.body, self.large_bdata)