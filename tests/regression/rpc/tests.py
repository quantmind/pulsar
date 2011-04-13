import inspect

from pulsar import test

from .testsite import handler


class TestRpcHandler(test.TestCase):
    
    def setUp(self):
        self.handler = handler()
        
    def testFunctions(self):
        h = self.handler
        f = h.listFunctions()
        self.assertEqual(len(f),1)
        self.assertEqual(len(h.subHandlers),2)
        ping = h['ping']
        self.assertTrue(isinstance(ping,object))
        self.assertEqual(ping.name,'ping')
        self.assertEqual(ping(None),'pong')
        add = h['calc.add']
        self.assertTrue(isinstance(add,object))
        self.assertEqual(add.name,'add')
        self.assertEqual(add(None,3,5),8)