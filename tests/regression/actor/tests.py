from time import sleep
from pulsar import test, spawn, Actor, ActorProxy, ActorRequest


#__all__ = ['TestActorThread']
__all__ = ['TestActorThread',
           'TestActorProcess']

def sleepfunc():
    sleep(2)
    

class TestActorThread(test.TestCase):
    impl = 'thread'
    
    def testStop(self):
        a = spawn(Actor,impl = self.impl)
        self.assertTrue(isinstance(a,ActorProxy))
        self.assertTrue(a.is_alive())
        self.assertEqual(a.impl.impl,self.impl)
        self.assertTrue(a.aid in self.arbiter.LIVE_ACTORS)
        self.stop(a)
        
    def testPing(self):
        a = spawn(Actor, impl = self.impl)
        cbk = self.Callback()
        r = self.arbiter.proxy.ping(a).add_callback(cbk)
        self.wait(lambda : not hasattr(cbk,'result'))
        self.assertEqual(cbk.result,'pong')
        self.assertFalse(r.rid in ActorRequest.REQUESTS)
        self.stop(a)
        
    def testInfo(self):
        a = spawn(Actor, impl = self.impl)
        cbk = self.Callback()
        r = self.arbiter.proxy.info(a).add_callback(cbk)
        self.wait(lambda : not hasattr(cbk,'result'))
        self.assertFalse(r.rid in ActorRequest.REQUESTS)
        info = cbk.result
        self.assertEqual(info['aid'],a.aid)
        self.assertEqual(info['pid'],a.pid)
        self.stop(a)
        
    def testSpawnFew(self):
        actors = (spawn(Actor, impl = self.impl) for i in range(10))
        for a in actors:
            self.assertTrue(a.aid in self.arbiter.LIVE_ACTORS)
            cbk = self.Callback()
            r = self.arbiter.proxy.ping(a).add_callback(cbk)
            self.wait(lambda : not hasattr(cbk,'result'))
            self.assertEqual(cbk.result,'pong')
            self.assertFalse(r.rid in ActorRequest.REQUESTS)
                
    def testTimeout(self):
        a = spawn(Actor, on_task = sleepfunc, impl = self.impl, timeout = 1)
        self.assertTrue(a.aid in self.arbiter.LIVE_ACTORS)
        self.wait(lambda : a.aid in self.arbiter.LIVE_ACTORS, timeout = 3)
        self.assertFalse(a.aid in self.arbiter.LIVE_ACTORS)
        

class TestActorProcess(TestActorThread):
    impl = 'process'        

