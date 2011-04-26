from time import sleep
from pulsar import test, spawn, Actor, ActorProxy, arbiter, ActorRequest


__all__ = ['TestActorThread']

    

class TestActorThread(test.TestCase):
    impl = 'process'
    
    def testStop(self):
        a = spawn(Actor,impl = self.impl)
        self.assertTrue(isinstance(a,ActorProxy))
        self.assertTrue(a.is_alive())
        self.assertEqual(a.actor.impl,self.impl)
        arb = arbiter()
        self.assertTrue(a.aid in arb.LIVE_ACTORS)
        a.stop()
        self.wait(lambda : not self.actor_exited(a))
        self.assertFalse(a.aid in arb.LIVE_ACTORS)
        
    def testPing(self):
        self.log.info('Create a new actor to ping')
        a = spawn(Actor, impl = self.impl)
        self.log.info('Get the arbiter')
        arb = self.arbiter()
        cbk = self.Callback()
        self.log.info('Pinging')
        r = arb.ping(a).add_callback(cbk)
        self.wait(lambda : not hasattr(cbk,'result'))
        self.assertEqual(cbk.result,'pong')
        self.assertFalse(r.rid in ActorRequest.REQUESTS)
        
    def _testTimeout(self):
        arb = arbiter()
        a = spawn(Actor, on_task = lambda : sleep(2), impl = self.impl, timeout = 1)
        self.assertTrue(a.aid in arb.LIVE_ACTORS)
        sleep(3)
        self.assertFalse(a.aid in arb.LIVE_ACTORS)        
        
    def _testInfo(self):
        #a = spawn(Actor,impl = self.impl)
        pass
        #ap = arb.proxy()
        #actor_info = ap.info(a)
        #self.sleep(2)
        

class TestActorProcess(TestActorThread):
    impl = 'process'        

