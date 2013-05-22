import time

import pulsar
from pulsar.apps.test import unittest, run_on_arbiter
from pulsar.apps.shell import InteractiveConsole, decode_line, PulsarShell


class DummyConsole(InteractiveConsole):
    
    def setup(self):
        pass
    
    def interact(self, timeout):
        time.sleep(timeout)
        
def start(actor):
    return PulsarShell(console_class=DummyConsole, workers=2).start()
    

class TestShell(unittest.TestCase):
    app = None
    @classmethod
    def setUpClass(cls):
        cls.app = yield pulsar.send('arbiter', 'run', start)
        
    @classmethod
    def tearDownClass(cls):
        if cls.app is not None:
            yield pulsar.send('arbiter', 'kill_actor', cls.app.name)
            
    def testApp(self):
        app = self.app
        self.assertEqual(app.name, 'shell')
        self.assertEqual(app.callable, None)
        self.assertEqual(app.cfg.console_class, DummyConsole)
        #self.assertEqual(app.cfg.workers, 1)
        #self.assertEqual(app.cfg.concurrency, 'thread')
        self.assertEqual(decode_line('bla'), 'bla')
        
    @run_on_arbiter
    def testTestWorker(self):
        arbiter = pulsar.get_actor()
        monitor = arbiter.get_actor('shell')
        while not monitor.managed_actors:
            yield pulsar.NOT_DONE
        self.assertEqual(len(monitor.managed_actors), 1)