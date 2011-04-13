from time import sleep
from threading import Thread
from multiprocessing import Process, Queue
from multiprocessing.queues import Empty

from pulsar import test
from pulsar.utils.eventloop import IOLoop


class IOLoopThread(Thread):
    
    def __init__(self, io):
        Thread.__init__(self)
        self.io = io
        
    def run(self):
        self.io.start()
        

class IOLoopProcess(Process):
        
    def __init__(self):
        super(IOLoopProcess,self).__init__()
        self.commands = Queue()
        
    def check_commands(self):
        while True:
            try:
                c = self.commands.get(timeout = 0.01)
            except Empty:
                break
            if c == 'STOP':
                self.commands.close()
                self.io.stop()
                break
            
    def stop(self):
        self.commands.put('STOP')
        
    def run(self):
        self.io = IOLoop()
        self.io.add_loop_task(self.check_commands)
        self.io.start()
        

dummy_handler = lambda : None

class TestIOLoop(test.TestCase):
    
    def create(self):
        io = IOLoop()
        self.assertFalse(io.running())
        self.assertFalse(io.stopped())
        return io
    
    def testCreate(self):
        self.create()
        
    def testFunctions(self):
        io = self.create()
        io.add_handler(None, dummy_handler, IOLoop.READ)
        
    def testStartStopInThread(self):
        io = self.create()
        tio = IOLoopThread(io)
        tio.start()
        sleep(0.1)
        io.start()
        self.assertTrue(io.running())
        self.assertFalse(io.stopped())
        io.stop()
        sleep(0.1)
        self.assertFalse(io.running())
        self.assertFalse(io.stopped())
        
    def testStartStopInProcess(self):
        pio = IOLoopProcess()
        self.assertFalse(pio.is_alive())
        pio.start()
        sleep(0.1)
        self.assertTrue(pio.is_alive())
        pio.stop()
        sleep(0.5)
        self.assertFalse(pio.is_alive())
