import time
import os
import sys
import traceback
import signal
from multiprocessing.queues import Empty

import pulsar
from pulsar.utils import system

from .base import Runner, ThreadQueue
from .workerpool import WorkerPool


__all__ = ['Arbiter']


class Arbiter(Runner):
    '''An Arbiter is an object which controls pools of workers'''
    CLOSE_TIMEOUT = 3
    WORKER_BOOT_ERROR = 3
    SIG_TIMEOUT = 0.001
    EXIT_SIGNALS = (signal.SIGINT,signal.SIGTERM,signal.SIGABRT,system.SIGQUIT)
    HaltServer = pulsar.HaltServer
    
    def __init__(self, app):
        self.pid = None
        self.socket = None
        self.app = app
        self.cfg = app.cfg
        self.pidfile = None
        self.reexec_pid = 0
        self._pools = []
        self.address = self.cfg.address
        self.debug = self.cfg.debug
        # Create the listener if not available
        if not self.socket and self.address:
            self.socket = system.create_socket(self)
        self.log = self.getLogger()
        self.ioloop = self.get_eventloop()
        self.ioloop.add_loop_task(self)
        self.SIG_QUEUE = ThreadQueue()
    
    # HIGH LEVEL API    
    def start(self):
        '''Start the Arbiter. The arbiter should be started in the main process
in the main thread'''
        self.run()
    
    def shut_down(self):
        '''Orderly shut down the arbiter'''
        self.signal(system.SIGQUIT)
    
    def is_alive(self):
        '''``True`` if the arbiter is running.'''
        return self.ioloop.running()
    
    # LOW LEVEL FUNCTIONS
    def __repr__(self):
        return self.__class__.__name__
    
    def __str__(self):
        return self.__repr__()
    
    def setup(self):
        self.addpool(self.cfg, self.socket)
    
    def __call__(self):
        sig = self.arbiter()
        if sig is None:
            for pool in self._pools:
                pool.arbiter_task()
        
    def addpool(self, cfg, socket = None, start = False):
        worker_class = cfg.worker_class
        pool = WorkerPool(self,
                          worker_class,
                          cfg.workers,
                          app = self.app,
                          timeout = cfg.timeout,
                          socket = socket)
        self._pools.append(pool)
        if start:
            pool.start()
    
    def make_pidfile(self):
        pass
        
    def run(self):
        self.init_process()
        
    def _run(self):
        """\
        Initialize the arbiter. Start listening and set pidfile if needed.
        """
        ioloop = self.ioloop
        self.pid = os.getpid()
        self.make_pidfile()
        self.log.debug("{0} booted".format(self))
        if self.socket:
            self.log.info("Listening at: %s (%s)" % (self.socket, self.pid))
        if self.cfg.when_ready:
            self.cfg.when_ready(self)
        try:
            ioloop.start()
        except StopIteration:
            self.halt('Stop Iteration')
        except KeyboardInterrupt:
            self.halt('Keyboard Interrupt')
        except self.HaltServer as e:
            self.halt(reason=e.reason, sig=e.signal)
        except SystemExit:
            raise
        except Exception:
            self.halt("Unhandled exception in main loop:\n%s" % traceback.format_exc())
    
    def halt(self, reason=None, sig=None):
        """ halt arbiter """
        _msg = lambda x : x if not reason else '{0}: {1}'.format(x,reason)
        
        if sig:
            msg = _msg('Shutting down')
            self.close()
            status = 0
        else:
            msg = _msg('Force termination')
            status = 1
        if self.pidfile is not None:
            self.pidfile.unlink()
        self.terminate()
        self.log.critical(msg)
        sys.exit(status)
    
    def is_alive(self):
        return self.ioloop.running()
    
    def stop(self):
        '''Alias of :meth:`close`'''
        self.close()
        
    def close(self):
        '''Stop the pools and the arbiter event loop.'''
        if self._pools:
            for pool in self._pools:
                pool.close()
                
            #timeout = self.CLOSE_TIMEOUT / float(len(self._pools))
            timeout = self.CLOSE_TIMEOUT
            for pool in self._pools:
                pool.join(timeout)
    
    def terminate(self):
        '''Force termination of pools and close arbiter event loop'''
        for pool in self._pools:
            pool.terminate()
        
    def server_info(self):
        started = self.started
        if not started:
            return
        uptime = time.time() - started
        server = {'uptime':uptime,
                  'version':pulsar.__version__,
                  'name':pulsar.SERVER_NAME,
                  'number_of_pools':len(self._pools),
                  'event_loops':self.ioloop.num_loops,
                  'socket':str(self.socket)}
        pools = []
        for p in self._pools:
            pools.append(p.info())
        return {'server':server,
                'pools':pools}

    def arbiter(self):
        '''Called by the Event loop to perform the Arbiter tasks'''
        sig = None
        while True:
            try:
                sig = self.SIG_QUEUE.get(timeout = self.SIG_TIMEOUT)
            except Empty:
                sig = None
                break
            except IOError:
                sig = None
                break
            if sig not in system.SIG_NAMES:
                self.log.info("Ignoring unknown signal: %s" % sig)
                sig = None
            else:        
                signame = system.SIG_NAMES.get(sig)
                if sig in self.EXIT_SIGNALS:
                    raise self.HaltServer('Received Signal {0}.'.format(signame),sig)
                handler = getattr(self, "handle_queued_%s" % signame.lower(), None)
                if not handler:
                    self.log.critical('Cannot handle signal "{0}". No Handle'.format(signame))
                    sig = None
                else:
                    self.log.info("Handling signal: %s" % signame)
                    handler()
        return sig                
                
    def signal(self, sig, frame = None):
        signame = system.SIG_NAMES.get(sig,None)
        if signame:
            self.log.warn('Received and queueing signal {0}.'.format(signame))
            self.SIG_QUEUE.put(sig)
        else:
            self.log.info('Received unknown signal "{0}". Skipping.'.format(sig))
    
