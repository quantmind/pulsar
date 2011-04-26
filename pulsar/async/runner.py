from multiprocessing import Process, current_process
from multiprocessing.queues import Queue
from threading import current_thread, Thread

from .eventloop import IOLoop


__all__ = ['Runner']

_main_thread = current_thread()


class Runner(object):
    '''Base class for classes with an event loop.
    '''
    DEF_PROC_NAME = 'pulsar'
    SIG_QUEUE = None
    
    def init_runner(self):
        '''Initialise the runner. This function
will block the current thread since it enters the event loop.
If the runner is a instance of a subprocess, this function
is called after fork by the run method.'''
        self.set_proctitle()
        self.setup()
        self.install_signals()
        
    def get_eventloop(self):
        return IOLoop(impl = self.get_ioimpl(), logger = pulsar.LogSelf(self,self.log))
        
    def get_ioimpl(self):
        '''Return the event-loop implementation. By default it returns ``None``.'''
        return None
    
    @property
    def started(self):
        if hasattr(self,'ioloop'):
            return self.ioloop._started
        
    def set_proctitle(self):
        '''Set the process title'''
        if self.isthread and hasattr(self,'cfg'):
            proc_name = self.cfg.proc_name or self.cfg.default_proc_name
            if proc_name:
                system.set_proctitle("{0} - {1}".format(proc_name,self))
        
    def current_thread(self):
        '''Return the current thread'''
        return current_thread()
    
    def current_process(self):
        return current_process()
    
    def install_signals(self):
        '''Initialise signals for correct signal handling.'''
        current = self.current_thread()
        if current == _main_thread and not self.isthread:
            self.log.info('Installing signals')
            sfun = getattr(self,'signal',None)
            for name in system.ALL_SIGNALS:
                func = getattr(self,'handle_{0}'.format(name.lower()),sfun)
                if func:
                    sig = getattr(signal,'SIG{0}'.format(name))
                    signal.signal(sig, func)
    
    def setup(self):
        pass
    
    def _run(self):
        """\
        This is the mainloop of a worker process. You should override
        this method in a subclass to provide the intended behaviour
        for your particular evil schemes.
        """
        raise NotImplementedError()
    
    @property
    def tid(self):
        '''Thread Name'''
        if self.isthread:
            return self.name
        else:
            return current_thread().name
    
    @property
    def isthread(self):
        return isinstance(self,Thread)

