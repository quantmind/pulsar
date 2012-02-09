import logging
import traceback
import time
import signal
import errno
import bisect
import socket

from pulsar import HaltServer
from pulsar.utils.system import IObase, IOpoll, close_on_exec, platform, Waker
from pulsar.utils.tools import gen_unique_id
from pulsar.utils.log import Synchronized
from pulsar.utils.collections import WeakList

from .defer import Deferred


__all__ = ['IOLoop']

def file_descriptor(fd):
    if hasattr(fd,'fileno'):
        return fd.fileno()
    else:
        return fd
        
        
class LoopGuard(object):
    
    def __init__(self, loop):
        self.loop = loop
        
    def __enter__(self):
        self.loop._running = True
        return self
        
    def __exit__(self, type, value, traceback):
        loop = self.loop
        loop._running = False
        loop._stopped = True
        loop._on_exit.callback(loop)
        
        
class IOLoop(IObase,Synchronized):
    """\
A level-triggered I/O event loop adapted from tornado.

:parameter io: The I/O implementation. If not supplied, the best possible
    implementation available will be used. On posix system this is ``epoll``,
    or else ``select``. It can be any other custom implementation as long as
    it has an ``epoll`` like interface. Pulsar ships with an additional
    I/O implementation based on distributed queue :class:`IOQueue`.
    
Example usage for a simple TCP server::

    import errno
    import functools
    import ioloop
    import socket

    def connection_ready(sock, fd, events):
        while True:
            try:
                connection, address = sock.accept()
            except socket.error, e:
                if e.args[0] not in (errno.EWOULDBLOCK, errno.EAGAIN):
                    raise
                return
            connection.setblocking(0)
            handle_connection(connection, address)

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.setblocking(0)
    sock.bind(("", port))
    sock.listen(128)

    io_loop = ioloop.IOLoop.instance()
    callback = functools.partial(connection_ready, sock)
    io_loop.add_handler(sock.fileno(), callback, io_loop.READ)
    io_loop.start()

When using the eventloop on a child process,
It should be instantiated after forking.

.. attribute:: num_lumps

    total number of loops
    
.. attribute:: POLL_TIMEOUT

    The timeout in seconds when polling with epol or select.
    
    Default: `0.5`
    """
    # Never use an infinite timeout here - it can stall epoll
    POLL_TIMEOUT = 0.5
    
    def __init__(self, io = None, logger = None,
                 pool_timeout = None, commnads = None,
                 name = None, ready = True):
        self._impl = io or IOpoll()
        self.POLL_TIMEOUT = pool_timeout if pool_timeout is not None\
                                 else self.POLL_TIMEOUT
        self.log = logger or logging.getLogger('ioloop')
        if hasattr(self._impl, 'fileno'):
            close_on_exec(self._impl.fileno())
        self._name = name
        self._handlers = {}
        self._events = {}
        self._callbacks = []
        self._timeouts = []
        self._loop_tasks = WeakList()
        self._started = None
        self._running = False
        self._stopped = False
        self.num_loops = 0
        self._blocking_signal_threshold = None
        self._waker = self.get_waker()
        self._on_exit = None
        self.ready = ready
        self.add_handler(self._waker,
                         lambda fd, events: self._waker.consume(),
                         self.READ)
        
    def __str__(self):
        if self.name:
            return '{0} - {1}'.format(self.__class__.__name__,self.name)
        else:
            return self.__class__.__name__
    
    def __repr__(self):
        return self.name or self.__class__.__name__
    
    @property
    def name(self):
        return self._name

    def get_waker(self):
        return getattr(self._impl,'waker',Waker)()
        
    def add_loop_task(self, task):
        '''Add a callable object to the list of tasks which are
executed at each iteration in the event loop.'''
        self._loop_tasks.append(task)
        
    def remove_loop_task(self, task):
        '''Remove the task from the list of tasks
executed at each iteration in the event loop.'''
        try:
            return self._loop_tasks.remove(task)
        except ValueError:
            pass
        
    def add_handler(self, fd, handler, events):
        """Registers the given *handler* to receive the given events for the
file descriptor *fd*.

:parameter fd: A file descriptor or an object with the ``fileno`` method.
:parameter handler: A callable which will be called when events occur on the
    file descriptor *fd*.
:rtype: ``True`` if the handler was succesfully added."""
        if fd is not None:
            fde = file_descriptor(fd)
            if fde not in self._handlers:
                self._handlers[fde] = handler
                self.log.debug('Registering fd {0} for "{1}"\
 with ioloop.'.format(fde,fd))
                self._impl.register(fde, events | self.ERROR)
                return True
            else:
                self.log.debug('Handler for "{0}"\
 already available.'.format(fd))

    def update_handler(self, fd, events):
        """Changes the events we listen for fd."""
        self._impl.modify(fd, events | self.ERROR)

    def remove_handler(self, fd):
        """Stop listening for events on fd."""
        fdd = file_descriptor(fd)
        self._handlers.pop(fdd, None)
        self._events.pop(fdd, None)
        try:
            self._impl.unregister(fdd)
        except (OSError, IOError):
            self.log.error("Error deleting {0} from IOLoop"\
                           .format(fd), exc_info=True)

    @Synchronized.make
    def _startup(self):
        if self._stopped:
            self._stopped = False
            return False
        if self._running:
            return False
        return True
    
    def do_loop_tasks(self):
        '''Perform tasks in the event loop. These tasks can be added and
removed using the :meth:`pulsar.IOLoop.add_loop_task` and
:meth:`pulsar.IOLoop.remove_loop_task` methods.

For example, a :class:`pulsar.Actor` add itself to the event loop tasks
so that it can perform its tasks at each event loop. Check the
:meth:`pulsar.Actor` method.'''
        for task in self._loop_tasks:
            try:
                task()
            except HaltServer:
                raise
            except:
                self.log.critical('Unhandled exception in loop task',
                                  exc_info = True)
        
    def start(self):
        """Starts the I/O loop.

        The loop will run until one of the I/O handlers calls stop(), which
        will make the loop stop after the current event iteration completes.
        """
        if not self._startup():
            return False
        with LoopGuard(self) as guard:
            self.log.debug("Starting event loop")
            self._started = time.time()
            self._on_exit = Deferred()
            
            while self._running:
                poll_timeout = self.POLL_TIMEOUT
                self.num_loops += 1
                # Prevent IO event starvation by delaying new callbacks
                # to the next iteration of the event loop.
                callbacks = self._callbacks
                if callbacks:
                    self._callbacks = []
                    _run_callback = self._run_callback 
                    for callback in callbacks:
                        _run_callback(callback)
    
                if self._callbacks:
                    poll_timeout = 0.0
    
                if self._timeouts:
                    now = time.time()
                    while self._timeouts and self._timeouts[0].deadline <= now:
                        timeout = self._timeouts.pop(0)
                        self._run_callback(timeout.callback)
                    if self._timeouts:
                        milliseconds = self._timeouts[0].deadline - now
                        poll_timeout = min(milliseconds, poll_timeout)
    
                # A chance to exit
                if not self.running():
                    self.log.debug('Exiting event loop')
                    break
    
                self.do_loop_tasks()
                
                if not self.ready:
                    continue
                
                if self._blocking_signal_threshold is not None:
                    # clear alarm so it doesn't fire while poll is waiting for
                    # events.
                    signal.setitimer(signal.ITIMER_REAL, 0, 0)
    
                try:
                    event_pairs = self._impl.poll(poll_timeout)
                except Exception as e:
                    # Depending on python version and IOLoop implementation,
                    # different exception types may be thrown and there are
                    # two ways EINTR might be signaled:
                    # * e.errno == errno.EINTR
                    # * e.args is like (errno.EINTR, 'Interrupted system call')
                    if (getattr(e, 'errno', None) == errno.EINTR or
                        (isinstance(getattr(e, 'args', None), tuple) and
                         len(e.args) == 2 and e.args[0] == errno.EINTR)):
                        continue
                    else:
                        raise
    
                if self._blocking_signal_threshold is not None:
                    signal.setitimer(signal.ITIMER_REAL,
                                     self._blocking_signal_threshold, 0)
    
                # Pop one fd at a time from the set of pending fds and run
                # its handler. Since that handler may perform actions on
                # other file descriptors, there may be reentrant calls to
                # this IOLoop that update self._events
                if event_pairs:
                    self._events.update(event_pairs)
                    _events = self._events
                    while _events:
                        fd, events = _events.popitem()
                        try:
                            self._handlers[fd](fd, events)
                        except (KeyboardInterrupt, SystemExit):
                            raise
                        except (OSError, IOError) as e:
                            if e.args[0] == errno.EPIPE:
                                # Happens when the client closes the connection
                                pass
                            else:
                                self.log.error(
                                    "Exception in I/O handler for fd %d",
                                              fd, exc_info=True)
                        except:
                            self.log.error("Exception in I/O handler for fd %d",
                                          fd, exc_info=True)

    def stop(self):
        '''Stop the loop after the current event loop iteration is complete.
If the event loop is not currently running, the next call to :meth:`start`
will return immediately.

To use asynchronous methods from otherwise-synchronous code (such as
unit tests), you can start and stop the event loop like this::

    ioloop = IOLoop()
    async_method(ioloop=ioloop, callback=ioloop.stop)
    ioloop.start()
  
:meth:`start` will return after async_method has run its callback,
whether that callback was invoked before or after ioloop.start.'''
        if self.running():
            self._running = False
            self.wake()
        return self._on_exit

    def running(self):
        """Returns true if this IOLoop is currently running."""
        return self._running
    
    def stopped(self):
        '''Return ``True`` if the loop have been stopped.'''
        return self._stopped

    def add_timeout(self, deadline, callback):
        """Calls the given callback at the time deadline from the I/O loop.

        Returns a handle that may be passed to remove_timeout to cancel.
        """
        timeout = _Timeout(deadline, callback)
        bisect.insort(self._timeouts, timeout)
        return timeout

    def remove_timeout(self, timeout):
        """Cancels a pending timeout.

        The argument is a handle as returned by add_timeout.
        """
        self._timeouts.remove(timeout)

    def add_callback(self, callback, wake = True):
        """Calls the given callback on the next I/O loop iteration.

        It is safe to call this method from any thread at any time.
        Note that this is the *only* method in IOLoop that makes this
        guarantee; all other interaction with the IOLoop must be done
        from that IOLoop's thread.  add_callback() may be used to transfer
        control from other threads to the IOLoop's thread.
        """
        self._callbacks.append(callback)
        if wake:
            self.wake()

    def wake(self):
        '''Wake up the eventloop.'''
        if not self.stopped():
            self._waker.wake()

    def _run_callback(self, callback):
        try:
            callback()
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handle_callback_exception(callback)

    def handle_callback_exception(self, callback):
        """This method is called whenever a callback run by the IOLoop
        throws an exception.

        By default simply logs the exception as an error.  Subclasses
        may override this method to customize reporting of exceptions.

        The exception itself is not passed explicitly, but is available
        in sys.exc_info.
        """
        self.log.error("Exception in callback %r", callback, exc_info=True)
    
    
class _Timeout(object):
    """An IOLoop timeout, a UNIX timestamp and a callback"""
    __slots__ = ('deadline', 'callback')

    def __init__(self, deadline, callback):
        self.deadline = deadline
        self.callback = callback

    def __lt__(self, other):
        return ((self.deadline, id(self.callback)) <
                (other.deadline, id(other.callback)))


class PeriodicCallback(object):
    """Schedules the given callback to be called periodically.

    The callback is called every callback_time seconds.
    """
    def __init__(self, callback, callback_time, ioloop):
        self.callback = callback
        self.callback_time = callback_time
        self.ioloop = ioloop
        self._running = False

    def start(self):
        self._running = True
        timeout = time.time() + self.callback_time
        self.ioloop.add_timeout(timeout, self._run)

    def stop(self):
        self._running = False

    def _run(self):
        if not self._running:
            return
        try:
            self.callback()
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            ioloop.log.error("Error in periodic callback", exc_info=True)
        if self._running:
            self.start()

