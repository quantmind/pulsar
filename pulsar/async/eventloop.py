import logging
import traceback
import time
import signal
import threading
import errno
import bisect
from multiprocessing import Pipe

from pulsar.utils.system import IObase, IOpoll, close_on_exec, platform
from pulsar.utils.collections import WeakList

from .defer import Deferred


__all__ = ['IOLoop','MainIOLoop']

def file_descriptor(fd):
    if hasattr(fd,'fileno'):
        return fd.fileno()
    else:
        return fd


def threadsafe(f):
    
    def _(self, *args, **kwargs):
        if not hasattr(self,'_lock'):
            self._lock = threading.Lock()
        self._lock.acquire()
        try:
            return f(self,*args,**kwargs)
        finally:
            self._lock.release()
    
    return _


class IOLoop(IObase):
    """\
A level-triggered I/O loop adapted from tornado

We use epoll if it is available, or else we fall back on select().
Example usage for a simple TCP server:

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

When using the eventloop on a child process, It should be instantiated after forking.
    """
    # Never use an infinite timeout here - it can stall epoll
    POLL_TIMEOUT = 0.2
    
    def __init__(self, impl=None, logger = None, pool_timeout = None, commnads = None):
        self._impl = impl or IOpoll()
        self.POLL_TIMEOUT = pool_timeout if pool_timeout is not None else self.POLL_TIMEOUT
        self.log = logger or logging.getLogger('ioloop')
        fd = file_descriptor(self._impl)
        if fd:
            close_on_exec(fd)
        self._handlers = {}
        self._events = {}
        self._callbacks = []
        self._timeouts = []
        self._loop_tasks = WeakList()
        self._started = None
        self._running = False
        self._stopped = False
        self.num_loops = 0
        '''Called when when the child process is forked'''
        self._blocking_signal_threshold = None
        if platform.type == 'posix':
            # Create a pipe that we send bogus data to when we want to wake
            # the I/O loop when it is idle
            self._waker_reader, self._waker_writer = Pipe(duplex = False)
            r = self._waker_reader
            self.add_handler(r, self.readbogus, self.READ)
        
    def readbogus(self, fd, events):
        r = self._waker_reader
        while r.poll():
            r.recv()
            #self.log.debug("Got wake up data {0}".format(r.recv()))

    def add_loop_task(self, task):
        '''Add a callable object to self.
The object will be called at each iteration in the loop.'''
        self._loop_tasks.append(task)
        
    def remove_loop_task(self, task):
        try:
            return self._loop_tasks.remove(task)
        except ValueError:
            pass
        
    def add_handler(self, fd, handler, events):
        """Registers the given handler to receive the given events for fd."""
        if fd is not None:
            fd = file_descriptor(fd)
            if fd not in self._handlers:
                self._handlers[fd] = handler
                self.log.debug('Registering file descriptor "{0}" with ioloop.'.format(fd))
                self._impl.register(fd, events | self.ERROR)
                return True

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
            self.log.debug("Error deleting {0} from IOLoop".format(fd), exc_info=True)

    def set_blocking_signal_threshold(self, seconds, action):
        """Sends a signal if the ioloop is blocked for more than s seconds.

        Pass seconds=None to disable.  Requires python 2.6 on a unixy
        platform.

        The action parameter is a python signal handler.  Read the
        documentation for the python 'signal' module for more information.
        If action is None, the process will be killed if it is blocked for
        too long.
        """
        if not hasattr(signal, "setitimer"):
            logging.error("set_blocking_signal_threshold requires a signal module "
                       "with the setitimer method")
            return
        self._blocking_signal_threshold = seconds
        if seconds is not None:
            signal.signal(signal.SIGALRM,
                          action if action is not None else signal.SIG_DFL)

    def set_blocking_log_threshold(self, seconds):
        """Logs a stack trace if the ioloop is blocked for more than s seconds.
        Equivalent to set_blocking_signal_threshold(seconds, self.log_stack)
        """
        self.set_blocking_signal_threshold(seconds, self.log_stack)

    def log_stack(self, signal, frame):
        """Signal handler to log the stack trace of the current thread.

        For use with set_blocking_signal_threshold.
        """
        logging.warning('IOLoop blocked for %f seconds in\n%s',
                        self._blocking_signal_threshold,
                        ''.join(traceback.format_stack(frame)))

    @threadsafe
    def _startup(self):
        if self._stopped:
            self._stopped = False
            return False
        if self._running:
            return False
        self._running = True
        return True
    
    def do_loop_tasks(self):
        for task in self._loop_tasks:
            task()
        
    def start(self):
        """Starts the I/O loop.

        The loop will run until one of the I/O handlers calls stop(), which
        will make the loop stop after the current event iteration completes.
        """
        if not self._startup():
            return False
        self.log.debug("Starting event loop")
        self._started = time.time()
        self._on_exit = Deferred()
        while True:
            poll_timeout = self.POLL_TIMEOUT
            self.num_loops += 1
            # Prevent IO event starvation by delaying new callbacks
            # to the next iteration of the event loop.
            callbacks = self._callbacks
            if callbacks:
                self._callbacks = []
                for callback in callbacks:
                    self._run_callback(callback)

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

            if not self.running():
                self.log.debug('Exiting event loop')
                break

            self.do_loop_tasks()
            
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
                            logging.error("Exception in I/O handler for fd %d",
                                          fd, exc_info=True)
                    except:
                        logging.error("Exception in I/O handler for fd %d",
                                      fd, exc_info=True)
        # reset the stopped flag so another start/stop pair can be issued
        self._on_exit.callback(self)
        self._stopped = False
        if self._blocking_signal_threshold is not None:
            signal.setitimer(signal.ITIMER_REAL, 0, 0)

    def stop(self):
        """Stop the loop after the current event loop iteration is complete.
        If the event loop is not currently running, the next call to start()
        will return immediately.

        To use asynchronous methods from otherwise-synchronous code (such as
        unit tests), you can start and stop the event loop like this:
          ioloop = IOLoop()
          async_method(ioloop=ioloop, callback=ioloop.stop)
          ioloop.start()
        ioloop.start() will return after async_method has run its callback,
        whether that callback was invoked before or after ioloop.start.
        """
        self.log.debug("Stopping event loop")
        self._running = False
        self._stopped = True
        self._wake()
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

    def add_callback(self, callback):
        """Calls the given callback on the next I/O loop iteration.

        It is safe to call this method from any thread at any time.
        Note that this is the *only* method in IOLoop that makes this
        guarantee; all other interaction with the IOLoop must be done
        from that IOLoop's thread.  add_callback() may be used to transfer
        control from other threads to the IOLoop's thread.
        """
        self._callbacks.append(callback)
        self._wake()

    def _wake(self):
        '''Wake up the eventloop'''
        if platform.type == 'posix':
            if self.running():
                try:
                    self._waker_writer.send(b'x')
                except IOError:
                    pass

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


class MainIOLoop(IOLoop):

    @classmethod
    def instance(cls, logger = None):
        """Returns a global MainIOLoop instance.

        Most single-threaded applications have a single, global IOLoop.
        Use this method instead of passing around IOLoop instances
        throughout your code.

        A common pattern for classes that depend on IOLoops is to use
        a default argument to enable programs with multiple IOLoops
        but not require the argument for simpler applications:

            class MyClass(object):
                def __init__(self, io_loop=None):
                    self.io_loop = io_loop or IOLoop.instance()
        """
        if not hasattr(cls, "_instance"):
            cls._instance = cls(logger = logger)
        return cls._instance
     
    @classmethod
    def initialized(cls):
        return hasattr(cls, "_instance")
    
    
class _Timeout(object):
    """An IOLoop timeout, a UNIX timestamp and a callback"""

    # Reduce memory overhead when there are lots of pending callbacks
    __slots__ = ['deadline', 'callback']

    def __init__(self, deadline, callback):
        self.deadline = deadline
        self.callback = callback

    def __lt__(self, other):
        return ((self.deadline, id(self.callback)) <
                (other.deadline, id(other.callback)))


class PeriodicCallback(object):
    """Schedules the given callback to be called periodically.

    The callback is called every callback_time milliseconds.
    """
    def __init__(self, callback, callback_time, io_loop=None):
        self.callback = callback
        self.callback_time = callback_time
        self.io_loop = io_loop or IOLoop.instance()
        self._running = False

    def start(self):
        self._running = True
        timeout = time.time() + self.callback_time / 1000.0
        self.io_loop.add_timeout(timeout, self._run)

    def stop(self):
        self._running = False

    def _run(self):
        if not self._running: return
        try:
            self.callback()
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            logging.error("Error in periodic callback", exc_info=True)
        if self._running:
            self.start()

