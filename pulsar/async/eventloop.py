import os
import sys
import logging
import traceback
import time
import signal
import errno
import bisect
import socket
from threading import current_thread

from pulsar import HaltServer, Timeout
from pulsar.utils.system import IObase, IOpoll, close_on_exec, platform, Waker
from pulsar.utils.security import gen_unique_id
from pulsar.utils.log import Synchronized
from pulsar.utils.structures import WeakList

from .defer import Deferred, is_async, maybe_async, thread_loop, make_async,\
                    log_failure

__all__ = ['IOLoop', 'PeriodicCallback', 'loop_timeout']

EXIT_EXCEPTIONS = (KeyboardInterrupt, SystemExit, HaltServer)

def file_descriptor(fd):
    if hasattr(fd,'fileno'):
        return fd.fileno()
    else:
        return fd


class ID:
    cpubound = False
    _name = None
    _pid = None
    _repr = ''
    _tid = None

    def __repr__(self):
        return self._repr

    def __str__(self):
        return self._repr

    @property
    def tid(self):
        return self._tid

    @property
    def pid(self):
        return self._pid

    @property
    def name(self):
        return self._name

    @property
    def fullname(self):
        return self._repr

    def setid(self):
        ct = current_thread()
        self._tid = ct.ident
        self._pid = os.getpid()


class LoopGuard(object):
    '''Context manager for the eventloop'''
    def __init__(self, loop):
        self.loop = loop

    def __enter__(self):
        loop = self.loop
        loop.log.debug("Starting %s", loop)
        loop._running = True
        loop.setid()
        loop._started = time.time()
        loop._on_exit = Deferred()
        loop.num_loops = 0
        return self

    def __exit__(self, type, value, traceback):
        loop = self.loop
        loop._running = False
        loop._tid = None
        loop._stopped = True
        loop.log.debug('Exiting event loop')
        loop._on_exit.callback(loop)


class IOLoop(IObase, ID, Synchronized):
    """\
A level-triggered I/O event loop adapted from tornado.

:parameter io: The I/O implementation. If not supplied, the best possible
    implementation available will be used. On posix system this is ``epoll``,
    or else ``select``. It can be any other custom implementation as long as
    it has an ``epoll`` like interface. Pulsar ships with an additional
    I/O implementation based on distributed queue :class:`IOQueue`.

.. attribute:: num_lumps

    total number of loops

.. attribute:: POLL_TIMEOUT

    The timeout in seconds when polling with epol or select.

    Default: `0.5`

.. attribute:: tid

    The thread id where the eventloop is running
"""
    # Never use an infinite timeout here - it can stall epoll
    POLL_TIMEOUT = 0.5

    def __init__(self, io=None, logger=None, pool_timeout=None, commnads=None,
                 name=None, ready=True):
        self._impl = io or IOpoll()
        self.POLL_TIMEOUT = pool_timeout if pool_timeout is not None\
                                 else self.POLL_TIMEOUT
        self.log = logger or logging.getLogger('ioloop')
        if hasattr(self._impl, 'fileno'):
            close_on_exec(self._impl.fileno())
        cname = self.__class__.__name__.lower()
        self._name = name or cname
        if self._name != cname:
            self._repr = '%s - %s' % (cname, self._name)
        else:
            self._repr = self._name
        self._handlers = {}
        self._events = {}
        self._callbacks = []
        self._timeouts = []
        self._loop_tasks = WeakList()
        self._starter = None
        self._started = None
        self._running = False
        self._stopped = False
        self.num_loops = 0
        self._blocking_signal_threshold = None
        self._waker = getattr(self._impl, 'waker', Waker)()
        self._on_exit = None
        self.ready = ready
        self.add_handler(self._waker,
                         lambda fd, events: self._waker.consume(),
                         self.READ)

    @property
    def cpubound(self):
        return getattr(self._impl, 'cpubound', False)

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
            fdd = file_descriptor(fd)
            if fdd not in self._handlers:
                self._handlers[fdd] = handler
                self._impl.register(fdd, events | self.ERROR)
                return True
            else:
                self.log.debug('Handler for %s already available.', fd)

    def update_handler(self, fd, events):
        """Changes the events we listen for fd."""
        self._impl.modify(file_descriptor(fd), events | self.ERROR)

    def remove_handler(self, fd):
        """Stop listening for events on fd."""
        fdd = file_descriptor(fd)
        self._handlers.pop(fdd, None)
        self._events.pop(fdd, None)
        try:
            self._impl.unregister(fdd)
        except (OSError, IOError):
            self.log.error("Error removing %s from IOLoop", fd, exc_info=True)

    def start(self, starter=None):
        if not self._startup(starter):
            return False
        if starter: # just start
            self._run()
        elif self._starter:
            self._starter.start()
        else:
            self._run()

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
        """Add a timeout *callback*. A timeout callback  it is called
at the time *deadline* from the :class:`IOLoop`.
It returns an handle that may be passed to remove_timeout to cancel."""
        timeout = _Timeout(deadline, callback)
        bisect.insort(self._timeouts, timeout)
        return timeout

    def remove_timeout(self, timeout):
        """Cancels a pending *timeout*. The argument is an handle as returned
by the :meth:`add_timeout` method."""
        self._timeouts.remove(timeout)

    def add_callback(self, callback, wake=True):
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

    ############################################################ INTERNALS
    @Synchronized.make
    def _startup(self, starter):
        if self._running:
            return False
        if starter:
            if self._starter and starter is not self._starter:
                raise RuntimeError('Cannot start. It needs to be started '\
                                   'by %s' % self._starter)
            self._starter = starter
        self._stopped = False
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
                result = task()
                #result = task()
                #loop_deferred(result, self)
            except HaltServer:
                raise
            except:
                self.log.critical('Unhandled exception in loop task',
                                  exc_info = True)

    def _run(self):
        """Runs the I/O loop until one of the I/O handlers calls stop(), which
will make the loop stop after the current event iteration completes."""
        with LoopGuard(self) as guard:
            while self._running:
                poll_timeout = self.POLL_TIMEOUT
                self.num_loops += 1
                # Prevent IO event starvation by delaying new callbacks
                # to the next iteration of the event loop.
                callbacks = self._callbacks
                if callbacks:
                    self._callbacks = []
                    #self.log.debug('Total of %s callbacks', len(callbacks))
                    _run_callback = self._run_callback
                    for callback in callbacks:
                        _run_callback(callback)
                #if self._callbacks:
                #    poll_timeout = 0.0
                if self._timeouts:
                    now = time.time()
                    while self._timeouts and self._timeouts[0].deadline <= now:
                        timeout = self._timeouts.pop(0)
                        self._run_callback(timeout.callback)
                    if self._timeouts:
                        milliseconds = self._timeouts[0].deadline - now
                        poll_timeout = min(milliseconds, poll_timeout)
                # A chance to exit
                if not self._running:
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
                        except EXIT_EXCEPTIONS:
                            raise
                        except (OSError, IOError) as e:
                            if e.args[0] == errno.EPIPE:
                                # Happens when the client closes the connection
                                pass
                            else:
                                self.log.error(
                                    "Exception in I/O handler for fd %s",
                                              fd, exc_info=True)
                        except:
                            self.log.error("Exception in I/O handler for fd %s",
                                          fd, exc_info=True)

    def _run_callback(self, callback):
        try:
            callback()
        except EXIT_EXCEPTIONS:
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


class _not_called_exception:

    def __init__(self, value):
        self.value = value

    def __call__(self):
        if not self.value.called:
            try:
                raise Timeout('"%s" timed out.' % self.value)
            except:
                self.value.callback(sys.exc_info())


def loop_timeout(value, timeout, ioloop=None):
    value = maybe_async(value)
    if timeout and is_async(value):
        ioloop = ioloop or thread_loop()
        return ioloop.add_timeout(time.time() + timeout,
                                  _not_called_exception(value))

