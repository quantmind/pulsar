from asyncio import Future, CancelledError, TimeoutError, sleep, gather

from .consts import MAX_ASYNC_WHILE
from .access import (
    get_event_loop, LOGGER, ensure_future, create_future
)


def return_false():
    return False


def chain_future(future, callback=None, errback=None, next=None):
    '''Chain a :class:`~asyncio.Future` to an existing ``future``.

    This function `chain` the ``next`` future to an existing ``future``.
    When the input ``future`` receive a result the optional
    ``callback`` is executed and its result set as the results of ``next``.
    If an exception occurs the optional ``errback`` is executed.

    :param future: the original :class:`~asyncio.Future` (can be a coroutine)
    :param callback: optional callback to execute on the result of ``future``
    :param errback: optional callback to execute on the exception of ``future``
    :param next: optional :class:`~asyncio.Future` to chain.
        If not provided a new future is created
    :return: the future ``next``
    '''
    loop = next._loop if next else None
    future = ensure_future(future, loop=loop)
    if next is None:
        next = create_future(future._loop)

    def _callback(fut):
        try:
            try:
                result = future.result()
            except Exception as exc:
                if errback:
                    result = errback(exc)
                    exc = None
                else:
                    raise
            else:
                if callback:
                    result = callback(result)
        except Exception as exc:
            next.set_exception(exc)
        else:
            if isinstance(result, Future):
                chain_future(result, next=next)
            else:
                next.set_result(result)

    future.add_done_callback(_callback)
    return next


def add_errback(future, callback, loop=None):
    '''Add a ``callback`` to a ``future`` executed only if an exception
    or cancellation has occurred.'''
    def _error_back(fut):
        if fut._exception:
            callback(fut.exception())
        elif fut.cancelled():
            callback(CancelledError())

    future = ensure_future(future, loop=None)
    future.add_done_callback(_error_back)
    return future


def maybe_async(value, *, loop=None):
    '''Handle a possible asynchronous ``value``.

    :parameter value: the value to convert to an asynchronous instance
        if it needs to.
    :parameter loop: optional :class:`.EventLoop`.
    :return: a :class:`.Future` or a synchronous ``value``.
    '''
    try:
        return ensure_future(value, loop=loop)
    except TypeError:
        return value


async def as_coroutine(value):
    try:
        value = await value
    except TypeError:
        pass
    return value


def as_gather(*args):
    """Same as :func:`~.asyncio.gather` but allows sync values
    """
    return gather(*[as_coroutine(arg) for arg in args])


async def async_while(timeout, while_clause, *args):
    '''The asynchronous equivalent of ``while while_clause(*args):``

    Use this function within a :ref:`coroutine <coroutine>` when you need
    to wait ``while_clause`` to be satisfied.

    :parameter timeout: a timeout in seconds after which this function stop.
    :parameter while_clause: while clause callable.
    :parameter args: optional arguments to pass to the ``while_clause``
        callable.
    :return: A :class:`.Future`.
    '''
    loop = get_event_loop()
    start = loop.time()
    di = 0.1
    interval = 0
    result = while_clause(*args)

    while result:
        interval = min(interval+di, MAX_ASYNC_WHILE)
        try:
            await sleep(interval, loop=loop)
        except TimeoutError:
            pass
        if timeout and loop.time() - start >= timeout:
            break
        result = while_clause(*args)

    return result


# ############################################################## Bench
class Bench:
    '''Execute a given number of asynchronous requests and wait for results.
    '''
    start = None
    '''The :meth:`~asyncio.BaseEventLoop.time` when the execution starts'''
    finish = None
    '''The :meth:`~asyncio.BaseEventLoop.time` when the execution finishes'''
    result = ()
    '''Tuple of results'''

    def __init__(self, times, loop=None):
        self._loop = loop or get_event_loop()
        self.times = times

    @property
    def taken(self):
        '''The total time taken for execution
        '''
        if self.finish:
            return self.finish - self.start

    def __call__(self, func, *args, **kwargs):
        self.start = self._loop.time()
        data = [func(*args, **kwargs) for _ in range(self.times)]
        self.result = gather(*data, loop=self._loop)
        return chain_future(self.result, callback=self._done)

    def _done(self, result):
        self.finish = self._loop.time()
        self.result = result
        return self


# ############################################################## AsyncObject
class AsyncObject:
    '''Interface for :ref:`async objects <async-object>`

    .. attribute:: _loop

        The :ref:`event loop <asyncio-event-loop>` associated with this object

    .. attribute:: _logger

        Optional logger instance, used by the :attr:`logger` attribute
    '''
    _logger = None
    _loop = None

    @property
    def logger(self):
        '''The logger for this object.

        It is either the :attr:`_logger` or the logger of the :attr:`_loop`
        '''
        return self._logger or getattr(self._loop, 'logger', LOGGER)

    @property
    def debug(self):
        '''True when in debug mode
        '''
        return getattr(self._loop, 'get_debug', return_false)()

    def timeit(self, method, times, *args, **kwargs):
        '''Useful utility for benchmarking an asynchronous ``method``.

        :param method: the name of the ``method`` to execute
        :param times: number of times to execute the ``method``
        :param args: positional arguments to pass to the ``method``
        :param kwargs: key-valued arguments to pass to the ``method``
        :return: a :class:`~asyncio.Future` which results in a :class:`Bench`
            object if successful

        The usage is simple::

            >>> b = self.timeit('asyncmethod', 100)
        '''
        bench = Bench(times, loop=self._loop)
        return bench(getattr(self, method), *args, **kwargs)
