from asyncio import Task, CancelledError, TimeoutError


class timeout:
    """timeout context manager.
    """
    __slots__ = ('_loop', '_timeout', '_cancelled', '_task', '_cancel_handler')

    def __init__(self, loop, timeout):
        self._loop = loop
        self._timeout = timeout
        self._task = None
        self._cancelled = False
        self._cancel_handler = None

    def __enter__(self):
        if self._timeout is None:
            return self
        self._task = Task.current_task(self._loop)
        tm = self._loop.time() + self._timeout
        self._cancel_handler = self._loop.call_at(tm, self._cancel_task)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is CancelledError and self._cancelled:
            self._cancel_handler = None
            self._task = None
            raise TimeoutError
        if self._timeout is not None and self._cancel_handler is not None:
            self._cancel_handler.cancel()
            self._cancel_handler = None
        self._task = None

    def _cancel_task(self):
        self._task.cancel()
        self._cancelled = True
