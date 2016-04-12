from pulsar import isawaitable

from greenlet import greenlet, getcurrent


class MustBeInChildGreenlet(RuntimeError):
    """Raised when an operation must be performed in a child greenlet
    """


class GreenletWorker(greenlet):
    pass


def wait(value, must_be_child=False):
    '''Wait for a possible asynchronous value to complete.
    '''
    current = getcurrent()
    parent = current.parent
    if must_be_child and not parent:
        raise MustBeInChildGreenlet('Cannot wait on main greenlet')
    return parent.switch(value) if parent else value


def run_in_greenlet(callable):
    """Decorator to run a ``callable`` on a new greenlet.

    A ``callable`` decorated with this decorator returns a coroutine
    """
    async def _(*args, **kwargs):
        green = greenlet(callable)
        # switch to the new greenlet
        result = green.switch(*args, **kwargs)
        # back to the parent
        while isawaitable(result):
            # keep on switching back to the greenlet if we get a Future
            try:
                result = green.switch((await result))
            except Exception as exc:
                result = green.throw(exc)

        return green.switch(result)

    return _
