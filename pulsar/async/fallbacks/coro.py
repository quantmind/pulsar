
class CoroutineReturn(StopIteration):

    def __init__(self, value):
        self.value = value


def coroutine_return(value=None):
    '''Use this function to return ``value`` from a
    :ref:`coroutine <coroutine>`.

    For example::

        def mycoroutine():
            a = yield ...
            yield ...
            ...
            coroutine_return('OK')

    If a coroutine does not invoke this function, its result is ``None``.
    '''
    raise CoroutineReturn(value)
