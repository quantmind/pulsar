from time import sleep

__all__ = ['retry_over_time']


def fxrange(start=1.0, stop=None, step=1.0, repeatlast=False):
    cur = start * 1.0
    while 1:
        if cur <= stop:
            yield cur
            cur += step
        else:
            if not repeatlast:
                break
            yield cur - step


def retry_over_time(fun, catch, args=[], kwargs={}, errback=None,
        max_retries=None, interval_start=2, interval_step=2, interval_max=30):
    """Retry the function over and over until max retries is exceeded.

    For each retry we sleep a for a while before we try again, this interval
    is increased for every retry until the max seconds is reached.

    :param fun: The function to try
    :param catch: Exceptions to catch, can be either tuple or a single
        exception class.
    :keyword args: Positional arguments passed on to the function.
    :keyword kwargs: Keyword arguments passed on to the function.
    :keyword errback: Callback for when an exception in ``catch`` is raised.
        The callback must take two arguments: ``exc`` and ``interval``, where
        ``exc`` is the exception instance, and ``interval`` is the time in
        seconds to sleep next..
    :keyword max_retries: Maximum number of retries before we give up.
        If this is not set, we will retry forever.
    :keyword interval_start: How long (in seconds) we start sleeping between
        retries.
    :keyword interval_step: By how much the interval is increased for each
        retry.
    :keyword interval_max: Maximum number of seconds to sleep between retries.

    """
    retries = 0
    interval_range = fxrange(interval_start,
                             interval_max + interval_start,
                             interval_step, repeatlast=True)

    for retries, interval in enumerate(interval_range):
        try:
            return fun(*args, **kwargs)
        except catch as exc:
            if max_retries and retries > max_retries:
                raise
            if errback:
                errback(exc, interval)
            sleep(interval)