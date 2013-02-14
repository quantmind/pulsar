.. module:: pulsar

.. _tutorials-coroutine:

=========================
Asynchronous Components
=========================


Coroutines
===================

The following::

    def my_async_generator(...):
        yield something_but_dont_care_what_it_returns()
        ...
        bla = yield something_and_care_what_it_returns()
        yield do_something(bla)
        
    result = maybe_async(my_async_generator())
    
    
Under the hood, a generator is transformed into a :class:`DeferredCoroutine`
via the :func:`maybe_async` function. The :class:`DeferredCoroutine`
allows one to write inlines asynchronous function in just one line::

    result = yield possibly_async_result()
    
Previously, pulsar used a generator approach, which required three lines
of code to obtain the same result::

    future = make_async(possibly_async_result())
    yield future
    result = future.result