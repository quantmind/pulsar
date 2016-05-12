.. module:: pulsar

.. _tutorials-coroutine:

=========================
Asynchronous Components
=========================

Pulsar is built on top of asyncio_ and therefore it uses all its asynchronous
components and syntax.
There are three, closely related, ways to create asynchronous components in
pulsar.


.. _future:

Futures
===================

Directly create a of :class:`~asyncio.Future`::

    import asyncio

    o = asyncio.Future()

or, equivalently::

    import pulsar

    o = pulsar.Future()


.. _coroutine:

Coroutines
===================

A :ref:`coroutine <coroutine>`, a generator which consumes values.
For example::

    async def my_async_generator(...):
        await something_but_dont_care_what_it_returns()
        ...
        bla = await something_and_care_what_it_returns()
        return await do_something(bla)

a coroutine is obtained by calling the coroutine function::

    o = my_async_generator()

Note that ``o`` is coroutine which has not yet started.


.. _task-component:

Task
===================

An :class:`asyncio.Task`, is a component which has been added to
pulsar asynchronous engine. It is created via the :func:`.async` function
when applied to a generator function::

    from pulsar import ensure_future

    task = ensure_future(my_async_generator())

A :class:`~asyncio.Task` is a subclass of :class:`~asyncio.Future` and
therefore it has the same API, for example, you can add callbacks to a task::

    task.add_done_callback(...)


A task consumes a coroutine until the coroutine yield an asynchronous component
not yet done. When this appends, the task pauses and returns the control of execution.
Before it returns, it adds a ``callback`` to the :class:`~asyncio.Future`
on which the coroutine is blocked to resume the coroutine once the future
is called.
A task in this state is said to be **suspended**.


.. _asyncio: http://python.readthedocs.org/en/latest/library/asyncio.html
