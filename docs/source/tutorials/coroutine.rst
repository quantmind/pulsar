.. module:: pulsar

.. _tutorials-coroutine:

=========================
Asynchronous Components
=========================

There are three, closely related, ways to create asynchronous components in
pulsar:

* Directly create a of :class:`~asyncio.Future`::

      import asyncio

      o = asyncio.Future()

  or, equivalently::

      import pulsar

      o = pulsar.Future()

* A :ref:`coroutine <coroutine>`, a generator which consumes values.
  For example::

      from pulsar import coroutine_return

      def my_async_generator(...):
          yield something_but_dont_care_what_it_returns()
          ...
          bla = yield something_and_care_what_it_returns()
          result = yield do_something(bla)
          coroutine_return(result)

  a coroutine is obtained by calling the generator function::

      o = my_async_generator()

  Note that ``o`` is coroutine which has not yet started.

.. _task-component:

* An :class:`asyncio.Task`, is a component which has been added to
  pulsar asynchronous engine. It is created via the :func:`.async` function
  when applied to a generator function or, equivalently, by the
  :func:`.in_loop` and :func:`.task` decorators::

      from pulsar import async

      def my_async_generator1(...):
          yield something_but_dont_care_what_it_returns()
          ...
          bla = yield something_and_care_what_it_returns()
          yield do_something(bla)

      task = async(my_async_generator2())

  A :class:`~asyncio.Task` is a subclass of :class:`~asyncio.Future` and
  therefore it has the same API, for example, you can add callbacks to a task::

      task.add_done_callback(...)


.. _coroutine:

Coroutines
===================
As mentioned above, a coroutine is a generator which consumes values. A pulsar
coroutine can consume synchronous values as well as :class:`~asyncio.Future`
and other :ref:`coroutines <coroutine>`.
Let's consider the following code::

    d = Future()

    def do_something(...):
          yield something_but_dont_care_what_it_returns()
          ...
          bla = yield something_and_care_what_it_returns()
          result = yield do_something(bla)
          coroutine_return(result)

    def my_async_generator():
          result = yield d
          yield do_something(result)

Then we create a coroutine by calling the ``my_async_generator`` generator
function::

    o = my_async_generator()

``o`` is has not yet started. To use it, it must be added to pulsar
asynchronous engine via the :func:`.async` function::

    task = async(o)

task is a :class:`~asyncio.Task` instance.

Coroutines can return values via the :func:`.coroutine_return` function.
Otherwise they always return ``None`` (unless exceptions occur).

Task
===================
A :class:`~asyncio.Task` is a specialised :class:`~asyncio.Future` which consumes
:ref:`coroutines <coroutine>`.
A coroutine is transformed into a :class:`~asyncio.Task`
via the :func:`.async` function or the :func:`.in_loop` and
:func:`.task` decorators.

A task consumes a coroutine until the coroutine yield an asynchronous component
not yet done. When this appends, the task pauses and returns the control of execution.
Before it returns, it adds a ``callback`` to the :class:`~asyncio.Future`
on which the coroutine is blocked to resume the coroutine once the future
is called.
A task in this state is said to be **suspended**.


Collections
============================
When dealing with several asynchronous components in a collection such as
a list, tuple, set or even a dictionary (values only, keys must be synchronous
python types), one can use the :func:`.multi_async` function to create
an asynchronous component which will be ready once all the components
are ready.
