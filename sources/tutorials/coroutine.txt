.. module:: pulsar

.. _tutorials-coroutine:

=========================
Asynchronous Components
=========================

First things first, what do we mean with the term **asynchronous components**?
There are three, closely related, ways to create asynchronous components:

* Create an instance of :class:`Deferred`::
  
      import pulsar
      
      o = pulsar.Deferred()
      
* A **generator**, or better a :ref:`coroutine <coroutine>`. For example::
  
      def my_async_generator(...):
          yield something_but_dont_care_what_it_returns()
          ...
          bla = yield something_and_care_what_it_returns()
          yield do_something(bla)

  Then an asynchronous component is obtained by calling the generator function::
  
      o = my_async_generator()
  
  Not that *o* is coroutine (a generator which consume values) which has not yet
  started.
  
* A :class:`Task`, is a component which has been added to pulsar asynchronous
  engine. It is created via the :class:`async` decorator::
  
      from pulsar import async
      
      @async()
      def my_async_generator(...):
          yield something_but_dont_care_what_it_returns()
          ...
          bla = yield something_and_care_what_it_returns()
          yield do_something(bla)
  
      task = my_async_generator()
      
  A :class:`Task` is a subclass of :class:`Deferred` and therefore it has
  the same API, for example, you can add callbacks to a task::
  
      task.add_callback(...)
 
.. _coroutine:
  
Coroutines
===================
The generator returned by the generator function above is treated by pulsar
asynchronous machinery as **coroutine**.
A coroutine is a generator that uses the **yield** to do more than just
generate values. Coroutines **consume** values by using
yield as an expression, on the right-hand-side of an assignment.

Under the hood, a generator is transformed into a :class:`Task`
via the :func:`maybe_async` function. The :class:`Task`
allows one to write inlines asynchronous function in just one line::

    result = yield possibly_async_result()
    

Collections
============================
When dealing with several asynchronous components in a collection such as
a list, tuple, set or even a dictionary (values only, keys must be synchronous
python types), one can use the :func:`multi_async` function to create
an asynchronous component which will be ready once all the components
are ready.