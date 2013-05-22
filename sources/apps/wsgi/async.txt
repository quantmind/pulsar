
.. _wsgi-async:

=======================================
Asynchronous WSGI
=======================================

This section describes the asynchronous WSGI specification used by pulsar
:ref:`WSGI server  <wsgi-server>` and :ref:`application handlers <wsgi-handlers>`.
It is a superset of the the `WSGI 1.0.1`_ specification for synchronous
server/middleware.
If an application handler is synchronous, this specification is exactly equivalent
to `WSGI 1.0.1`_. The changes with respect `WSGI 1.0.1`_ only concerns
asynchronous responses and nothing else.

Introduction
========================

The WSGI interface has two sides: the ``server`` or ``gateway`` side, and the
``application`` or ``framework`` side. The server side invokes a callable object,
here referred as **application handler**, that is provided by the
application side.


Application handlers
=============================

An asynchronous :ref:`application handler <wsgi-handlers>` must conform
with the standard `WSGI 1.0.1`_ specification with the following two
exceptions:

* It can return a :class:`pulsar.Deferred`.
* If it returns a :class:`pulsar.Deferred`, the deferred, when called, i.e.
  the deferred get its :meth:`pulsar.Deferred.callback` method invoked,
  the result must be an :ref:`asynchronous iterable <wsgi-async-iter>`.
  
.. _wsgi-async-iter:

Asynchronous Iterable
========================

An asynchronous iterable is an iterable over a combination of ``bytes`` or
:class:`pulsar.Deferred` which result in ``bytes``.  
For eaxample this could be an asynchronous iterable::

    def simple_async():
        yield b'hello'
        c = pulsar.Deferred()
        c.callback(b' ')
        yield c
        yield b'World!'


.. _`WSGI 1.0.1`: http://www.python.org/dev/peps/pep-3333/