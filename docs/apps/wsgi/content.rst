.. _apps-wsgi-content:

===============================
Asynchronous Content
===============================

.. module:: pulsar.apps.wsgi

Content classes can operate instead or in conjunction with a template engine,
their main purpose is to do what a web framework does: to provide a set of
tools working together to concatenate ``strings`` to return as a
response to an :ref:`HTTP client request <app-wsgi-request>`.

A string can be ``html``, ``json``, ``plain text`` or any other valid HTTP
content type.

Design
===============

The :meth:`~String.stream` method is responsible for the streaming
of ``strings`` or :ref:`asynchronous components  <tutorials-coroutine>`.
It can be overwritten by subclasses to customise the way an
:class:`String` streams its :attr:`~String.children`.

On the other hand, the :meth:`~String.to_string` method is responsible
for the concatenation of ``strings`` and, like :meth:`~String.stream`,
it can be customised by subclasses.


Asynchronous String
=====================

.. autoclass:: String
   :members:
   :member-order: bysource


.. _wsgi-html:

Asynchronous Html
=====================

.. autoclass:: Html
   :members:
   :member-order: bysource

.. _wsgi-html-document:

Html Document
==================

The :class:`.HtmlDocument` class is a python representation of an
`HTML5 document`_, the latest standard for HTML.
It can be used to build a web site page in a pythonic fashion rather than
using template languages::

    >>> from pulsar.apps.wsgi import HtmlDocument

    >>> doc = HtmlDocument(title='My great page title')
    >>> doc.head.add_meta(name="description", content=...)
    >>> doc.head.scripts.append('jquery')
    ...
    >>> doc.body.append(...)


Document
~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: HtmlDocument
   :members:
   :member-order: bysource

.. _wsgi-html-head:

Head
~~~~~~~~~~

.. autoclass:: Head
   :members:
   :member-order: bysource

Media
~~~~~~~~~~

.. autoclass:: Media
   :members:
   :member-order: bysource

Scripts
~~~~~~~~~~

.. autoclass:: Scripts
   :members:
   :member-order: bysource

Links
~~~~~~~~~~

.. autoclass:: Links
   :members:
   :member-order: bysource

Html Factory
=================

.. autofunction:: html_factory


.. _`HTML5 document`: http://www.w3schools.com/html/html5_intro.asp
