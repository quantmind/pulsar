=====================
Utilities
=====================

HTTP Parsers
================

Pulsar ships with its own :ref:`HTTP parser <tools-http-parser>` used by
both server and the :ref:`HTTP client <apps-http>`. Headers are collected using
the :ref:`Headers data structure <tools-http-headers>` which exposes a
list/dictionary-type interface.

At runtime, pulsar checks if the http-parser_ package and cython_
are available. If this is the case, it switches the default HTTP parser
to be the one provided by the :mod:`http_parser.paser` module.
To check if the C parser is the default parser::

    from pulsar.lib import hasextensions

Authentication
=================

.. automodule:: pulsar.apps.wsgi.plugins


Structures
=================

.. automodule:: pulsar.apps.wsgi.structures
    :members:


.. _http-parser: https://github.com/benoitc/http-parser
.. _cython: http://cython.org/
