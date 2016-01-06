=====================
Utilities
=====================

HTTP Parsers
================

Pulsar ships with its own :ref:`HTTP parser <tools-http-parser>` used by
both server and the :ref:`HTTP client <apps-http>`. Headers are collected using
the :ref:`Headers data structure <tools-http-headers>` which exposes a
list/dictionary-type interface.


Authentication
=================

.. automodule:: pulsar.apps.wsgi.auth


Structures
=================

.. automodule:: pulsar.apps.wsgi.structures
    :members:

Miscellaneous
================

.. automodule:: pulsar.apps.wsgi.utils
    :members:


.. _http-parser: https://github.com/benoitc/http-parser
.. _cython: http://cython.org/
