.. _apps-wsgi:

=================
WSGI
=================

The :mod:`~.apps.wsgi` module implements a :ref:`web server <wsgi-server>`
and several web :ref:`application handlers <wsgi-handlers>` which
conform with pulsar :ref:`WSGI asynchronous specification <wsgi-async>`.
In addition, the module contains several utilities which facilitate the
development of server side asynchronous web applications.

.. toctree::
   :maxdepth: 2

   intro
   async
   routing
   wrappers
   middleware
   response
   content
   tools
