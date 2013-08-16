.. _apps-wsgi:

=================
WSGI 
=================

The :mod:`pulsar.apps.wsgi` module implements a :ref:`web server <wsgi-server>`
and several web :ref:`application handlers <wsgi-handlers>` which
conform with pulsar :ref:`WSGI asynchronous specification <wsgi-async>`.
In addition, the module contains several utilities which facilitate the
development of server side asynchronous web applications.

:class:`pulsar.apps.wsgi.WSGIServer` is the server class and it is all you need
if you have a WSGI handler to run. It can be used
with any web framework, synchronous or asynchronous.
For example, the :mod:`pulsar.apps.pulse` module implements a django_
application for serving django sites with pulsar. 

.. toctree::
   :maxdepth: 2
   
   intro
   async
   handlers
   wrappers
   content
   middleware
   tools
   
   
.. _django: https://www.djangoproject.com/