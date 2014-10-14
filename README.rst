.. image:: http://quantmind.github.io/pulsar/images/pulsar-banner.svg
   :alt: pulsar
   :width: 90%

Event driven concurrent framework for python.
With pulsar you can write asynchronous servers performing one or several
activities in different threads and/or processes.

:Master CI: |master-build|_ |coverage-master|
:Dev CI: |dev-build|_ |coverage-dev|
:Documentation: http://pythonhosted.org/pulsar/
:Downloads: http://pypi.python.org/pypi/pulsar
:Source: https://github.com/quantmind/pulsar
:Mailing list: `google user group`_
:Design by: `Quantmind`_ and `Luca Sbardella`_
:Platforms: Linux, OSX, Windows. Python 2.7, 3.3, 3.4 and pypy_
:Keywords: client, server, asynchronous, concurrency, actor, thread, process,
    socket, task queue, wsgi, websocket, redis, json-rpc


.. |master-build| image:: https://api.travis-ci.org/quantmind/pulsar.png?branch=master
.. _master-build: http://travis-ci.org/quantmind/pulsar
.. |dev-build| image:: https://api.travis-ci.org/quantmind/pulsar.png?branch=dev
.. _dev-build: http://travis-ci.org/quantmind/pulsar
.. |coverage-master| image:: https://coveralls.io/repos/quantmind/pulsar/badge.png?branch=master
  :target: https://coveralls.io/r/quantmind/pulsar?branch=master
.. |coverage-dev| image:: https://coveralls.io/repos/quantmind/pulsar/badge.png?branch=dev
  :target: https://coveralls.io/r/quantmind/pulsar?branch=dev


An example of a web server written with ``pulsar`` which responds with
"Hello World!" for every request::


    from pulsar.apps import wsgi

    def hello(environ, start_response):
        data = b'Hello World!\n'
        response_headers = [
            ('Content-type','text/plain'),
            ('Content-Length', str(len(data)))
        ]
        start_response('200 OK', response_headers)
        return [data]


    if __name__ == '__main__':
        wsgi.WSGIServer(callable=hello).start()


Pulsar's goal is to provide an easy way to build scalable network programs.
In the ``Hello world!`` web server example above, many client
connections can be handled concurrently.
Pulsar tells the operating system (through epoll or select) that it should be
notified when a new connection is made, and then it goes to sleep.

Pulsar uses the multiprocessing_ module from the standard python library and
it can be configured to run in multi-processing mode, multi-threading mode or
a combination of the two.

Installing
============

Pulsar requires and install the following packages:

* trollius_
* asyncio_ (python 3.3 only)

Install via ``pip``::

    pip install pulsar

or downloading the tarball from pypi_.

If cython_ is available, c extensions will be compiled and installed.


Applications
==============
Pulsar design allows for a host of different asynchronous applications
to be implemented in an elegant and efficient way.
Out of the box it is shipped with the the following:

* Socket servers
* WSGI server
* JSON-RPC
* Web Sockets
* Task queue
* Shell
* Test suite
* Data stores
* django_ integration

.. _examples:

Examples
=============
Check out the ``examples`` directory for various working applications.
It includes:

* Hello world! wsgi example
* An Httpbin wsgi application
* An HTTP Proxy server
* A JSON-RPC Calculator server
* A taskqueue application with a JSON-RPC interface
* Websocket random graph.
* Websocket chat room.
* django_ web site with a websocket based chat room.
* A web mail application which uses twisted_ IMAP4 API.
* The `dining philosophers problem <http://en.wikipedia.org/wiki/Dining_philosophers_problem>`_.
* Asynchronous shell.


Design
=============
Pulsar internals are based on `actors primitive`_. ``Actors`` are the *atoms*
of pulsar's concurrent computation, they do not share state between them,
communication is achieved via asynchronous inter-process message passing,
implemented using the standard python socket library.

Two special classes of actors are the ``Arbiter``, used as a singleton_,
and the ``Monitor``, a manager of several actors performing similar functions.
The Arbiter runs the main eventloop and it controls the life of all actors.
Monitors manage group of actors performing similar functions, You can think
of them as a pool of actors.

More information about design and philosophy in the documentation.


Add-ons
=========
Pulsar checks if some additional libraries are available at runtime, and
uses them to add additional functionalities or improve performance:

* setproctitle_: if installed, pulsar can use it to change the processes names
  of the running application.
* psutil_: if installed, a ``system`` key is available in the dictionary
  returned by Actor info method.
* ujson_: if installed it is used instead of the native ``json`` module.
* django_: required by the ``pulsar.apps.pulse`` application.
* unidecode_: to enhance the ``slugify`` function


Running Tests
==================
Pulsar test suite uses the pulsar test application.
If not running on python 3.4 or above the mock_ is needed. To run tests::

    python runtests.py

For options and help type::

    python runtests.py -h

pep8_ check (requires pep8 package)::

    python runtests.py --pep8 pulsar


.. _kudo:

Kudos
============
Pulsar project started as a fork of gunicorn_
and since version 0.5 has been implemented on top of asyncio
(tulip_ and PEP-3156_).
Pulsar uses several snippet of code from around the open-source
community, in particular:

* A python HTTP Parser originally written by benoitc_.
* A ``url`` Rule class originally from werkzeug_.

.. _contributing:

Contributing
=================
Development of pulsar_ happens at Github. We very much welcome your contribution
of course. To do so, simply follow these guidelines:

* Fork pulsar_ on github
* Create a topic branch ``git checkout -b my_branch``
* Push to your branch ``git push origin my_branch``
* Create an issue at https://github.com/quantmind/pulsar/issues with
  pull request for the **dev branch**.

A good ``pull`` request should:

* Cover one bug fix or new feature only
* Include tests to cover the new code (inside the ``tests`` directory)
* Preferably have one commit only (you can use rebase_ to combine several
  commits into one)
* Make sure ``pep8`` tests pass::

    python runtests.py --pep8 pulsar examples tests

.. _license:

License
=============
This software is licensed under the BSD_ 3-clause License. See the LICENSE
file in the top distribution directory for the full license text.

.. _asyncio: https://pypi.python.org/pypi/asyncio
.. _gunicorn: http://gunicorn.org/
.. _nodejs: http://nodejs.org/
.. _twisted: http://twistedmatrix.com/trac/
.. _multiprocessing: http://docs.python.org/library/multiprocessing.html
.. _`actors primitive`: http://en.wikipedia.org/wiki/Actor_model
.. _mock: http://pypi.python.org/pypi/mock
.. _setproctitle: http://code.google.com/p/py-setproctitle/
.. _psutil: http://code.google.com/p/psutil/
.. _pypi: http://pypi.python.org/pypi/pulsar
.. _pypy: http://pypy.org/
.. _BSD: http://opensource.org/licenses/BSD-3-Clause
.. _pulsar: https://github.com/quantmind/pulsar
.. _singleton: http://en.wikipedia.org/wiki/Singleton_pattern
.. _benoitc: https://github.com/benoitc
.. _werkzeug: http://werkzeug.pocoo.org/
.. _django: https://www.djangoproject.com/
.. _tulip: https://code.google.com/p/tulip/
.. _pep-3156: http://www.python.org/dev/peps/pep-3156/
.. _cython: http://cython.org/
.. _`google user group`: https://groups.google.com/forum/?fromgroups#!forum/python-pulsar
.. _pep8: http://www.python.org/dev/peps/pep-0008/
.. _ujson: https://pypi.python.org/pypi/ujson
.. _trollius: https://pypi.python.org/pypi/trollius
.. _rebase: https://help.github.com/articles/about-git-rebase
.. _unidecode: https://pypi.python.org/pypi/Unidecode
.. _`Luca Sbardella`: http://lucasbardella.com
.. _`Quantmind`: http://quantmind.com
