.. _apps-http:

=============================
Asynchronous HTTP client
=============================

Pulsar ships with a fully featured, :class:`.HttpClient`
class for multiple asynchronous HTTP requests. The client has an
has no dependencies and API
very similar to python requests_ library.

.. contents::
   :local:
   :depth: 2


Getting Started
==================

To get started, one builds a client for multiple sessions::

    from pulsar.apps import http
    sessions = http.HttpClient()

and than makes requests, in a coroutine::

    async def mycoroutine():
        ...
        response = await sessions.get('http://www.bbc.co.uk')
        return response.text()


The ``response`` is an :class:`HttpResponse` object which contains all the
information about the request and the result:

    >>> request = response.request
    >>> print(request.headers)
    Connection: Keep-Alive
    User-Agent: pulsar/0.8.2-beta.1
    Accept-Encoding: deflate, gzip
    Accept: */*
    >>> response.status_code
    200
    >>> print(response.headers)
    ...

The :attr:`~.ProtocolConsumer.request` attribute of :class:`HttpResponse`
is an instance of the original :class:`.HttpRequest`.

Passing Parameters In URLs
=============================

You can attach parameters to the ``url`` by passing the
``params`` dictionary::

    response = sessions.get('http://bla.com',
                            params={'page': 2, 'key': 'foo'})
    response.url   // 'http://bla.com?page=2&key=foo'


You can also pass a list of items as a value:

   params = {key1': 'value1', 'key2': ['value2', 'value3']}
   response = sessions.get('http://bla.com', params=params)
   response.url   // http://bla.com?key1=value1&key2=value2&key2=value3


Posting simple data
=============================

Posting data is as simple as passing the ``data`` parameter::

    sessions.post(..., data={'entry1': 'bla', 'entry2': 'doo'})


Posting JSON data
=============================

Posting data is as simple as passing the ``data`` parameter::

    sessions.post(..., json={'entry1': 'bla', 'entry2': 'doo'})

Posting file data
=============================

Posting data is as simple as passing the ``data`` parameter::

    files = {'file': open('report.xls', 'rb')}
    sessions.post(..., files=files)



.. _http-cookie:

Cookie support
================

Cookies are handled by storing cookies received with responses in a sessions
object. To disable cookie one can pass ``store_cookies=False`` during
:class:`.HttpClient` initialisation.

If a response contains some Cookies, you can get quick access to them::

    response = await sessions.get(...)
    type(response.cookies)
    <type 'dict'>

To send your own cookies to the server, you can use the cookies parameter::

    response = await sessions.get(..., cookies={'sessionid': 'test'})


.. _http-authentication:

Authentication
======================

Authentication, either ``basic`` or ``digest``, can be added
by passing the ``auth`` parameter during a request. For basic authentication::

    sessions.get(..., auth=('<username>','<password>'))

same as::

    from pulsar.apps.http import HTTPBasicAuth

    sessions.get(..., auth=HTTPBasicAuth('<username>','<password>'))

or digest::

    from pulsar.apps.http import HTTPDigestAuth

    sessions.get(..., auth=HTTPDigestAuth('<username>','<password>'))


In either case the authentication is handled by adding additional headers
to your requests.

TLS/SSL
=================
Supported out of the box::

    sessions.get('https://github.com/timeline.json')

The :class:`.HttpClient` can verify SSL certificates for HTTPS requests,
just like a web browser. To check a host's SSL certificate, you can use the
``verify`` argument::

    sessions = HttpClient()
    sessions.verify       // True
    sessions = HttpClient(verify=False)
    sessions.verify       // False

By default, ``verify`` is set to True.

You can override the ``verify`` argument during requests too::

    sessions.get('https://github.com/timeline.json')
    sessions.get('https://locahost:8020', verify=False)

You can pass ``verify`` the path to a CA_BUNDLE file or directory with
certificates of trusted CAs::

    sessions.get('https://locahost:8020', verify='/path/to/ca_bundle')


.. _http-streaming:

Streaming
=========================

This is an event-driven client, therefore streaming support is native.

The raw stream
~~~~~~~~~~~~~~~~~~~~~

The easyiest way to use streaming is to pass the ``stream=True`` parameter
during a request and access the :attr:`HttpResponse.raw` attribute.
For example::

    async def body_coroutine(url):
        # wait for response headers
        response = await sessions.get(url, stream=True)
        #
        async for data in response.raw:
           # data is a chunk of bytes
           ...


The ``raw`` attribute is an asynchronous iterable over bytes and it can be
iterated once only. When iterating over a ``raw`` attribute which has
been already iterated, :class:`.StreamConsumedError` is raised.

The attribute has the ``read`` method for reading the whole body at once::

      await response.raw.read()

Data processed hook
~~~~~~~~~~~~~~~~~~~~~

Another approach to streaming is to use the
:ref:`data_processed <http-many-time-events>` event handler.
For example::

    def new_data(response, **kw):
        if response.status_code == 200:
            data = response.recv_body()
            # do something with this data

    response = sessions.get(..., data_processed=new_data)

The response :meth:`~.HttpResponse.recv_body` method fetches the parsed body
of the response and at the same time it flushes it.
Check the :ref:`proxy server <tutorials-proxy-server>` example for an
application using the :class:`HttpClient` streaming capabilities.

.. _http-websocket:

WebSocket
==============

The http client support websocket upgrades. First you need to have a
websocket handler, a class derived from :class:`.WS`::

    from pulsar.apps import ws

    class Echo(ws.WS):

        def on_message(self, websocket, message):
            websocket.write(message)

The websocket response is obtained by::

    ws = await sessions.get('ws://...', websocket_handler=Echo())

.. _http-redirects:

Redirects & Decompression
=============================

[TODO]

Synchronous Mode
=====================

Can be used in :ref:`synchronous mode <tutorials-synchronous>`::

    sessions = HttpClient(loop=new_event_loop())

Events
==============
:ref:`Events <event-handling>` control the behaviour of the
:class:`.HttpClient` when certain conditions occur. They are useful for
handling standard HTTP event such as :ref:`redirects <http-redirects>`,
:ref:`websocket upgrades <http-websocket>`,
:ref:`streaming <http-streaming>` or anything your application
requires.

.. _http-one-time-events:

One time events
~~~~~~~~~~~~~~~~~~~

There are three :ref:`one time events <one-time-event>` associated with an
:class:`HttpResponse` object:

* ``pre_request``, fired before the request is sent to the server. Callbacks
  receive the *response* argument.
* ``on_headers``, fired when response headers are available. Callbacks
  receive the *response* argument.
* ``post_request``, fired when the response is done. Callbacks
  receive the *response* argument.

Adding event handlers can be done at sessions level::

    def myheader_handler(response, exc=None):
        if not exc:
            print('got headers!')

    sessions.bind_event('on_headers', myheader_handler)

or at request level::

    sessions.get(..., on_headers=myheader_handler)

By default, the :class:`HttpClient` has one ``pre_request`` callback for
handling `HTTP tunneling`_, three ``on_headers`` callbacks for
handling *100 Continue*, *websocket upgrade* and :ref:`cookies <http-cookie>`,
and one ``post_request`` callback for handling redirects.

.. _http-many-time-events:

Many time events
~~~~~~~~~~~~~~~~~~~

In addition to the three :ref:`one time events <http-one-time-events>`,
the :class:`.HttpClient` supports two additional
events which can occur several times while processing a given response:

* ``data_received`` is fired when new data has been received but not yet
  parsed
* ``data_processed`` is fired just after the data has been parsed by the
  :class:`.HttpResponse`. This is the event one should bind to when performing
  :ref:`http streaming <http-streaming>`.


both events support handlers with a signature::

    def handler(response, data=None):
        ...

where ``response`` is the :class:`.HttpResponse` handling the request and
``data`` is the **raw** data received.

.. module:: pulsar.apps.http

API
==========

The main classes here are the :class:`.HttpClient`, a subclass of
:class:`.AbstractClient`, the :class:`.HttpResponse`, returned by http
requests and the :class:`.HttpRequest`.


HTTP Client
~~~~~~~~~~~~~~~~~~

.. autoclass:: HttpClient
   :members:
   :member-order: bysource


HTTP Request
~~~~~~~~~~~~~~~~~~

.. autoclass:: HttpRequest
   :members:
   :member-order: bysource

HTTP Response
~~~~~~~~~~~~~~~~~~

.. autoclass:: HttpResponse
   :members:
   :member-order: bysource


.. _module:: pulsar.apps.http.oauth

OAuth1
~~~~~~~~~~~~~~~~~~

.. autoclass:: OAuth1
   :members:
   :member-order: bysource

OAuth2
~~~~~~~~~~~~~~~~~~

.. autoclass:: OAuth2
   :members:
   :member-order: bysource


.. _requests: http://docs.python-requests.org/
.. _`uri scheme`: http://en.wikipedia.org/wiki/URI_scheme
.. _`HTTP tunneling`: http://en.wikipedia.org/wiki/HTTP_tunnel
