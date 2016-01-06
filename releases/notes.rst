Core
-----------------
* Added ``debug`` properties to all ``AsyncObject``. The property returns the event loop
  debug mode flag

HttpClient
----------------
* Backward incompatible changes with API much closer to requests_ and far better support for streaming both uploads and downloads
* Added ``content`` attribute to ``HttpResponse``, in line with requests_
* Ability to pass ``stream=True`` during a request, same API as python requests_
* Added the ``raw`` property to the Http Response, it can be used in conjunction with
  ``stream`` to stream http data. Similar API to requests_
* Renamed ``proxy_info`` to ``proxies``, same API as python requests_
* You can now pass ``proxies`` dictionary during a request
* Stream uploads by passing a generator as ``data`` parameter
* Better websocket upgrade process
* Tidy up ``CONNECT`` request url (for tunneling)
* Added tests for proxyserver example using requests_

WSGI
------
* Both ``wsgi`` and ``http`` apps use the same ``pulsar.utils.httpurl.http_chunks``
  function for transfer-encoding ``chunked``
* ``render_error`` escapes the Exception message to prevent XSS_

Data Store
-----------
* Better ``pulsards_url`` function, default value form ``cfg.data_store``
* ``key_value_save`` set to empty list by default (no persistence)

Examples
-------------
* Refactored proxy server example
* Updated django chat example so that warning are no longer issued

.. _requests: http://docs.python-requests.org/
.. _XSS: https://en.wikipedia.org/wiki/Cross-site_scripting
