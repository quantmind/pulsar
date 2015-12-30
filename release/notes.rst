Api
-------------

HttpClient
~~~~~~~~~~~~~
* Added ``content`` attribute to ``HttpResponse``, in line with requests
* Ability to pass ``stream=True`` during to a request, same API as python requests_
* Added the ``raw`` property to the Http Response, it can be used in conjunction with
  ``stream`` to stream http data. Similar API to requests_
* Renamed ``proxy_info`` to ``proxies``, same API as python requests_
* You can now pass ``proxies`` dictionary during a request

Protocols
~~~~~~~~~~~~~
* Added ``debug`` properties to all ``AsyncObject``. The property returns the event loop
  debug mode flag

.. _requests: http://docs.python-requests.org/
