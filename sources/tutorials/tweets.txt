.. _tutorials-tweets:

==================================
Twitter Streaming
==================================

The code for this example is located in the :mod:`examples.tweets` module.
This example uses the streaming capabilities of the :class:`.HttpClient`
to hook into `twitter streaming api`_ and send tweets into a processing queue.

.. image:: /_static/monitors.svg
  :width: 600 px

To run the server::

    python3 manage.py

Alternatively, one could use redis_ as pub/sub rather than pulsar datastore by::

    python3 manage.py --data-store redis://127.0.0.1:6379


Implementation
========================

This example requires the oauthlib_ library which is used by pulsar :class:`.HttpClient`
to authenticate with twitter OAuth_.

The server is composed of three, independent, pulsar :ref:`applications <design-application>`:

* A :class:`.WSGIServer` for serving:

  * the web page displaying tweets
  * the websocket url for streaming tweets.
  * Static files

  The wsgi callable is implemented in the :class:`.TwitterSite` handler.
* :ref:`Pulsar data store <pulsar-data-store>` for the publish/subscribe
  messaging paradigm. It is possible to replace this component with redis pub/sub
  by passing a redis url to the :ref:`data_store <setting-data_store>` parameter.
* The :class:`.Twitter` application implemented in this tutorial. The application connects to
  Twitter streaming API and **publish** tweets to the data store

These three applications have their own process domain and do not share state,
therefore they could be placed in different machines if necessary.

Twitter Application
========================

.. automodule:: examples.tweets.twitter

Twitter Site
========================

.. automodule:: examples.tweets.web


.. _`filtering tweets`: https://dev.twitter.com/docs/api/1.1/post/statuses/filter
.. _`twitter streaming api`: https://dev.twitter.com/streaming/overview
.. _oauthlib: https://pypi.python.org/pypi/oauthlib
.. _OAuth: https://dev.twitter.com/oauth
.. _redis: http://redis.io/
