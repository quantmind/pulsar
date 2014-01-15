'''
A WSGI Middleware is a function or callable object similar to a
:ref:`WSGI application handlers <wsgi-handlers>`
with the only difference in that it can also return ``None``.

Middleware can be used in conjunction with a
:ref:`WsgiHandler <wsgi-handler>` or any
other handler which iterate through a list of middleware in a similar
way (for example django wsgi handler).

.. important::

    An asynchronous WSGI middleware is a callble accepting a WSGI
    ``environ`` and ``start_response`` as the only input paramaters and
    it must returns an :ref:`asynchronous iterator <wsgi-async-iter>`
    or nothing.

The two most important wsgi middleware in pulsar are:

* the :ref:`Router <wsgi-router>` for serving dynamic web applications
* the :ref:`MediaRouter <wsgi-media-router>` for serving static files

In addition, pulsar provides with the following four middlewares which don't
serve requests, instead they perform initialisation and sanity checks.


.. _wsgi-additional-middleware:

Clean path
~~~~~~~~~~~~~~~~~~
.. autofunction:: clean_path_middleware

Cookie
~~~~~~~~~~~~~~~~~~
.. autofunction:: cookies_middleware

Authorization
~~~~~~~~~~~~~~~~~~
.. autofunction:: authorization_middleware


.. _wait-for-body-middleware:

Wait for request body
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. autofunction:: wait_for_body_middleware

'''
import re

import pulsar
from pulsar import async, coroutine_return
from pulsar.utils.httpurl import BytesIO, parse_cookie

from .auth import parse_authorization_header


__all__ = ['clean_path_middleware',
           'cookies_middleware',
           'authorization_middleware',
           'wait_for_body_middleware']


def clean_path_middleware(environ, start_response=None):
    '''Clean url from double slashes and redirect if needed.'''
    path = environ['PATH_INFO']
    if path and '//' in path:
        url = re.sub("/+", '/', path)
        if not url.startswith('/'):
            url = '/%s' % url
        qs = environ['QUERY_STRING']
        if qs:
            url = '%s?%s' % (url, qs)
        raise pulsar.HttpRedirect(url)


def cookies_middleware(environ, start_response=None):
    '''Parse the ``HTTP_COOKIE`` key in ``environ``.

    Set the new ``http.cookie`` key in ``environ`` with a dictionary
    of cookies obtained via the :func:`pulsar.utils.httpurl.parse_cookie`
    function.
    '''
    c = environ.get('http.cookie')
    if not isinstance(c, dict):
        c = environ.get('HTTP_COOKIE', '')
        if not c:
            c = {}
        else:
            if not isinstance(c, str):
                c = c.encode('utf-8')
            c = parse_cookie(c)
        environ['http.cookie'] = c


def authorization_middleware(environ, start_response=None):
    '''Parse the ``HTTP_AUTHORIZATION`` key in the ``environ``.

    If available, set the ``http.authorization`` key in ``environ`` with
    the result obtained from
    :func:`pulsar.apps.wsgi.auth.parse_authorization_header` function.
    '''
    key = 'http.authorization'
    c = environ.get(key)
    if c is None:
        code = 'HTTP_AUTHORIZATION'
        if code in environ:
            environ[key] = parse_authorization_header(environ[code])


def wait_for_body_middleware(environ, start_response=None):
    '''Use this middleware to wait for the full body.

    This middleware wait for the full body to be received before letting
    other middleware to be processed.

    Useful when using synchronous web-frameworks.
    '''
    if environ['wsgi.input']:
        return async(_wait_for_body_middleware(environ, start_response))


def _wait_for_body_middleware(environ, start_response):
    stream = environ['wsgi.input']
    chunk = yield stream.read()
    environ['wsgi.input'] = BytesIO(chunk)
    coroutine_return(None)
