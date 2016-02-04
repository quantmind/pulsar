'''
.. _wsgi-response-middleware:

Response middleware are callable objects which can be used in conjunction
with pulsar :ref:`WsgiHandler <wsgi-handler>`.
They must return a :ref:`WsgiResponse <wsgi-response>` which can be the
same as the one passed to the callable or a brand new one.

Interface
=================

.. autoclass:: ResponseMiddleware
   :members:
   :member-order: bysource

GZip Middleware
=================
.. autoclass:: GZipMiddleware
   :members:
   :member-order: bysource


AccessControl
=================
.. autoclass:: AccessControl
   :members:
   :member-order: bysource

'''
import re
from gzip import GzipFile

from pulsar.utils.httpurl import BytesIO


re_accepts_gzip = re.compile(r'\bgzip\b')
re_media_type = re.compile(r'^(image|audio|video)/.+')


__all__ = ['AccessControl', 'GZipMiddleware']


class ResponseMiddleware:
    '''Base class for response middlewares.

    A response middleware is used by a :ref:`WsgiHandler <wsgi-handler>`,
    it is a callable used to manipulate a :ref:`WsgiResponse <wsgi-response>`.

    The focus of this class is the :meth:`execute` method where
    the middleware logic is implemented.
    '''
    def version(self, environ):
        return environ.get('wsgi.version')

    def available(self, environ, response):
        '''Check if this :class:`ResponseMiddleware` can be applied to
        the ``response`` object.

        :param environ: a WSGI environ dictionary.
        :param response: a :class:`pulsar.apps.wsgi.wrappers.WsgiResponse`
        :return: ``True`` or ``False``.
        '''
        return True

    def __call__(self, environ, response):
        if not self.available(environ, response):
            return response
        resp = self.execute(environ, response)
        return resp if resp is not None else response

    def execute(self, environ, response):
        '''Manipulate *response*, called only if the :meth:`available`
method returns ``True``.'''
        pass


class AccessControl(ResponseMiddleware):
    '''A response middleware which add the ``Access-Control-Allow-Origin``
    response header.
    '''
    def __init__(self, origin='*', methods=None):
        self.origin = origin
        self.methods = methods

    def available(self, environ, response):
        return response.status_code == 200

    def execute(self, environ, response):
        response.headers['Access-Control-Allow-Origin'] = self.origin
        if self.methods:
            response.headers['Access-Control-Allow-Methods'] = self.methods


class GZipMiddleware(ResponseMiddleware):
    """A :class:`ResponseMiddleware` for compressing content if the request
allows gzip compression. It sets the Vary header accordingly.

The compression implementation is from
http://jython.xhaus.com/http-compression-in-python-and-jython
    """
    def __init__(self, min_length=200):
        self.min_length = min_length

    def available(self, environ, response):
        # It's not worth compressing non-OK or really short responses
        try:
            if response.status_code == 200 and not response.is_streamed:
                if response.length() < self.min_length:
                    return False
                headers = response.headers
                ctype = headers.get('Content-Type', '').lower()
                # Avoid gzipping if we've already got a content-encoding.
                if 'Content-Encoding' in headers:
                    return False
                # MSIE have issues with gzipped response of various
                # content types.
                if "msie" in environ.get('HTTP_USER_AGENT', '').lower():
                    if not ctype.startswith("text/") or "javascript" in ctype:
                        return False
                ae = environ.get('HTTP_ACCEPT_ENCODING', '')
                if not re_accepts_gzip.search(ae):
                    return False
                if re_media_type.match(ctype):
                    return False
                return True
        except Exception:
            raise

    def execute(self, environ, response):
        headers = response.headers
        headers.add_header('Vary', 'Accept-Encoding')
        content = b''.join(response.content)
        response.content = (self.compress_string(content),)
        response.headers['Content-Encoding'] = 'gzip'

    def compress_string(self, s):
        zbuf = BytesIO()
        zfile = GzipFile(mode='wb', compresslevel=6, fileobj=zbuf)
        zfile.write(s)
        zfile.close()
        return zbuf.getvalue()
