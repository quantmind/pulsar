'''Response middleware
'''
import re
from gzip import GzipFile

from pulsar.utils.py2py3 import BytesIO

re_accepts_gzip = re.compile(r'\bgzip\b')


__all__ = ['AccessControl','GZipMiddleware']


class AccessControl(object):
    '''A response middleware which add the ``Access-Control-Allow-Origin``
response header.'''
    def __init__(self, origin = '*', methods = None):
        self.origin = origin
        self.methods = methods
        
    def __call__(self, response):
        if response.status_code != 200:
            return
        response.headers['Access-Control-Allow-Origin'] = self.origin
        if self.methods:
            response.headers['Access-Control-Allow-Methods'] = self.methods
                       

class GZipMiddleware(object):
    """
    This middleware compresses content if the browser allows gzip compression.
    It sets the Vary header accordingly, so that caches will base their storage
    on the Accept-Encoding header.
    """
    def __init__(self, min_length = 200):
        self.min_length = min_length
        
    def __call__(self, response):
        # It's not worth compressing non-OK or really short responses.
        if response.status_code != 200 or response.is_streamed:
            return response
        
        headers = response.headers
        headers.add('Vary','Accept-Encoding')

        # Avoid gzipping if we've already got a content-encoding.
        if 'Content-Encoding' in response:
            return response
        
        environ = response.environ
        
        # MSIE have issues with gzipped response of various content types.
        if "msie" in environ.get('HTTP_USER_AGENT', '').lower():
            ctype = headers.get('Content-Type', '').lower()
            if not ctype.startswith("text/") or "javascript" in ctype:
                return

        ae = environ.get('HTTP_ACCEPT_ENCODING', '')
        if not re_accepts_gzip.search(ae):
            return response

        content = response.content
        if not content:
            return response
        
        c = b''.join(content)
        if len(c) < self.min_length:
            return
        
        response.content = (self.compress_string(c),)
        response.headers['Content-Encoding'] = 'gzip'
    
    # From http://www.xhaus.com/alan/python/httpcomp.html#gzip
    def compress_string(self, s):
        zbuf = BytesIO()
        zfile = GzipFile(mode='wb', compresslevel=6, fileobj=zbuf)
        zfile.write(s)
        zfile.close()
        return zbuf.getvalue()
