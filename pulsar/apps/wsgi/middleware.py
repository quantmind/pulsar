'''Response middleware
'''
import re
from gzip import GzipFile

from pulsar.utils.py2py3 import BytesIO

re_accepts_gzip = re.compile(r'\bgzip\b')


__all__ = ['AccessControl','GZipMiddleware','is_streamed']


def is_streamed(content):
    try:
        len(content)
    except TypeError:
        return True
    return False


class AccessControl(object):
    '''A response middleware which add the ``Access-Control-Allow-Origin``
response header.'''
    def __init__(self, origin = '*', methods = None):
        self.origin = origin
        self.methods = methods
        
    def __call__(self, environ, start_response, response):
        if response.status_code != 200:
            return
        response.headers['Access-Control-Allow-Origin'] = self.origin
        if self.methods:
            response.headers['Access-Control-Allow-Methods'] = self.methods
  

class GZipMiddleware(object):
    """Response middleware for compressing content if the browser allows
gzip compression. It sets the Vary header accordingly, so that caches will
base their storage on the Accept-Encoding header.
    """
    def __init__(self, min_length = 200):
        self.min_length = min_length
        
    def __call__(self, environ, start_response, response):
        # It's not worth compressing non-OK or really short responses.
        content = response.content
        if not content or response.status_code != 200 or is_streamed(content):
            return
        content = b''.join(content)
        if len(content) < self.min_length:
            return
        
        headers = response.headers
        # Avoid gzipping if we've already got a content-encoding.
        if 'Content-Encoding' in headers:
            return
        
        # MSIE have issues with gzipped response of various content types.
        if "msie" in environ.get('HTTP_USER_AGENT', '').lower():
            ctype = headers.get('Content-Type', '').lower()
            if not ctype.startswith("text/") or "javascript" in ctype:
                return

        ae = environ.get('HTTP_ACCEPT_ENCODING', '')
        if not re_accepts_gzip.search(ae):
            return
        
        if hasattr(headers,'add'):
            headers.add('Vary','Accept-Encoding')
        else:
            #TODO
            #need to uppend
            headers['Vary'] = 'Accept-Encoding'
            
        response.content = self.compress_string(content)
        response.headers['Content-Encoding'] = 'gzip'
    
    # From http://www.xhaus.com/alan/python/httpcomp.html#gzip
    def compress_string(self, s):
        zbuf = BytesIO()
        zfile = GzipFile(mode='wb', compresslevel=6, fileobj=zbuf)
        zfile.write(s)
        zfile.close()
        return zbuf.getvalue()
