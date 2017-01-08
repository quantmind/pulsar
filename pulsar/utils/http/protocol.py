import zlib
import re

from multidict import CIMultiDict


METHOD_RE = re.compile("[A-Z0-9$-_.]{3,20}")
VERSION_RE = re.compile("HTTP/(\d+).(\d+)")
STATUS_RE = re.compile("(\d{3})\s*(\w*)")
HEADER_RE = re.compile("[\x00-\x1F\x7F()<>@,;:\[\]={} \t\\\\\"]")

CHARSET = 'ISO-8859-1'


class Protocol:

    def __init__(self, decompress=True):
        self.headers = CIMultiDict()
        self._decompress = decompress

    def on_message_begin(self):
        pass

    def on_header(self, name, value):
        name = name.encode(CHARSET)
        if HEADER_RE.search(name):
            raise InvalidHeader("invalid header name %s" % name)
        self.headers[name.encode(CHARSET)] = value.encode(CHARSET)

    def on_headers_complete(self):
        pass

    def decompress(self):
        if self._decompress and 'Content-Encoding' in self._headers:
            encoding = self._headers['Content-Encoding']
            if encoding == "gzip":
                self.__decompress_obj = zlib.decompressobj(16 + zlib.MAX_WBITS)
                self.__decompress_first_try = False
            elif encoding == "deflate":
                self.__decompress_obj = zlib.decompressobj()
