'''Original code from https://github.com/benoitc/http-parser

2011 (c) Benoît Chesneau <benoitc@e-engura.org>

Permission is hereby granted, free of charge, to any person
obtaining a copy of this software and associated documentation
files (the "Software"), to deal in the Software without
restriction, including without limitation the rights to use,
copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the
Software is furnished to do so, subject to the following
conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
OTHER DEALINGS IN THE SOFTWARE.

Modified and adapted for pulsar.
'''
import os
import sys
from http cimport *

ispy3k = sys.version_info[0] == 3

if ispy3k:
    from urllib.parse import urlsplit
else:
    from urlparse import urlsplit

    
class _ParserData:

    def __init__(self, decompress=False):
        self.url = ""
        self.body = []
        self.headers = {}
        self.environ = {}
        
        self.decompress = decompress
        self.decompressobj = None

        self.chunked = False

        self.headers_complete = False
        self.partial_body = False
        self.message_begin = False
        self.message_complete = False
        
        self._last_field = ""
        self._last_was_value = False
        
        
cdef class HttpParser:
    """Cython HTTP parser wrapping http_parser."""

    cdef http_parser _parser
    cdef http_parser_settings _settings
    cdef object _data

    cdef str _path
    cdef str _query_string
    cdef str _fragment
    cdef object _parsed_url

    def __init__(self, kind=2, decompress=False):
        """ constructor of HttpParser object.
:attr kind: Int,  could be 0 to parseonly requests, 
1 to parse only responses or 2 if we want to let
the parser detect the type. 
        """
        # set parser type
        if kind == 2:
            parser_type = HTTP_BOTH
        elif kind == 1:
            parser_type = HTTP_RESPONSE
        elif kind == 0:
            parser_type = HTTP_REQUEST

        # initialize parser
        http_parser_init(&self._parser, parser_type)
        self._data = _ParserData(decompress=decompress)
        self._parser.data = <void *>self._data
        self._parsed_url = None
        self._path = ""
        self._query_string = ""
        self._fragment = ""

        # set callback
        self._settings.on_url = <http_data_cb>on_url_cb
        self._settings.on_body = <http_data_cb>on_body_cb
        self._settings.on_header_field = <http_data_cb>on_header_field_cb
        self._settings.on_header_value = <http_data_cb>on_header_value_cb
        self._settings.on_headers_complete = <http_cb>on_headers_complete_cb
        self._settings.on_message_begin = <http_cb>on_message_begin_cb
        self._settings.on_message_complete = <http_cb>on_message_complete_cb

    def execute(self, char *data, size_t length):
        """ Execute the parser with the last chunk. We pass the length
        to let the parser know when EOF has been received. In this case
        length == 0.

        :return recved: Int, received length of the data parsed. if
        recvd != length you should return an error.
        """
        return http_parser_execute(&self._parser, &self._settings,
                data, length)

    def get_version(self):
        """ get HTTP version """
        return (self._parser.http_major, self._parser.http_minor)

    def get_method(self):
        """ get HTTP method as string"""
        return http_method_str(<http_method>self._parser.method)

    def get_status_code(self):
        """ get status code of a response as integer """
        return self._parser.status_code

    def get_url(self):
        """ get full url of the request """
        return self._data.url

    def maybe_parse_url(self):
        raw_url = self.get_url()
        if not self._parsed_url and raw_url:
            parts = urlsplit(raw_url)
            self._parsed_url = parts
            self._path = parts.path or ""
            self._query_string = parts.query or ""
            self._fragment = parts.fragment or ""

    def get_path(self):
        """ get path of the request (url without query string and
        fragment """
        self.maybe_parse_url()
        return self._path

    def get_query_string(self):
        """ get query string of the url """
        self.maybe_parse_url()
        return self._query_string

    def get_fragment(self):
        """ get fragment of the url """
        self.maybe_parse_url()
        return self._fragment

    def get_headers(self):
        """get request/response headers dictionary."""
        return list(self._data.headers.items())
    
    def get_protocol(self):
        return None
    
    def get_body(self):
        return self._data.body

    def recv_body(self):
        """ return last chunk of the parsed body"""
        body = b''.join(self._data.body)
        self._data.body = []
        self._data.partial_body = False
        return body

    def recv_body_into(self, barray):
        """ Receive the last chunk of the parsed body and store the data
        in a buffer rather than creating a new string. """
        l = len(barray)
        body = b''.join(self._data.body)
        m = min(len(body), l)
        data, rest = body[:m], body[m:]
        barray[0:m] = data
        if not rest:
            self._data.body = []
            self._data.partial_body = False
        else:
            self._data.body = [rest]
        return m

    def is_upgrade(self):
        """ Do we get upgrade header in the request. Useful for
        websockets """
        return self._parser_upgrade

    def is_headers_complete(self):
        """ return True if all headers have been parsed. """ 
        return self._data.headers_complete

    def is_partial_body(self):
        """ return True if a chunk of body have been parsed """
        return self._data.partial_body

    def is_message_begin(self):
        """ return True if the parsing start """
        return self._data.message_begin

    def is_message_complete(self):
        """ return True if the parsing is done (we get EOF) """
        return self._data.message_complete

    def is_chunked(self):
        """ return True if Transfer-Encoding header value is chunked"""
        te = self._data.headers.get('transfer-encoding', '').lower()
        return te == 'chunked'
