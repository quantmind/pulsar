# -*- coding: utf-8 -
#
# This file is part of http-parser released under the MIT license. 
# See the NOTICE for more information.

from libc.stdlib cimport *
import os
from urllib import unquote

cdef extern from "Python.h":

    object PyString_FromStringAndSize(char *s, Py_ssize_t len)

cdef extern from "http_parser.h" nogil:
    
    cdef enum http_method:
        HTTP_DELETE, HTTP_GET, HTTP_HEAD, HTTP_POST, HTTP_PUT,
        HTTP_CONNECT, HTTP_OPTIONS, HTTP_TRACE, HTTP_COPY, HTTP_LOCK,
        HTTP_MKCOL, HTTP_MOVE, HTTP_PROPFIND, HTTP_PROPPATCH, HTTP_UNLOCK, 
        HTTP_REPORT, HTTP_MKACTIVITY, HTTP_CHECKOUT, HTTP_MERGE, HTTP_MSEARCH,
        HTTP_NOTIFY, HTTP_SUBSCRIBE, HTTP_UNSUBSCRIBE


    cdef enum http_parser_type:
        HTTP_REQUEST, HTTP_RESPONSE, HTTP_BOTH

    cdef struct http_parser:
        int content_length
        unsigned short http_major
        unsigned short http_minor
        unsigned short status_code
        unsigned char method
        char upgrade
        void *data

    ctypedef int (*http_data_cb) (http_parser*, char *at, size_t length)
    ctypedef int (*http_cb) (http_parser*)
    
    struct http_parser_settings:
        http_cb on_message_begin
        http_data_cb on_path
        http_data_cb on_query_string
        http_data_cb on_url
        http_data_cb on_fragment
        http_data_cb on_header_field
        http_data_cb on_header_value
        http_cb on_headers_complete
        http_data_cb on_body
        http_cb on_message_complete

    void http_parser_init(http_parser *parser, 
            http_parser_type ptype)
    
    size_t http_parser_execute(http_parser *parser, 
            http_parser_settings *settings, char *data,
            size_t len)

    int http_should_keep_alive(http_parser *parser)

    char *http_method_str(http_method)


cdef int on_path_cb(http_parser *parser, char *at,
        size_t length):
    res = <object>parser.data
    value = PyString_FromStringAndSize(at, length)

    res.path = value
    res.environ['PATH_INFO'] = unquote(value)
    
    if 'on_path' in res.callbacks:
        res.callbacks['on_path'](value)
    return 0

cdef int on_query_string_cb(http_parser *parser, char *at, 
        size_t length):
    res = <object>parser.data
    value = PyString_FromStringAndSize(at, length)
    res.query_string = value
    res.environ['QUERY_STRING'] = value
    
    if 'on_query_string' in res.callbacks:
        res.callbacks['on_query_string'](value)
    return 0

cdef int on_url_cb(http_parser *parser, char *at,
        size_t length):
    res = <object>parser.data
    value = PyString_FromStringAndSize(at, length)
    res.url = value
    res.environ['RAW_URI'] = value
    if 'on_url' in res.callbacks:
        res.callbacks['on_url'](value)
    return 0

cdef int on_fragment_cb(http_parser *parser, char *at, 
        size_t length):
    res = <object>parser.data
    value = PyString_FromStringAndSize(at, length)
    res.fragment = value
    
    if 'on_fragment' in res.callbacks:
        res.callbacks['on_fragment'](value)

    return 0

cdef int on_header_field_cb(http_parser *parser, char *at, 
        size_t length):
    header_field = PyString_FromStringAndSize(at, length)
    res = <object>parser.data
   
    if res._last_was_value:
        res._last_field = ""
    res._last_field += header_field
    res._last_was_value = False

    if 'on_header_field' in res.callbacks:
        res.callbacks['on_header_field'](header_field, res._state_is_value)


    return 0

cdef int on_header_value_cb(http_parser *parser, char *at, 
        size_t length):
    res = <object>parser.data
    header_value = PyString_FromStringAndSize(at, length)
    
    # update wsgi environ
    key =  res._last_field.upper().replace('-','_')
    if key not in ("CONTENT_LENGTH", "CONTENT_TYPE", "SCRIPT_NAME"):
        key = 'HTTP_' + key

    res.environ[key] = res.environ.get(key, '') + header_value

    # add to headers
    res.headers[res._last_field] = res.headers.get(res._last_field,
            '') + header_value

    if 'on_header_value' in res.callbacks:
        res.callbacks['on_header_value'](res._last_field, header_value)

    res._last_was_value = True
    return 0

cdef int on_headers_complete_cb(http_parser *parser):
    res = <object>parser.data
    res.headers_complete = True

    if 'on_headers_complete' in res.callbacks:
        res.callbacks['on_headers_complete']()

    return 0

cdef int on_message_begin_cb(http_parser *parser):
    res = <object>parser.data
    res.message_begin = True
    
    if 'on_message_begin' in res.callbacks:
        res.callbacks['on_message_begin']()
    return 0

cdef int on_body_cb(http_parser *parser, char *at, 
        size_t length):
    res = <object>parser.data
    value = PyString_FromStringAndSize(at, length)

    res.partial_body = True
    res.body.append(value)

    if 'on_body' in res.callbacks:
        res.callbacks['on_body'](value)

    return 0

cdef int on_message_complete_cb(http_parser *parser):
    res = <object>parser.data
    res.message_complete = True
    if 'on_message_complete' in res.callbacks:
        res.callbacks['on_message_complete']()

    return 0


class _ParserData(object):

    def __init__(self, callbacks=None):
        self.callbacks = callbacks or {}
        self.path = ""
        self.query_string = ""
        self.url = ""
        self.fragment = ""
        self.body = []
        self.headers = {}
        self.environ = {}

        self.headers_complete = False
        self.partial_body = False
        self.message_begin = False
        self.message_complete = False
        
        self._last_field = ""
        self._last_was_value = False
        
        
cdef class HttpParser:
    """ Low level HTTP parser.  """

    cdef http_parser _parser
    cdef http_parser_settings _settings
    cdef object _data

    def __init__(self, kind=2, callbacks=None):
        """ constructor of HttpParser object. 
        
        
        :attr kind: Int,  could be 0 to parseonly requests, 
        1 to parse only responses or 2 if we want to let
        the parser detect the type. 

        :attr callbacks: list of callbacks we want to pass to the
        parser::

            on_message_begin()
            on_path(path)
            on_query_string(query_string)
            on_url(url)
            on_fragment(fragment)
            on_header_field(field, last_was_value)
            on_header_value(key, value)
            on_headers_complete()
            on_body(chunk)
            on_message_complete()
        
        Callbacks are useful for those who want to parse an HTTP stream
        asynchronously.
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
        self._data = _ParserData(callbacks=None)
        self._parser.data = <void *>self._data

        # set callback
        self._settings.on_path = <http_data_cb>on_path_cb
        self._settings.on_query_string = <http_data_cb>on_query_string_cb
        self._settings.on_url = <http_data_cb>on_url_cb
        self._settings.on_fragment = <http_data_cb>on_fragment_cb
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

    def get_path(self):
        """ get path of the request (url without query string and
        fragment """
        return self._data.path

    def get_query_string(self):
        """ get query string of the url """
        return self._data.query_string

    def get_fragment(self):
        """ get fragment of the url """
        return self._data.fragment

    def get_headers(self):
        """ get request/response headers """
        return self._data.headers

    def get_wsgi_environ(self, initial=None):
        """ get WSGI environ based on the current request """
        environ = initial or {}
        environ.update(self._data.environ)

        script_name = environ.get('HTTP_SCRIPT_NAME', 
                os.environ.get("SCRIPT_NAME", ""))

        if script_name:
            path_info = self.get_path() 
            path_info = path_info.split(script_name, 1)[1]
            environ.update({
                'PATH_INFO': path_info,
                'SCRIPT_NAME': script_name})

        if environ.get('HTTP_X_FORWARDED_PROTOCOL', '').lower() == "ssl":
            environ['wsgi.url_scheme']= "https"
        elif environ.get('HTTP_X_FORWARDED_SSL', '').lower() == "on":
            environ['wsgi.url_scheme'] = "https"
        else:
            environ['wsgi.url_scheme'] = "http"

        # add missing environ var
        environ.update({
            'REQUEST_METHOD': self.get_method(),
            'SERVER_PROTOCOL': "HTTP/%s" % ".".join(map(str, 
                self.get_version()))})
        return environ

    def recv_body(self):
        """ return last chunk of the parsed body"""
        body = "".join(self._data.body)
        self._data.body = []
        self._data.partial_body = False
        return body

    def recv_body_into(self, b):
        """ Receive the last chunk of the parsed bodyand store the data
        in a buffer rather than creating a new string. """
        l = len(b)
        body = "".join(self._data.body)
        m = min(len(body), l)
        data, rest = body[:m], body[m:]
        b[0:m] = data
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

    def should_keep_alive(self):
        """ return True if the connection should be kept alive
        """
        return http_should_keep_alive(&self._parser)
