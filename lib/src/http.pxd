import zlib
from libc.stdlib cimport *
from cpython cimport PyBytes_FromStringAndSize

    
cdef inline bytes_to_str(b):
    try:
        return str(b, 'latin1')
    except TypeError:
        return b
        

cdef extern from "http_parser.h" nogil:
    
    cdef enum http_method:
        HTTP_DELETE, HTTP_GET, HTTP_HEAD, HTTP_POST, HTTP_PUT,
        HTTP_CONNECT, HTTP_OPTIONS, HTTP_TRACE, HTTP_COPY, HTTP_LOCK,
        HTTP_MKCOL, HTTP_MOVE, HTTP_PROPFIND, HTTP_PROPPATCH, HTTP_UNLOCK, 
        HTTP_REPORT, HTTP_MKACTIVITY, HTTP_CHECKOUT, HTTP_MERGE, HTTP_MSEARCH,
        HTTP_NOTIFY, HTTP_SUBSCRIBE, HTTP_UNSUBSCRIBE, HTTP_PATCH
    
        
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
        http_data_cb on_url
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


cdef inline int on_url_cb(http_parser *parser, char *at,
                          size_t length):
    res = <object>parser.data
    value = bytes_to_str(PyBytes_FromStringAndSize(at, length))
    res.url = value
    res.environ['RAW_URI'] = value
    return 0


cdef inline int on_header_field_cb(http_parser *parser, char *at, 
        size_t length):
    header_field = PyBytes_FromStringAndSize(at, length)
    res = <object>parser.data
  
    if res._last_was_value:
        res._last_field = ""
    res._last_field += bytes_to_str(header_field)
    res._last_was_value = False
    return 0


cdef inline int on_header_value_cb(http_parser *parser, char *at, 
                                   size_t length):
    res = <object>parser.data
    header_value = bytes_to_str(PyBytes_FromStringAndSize(at, length))
    res.headers[res._last_field] = header_value
    res._last_was_value = True
    return 0


cdef inline int on_headers_complete_cb(http_parser *parser):
    res = <object>parser.data
    res.headers_complete = True

    if res.decompress:
        encoding = res.headers.get('content-encoding')
        if encoding == 'gzip':
            res.decompressobj = zlib.decompressobj(16+zlib.MAX_WBITS)
            del res.headers['content-encoding']
        elif encoding == 'deflate':
            res.decompressobj = zlib.decompressobj()
            del res.headers['content-encoding']
        else:
            res.decompress = False
    
    return 0


cdef inline int on_message_begin_cb(http_parser *parser):
    res = <object>parser.data
    res.message_begin = True
    return 0


cdef inline int on_body_cb(http_parser *parser, char *at, 
        size_t length):
    res = <object>parser.data
    value = PyBytes_FromStringAndSize(at, length)

    res.partial_body = True

    # decompress the value if needed
    if res.decompress:
        value = res.decompressobj.decompress(value)

    res.body.append(value)
    return 0


cdef inline int on_message_complete_cb(http_parser *parser):
    res = <object>parser.data
    res.message_complete = True
    return 0
    
