cimport chttp

cdef class RequestParser:
    cdef chttp.http_parser *_c_http_parser
    
    def __cinit__(self):
        self._c_http_parser = chttp.create_request_parser()
        
    def __dealloc__(self):
        if self._c_http_parser is not NULL:
            chttp.http_free_parser(self._c_http_parser)
            
    def keep_alive(self):
        return chttp.http_should_keep_alive(self._c_http_parser) != 0
            
            
cdef class ResponseParser:
    cdef chttp.http_parser *_c_http_parser
    
    def __cinit__(self):
        self._c_http_parser = chttp.create_response_parser()
        
    def __dealloc__(self):
        if self._c_http_parser is not NULL:
            chttp.http_free_parser(self._c_http_parser)
            
    def keep_alive(self):
        return chttp.http_should_keep_alive(self._c_http_parser) != 0
            