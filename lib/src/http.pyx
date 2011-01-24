cimport chttp

cdef class RequestParser:
    cdef chttp.http_parser *_c_http_parser
    
    def __cinit__(self):
        self._c_http_parser = parser = chttp.create_request_parser()
        
    def __dealloc__(self):
        if self._c_http_parser is not NULL:
            chttp.http_free_parser(self._c_http_parser)
            
            