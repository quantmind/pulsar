
cdef extern from "http_parser.h":

    ctypedef struct http_parser:
        pass
    
    
    http_parser* create_request_parser()
    
    void http_free_parser(http_parser* parser)
    