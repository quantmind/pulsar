
cdef extern from "interface.h":

    ctypedef struct http_parser:
        pass
    
    http_parser* create_request_parser()
    
    http_parser* create_response_parser()
    
    void http_free_parser(http_parser *parser)
    
    int http_should_keep_alive(http_parser *parser)
    