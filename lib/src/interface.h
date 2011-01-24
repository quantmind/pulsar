
#ifndef http_interface_h
#define http_interface_h

#ifdef __cplusplus
extern "C" {
#endif

#include <http_parser.h>


inline http_parser* create_request_parser();
inline http_parser* create_response_parser();
inline void http_free_parser(http_parser*);


#ifdef __cplusplus
}
#endif

#endif	//	http_interface_h
