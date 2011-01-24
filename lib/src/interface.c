#include <interface.h>

http_parser* create_request_parser() {
	http_parser *parser = malloc(sizeof(http_parser));
	http_parser_init(parser, HTTP_REQUEST);
	return parser;
}

http_parser* create_response_parser() {
	http_parser *parser = malloc(sizeof(http_parser));
	http_parser_init(parser, HTTP_RESPONSE);
	return parser;
}

void http_free_parser(http_parser* p) {
	free(p);
}
