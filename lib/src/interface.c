#include "interface.h"

static http_parser_settings settings_null =
  {.on_message_begin = 0
  ,.on_header_field = 0
  ,.on_header_value = 0
  ,.on_path = 0
  ,.on_url = 0
  ,.on_fragment = 0
  ,.on_query_string = 0
  ,.on_body = 0
  ,.on_headers_complete = 0
  ,.on_message_complete = 0
  };

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
	p = 0;
}

unsigned parse(http_parser* p, const char *buf, unsigned len) {
  return http_parser_execute(p, &settings_null, buf, len);
}
