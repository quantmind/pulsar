from pulsar import lib, SERVER_SOFTWARE, Deferred, to_string
from pulsar.utils.http import Headers, is_hoppish, http_date

__all__ = ['HttpResponse']


class HttpResponse(Deferred):
    
    def __init__(self, request):
        super(HttpResponse,self).__init__()
        self.request = request
        self.stream = request.stream
        self.version = SERVER_SOFTWARE
        self.status = None
        self.chunked = False
        self.should_keep_alive = request.should_keep_alive
        self.headers = Headers()
        self.headers_sent = False
        
    def force_close(self):
        self.should_keep_alive = False
        
    def start_response(self, status, headers, exc_info=None):
        if exc_info:
            try:
                if self.status and self.headers_sent:
                    raise (exc_info[0], exc_info[1], exc_info[2])
            finally:
                exc_info = None
        elif self.status is not None:
            raise AssertionError("Response headers already set!")

        self.status = status
        self.process_headers(headers)
        return self.write
    
    def process_headers(self, headers):
        for name, value in headers:
            name = to_string(name)
            value = to_string(value)
            if is_hoppish(name):
                lname = name.lower().strip()
                if lname == "transfer-encoding":
                    if value.lower().strip() == "chunked":
                        self.chunked = True
                elif lname == "connection":
                    # handle websocket
                    if value.lower().strip() != "upgrade":
                        continue
                else:
                    # ignore hopbyhop headers
                    continue
            self.headers[name.strip()] = value.strip()
            
    def default_headers(self):
        connection = "keep-alive" if self.should_keep_alive else "close"
        return [
            "HTTP/1.1 %s\r\n" % self.status,
            "Server: %s\r\n" % self.version,
            "Date: %s\r\n" % http_date(),
            "Connection: %s\r\n" % connection
        ]
    
    def send_headers(self):
        if self.headers_sent:
            return
        tosend = self.default_headers()
        tosend.extend(["%s: %s\r\n" % (n, v) for n, v in self.headers])
        data = '{0}\r\n'.format(''.join(tosend))
        self.stream.write(data, self.on_header_send)
        self.headers_sent = True

    def on_header_send(self):
        self.headers_sent = True
        
    def write(self, data):
        self.send_headers()
        for elem in data:
            callback = lambda : self.write_chunk(data)
            self.stream.write(data, callback)
            
    def write_chunk(self, ):
        