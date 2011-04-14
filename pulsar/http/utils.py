import time
import textwrap
import socket

from pulsar import ispy3k

CHUNK_SIZE = (16 * 1024)
CHARSET = 'iso-8859-1'
ERRORS='strict'
MAX_BODY = 1024 * 132

weekdayname = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
monthname = [None,
             'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
             'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

# Server and Date aren't technically hop-by-hop
# headers, but they are in the purview of the
# origin server which the WSGI spec says we should
# act like. So we drop them and add our own.
#
# In the future, concatenation server header values
# might be better, but nothing else does it and
# dropping them is easier.
HOP_HEADERS = set("""
    connection keep-alive proxy-authenticate proxy-authorization
    te trailers transfer-encoding upgrade
    server date
    """.split())


if ispy3k:
    
    def to_string(data, charset=CHARSET, errors=ERRORS):
        if isinstance(data,bytes):
            return data.decode(charset,errors)
        return data
    
    
    def to_bytes(data, charset=CHARSET, errors=ERRORS):
        if isinstance(data,str):
            return data.encode(charset,errors)
        return data

else:
    # Don't bother
    def to_string(data, charset=CHARSET, errors=ERRORS):
        return data
    
    def to_bytes(data, charset=CHARSET, errors=ERRORS):
        return data
    
    
def is_hoppish(header):
    return header.lower().strip() in HOP_HEADERS


def http_date(timestamp=None):
    """Return the current date and time formatted for a message header."""
    if timestamp is None:
        timestamp = time.time()
    year, month, day, hh, mm, ss, wd, y, z = time.gmtime(timestamp)
    s = "%s, %02d %3s %4d %02d:%02d:%02d GMT" % (
            weekdayname[wd],
            day, monthname[month], year,
            hh, mm, ss)
    return s


def close(sock):
    if sock:
        try:
            sock.close()
        except socket.error:
            pass
    

def write_chunk(sock, data):
    chunk = "".join(("%X\r\n" % len(data), data, "\r\n"))
    sock.sendall(to_bytes(chunk))

    
def write(sock, data, chunked=False):
    if chunked:
        write_chunk(sock, data)
    else:
        sock.sendall(to_bytes(data))


def write_nonblock(sock, data, chunked=False):
    timeout = sock.gettimeout()
    if timeout != 0.0:
        try:
            sock.setblocking(0)
            return write(sock, data, chunked)
        finally:
            sock.setblocking(1)
    else:
        return write(sock, data, chunked)
    
    
def writelines(sock, lines, chunked=False):
    for line in list(lines):
        write(sock, line, chunked)


def write_error(sock, msg):
    html = textwrap.dedent("""\
    <html>
        <head>
            <title>Internal Server Error</title>
        </head>
        <body>
            <h1>Internal Server Error</h1>
            <h2>WSGI Error Report:</h2>
            <pre>%s</pre>
        </body>
    </html>
    """) % msg
    http = textwrap.dedent("""\
    HTTP/1.1 500 Internal Server Error\r
    Connection: close\r
    Content-Type: text/html\r
    Content-Length: %d\r
    \r
    %s
    """) % (len(html), html)
    write_nonblock(sock, http)