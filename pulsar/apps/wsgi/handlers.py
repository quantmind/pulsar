import textwrap

import pulsar
from pulsar import net

from .wsgi import WsgiResponse

__all__ = ['HttpHandler','HttpPoolHandler']


class HttpHandler(object):
    '''Handle HTTP requests and delegate the response to the worker'''

    def __init__(self, worker, socket):
        self.worker = worker
        self.socket = socket
        self.iostream = pulsar.IOStream if self.worker.app.cfg.synchronous\
                         else pulsar.AsyncIOStream
        
    def __call__(self, fd, events):
        client, addr = self.socket.accept()
        if client:
            stream = self.iostream(actor = self.worker, socket = client)
            request = net.HttpRequest(stream, addr,
                                      timeout = self.worker.cfg.keepalive)
            self.handle(request)

    def handle(self, request):
        self.worker.handle_request(request)


class HttpPoolHandler(HttpHandler):
    '''This is used when the monitor is using thread-based workers.'''
    def handle(self, request):
        self.worker.put(request)


def handle_http_error(response, e):
    '''The default handler for errors while serving an Http requests.
:parameter response: an instance of :class:`WsgiResponse`.
:parameter e: the exception instance.
'''
    actor = pulsar.get_actor()
    code = getattr(e,'status_code',500)
    response.content_type = 'text/html'
    if code == 500:
        actor.log.critical('Unhandled exception during WSGI response',
                           exc_info = True)
        mesg = 'An exception has occured while evaluating your request.'
    else:
        actor.log.info('WSGI {0} status code'.format(code))
        if code == 404:
            mesg = 'Cannot find what you are looking for.'
        else:
            mesg = ''
    response.status_code = code
    encoding = 'utf-8'
    reason = response.status
    content = textwrap.dedent("""\
    <!DOCTYPE html>
    <html>
      <head>
        <title>{0[reason]}</title>
      </head>
      <body>
        <h1>{0[reason]}</h1>
        {0[mesg]}
        <h3>{0[version]}</h3>
      </body>
    </html>
    """).format({"reason": reason, "mesg": mesg,
                 "version": pulsar.SERVER_SOFTWARE})
    response.content = content.encode(encoding,'replace')
    return response
    

################################################################################
#    WSGI SETTING
################################################################################

class WsgiSetting(pulsar.Setting):
    virtual = True
    app = 'wsgi'
    

class Bind(WsgiSetting):
    name = "bind"
    flags = ["-b", "--bind"]
    meta = "ADDRESS"
    default = "127.0.0.1:{0}".format(pulsar.DEFAULT_PORT)
    desc = """\
        The socket to bind.
        
        A string of the form: 'HOST', 'HOST:PORT', 'unix:PATH'. An IP is a valid
        HOST.
        """

        
class Sync(WsgiSetting):
    name = "synchronous"
    flags = ["--sync"]
    action = 'store_true'
    default = False
    validator = pulsar.validate_bool
    desc = """\
        Set the socket to synchronous (blocking) mode.
        """

        
class Backlog(WsgiSetting):
    name = "backlog"
    flags = ["--backlog"]
    validator = pulsar.validate_pos_int
    type = int
    default = 2048
    desc = """\
        The maximum number of pending connections.    
        
        This refers to the number of clients that can be waiting to be served.
        Exceeding this number results in the client getting an error when
        attempting to connect. It should only affect servers under significant
        load.
        
        Must be a positive integer. Generally set in the 64-2048 range.    
        """


class Keepalive(WsgiSetting):
    name = "keepalive"
    flags = ["--keep-alive"]
    validator = pulsar.validate_pos_int
    type = int
    default = 5
    desc = """\
        The number of seconds to wait for requests on a Keep-Alive connection.
        
        Generally set in the 1-5 seconds range.    
        """
        
        
class HttpParser(WsgiSetting):
    name = "http_parser"
    flags = ["--http-parser"]
    desc = """\
        The HTTP Parser to use. By default it uses the fastest possible.    
        
        Specify `python` if you wich to use the pure python implementation    
        """
            
    
class ResponseMiddleware(WsgiSetting):
    name = "response_middleware"
    flags = ["--response-middleware"]
    nargs = '*'
    desc = """\
    Response middleware to add to the wsgi handler    
    """
    
    
class HttpError(WsgiSetting):
    name = "handle_http_error"
    validator = pulsar.validate_callable(2)
    type = "callable"
    default = staticmethod(handle_http_error)
    desc = """\
Render an error occured while serving the WSGI application.

The callable needs to accept two instance variables for the response
and the error instance."""
    